package oplog

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/internal/cluster"
	"github.com/percona/mongodb-backup/mdbstructs"
	"github.com/pkg/errors"
)

type chanDataTye []byte

type OplogTail struct {
	session             *mgo.Session
	oplogCollection     string
	startOplogTimestamp *bson.MongoTimestamp
	lastOplogTimestamp  *bson.MongoTimestamp

	totalSize         int64
	docsCount         int64
	remainingBytes    []byte
	nextChunkPosition int

	dataChan chan chanDataTye
	stopChan chan bool
	readFunc func([]byte) (int, error)
	lock     sync.Mutex
	isEOF    bool
	running  bool
}

const (
	oplogDB = "local"
)

var (
	mgoIterBatch    = 100
	mgoIterPrefetch = 1.0
)

func Open(session *mgo.Session) (*OplogTail, error) {
	ot, err := open(session)
	if err != nil {
		return nil, err
	}
	go ot.tail()
	return ot, nil
}

func OpenAt(session *mgo.Session, t time.Time, c uint32) (*OplogTail, error) {
	ot, err := open(session)
	if err != nil {
		return nil, err
	}
	mongoTimestamp, err := bson.NewMongoTimestamp(t, c)
	if err != nil {
		return nil, err
	}
	ot.startOplogTimestamp = &mongoTimestamp
	go ot.tail()
	return ot, nil
}

func open(session *mgo.Session) (*OplogTail, error) {
	if session == nil {
		return nil, fmt.Errorf("Invalid session (nil)")
	}

	oplogCol, err := determineOplogCollectionName(session)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot determine the oplog collection name")
	}

	ot := &OplogTail{
		session:         session,
		oplogCollection: oplogCol,
		dataChan:        make(chan chanDataTye, 1),
		stopChan:        make(chan bool),
		running:         true,
	}
	ot.readFunc = makeReader(ot)
	//go ot.tail()
	return ot, nil
}

// Implement the Reader interface to be able to pipe it into an S3 stream or through an
// encrypter
func (ot *OplogTail) Read(buf []byte) (int, error) {
	n, err := ot.readFunc(buf)
	if err == nil {
		ot.docsCount++
		ot.totalSize += int64(n)
	}
	return n, err
}

func (ot *OplogTail) Size() int64 {
	return ot.totalSize
}

func (ot *OplogTail) Count() int64 {
	return ot.docsCount
}

func (ot *OplogTail) Close() error {
	if ot.isRunning() {
		close(ot.stopChan)
		return nil
	}
	return fmt.Errorf("Tailer is already closed")
}

func (ot *OplogTail) isRunning() bool {
	ot.lock.Lock()
	defer ot.lock.Unlock()
	return ot.running
}

func (ot *OplogTail) setRunning(state bool) {
	ot.lock.Lock()
	defer ot.lock.Unlock()
	ot.running = state
}

func (ot *OplogTail) tail() {
	col := ot.session.DB(oplogDB).C(ot.oplogCollection)
	comment := "github.com/percona/mongodb-backup/internal/oplog.(*OplogTail).tail()"
	iter := col.Find(ot.tailQuery()).LogReplay().Comment(comment).Batch(mgoIterBatch).Prefetch(mgoIterPrefetch).Iter()
	for {
		select {
		case <-ot.stopChan:
			iter.Close()
			ot.setRunning(false)
			return
		default:
		}
		result := bson.Raw{}
		if iter.Next(&result) {
			oplog := mdbstructs.OplogTimestampOnly{}
			err := result.Unmarshal(&oplog)
			if err == nil {
				ot.dataChan <- result.Data
				ot.lock.Lock()
				if ot.startOplogTimestamp == nil {
					ot.startOplogTimestamp = &oplog.Timestamp
				}
				ot.lastOplogTimestamp = &oplog.Timestamp
				ot.lock.Unlock()
				continue
			}
		}
		if iter.Timeout() {
			continue
		}
		if iter.Err() != nil {
			iter.Close()
		}
		iter = col.Find(ot.tailQuery()).LogReplay().Comment(comment).Batch(mgoIterBatch).Prefetch(mgoIterPrefetch).Iter()
	}
}

// tailQuery returns a bson.M query filter for the oplog tail
// Criteria:
//   1. If 'lastOplogTimestamp' is defined, tail all non-noop oplogs with 'ts' $gt that ts
//   2. Or, if 'startOplogTimestamp' is defined, tail all non-noop oplogs with 'ts' $gte that ts
//   3. Or, tail all non-noop oplogs with 'ts' $gt 'lastWrite.OpTime.Ts' from the result of the "isMaster" mongodb server command
//   4. Or, tail all non-noop oplogs with 'ts' $gte now.
func (ot *OplogTail) tailQuery() bson.M {
	query := bson.M{"op": bson.M{"$ne": mdbstructs.OperationNoop}}

	ot.lock.Lock()
	if ot.lastOplogTimestamp != nil {
		query["ts"] = bson.M{"$gt": *ot.lastOplogTimestamp}
		ot.lock.Unlock()
		return query
	} else if ot.startOplogTimestamp != nil {
		query["ts"] = bson.M{"$gte": *ot.startOplogTimestamp}
		ot.lock.Unlock()
		return query
	}
	ot.lock.Unlock()

	isMaster, err := cluster.NewIsMaster(ot.session)
	if err != nil {
		mongoTimestamp, _ := bson.NewMongoTimestamp(time.Now(), 0)
		query["ts"] = bson.M{"$gte": mongoTimestamp}
	} else {
		query["ts"] = bson.M{"$gt": isMaster.IsMasterDoc().LastWrite.OpTime.Ts}
	}
	return query
}

func determineOplogCollectionName(session *mgo.Session) (string, error) {
	isMaster, err := cluster.NewIsMaster(session)
	if err != nil {
		return "", errors.Wrap(err, "Cannot determine the oplog collection name")
	}

	if len(isMaster.IsMasterDoc().Hosts) > 0 {
		return "oplog.rs", nil
	}
	if !isMaster.IsMasterDoc().IsMaster {
		return "", fmt.Errorf("not connected to master")
	}
	return "oplog.$main", nil
}

func makeReader(ot *OplogTail) func([]byte) (int, error) {
	return func(buf []byte) (int, error) {
		// Read comment #2 below before reading this
		// If in the previous call to Read, the provided buffer was smaller
		// than the document we had from the oplog, we have to return the
		// remaining bytes instead of reading a new document from the oplog.
		// Again, the provided buffer could be smaller than the remaining
		// bytes of the provious document.
		// ot.nextChunkPosition keeps track of the starting position of the
		// remaining bytes to return.
		// Run: go test -v -run TestReadIntoSmallBuffer
		// to see how it works.
		if ot.remainingBytes != nil {
			nextChunkSize := len(ot.remainingBytes)
			responseSize := nextChunkSize - ot.nextChunkPosition

			if len(buf) < nextChunkSize-ot.nextChunkPosition {
				copy(buf, ot.remainingBytes[ot.nextChunkPosition:])
				ot.nextChunkPosition += len(buf)
				return len(buf), nil
			}

			// This is the last chunk of data in ot.remainingBytes
			// After filling the destination buffer, clean ot.remainingBytes
			// so next call to the Read method will go through the select
			// below, to read a new document from the oplog tailer.
			copy(buf, ot.remainingBytes[ot.nextChunkPosition:])
			ot.remainingBytes = nil
			ot.nextChunkPosition = 0
			return responseSize, nil
		}

		select {
		case <-ot.stopChan:
			ot.readFunc = func([]byte) (int, error) {
				return 0, fmt.Errorf("file alredy closed")
			}
			return 0, io.EOF
		case doc := <-ot.dataChan:
			// Comment #2
			// The buffer size where we have to copy the oplog document
			// could be smaller than the document. In that case, we must
			// keep the remaining bytes of the document for the next call
			// to the Read method.
			docSize := len(doc)
			bufSize := len(buf)
			retSize := len(doc)
			if bufSize < docSize {
				retSize = bufSize
			}

			if len(buf) < docSize {
				ot.remainingBytes = make([]byte, docSize-bufSize)
				copy(ot.remainingBytes, doc[bufSize:])
				ot.nextChunkPosition = 0
			}
			copy(buf, doc)
			return retSize, nil
		}
	}
}
