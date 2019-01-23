package writer

import (
	"compress/gzip"
	"io"
	"os"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/snappy"
	"github.com/percona/percona-backup-mongodb/internal/awsutils"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
)

type BackupWriter struct {
	writers []io.WriteCloser
}

type flusher interface {
	Flush() error
}

func (bw *BackupWriter) Close() error {
	var err error
	for i := len(bw.writers) - 1; i >= 0; i-- {
		if _, ok := bw.writers[i].(flusher); ok {
			if err = bw.writers[i].(flusher).Flush(); err != nil {
				break
			}
		}
		if err = bw.writers[i].Close(); err != nil {
			break
		}
	}
	return nil
}

func (bw *BackupWriter) Write(p []byte) (int, error) {
	return bw.writers[len(bw.writers)-1].Write(p)
}

func NewBackupWriter(dir, name string, destinationType pb.DestinationType, compressionType pb.CompressionType,
	cypher pb.Cypher) (*BackupWriter, error) {
	bw := &BackupWriter{
		writers: []io.WriteCloser{},
	}

	awsSession, err := awsutils.GetAWSSession()
	if err != nil {
		return nil, errors.Wrap(err, "cannot get an AWS session")
	}
	switch destinationType {
	case pb.DestinationType_DESTINATION_TYPE_FILE:
		filepath := path.Join(dir, name)
		fw, err := os.Create(filepath)
		if err != nil {
			return nil, errors.Wrapf(err, "Cannot create destination file: %s", filepath)
		}
		bw.writers = append(bw.writers, fw)
	case pb.DestinationType_DESTINATION_TYPE_AWS:
		// s3.Uploader runs synchronously and receives an io.Reader but here, we are implementing
		// writers so, we need to create an io.Pipe and run uploader.Upload in a go-routine
		pr, pw := io.Pipe()
		go func() {
			uploader := s3manager.NewUploader(awsSession)
			uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(dir),
				Key:    aws.String(name),
				Body:   pr,
			})
		}()
		bw.writers = append(bw.writers, pw)
	}

	switch compressionType {
	case pb.CompressionType_COMPRESSION_TYPE_GZIP:
		gzw := gzip.NewWriter(bw.writers[len(bw.writers)-1])
		bw.writers = append(bw.writers, gzw)
	case pb.CompressionType_COMPRESSION_TYPE_LZ4:
		lz4w := lz4.NewWriter(bw.writers[len(bw.writers)-1])
		bw.writers = append(bw.writers, lz4w)
	case pb.CompressionType_COMPRESSION_TYPE_SNAPPY:
		snappyw := snappy.NewWriter(bw.writers[len(bw.writers)-1])
		bw.writers = append(bw.writers, snappyw)
	}

	switch cypher {
	case pb.Cypher_CYPHER_NO_CYPHER:
		//TODO: Add cyphers
	}

	return bw, nil
}
