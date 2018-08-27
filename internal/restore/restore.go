package restore

import (
	"fmt"
	"io"
	"sync"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/mongorestore"
	"github.com/pkg/errors"
)

type MongoRestoreInput struct {
	Archive  string
	DryRun   bool // Used only for testing
	Host     string
	Port     string
	Username string
	Password string
	AuthDB   string
	Gzip     bool
	Oplog    bool
	Threads  int
	Reader   io.ReadCloser
}

type MongoRestore struct {
	*MongoRestoreInput
	mongorestore *mongorestore.MongoRestore

	lastError error
	waitChan  chan error
	//
	lock    *sync.Mutex
	running bool
}

func NewMongoRestore(i *MongoRestoreInput) (*MongoRestore, error) {
	if i.Reader == nil && i.Archive == "" {
		return nil, fmt.Errorf("You need to specify an archive or a reader")
	}

	// TODO: SSL?
	connOpts := &options.Connection{
		Host: i.Host,
		Port: i.Port,
	}

	toolOpts := &options.ToolOptions{
		AppName:    "mongodump",
		VersionStr: "0.0.1",
		Connection: connOpts,
		Auth:       &options.Auth{},
		Namespace:  &options.Namespace{},
		URI:        &options.URI{},
	}
	if i.Username != "" && i.Password != "" {
		toolOpts.Auth.Username = i.Username
		toolOpts.Auth.Password = i.Password
		if i.AuthDB != "" {
			toolOpts.Auth.Source = i.AuthDB
		}
	}

	inputOpts := &mongorestore.InputOptions{
		Gzip:                   i.Gzip,
		Archive:                i.Archive,
		Objcheck:               true,
		RestoreDBUsersAndRoles: false,
	}

	outputOpts := &mongorestore.OutputOptions{
		Drop:   true,
		DryRun: false,

		// By default mongorestore uses a write concern of 'majority'.
		// Cannot be used simultaneously with write concern options in a URI.
		WriteConcern:             "majority",
		NoIndexRestore:           false,
		NoOptionsRestore:         false,
		KeepIndexVersion:         false,
		MaintainInsertionOrder:   true,
		NumParallelCollections:   4,
		NumInsertionWorkers:      1,
		StopOnError:              false,
		BypassDocumentValidation: false,
		// TempUsersColl            string `long:"tempUsersColl" default:"tempusers" hidden:"true"`
		// TempRolesColl            string `long:"tempRolesColl" default:"temproles" hidden:"true"`
		// BulkBufferSize           int    `long:"batchSize" default:"1000" hidden:"true"`
	}

	provider, err := db.NewSessionProvider(*toolOpts)
	if err != nil {
		return nil, errors.Wrap(err, "cannot instantiate a session provider")
	}
	if provider == nil {
		return nil, fmt.Errorf("Cannot set session provider (nil)")
	}

	restore := &mongorestore.MongoRestore{
		ToolOptions:     toolOpts,
		OutputOptions:   outputOpts,
		InputOptions:    inputOpts,
		NSOptions:       &mongorestore.NSOptions{},
		SessionProvider: provider,
	}

	if err := restore.ParseAndValidateOptions(); err != nil {
		return nil, err
	}

	restore.InputReader = nil

	return &MongoRestore{
		MongoRestoreInput: i,
		mongorestore:      restore,
		lock:              &sync.Mutex{},
		running:           false,
	}, nil
}

func (mr *MongoRestore) LastError() error {
	return mr.lastError
}

func (mr *MongoRestore) Start() error {
	if mr.isRunning() {
		return fmt.Errorf("Dumper already running")
	}
	mr.waitChan = make(chan error, 1)
	mr.setRunning(true)
	go mr.restore()

	return nil
}

func (mr *MongoRestore) Stop() error {
	if !mr.isRunning() {
		return fmt.Errorf("The dumper is not running")
	}
	mr.mongorestore.HandleInterrupt()
	return mr.Wait()
}

func (mr *MongoRestore) Wait() error {
	if !mr.isRunning() {
		return fmt.Errorf("The dumper is not running")
	}
	mr.setRunning(false)
	defer close(mr.waitChan)
	mr.lastError = <-mr.waitChan
	return mr.lastError
}

func (mr *MongoRestore) restore() {
	err := mr.mongorestore.Restore()
	mr.waitChan <- err
}

func (mr *MongoRestore) isRunning() bool {
	mr.lock.Lock()
	defer mr.lock.Unlock()
	return mr.running
}

func (mr *MongoRestore) setRunning(status bool) {
	mr.lock.Lock()
	defer mr.lock.Unlock()
	mr.running = status
}
