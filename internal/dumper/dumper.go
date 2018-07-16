package dumper

import (
	"fmt"
	"io"
	"sync"

	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/mongodump"
	"github.com/pkg/errors"
)

type MongodumpInput struct {
	Archive string
	Host    string
	Port    string
	Gzip    bool
	Oplog   bool
	Threads int
	Writer  io.WriteCloser
}

type Mongodump struct {
	*MongodumpInput
	mongodump *mongodump.MongoDump

	lastError error
	waitChan  chan error
	//
	lock    *sync.Mutex
	running bool
}

func NewMongodump(i *MongodumpInput) (*Mongodump, error) {
	if i.Writer == nil {
		return nil, fmt.Errorf("Writer cannot be null")
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
	}

	outputOpts := &mongodump.OutputOptions{
		// Archive = "-" means, for mongodump, use the provider Writer
		// instead of creating a file. This is not clear at plain sight,
		// you nee to look the code to discover it.
		Archive: "-",
		Gzip:    i.Gzip,
		Oplog:   i.Oplog,
		NumParallelCollections: i.Threads,
	}

	dump := &mongodump.MongoDump{
		ToolOptions:   toolOpts,
		OutputOptions: outputOpts,
		InputOptions:  &mongodump.InputOptions{},
	}

	if err := dump.ValidateOptions(); err != nil {
		return nil, err
	}

	dump.OutputWriter = i.Writer

	return &Mongodump{
		MongodumpInput: i,
		mongodump:      dump,
		lock:           &sync.Mutex{},
		running:        false,
	}, nil
}

func (md *Mongodump) LastError() error {
	return md.lastError
}

func (md *Mongodump) Start() error {
	if md.isRunning() {
		return fmt.Errorf("Dumper already running")
	}
	md.waitChan = make(chan error, 1)
	if err := md.mongodump.Init(); err != nil {
		return errors.Wrapf(err, "cannot start mongodump")
	}
	md.setRunning(true)
	go md.dump()

	return nil
}

func (md *Mongodump) Stop() error {
	if !md.isRunning() {
		return fmt.Errorf("The dumper is not running")
	}
	md.mongodump.HandleInterrupt()
	return md.Wait()
}

func (md *Mongodump) Wait() error {
	if !md.isRunning() {
		return fmt.Errorf("The dumper is not running")
	}
	md.setRunning(false)
	defer close(md.waitChan)
	md.lastError = <-md.waitChan
	return md.lastError
}

func (md *Mongodump) dump() {
	err := md.mongodump.Dump()
	md.waitChan <- err
}

func (md *Mongodump) isRunning() bool {
	md.lock.Lock()
	defer md.lock.Unlock()
	return md.running
}

func (md *Mongodump) setRunning(status bool) {
	md.lock.Lock()
	defer md.lock.Unlock()
	md.running = status
}
