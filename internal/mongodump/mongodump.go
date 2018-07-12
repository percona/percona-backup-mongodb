package mongodump

import (
	"log"

	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/mongodump"
)

type Mongodump struct {
	Host    string
	Port    string
	Out     string
	Gzip    bool
	Oplog   bool
	Threads int
}

func (md *Mongodump) Dump() {
	conn_opts := &options.Connection{
		Host: md.Host,
		Port: md.Port,
	}

	tool_opts := &options.ToolOptions{
		AppName:    "mongodump",
		VersionStr: "0.0.1",
		Connection: conn_opts,
		Auth:       &options.Auth{},
		Namespace:  &options.Namespace{},
	}

	out_opts := &mongodump.OutputOptions{
		Archive: "true",
		Gzip:    md.Gzip,
		Oplog:   md.Oplog,
		NumParallelCollections: md.Threads,
	}

	dump := &mongodump.MongoDump{
		ToolOptions:   tool_opts,
		OutputOptions: out_opts,
		InputOptions:  &mongodump.InputOptions{},
	}

	var err error
	err = dump.Init()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Initialized mongodump!")

	log.Println("Starting mongodump")
	err = dump.Dump()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Done mongodump!")
}
