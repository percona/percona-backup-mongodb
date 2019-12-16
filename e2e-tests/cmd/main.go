package main

import (
	"context"
	"log"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg"
)

func main() {
	ctx := context.Background()
	// mgo, err := pkg.NewMongo(ctx, "mongodb://dba:test1234@127.0.0.1:27017/")
	// if err != nil {
	// 	log.Fatalln("connect to mongo:", err)
	// }
	// log.Println("connected to mongo")

	// log.Println("genrating ballast data")
	// err = mgo.Fill(1e5)
	// if err != nil {
	// 	log.Fatalln("genrating ballast:", err)
	// }
	// log.Println("ballast data generated")

	pbm, err := pkg.NewPBM(ctx, "unix:///var/run/docker.sock")
	if err != nil {
		log.Fatalln("connect to mongo:", err)
	}
	log.Println("connected to pbm")

	log.Println("starting the backup")
	err = pbm.BackupStart()
	if err != nil {
		log.Fatalln("starting backup:", err)
	}
	log.Println("backup started")
}
