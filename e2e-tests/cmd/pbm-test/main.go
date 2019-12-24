package main

import (
	"context"
	"log"
	"time"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg"
)

const (
	cnMongos    = "mongodb://dba:test1234@mongos:27017/"
	cnConfigsrv = "mongodb://dba:test1234@cfg01:27017/"
	cnRS01      = "mongodb://dba:test1234@rs101:27017/"
	cnRS02      = "mongodb://dba:test1234@rs201:27017/"
)

func main() {
	ctx := context.Background()

	mongos := mgoConn(ctx, cnMongos)
	mongo01 := mgoConn(ctx, cnRS01)
	mongo02 := mgoConn(ctx, cnRS02)
	mongopbm := pbmConn(ctx, cnConfigsrv)

	log.Println("genrating ballast data")
	err := mongos.Fill(1e4)
	if err != nil {
		log.Fatalln("genrating ballast:", err)
	}
	log.Println("ballast data generated")

	dbhash01, err := mongo01.Hashes()
	if err != nil {
		log.Fatalln("get db hashes rs01:", err)
	}
	log.Println("current rs01 db hash", dbhash01["_all_"])

	dbhash02, err := mongo02.Hashes()
	if err != nil {
		log.Fatalln("get db hashes rs02:", err)
	}
	log.Println("current rs02 db hash", dbhash02["_all_"])

	pbm, err := pkg.NewPBM(ctx, "unix:///var/run/docker.sock")
	if err != nil {
		log.Fatalln("connect to mongo:", err)
	}
	log.Println("connected to pbm")

	log.Println("apply config")
	err = pbm.ApplyConfig()
	if err != nil {
		l, _ := pbm.ContainerLogs()
		log.Fatalf("apply config: %v\nconatiner logs: %s\n", err, l)
	}

	log.Println("starting the backup")
	bcpName, err := pbm.Backup()
	if err != nil {
		l, _ := pbm.ContainerLogs()
		log.Fatalf("starting backup: %v\nconatiner logs: %s\n", err, l)
	}
	log.Printf("backup started '%s'\n", bcpName)

	log.Println("waiting for the backup")
	err = pbm.CheckBackup(bcpName, time.Second*240)
	if err != nil {
		log.Fatalln("check backup state:", err)
	}
	log.Printf("backup finished '%s'\n", bcpName)

	log.Println("deleteing data")
	c, err := mongos.DeleteData()
	if err != nil {
		log.Fatalln("deleting data:", err)
	}
	log.Printf("deleted %d documents", c)

	log.Println("restoring the backup")
	err = pbm.Restore(bcpName)
	if err != nil {
		log.Fatalln("restoring the backup:", err)
	}

	log.Println("waiting for the restore")
	err = mongopbm.CheckRestore(bcpName, time.Minute*15)
	if err != nil {
		log.Fatalln("check backup restore:", err)
	}
	log.Printf("restore finished '%s'\n", bcpName)

	log.Println("Checking restored backup")
	dbrhash01, err := mongo01.Hashes()
	if err != nil {
		log.Fatalln("get db hashes rs01:", err)
	}
	log.Println("current rs01 db hash", dbhash01["_all_"])

	dbrhash02, err := mongo02.Hashes()
	if err != nil {
		log.Fatalln("get db hashes rs02:", err)
	}
	log.Println("current rs02 db hash", dbhash02["_all_"])

	if rh, ok := dbrhash01["_all_"]; !ok || rh != dbhash01["_all_"] {
		log.Fatalf("rs01 hashes not equal: %s != %s\n", rh, dbhash01["_all_"])
	}

	if rh, ok := dbrhash02["_all_"]; !ok || rh != dbhash02["_all_"] {
		log.Fatalf("rs02 hashes not equal: %s != %s\n", rh, dbhash02["_all_"])
	}

	log.Println("ok!")
}

func mgoConn(ctx context.Context, uri string) *pkg.Mongo {
	m, err := pkg.NewMongo(ctx, uri)
	if err != nil {
		log.Fatalln("connect to mongo:", err)
	}
	log.Println("connected to", uri)
	return m
}
func pbmConn(ctx context.Context, uri string) *pkg.MongoPBM {
	m, err := pkg.NewMongoPBM(ctx, uri)
	if err != nil {
		log.Fatalln("connect to mongo:", err)
	}
	log.Println("connected to", uri)
	return m
}
