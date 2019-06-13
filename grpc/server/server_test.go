package server

import (
	"fmt"
	"os"
	"sync"
	"testing"

	pb "github.com/percona/percona-backup-mongodb/proto/messages"
)

func TestOne(t *testing.T) {
	rsets := map[string]*pb.ReplicasetMetadata{
		//"csReplSet": &pb.ReplicasetMetadata{
		//	ClusterId:            "5d029f0ce99d7cc6315ae9c4",
		//	ReplicasetUuid:       "5d029f0ae99d7cc6315ae99b",
		//	ReplicasetName:       "csReplSet",
		//	DbBackupName:         "2019-06-13T19:08:30Z_csReplSet.dump.gz",
		//	OplogBackupName:      "2019-06-13T19:08:30Z_csReplSet.oplog.gz",
		//	XXX_NoUnkeyedLiteral: struct{}{},
		//	XXX_unrecognized:     nil,
		//	XXX_sizecache:        0,
		//},
		"rs1": &pb.ReplicasetMetadata{
			ClusterId:            "5d029f0ce99d7cc6315ae9c4",
			ReplicasetUuid:       "5d029f09ea26d603432678aa",
			ReplicasetName:       "rs1",
			DbBackupName:         "2019-06-13T19:08:30Z_rs1.dump.gz",
			OplogBackupName:      "2019-06-13T19:08:30Z_rs1.oplog.gz",
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		},
		"rs2": &pb.ReplicasetMetadata{
			ClusterId:            "5d029f0ce99d7cc6315ae9c4",
			ReplicasetUuid:       "5d029f0abf2740ad8fce7635",
			ReplicasetName:       "rs2",
			DbBackupName:         "2019-06-13T19:08:30Z_rs2.dump.gz",
			OplogBackupName:      "2019-06-13T19:08:30Z_rs2.oplog.gz",
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		},
	}
	sources := make(map[string]RestoreSource)
	wga := &sync.WaitGroup{}
	ch := make(chan pb.CanRestoreBackupResponse)

	wga.Add(1)
	s := NewMessagesServer(os.TempDir())
	for i := 17001; i < 17007; i++ {
		id := fmt.Sprintf("%d", i)
		s.clients[id] = &Client{ID: "127.0.0.1:" + id}
	}
	go s.getRestoreSrcResponses(sources, ch, wga)

	resps := []pb.CanRestoreBackupResponse{
		{
			ClientId:   "127.0.0.1:17002",
			Replicaset: "rs1",
			IsPrimary:  false,
			CanRestore: true,
			Host:       "127.0.0.1",
			Port:       "17002",
		},
		{
			ClientId:   "127.0.0.1:17001",
			Replicaset: "rs1",
			IsPrimary:  true,
			CanRestore: false,
			Host:       "127.0.0.1",
			Port:       "17001",
		},
		{
			ClientId:   "127.0.0.1:17003",
			Replicaset: "rs1",
			IsPrimary:  false,
			CanRestore: false,
			Host:       "127.0.0.1",
			Port:       "17003",
		},
		{
			ClientId:   "127.0.0.1:17004",
			Replicaset: "rs2",
			IsPrimary:  false,
			CanRestore: true,
			Host:       "127.0.0.1",
			Port:       "17004",
		},
		{
			ClientId:   "127.0.0.1:17005",
			Replicaset: "rs2",
			IsPrimary:  true,
			CanRestore: false,
			Host:       "127.0.0.1",
			Port:       "17005",
		},
		{
			ClientId:   "127.0.0.1:17006",
			Replicaset: "rs2",
			IsPrimary:  false,
			CanRestore: false,
			Host:       "127.0.0.1",
			Port:       "17006",
		},
	}

	for _, resp := range resps {
		ch <- resp
	}
	close(ch)
	wga.Wait()
	if err := validateRestoreSources(sources, rsets, "storageName string"); err != nil {
		t.Errorf("Invalid restore sources count: %s", err)
	}
}
