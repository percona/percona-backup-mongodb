package test_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/percona-backup-mongodb/bsonfile"
	"github.com/percona/percona-backup-mongodb/grpc/server"
	"github.com/percona/percona-backup-mongodb/internal/reader"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	"github.com/percona/percona-backup-mongodb/internal/testutils/grpc"
	testGrpc "github.com/percona/percona-backup-mongodb/internal/testutils/grpc"
	pbapi "github.com/percona/percona-backup-mongodb/proto/api"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	dbName                 = "test"
	colName                = "test_col"
	localFileSystemStorage = "local-filesystem"
	bulkSize               = 5000
)

func TestMain(m *testing.M) {
	log.SetLevel(log.ErrorLevel)
	log.SetFormatter(&log.TextFormatter{})

	if os.Getenv("DEBUG") == "1" {
		log.SetLevel(log.DebugLevel)
	}

	status := m.Run()

	if os.Getenv("KEEP_DATA") == "" {
		if err := testutils.CleanTempDirAndBucket(); err != nil {
			log.Warnf("Cannot clean up temp dir and buckets: %s", err)
		}
		fmt.Println("Deleting testing data (env var KEEP_DATA is empty)")
		fmt.Printf("Directory %s was deleted\n", testutils.TestingStorages().Storages["local-filesystem"].Filesystem.Path)
		fmt.Printf("S3 bucket %s was deleted\n", testutils.TestingStorages().Storages["s3-us-west"].S3.Bucket)
	} else {
		fmt.Println("Keeping testing data (env var KEEP_DATA is not empty)")
		fmt.Printf("Directory %s was not deleted\n", testutils.TestingStorages().Storages["local-filesystem"].Filesystem.Path)
		fmt.Printf("S3 bucket %s was not deleted\n", testutils.TestingStorages().Storages["s3-us-west"].S3.Bucket)
	}

	os.Exit(status)
}

func TestGlobalWithDaemon(t *testing.T) {
	tmpDir := getTempDir(t)
	log.Printf("Using %s as the temporary directory", tmpDir)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all agents: %s", err)
	}

	log.Debug("Getting list of connected clients")
	clientsList := d.MessagesServer.Clients()
	if len(clientsList) != d.ClientsCount() {
		t.Errorf("Want %d connected clients, got %d", d.ClientsCount(), len(clientsList))
	}

	log.Debug("Getting list of clients by replicaset")
	clientsByReplicaset := d.MessagesServer.ClientsByReplicaset()
	log.Debugf("Clients by replicaset: %+v\n", clientsByReplicaset)

	var firstClient server.Client
	for _, clients := range clientsByReplicaset {
		if len(clients) == 0 {
			break
		}

		for _, client := range clients {
			if client.NodeType == pb.NodeType_NODE_TYPE_MONGOS {
				continue
			}
			firstClient = client
			break
		}
	}

	log.Debugf("Getting first client status")
	status, err := firstClient.GetStatus()
	log.Debugf("Client status: %+v\n", status)
	if err != nil {
		t.Errorf("Cannot get first client status: %s", err)
	}
	if status.BackupType != pb.BackupType_BACKUP_TYPE_LOGICAL {
		t.Errorf("The default backup type should be 0 (Logical). Got backup type: %v", status.BackupType)
	}

	log.Debugf("Getting backup source")
	backupSource, err := firstClient.GetBackupSource()
	log.Debugf("Backup source: %+v\n", backupSource)
	if err != nil {
		t.Errorf("Cannot get backup source: %s", err)
	}
	if backupSource == "" {
		t.Error("Received empty backup source")
	}

	wantbs := map[string]*server.Client{
		"5b9e5545c003eb6bd5e94803": {
			ID:             "127.0.0.1:17003",
			NodeType:       pb.NodeType(3),
			NodeName:       "127.0.0.1:17003",
			ClusterID:      "5b9e5548fcaf061d1ed3830d",
			ReplicasetName: "rs1",
			ReplicasetUUID: "5b9e5545c003eb6bd5e94803",
		},
		"5b9e55461f4c8f50f48c6002": {
			ID:             "127.0.0.1:17006",
			NodeType:       pb.NodeType(3),
			NodeName:       "127.0.0.1:17006",
			ClusterID:      "5b9e5548fcaf061d1ed3830d",
			ReplicasetName: "rs2",
			ReplicasetUUID: "5b9e55461f4c8f50f48c6002",
		},
		"5b9e5546fcaf061d1ed382ed": {
			ID:             "127.0.0.1:17007",
			NodeType:       pb.NodeType(4),
			NodeName:       "127.0.0.1:17007",
			ClusterID:      "",
			ReplicasetName: "csReplSet",
			ReplicasetUUID: "5b9e5546fcaf061d1ed382ed",
		},
	}
	backupSources, err := d.MessagesServer.BackupSourceByReplicaset()
	if err != nil {
		t.Fatalf("Cannot get backup sources by replica set: %s", err)
	}

	okCount := 0
	for _, s := range backupSources {
		for _, bs := range wantbs {
			if s.ID == bs.ID && s.NodeType == bs.NodeType && s.NodeName == bs.NodeName && bs.ReplicasetUUID == s.ReplicasetUUID {
				okCount++
			}
		}
	}
}

func TestBackups(t *testing.T) {
	tmpDir := getTempDir(t)
	os.RemoveAll(tmpDir) // Cleanup before start. Don't check for errors. The path might not exist
	err := os.MkdirAll(tmpDir, os.ModePerm)
	if err != nil {
		t.Fatalf("Cannot create temp dir %s: %s", tmpDir, err)
	}
	log.Printf("Using %s as the temporary directory", tmpDir)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()

	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all agents: %s", err)
	}

	type backupParams struct {
		Name   string
		Params *pb.StartBackup
	}
	msgs := []backupParams{}

	// 1: "COMPRESSION_TYPE_NO_COMPRESSION",
	// 2: "COMPRESSION_TYPE_GZIP",
	compressionTypes := []int32{1, 2}

	for stgName := range testutils.TestingStorages().Storages {
		for _, compressionType := range compressionTypes {
			msgs = append(msgs, backupParams{
				Name: stgName + "_" + pb.CompressionType_name[compressionType],
				Params: &pb.StartBackup{
					BackupType:      pb.BackupType_BACKUP_TYPE_LOGICAL,
					CompressionType: pb.CompressionType(compressionType),
					Cypher:          pb.Cypher_CYPHER_NO_CYPHER,
					OplogStartTime:  time.Now().UTC().Unix(),
					NamePrefix:      time.Now().UTC().Format(time.RFC3339),
					Description:     "test_backup_" + pb.CompressionType_name[compressionType] + "_" + stgName,
					StorageName:     stgName,
				},
			})
		}
	}

	for i, msg := range msgs {
		params := msg
		testNum := i + 1
		t.Run(params.Name, func(t *testing.T) {
			testBackups(t, testNum, params.Params, d)
		})
	}
}

func testBackups(t *testing.T, testNum int, params *pb.StartBackup, d *grpc.Daemon) {
	ndocs := int64(1000)
	wg := &sync.WaitGroup{}

	// Genrate random data so we have something in the oplog
	oplogGeneratorStopChan := make(chan bool)
	s1Session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		log.Fatalf("Cannot connect to the DB: %s", err)
	}
	s1Session.SetMode(mgo.Strong, true)
	defer s1Session.Close()

	generateDataToBackup(t, s1Session, ndocs)
	wg.Add(1)
	go generateOplogTraffic(s1Session, ndocs, oplogGeneratorStopChan, wg)

	// Also generate random data on shard 2
	s2Session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard2ReplsetName))
	if err != nil {
		log.Fatalf("Cannot connect to the DB: %s", err)
	}
	s2Session.SetMode(mgo.Strong, true)
	defer s2Session.Close()

	cleanupDB(t, s1Session)
	cleanupDB(t, s2Session)

	generateDataToBackup(t, s2Session, ndocs)
	wg.Add(1)
	go generateOplogTraffic(s2Session, ndocs, oplogGeneratorStopChan, wg)

	backupNamePrefix := time.Now().UTC().Format(time.RFC3339)

	err = d.MessagesServer.StartBackup(params)
	if err != nil {
		t.Fatalf("Cannot start backup: %s", err)
	}
	if err := d.MessagesServer.WaitBackupFinish(); err != nil {
		t.Errorf("WaitBackupFinish failed: %s", err)
	}

	log.Info("Stopping the oplog tailer")
	err = d.MessagesServer.StopOplogTail()
	if err != nil {
		t.Fatalf("Cannot stop the oplog tailer: %s", err)
	}

	close(oplogGeneratorStopChan)
	if err := d.MessagesServer.WaitOplogBackupFinish(); err != nil {
		t.Errorf("WaitBackupFinish failed: %s", err)
	}

	// Test list backups
	log.Debug("Testing backup metadata")
	mdFilename := backupNamePrefix + ".json"
	if err := d.MessagesServer.WriteServerBackupMetadata(mdFilename); err != nil {
		t.Errorf("Cannot write backup metadata to file %s: %s", mdFilename, err)
	}
	bms, err := d.MessagesServer.ListBackups()
	if err != nil {
		t.Errorf("Cannot get backups metadata listing: %s", err)
	} else {
		if bms == nil {
			t.Errorf("Backups metadata listing is nil")
		} else {
			if len(bms) != testNum {
				t.Errorf("Backups metadata list count is invalid. Want %d, got %d", testNum, len(bms))
			}
			if _, ok := bms[mdFilename]; !ok {
				t.Errorf("Backup metadata for %q doesn't exists", mdFilename)
			}
		}
	}
	maxRs1NumBeforeRestore, err := getMaxNumberFromOplog(params, "_rs1.oplog")
	if err != nil {
		t.Errorf("Cannot get max number from oplog for rs1: %s", err)
	}
	maxRs2NumBeforeRestore, err := getMaxNumberFromOplog(params, "_rs2.oplog")
	if err != nil {
		t.Errorf("Cannot get max number from oplog for rs1: %s", err)
	}

	cleanupDB(t, s1Session)
	cleanupDB(t, s2Session)

	log.Info("Starting restore test")
	md, err := d.APIServer.LastBackupMetadata(context.Background(), &pbapi.LastBackupMetadataParams{})
	if err != nil {
		t.Fatalf("Cannot get last backup metadata to start the restore process: %s", err)
	}

	st, err := testutils.TestingStorages().Get(params.StorageName)
	if err != nil {
		t.Errorf("Cannot load storage %q: %s", params.StorageName, err)
	}
	if st.Type == "s3" {
		time.Sleep(15 * time.Second) // give it some time to refresh
	}

	testRestoreWithMetadata(t, d, md, params.StorageName)

	s1Session.Refresh()
	s1Session.ResetIndexCache()
	type maxNumber struct {
		Number int64 `bson:"number"`
	}
	var afterMaxS1, afterMaxS2 maxNumber
	err = s1Session.DB(dbName).C(colName).Find(nil).Sort("-number").Limit(1).One(&afterMaxS1)
	if err != nil {
		log.Errorf("Cannot get the max 'number' field in %s.%s: %s", dbName, colName, err)
	}

	err = s2Session.DB(dbName).C(colName).Find(nil).Sort("-number").Limit(1).One(&afterMaxS2)
	if err != nil {
		log.Errorf("Cannot get the max 'number' field in %s.%s: %s", dbName, colName, err)
	}

	if afterMaxS1.Number < maxRs1NumBeforeRestore {
		t.Errorf("Invalid documents count after restore is shard 1. Before restore: %d > after restore: %d",
			ndocs,
			afterMaxS1.Number,
		)
	}

	if afterMaxS2.Number < maxRs2NumBeforeRestore {
		t.Errorf("Invalid documents count after restore is shard 2. Before restore: %d > after restore: %d",
			ndocs,
			afterMaxS2.Number,
		)
	}
	wg.Wait()
}

func TestClientDisconnect(t *testing.T) {
	tmpDir := getTempDir(t)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all agents: %s", err)
	}

	clientsCount1 := len(d.MessagesServer.Clients())
	// Disconnect a client to check if the server detects the disconnection immediately
	if err := d.Clients()[0].Stop(); err != nil {
		t.Errorf("Cannot stop agent %s: %s", d.Clients()[0].ID(), err)
	}

	time.Sleep(2 * time.Second)

	clientsCount2 := len(d.MessagesServer.Clients())

	if clientsCount2 >= clientsCount1 {
		t.Errorf("Invalid clients count. Want < %d, got %d", clientsCount1, clientsCount2)
	}
}

func TestListStorages(t *testing.T) {
	tmpDir := getTempDir(t)

	log.Printf("Using %s as the temporary directory", tmpDir)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all agents: %s", err)
	}

	storagesList, err := d.MessagesServer.ListStorages()
	if err != nil {
		t.Errorf("Cannot list storages: %s", err)
	}
	if len(storagesList) != len(testutils.TestingStorages().Storages) {
		t.Errorf("Invalid number of storages. Want %d, got %d", len(testutils.TestingStorages().Storages), len(storagesList))
	}

	localhost := "127.0.0.1"
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Cannot get hostname: %s", err)
	}

	wantMatchingClients := []string{
		localhost + ":" + testutils.MongoDBShard1PrimaryPort,
		localhost + ":" + testutils.MongoDBShard1Secondary1Port,
		localhost + ":" + testutils.MongoDBShard1Secondary2Port,
		localhost + ":" + testutils.MongoDBShard2PrimaryPort,
		localhost + ":" + testutils.MongoDBShard2Secondary1Port,
		localhost + ":" + testutils.MongoDBShard2Secondary2Port,
		localhost + ":" + testutils.MongoDBConfigsvr1Port,
		hostname + ":" + testutils.MongoDBMongosPort,
	}
	for name, stg := range storagesList {
		if !reflect.DeepEqual(stg.MatchClients, wantMatchingClients) {
			t.Errorf("Invalid list of matching clients for storage %s.\nWant: %v\nGot : %v",
				name,
				wantMatchingClients,
				stg.MatchClients,
			)
		}
	}
}

func TestValidateReplicasetAgents(t *testing.T) {
	tmpDir := getTempDir(t)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all agents: %s", err)
	}

	if err := d.MessagesServer.ValidateReplicasetAgents(); err != nil {
		t.Errorf("Invalid number of connected agents: %s", err)
	}

	log.Info("Stopping agents connected to replicaset rs1")
	time.Sleep(10 * time.Second)
	for _, client := range d.Clients() {
		if client.ReplicasetName() == "rs1" {
			log.Infof("Stopping client: %s, rs: %s\n", client.NodeName(), client.ReplicasetName())
			if err := client.Stop(); err != nil {
				t.Errorf("Cannot stop client %s: %s", client.ID(), err)
			}
			time.Sleep(1 * time.Second)
		}
	}

	if err := d.MessagesServer.ValidateReplicasetAgents(); err == nil {
		t.Errorf("We manually disconnected agents from rs1. ValidateReplicasetAgents should return an error")
	}

	if err := d.MessagesServer.StartBackup(&pb.StartBackup{}); err == nil {
		t.Errorf("We manually disconnected agents from rs1. StartBackup/ValidateReplicasetAgents should return an error")
	}
}

func TestBackupSourceByReplicaset(t *testing.T) {
	tmpDir := getTempDir(t)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all agents: %s", err)
	}

	bs, err := d.MessagesServer.BackupSourceByReplicaset()
	if err != nil {
		t.Errorf("Cannot get the BackupSourceByReplicaset: %s", err)
	}

	// Lets's stop the "winner" client for rs1. That way, next call to BackupSourceByReplicaset should fail
	// since there won't be an agent running on that MongoDB instance
	time.Sleep(2 * time.Second)
	for _, client := range d.Clients() {
		if client.NodeName() == bs["rs1"].NodeName {
			if err := client.Stop(); err != nil {
				t.Errorf("Cannot stop client %s: %s", client.NodeName(), err)
			}
		}
	}

	time.Sleep(2 * time.Second)

	bs2, err := d.MessagesServer.BackupSourceByReplicaset()
	for id, client := range bs2 {
		fmt.Printf("-> id: %v, Node name: %s\n", id, client.NodeName)
	}
	if err == nil {
		t.Errorf("BackupSourceByReplicaset should fail since we manually killed the 'winner' agent")
	}
	if bs2 != nil {
		t.Errorf("BackupSourceByReplicaset should fail since we manually killed the 'winner' agent (want nil response)")
	}

	time.Sleep(2 * time.Second)
}

func TestBackupWithNoOplogActivity(t *testing.T) {
	ndocs := int64(1000)
	tmpDir := getTempDir(t)
	os.RemoveAll(tmpDir) // Cleanup before start. Don't check for errors. The path might not exist
	err := os.MkdirAll(tmpDir, os.ModePerm)
	if err != nil {
		t.Fatalf("Cannot create temp dir %s: %s", tmpDir, err)
	}
	log.Printf("Using %s as the temporary directory", tmpDir)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all agents: %s", err)
	}

	s1Session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		log.Fatalf("Cannot connect to the DB: %s", err)
	}
	s1Session.SetMode(mgo.Strong, true)
	generateDataToBackup(t, s1Session, ndocs)

	backupNamePrefix := time.Now().UTC().Format(time.RFC3339)

	err = d.MessagesServer.StartBackup(&pb.StartBackup{
		BackupType:      pb.BackupType_BACKUP_TYPE_LOGICAL,
		CompressionType: pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
		Cypher:          pb.Cypher_CYPHER_NO_CYPHER,
		OplogStartTime:  time.Now().UTC().Unix(),
		NamePrefix:      backupNamePrefix,
		Description:     "general_test_backup",
		StorageName:     "local-filesystem",
	})
	if err != nil {
		t.Fatalf("Cannot start backup: %s", err)
	}
	if err := d.MessagesServer.WaitBackupFinish(); err != nil {
		t.Errorf("WaitBackupFinish has failed: %s", err)
	}
	if err := d.MessagesServer.StopOplogTail(); err != nil {
		t.Errorf("Cannot stop oplog tailer: %s", err)
	}
	if err := d.MessagesServer.WaitOplogBackupFinish(); err != nil {
		t.Errorf("WaitOplogBackupFinish has failed: %s", err)
	}

	// Test list backups
	log.Debug("Testing backup metadata")
	mdFilename := backupNamePrefix + ".json"
	if err := d.MessagesServer.WriteServerBackupMetadata(mdFilename); err != nil {
		t.Errorf("Cannot write backup metadata to file %s: %s", mdFilename, err)
	}
	bms, err := d.MessagesServer.ListBackups()
	if err != nil {
		t.Errorf("Cannot get backups metadata listing: %s", err)
	} else {
		if bms == nil {
			t.Errorf("Backups metadata listing is nil")
		} else {
			if len(bms) != 1 {
				t.Errorf("Backups metadata listing is empty")
			}
			if _, ok := bms[mdFilename]; !ok {
				t.Errorf("Backup metadata for %q doesn't exists", mdFilename)
			}
		}
	}

	cleanupDB(t, s1Session)
}

func TestRefreshClients(t *testing.T) {
	stgs := testutils.TestingStorages()

	fsStorage, err := stgs.Get("local-filesystem")
	if err != nil {
		t.Fatalf("Cannot get local-filesystem storage")
	}
	tmpDir := fsStorage.Filesystem.Path

	os.RemoveAll(tmpDir) // Cleanup before start. Don't check for errors. The path might not exist
	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		t.Fatalf("Cannot create temp dir %s: %s", tmpDir, err)
	}
	log.Printf("Using %s as the temporary directory", tmpDir)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()

	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all agents: %s", err)
	}

	if err := d.MessagesServer.RefreshClients(); err != nil {
		t.Errorf("Cannot refresh all clients info: %s", err)
	}
}

func testRestoreWithMetadata(t *testing.T, d *testGrpc.Daemon, md *pb.BackupMetadata, storageName string) {
	if err := d.MessagesServer.RestoreBackUp(md, storageName, true); err != nil {
		t.Errorf("Cannot restore using backup metadata: %s", err)
	}
	log.Infof("Wating restore to finish")
	if err := d.MessagesServer.WaitRestoreFinish(); err != nil {
		t.Errorf("WaitRestoreFinish has failed: %s", err)
	}
}

func TestConfigServerClusterID(t *testing.T) {
	tmpDir := getTempDir(t)
	os.RemoveAll(tmpDir) // Cleanup before start. Don't check for errors. The path might not exist
	err := os.MkdirAll(tmpDir, os.ModePerm)
	if err != nil {
		t.Fatalf("Cannot create temp dir %s: %s", tmpDir, err)
	}
	log.Printf("Using %s as the temporary directory", tmpDir)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()

	portRsList := []testGrpc.PortRs{
		{Port: testutils.MongoDBConfigsvr1Port, Rs: testutils.MongoDBConfigsvrReplsetName},
	}

	if err := d.StartAgents(portRsList); err != nil {
		t.Fatalf("Cannot start config server: %s", err)
	}

	clientsList := d.MessagesServer.Clients()
	for i, client := range clientsList {
		if client.ClusterID == "" {
			t.Errorf("Cluster ID is empty for node %v", i)
		}
	}
}

func TestAllServersCmdLineOpts(t *testing.T) {
	tmpDir := getTempDir(t)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()

	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all clients: %s", err)
	}

	cmdLineOpts, err := d.MessagesServer.AllServersCmdLineOpts()
	if err != nil {
		t.Errorf("Cannot get cmdLineOpts from all clients: %s", err)
	}
	want := 8 // rs1: 3 agents, rs2: 3 agents, 1 config server and 1 mongos
	if len(cmdLineOpts) != want {
		t.Errorf("Invalid cmdLineOpts clients count. Want %d, got %d", want, len(cmdLineOpts))
	}
}

func getMaxNumberFromOplog(params *pb.StartBackup, suffix string) (int64, error) {
	extension := ""
	switch params.CompressionType {
	case pb.CompressionType_COMPRESSION_TYPE_GZIP:
		extension = ".gz"
	case pb.CompressionType_COMPRESSION_TYPE_LZ4:
		extension = "lz4" // Not implemented yet
	}
	doc, err := getLastOplogDoc(params, suffix+extension)
	if err != nil {
		return 0, err
	}
	return getMaxNumber(doc)
}

func getLastOplogDoc(msg *pb.StartBackup, nameSuffix string) (bson.M, error) {
	stg, err := testutils.TestingStorages().Get(msg.StorageName)
	if err != nil {
		return nil, err
	}

	filename := msg.NamePrefix + nameSuffix
	rdr, err := reader.MakeReader(filename, stg, msg.CompressionType, msg.Cypher)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create a reader for %s", filename)
	}

	br, err := bsonfile.NewBSONReader(rdr)
	if err != nil {
		return nil, err
	}
	doc := bson.M{}
	for {
		if err := br.UnmarshalNext(&doc); err != nil {
			break
		}
	}
	return doc, nil
}

func getMaxNumber(doc bson.M) (int64, error) {
	if o, ok := doc["o"]; ok {
		if bo, ok := o.(bson.M); ok {
			if number, ok := bo["number"]; ok {
				switch n := number.(type) {
				case int:
					return int64(n), nil
				case int64:
					return n, nil
				default:
					return 0, fmt.Errorf("number %v is not int or int64: %T", number, number)
				}
			} else {
				return 0, fmt.Errorf("there is no 'number' field in the doc: %+v", doc)
			}
		} else {
			return 0, fmt.Errorf("'o' field is not bson.M: %+v", doc)
		}
	}
	return 0, fmt.Errorf("there is no 'o' field in the document")
}

func cleanupDB(t *testing.T, session *mgo.Session) {
	dbs, err := session.DatabaseNames()
	if err != nil {
		t.Fatalf("Cannot list databases for clean up: %s", err)
	}
	for _, db := range dbs {
		if db == dbName {
			if err := session.DB(db).DropDatabase(); err != nil {
				t.Errorf("Cannot drop %s db: %s", db, err)
			}
			break
		}
	}
	session.Refresh()
}

func generateDataToBackup(t *testing.T, session *mgo.Session, ndocs int64) {
	cols, err := session.DB(dbName).CollectionNames()
	if err != nil {
		t.Fatalf("Cannot list collections to clean up: %s", err)
	}
	for _, col := range cols {
		if col == colName {
			if err := session.DB(dbName).C(colName).DropCollection(); err != nil {
				t.Errorf("Cannot drop the %s collection: %s", colName, err)
			}
			if err := session.DB(dbName).C(colName).EnsureIndexKey("number"); err != nil {
				t.Errorf("Cannot ensure index 'number' for collection %q: %s", colName, err)
			}
		}
	}

	session.Refresh()
	bulkCount := 0
	bulk := session.DB(dbName).C(colName).Bulk()

	for i := int64(0); i < ndocs; i++ {
		doc := bson.M{"number": i}
		bulk.Insert(doc)
		bulkCount++
		if bulkCount == bulkSize {
			if _, err := bulk.Run(); err != nil {
				t.Fatalf("Cannot insert data to back up (1): %s", err)
			}
			bulk = session.DB(dbName).C(colName).Bulk()
			bulkCount = 0
		}
	}

	if _, err := bulk.Run(); err != nil {
		t.Fatalf("Cannot insert data to back up (2): %s", err)
	}
}

// ndocs is the number of documents created for the pre-loaded data.
// we must start counting from ndocs+1 to avoid duplicated numbers
func generateOplogTraffic(session *mgo.Session, ndocs int64, stop chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)

	for {
		ndocs++
		select {
		case <-stop:
			ticker.Stop()
			return
		case <-ticker.C:
			err := session.DB(dbName).C(colName).Insert(bson.M{"number": ndocs})
			if err != nil {
				//panic(fmt.Errorf("insert to test.test failed: %v", err.Error()))
				return
			}
		}
	}
}

func getTempDir(t *testing.T) string {
	stgs := testutils.TestingStorages()
	stg, err := stgs.Get(localFileSystemStorage)
	if err != nil {
		t.Fatalf("Cannot get local filesystem storage: %s", err)
	}
	return stg.Filesystem.Path
}

func getCleanTempDir(t *testing.T) string {
	stgs := testutils.TestingStorages()
	stg, err := stgs.Get(localFileSystemStorage)
	if err != nil {
		t.Fatalf("Cannot get local filesystem storage: %s", err)
	}
	if _, err := os.Stat(stg.Filesystem.Path); !os.IsNotExist(err) {
		if err := os.RemoveAll(stg.Filesystem.Path); err != nil {
			t.Fatalf("Cannot clean the temporary before starting the test: %s", err)
		}
	}
	if err := os.MkdirAll(stg.Filesystem.Path, os.ModePerm); err != nil {
		t.Fatalf("Cannot create temporary directory: %s", err)
	}
	return stg.Filesystem.Path
}
