package test_test

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/percona-backup-mongodb/bsonfile"
	"github.com/percona/percona-backup-mongodb/grpc/server"
	"github.com/percona/percona-backup-mongodb/internal/awsutils"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	testGrpc "github.com/percona/percona-backup-mongodb/internal/testutils/grpc"
	pbapi "github.com/percona/percona-backup-mongodb/proto/api"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"github.com/percona/percona-backup-mongodb/storage"
	log "github.com/sirupsen/logrus"
)

var (
	port, apiPort             string
	grpcServerShutdownTimeout = 30
	keepS3Data                bool
	keepLocalFiles            bool
	storages                  *storage.Storages
)

const (
	dbName  = "test"
	colName = "test_col"
)

type cliOptions struct {
	clientID   *string
	tls        *bool
	caFile     *string
	serverAddr *string
}

func TestMain(m *testing.M) {
	log.SetLevel(log.ErrorLevel)
	log.SetFormatter(&log.TextFormatter{})

	flag.BoolVar(&keepS3Data, "keep-s3-data", false, "Do not delete S3 testing bucket and file")
	flag.BoolVar(&keepLocalFiles, "keep-local-files", false, "Do not files downloaded from the S3 bucket")
	flag.Parse()

	if os.Getenv("DEBUG") == "1" {
		log.SetLevel(log.DebugLevel)
	}

	if port = os.Getenv("GRPC_PORT"); port == "" {
		port = "10000"
	}
	if apiPort = os.Getenv("GRPC_API_PORT"); apiPort == "" {
		apiPort = "10001"
	}

	os.Exit(m.Run())
}

func TestGlobalWithDaemon(t *testing.T) {
	ndocs := 1000
	tmpDir := path.Join(os.TempDir(), "dump_test")
	os.RemoveAll(tmpDir)       // Cleanup before start. Don't check for errors. The path might not exist
	defer os.RemoveAll(tmpDir) // Clean up after testing.
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
	defer testutils.CleanTempDir()
	d.StartAllAgents()

	log.Debug("Getting list of connected clients")
	clientsList := d.MessagesServer.Clients()
	log.Debugf("Clients: %+v\n", clientsList)
	if len(clientsList) != d.ClientsCount() {
		t.Errorf("Want %d connected clients, got %d", d.ClientsCount(), len(clientsList))
	}

	log.Debug("Getting list of clients by replicaset")
	clientsByReplicaset := d.MessagesServer.ClientsByReplicaset()
	log.Debugf("Clients by replicaset: %+v\n", clientsByReplicaset)

	var firstClient server.Client
	for _, client := range clientsByReplicaset {
		if len(client) == 0 {
			break
		}
		firstClient = client[0]
		break
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

	// Genrate random data so we have something in the oplog
	oplogGeneratorStopChan := make(chan bool)
	s1Session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		log.Fatalf("Cannot connect to the DB: %s", err)
	}
	s1Session.SetMode(mgo.Strong, true)
	generateDataToBackup(t, s1Session, ndocs)
	go generateOplogTraffic(t, s1Session, ndocs, oplogGeneratorStopChan)

	// Also generate random data on shard 2
	s2Session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard2ReplsetName))
	if err != nil {
		log.Fatalf("Cannot connect to the DB: %s", err)
	}
	s2Session.SetMode(mgo.Strong, true)
	generateDataToBackup(t, s2Session, ndocs)
	go generateOplogTraffic(t, s2Session, ndocs, oplogGeneratorStopChan)

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
	d.MessagesServer.WaitBackupFinish()

	/*
		lastOplogDocRs1 has this structure:

		bson.M{
		  "ns":   "test.test_col",
		  "wall": time.Time{
		      wall: 0x1f2c58c0,
		      ext:  63677876029,
		      loc:  (*time.Location)(nil),
		  },
		  "o": bson.M{
		      "_id":    "[\xedP=\xe3\xdc3\x91z~\x84\x8e",
		      "number": int64(1122),
		  },
		  "ts": bson.MongoTimestamp(6624038849855094837),
		  "t":  int64(1),
		  "h":  int64(-8176934266205295323),
		  "v":  int(2),
		  "op": "i",
		}

		The oplog data generator only creates documents in the test_col collection and we are only counting
		in the 'number' field so, that's the field we want to know was was the last document the oplog tailer
		backed up
	*/
	rs1OplogFile := path.Join(tmpDir, fmt.Sprintf("%s_rs1.oplog", backupNamePrefix))
	rs1LastOplogDoc, err := getLastOplogDoc(rs1OplogFile)
	if err != nil {
		t.Fatalf("Cannot continue. Cannot open oplog dump for rs1: %s", err)
	}
	rs2OplogFile := path.Join(tmpDir, fmt.Sprintf("%s_rs2.oplog", backupNamePrefix))
	rs2LastOplogDoc, err := getLastOplogDoc(rs2OplogFile)
	if err != nil {
		t.Fatalf("Cannot continue. Cannot open oplog dump for rs1: %s", err)
	}

	log.Info("Stopping the oplog tailer")
	err = d.MessagesServer.StopOplogTail()
	if err != nil {
		t.Fatalf("Cannot stop the oplog tailer: %s", err)
	}

	close(oplogGeneratorStopChan)
	d.MessagesServer.WaitOplogBackupFinish()

	// Test list backups
	log.Debug("Testing backup metadata")
	mdFilename := backupNamePrefix + ".json"
	d.MessagesServer.WriteBackupMetadata(mdFilename)
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

	cleanupDBForRestore(t, s1Session)
	cleanupDBForRestore(t, s2Session)

	log.Info("Starting restore test")
	md, err := d.APIServer.LastBackupMetadata(context.Background(), &pbapi.LastBackupMetadataParams{})
	if err != nil {
		t.Fatalf("Cannot get last backup metadata to start the restore process: %s", err)
	}

	testRestoreWithMetadata(t, d, md, "local-filesystem")

	type maxNumber struct {
		Number int64 `bson:"number"`
	}
	var afterMaxS1, afterMaxS2 maxNumber
	err = s1Session.DB(dbName).C(colName).Find(nil).Sort("-number").Limit(1).One(&afterMaxS1)
	if err != nil {
		log.Fatalf("Cannot get the max 'number' field in %s.%s: %s", dbName, colName, err)
	}

	err = s2Session.DB(dbName).C(colName).Find(nil).Sort("-number").Limit(1).One(&afterMaxS2)
	if err != nil {
		log.Fatalf("Cannot get the max 'number' field in %s.%s: %s", dbName, colName, err)
	}

	if afterMaxS1.Number < int64(rs1LastOplogDoc["o"].(bson.M)["number"].(int)) {
		t.Errorf("Invalid documents count after restore is shard 1. Before restore: %d > after restore: %d", rs1LastOplogDoc["o"].(bson.M)["number"].(int64), afterMaxS1.Number)
	}

	if afterMaxS2.Number < int64(rs2LastOplogDoc["o"].(bson.M)["number"].(int)) {
		t.Errorf("Invalid documents count after restore is shard 2. Before restore: %d > after restore: %d", rs2LastOplogDoc["o"].(bson.M)["number"].(int64), afterMaxS2.Number)
	}

	// we can compare lastOplogDocRsx o.number document because initially we have 100 rows in the table
	// and the oplog generator is starting to count from 101 sequentially, so last inserted document = count
	rs1BeforeCount := int64(rs1LastOplogDoc["o"].(bson.M)["number"].(int))
	rs2BeforeCount := int64(rs2LastOplogDoc["o"].(bson.M)["number"].(int))
	rs1AfterCount, _ := s1Session.DB(dbName).C(colName).Find(nil).Count()
	rs2AfterCount, _ := s2Session.DB(dbName).C(colName).Find(nil).Count()

	if int64(rs1AfterCount) < rs1BeforeCount {
		t.Errorf("Invalid documents count in rs1. Want %d, got %d", rs1BeforeCount, rs1AfterCount)
	}
	if int64(rs2AfterCount) < rs2BeforeCount {
		t.Errorf("Invalid documents count in rs2. Want %d, got %d", rs2BeforeCount, rs2AfterCount)
	}
}

func TestBackupToS3(t *testing.T) {
	ndocs := 100
	storageName := "s3-us-west"
	tmpDir := path.Join(os.TempDir(), "dump_test")
	os.RemoveAll(tmpDir)       // Cleanup before start. Don't check for errors. The path might not exist
	defer os.RemoveAll(tmpDir) // Clean up after testing.
	err := os.MkdirAll(tmpDir, os.ModePerm)
	if err != nil {
		t.Fatalf("Cannot create temp dir %s: %s", tmpDir, err)
	}
	log.Printf("Using %s as the temporary directory", tmpDir)

	stg, err := testutils.TestingStorages().Get("s3-us-west")
	if err != nil {
		t.Fatalf("Cannot get storage named s3-us-west")
	}
	bucket := stg.S3.Bucket
	defer testutils.CleanTempDir()

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	diag("Starting AWS session")
	sess, err := awsutils.GetAWSSessionFromStorage(stg.S3)
	if err != nil {
		t.Skipf("Cannot start AWS session. Skipping S3 test: %s", err)
	}

	diag("Creating S3 service client")
	// Create S3 service client
	svc := s3.New(sess)
	diag("Checking if bucket %s exists", bucket)

	exists, err := testutils.BucketExists(svc, bucket)
	if err != nil {
		t.Fatalf("Cannot check if bucket %q exists: %s", bucket, err)
	}
	if !exists {
		if err := testutils.CreateBucket(svc, bucket); err != nil {
			t.Fatalf("Unable to create bucket %q, %v", bucket, err)
		}
	}
	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	d.StartAllAgents()

	// Genrate random data so we have something in the oplog
	oplogGeneratorStopChan := make(chan bool)
	s1Session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		log.Fatalf("Cannot connect to the DB: %s", err)
	}
	s1Session.SetMode(mgo.Strong, true)
	generateDataToBackup(t, s1Session, ndocs)
	go generateOplogTraffic(t, s1Session, ndocs, oplogGeneratorStopChan)

	// Also generate random data on shard 2
	s2Session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard2ReplsetName))
	if err != nil {
		log.Fatalf("Cannot connect to the DB: %s", err)
	}
	s2Session.SetMode(mgo.Strong, true)
	generateDataToBackup(t, s2Session, ndocs)
	go generateOplogTraffic(t, s2Session, ndocs, oplogGeneratorStopChan)

	backupNamePrefix := time.Now().UTC().Format(time.RFC3339)

	err = d.MessagesServer.StartBackup(&pb.StartBackup{
		BackupType:      pb.BackupType_BACKUP_TYPE_LOGICAL,
		CompressionType: pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
		Cypher:          pb.Cypher_CYPHER_NO_CYPHER,
		OplogStartTime:  time.Now().UTC().Unix(),
		NamePrefix:      backupNamePrefix,
		Description:     "general_test_backup",
		StorageName:     storageName,
	})
	if err != nil {
		t.Fatalf("Cannot start backup: %s", err)
	}
	d.MessagesServer.WaitBackupFinish()

	/*
		lastOplogDocRs1 has this structure:
		bson.M{
		  "ns":   "test.test_col",
		  "wall": time.Time{
		      wall: 0x1f2c58c0,
		      ext:  63677876029,
		      loc:  (*time.Location)(nil),
		  },
		  "o": bson.M{
		      "_id":    "[\xedP=\xe3\xdc3\x91z~\x84\x8e",
		      "number": int64(1122),
		  },
		  "ts": bson.MongoTimestamp(6624038849855094837),
		  "t":  int64(1),
		  "h":  int64(-8176934266205295323),
		  "v":  int(2),
		  "op": "i",
		}

		The oplog data generator only creates documents in the test_col collection and we are only counting
		in the 'number' field so, that's the field we want to know was was the last document the oplog tailer
		backed up
	*/

	close(oplogGeneratorStopChan)
	d.MessagesServer.StopOplogTail()
	d.MessagesServer.WaitOplogBackupFinish()

	// Sometimes it takes some time until the session can see all the files.
	if err := waitForFiles(svc, bucket, backupNamePrefix); err != nil {
		t.Errorf("Wait for files in bucket timeout: %s", err)
	}

	rs1LastOplogDoc, err := getLastOplogDocFromS3(svc, bucket, backupNamePrefix, "rs1", t)
	if err != nil {
		t.Fatalf("Cannot continue. Cannot open oplog dump for rs1: %s", err)
	}

	rs2LastOplogDoc, err := getLastOplogDocFromS3(svc, bucket, backupNamePrefix, "rs2", t)
	if err != nil {
		t.Fatalf("Cannot continue. Cannot open oplog dump for rs1: %s", err)
	}

	log.Info("Stopping the oplog tailer")
	err = d.MessagesServer.StopOplogTail()
	if err != nil {
		t.Fatalf("Cannot stop the oplog tailer: %s", err)
	}

	// Test list backups
	log.Debug("Testing backup metadata")
	mdFilename := backupNamePrefix + ".json"
	d.MessagesServer.WriteBackupMetadata(mdFilename)
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

	cleanupDBForRestore(t, s1Session)
	cleanupDBForRestore(t, s2Session)

	log.Info("Starting restore test")
	md, err := d.APIServer.LastBackupMetadata(context.Background(), &pbapi.LastBackupMetadataParams{})
	if err != nil {
		t.Fatalf("Cannot get last backup metadata to start the restore process: %s", err)
	}

	testRestoreWithMetadata(t, d, md, storageName)

	type maxNumber struct {
		Number int64 `bson:"number"`
	}
	var afterMaxS1, afterMaxS2 maxNumber
	err = s1Session.DB(dbName).C(colName).Find(nil).Sort("-number").Limit(1).One(&afterMaxS1)
	if err != nil {
		log.Fatalf("Cannot get the max 'number' field in %s.%s: %s", dbName, colName, err)
	}
	fmt.Printf("after max s1: %+v\n", afterMaxS1)

	err = s2Session.DB(dbName).C(colName).Find(nil).Sort("-number").Limit(1).One(&afterMaxS2)
	if err != nil {
		log.Fatalf("Cannot get the max 'number' field in %s.%s: %s", dbName, colName, err)
	}
	fmt.Printf("after max s2: %+v\n", afterMaxS2)

	if afterMaxS1.Number < int64(rs1LastOplogDoc["o"].(bson.M)["number"].(int)) {
		t.Errorf("Invalid documents count after restore is shard 1. Before restore: %d > after restore: %d", rs1LastOplogDoc["o"].(bson.M)["number"].(int64), afterMaxS1.Number)
	}

	if afterMaxS2.Number < int64(rs2LastOplogDoc["o"].(bson.M)["number"].(int)) {
		t.Errorf("Invalid documents count after restore is shard 2. Before restore: %d > after restore: %d", rs2LastOplogDoc["o"].(bson.M)["number"].(int64), afterMaxS2.Number)
	}

	// we can compare lastOplogDocRsx o.number document because initially we have 100 rows in the table
	// and the oplog generator is starting to count from 101 sequentially, so last inserted document = count
	rs1BeforeCount := int64(rs1LastOplogDoc["o"].(bson.M)["number"].(int))
	rs2BeforeCount := int64(rs2LastOplogDoc["o"].(bson.M)["number"].(int))
	rs1AfterCount, err := s1Session.DB(dbName).C(colName).Find(nil).Count()
	rs2AfterCount, err := s2Session.DB(dbName).C(colName).Find(nil).Count()

	if int64(rs1AfterCount) < rs1BeforeCount {
		t.Errorf("Invalid documents count in rs1. Want %d, got %d", rs1BeforeCount, rs1AfterCount)
	}
	if int64(rs2AfterCount) < rs2BeforeCount {
		t.Errorf("Invalid documents count in rs2. Want %d, got %d", rs2BeforeCount, rs2AfterCount)
	}

	// Clean up after testing
	if !keepS3Data {
		diag("Deleting bucket %q", bucket)
		if err = awsutils.EmptyBucket(svc, bucket); err != nil {
			t.Errorf("Cannot delete bucket %q: %s", bucket, err)
		}

		diag("Deleting bucket %q", bucket)
		if err = testutils.DeleteBucket(svc, bucket); err != nil {
			t.Errorf("Cannot delete bucket %q: %s", bucket, err)
		}
	} else {
		diag("Skipping deletion of %q bucket", bucket)
	}
}

func TestClientDisconnect(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "pbm_")
	if err != nil {
		t.Fatalf("Cannot create temporary directory for TestClientDisconnect: %s", err)
	}
	defer os.RemoveAll(tmpDir) // Clean up
	log.Printf("Using %s as the temporary directory", tmpDir)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	defer testutils.CleanTempDir()
	d.StartAllAgents()

	clientsCount1 := len(d.MessagesServer.Clients())
	// Disconnect a client to check if the server detects the disconnection immediately
	d.Clients()[0].Stop()

	time.Sleep(2 * time.Second)

	clientsCount2 := len(d.MessagesServer.Clients())

	if clientsCount2 >= clientsCount1 {
		t.Errorf("Invalid clients count. Want < %d, got %d", clientsCount1, clientsCount2)
	}
}

func TestListStorages(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "pbm_")
	if err != nil {
		t.Fatalf("Cannot create temporary directory for TestClientDisconnect: %s", err)
	}
	defer os.RemoveAll(tmpDir) // Clean up
	log.Printf("Using %s as the temporary directory", tmpDir)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	defer testutils.CleanTempDir()
	d.StartAllAgents()

	storagesList, err := d.MessagesServer.ListStorages()
	if err != nil {
		t.Errorf("Cannot list storages: %s", err)
	}
	if len(storagesList) != 2 {
		t.Errorf("Invalid number of storages. Want 2, got %d", len(storagesList))
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
			t.Errorf("Invalid list of matching clients for storage %s.\nWant: %v\nGot : %v", name, wantMatchingClients, stg.MatchClients)
		}
	}

}

func TestValidateReplicasetAgents(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "pbm_")
	if err != nil {
		t.Fatalf("Cannot create temporary directory for TestClientDisconnect: %s", err)
	}
	defer os.RemoveAll(tmpDir) // Clean up
	log.Printf("Using %s as the temporary directory", tmpDir)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	defer testutils.CleanTempDir()
	d.StartAllAgents()

	if err := d.MessagesServer.ValidateReplicasetAgents(); err != nil {
		t.Errorf("Invalid number of connected agents: %s", err)
	}

	log.Info("Stopping agents connected to replicaset rs1")
	time.Sleep(10 * time.Second)
	for _, client := range d.Clients() {
		if client.ReplicasetName() == "rs1" {
			log.Infof("Stopping client: %s, rs: %s\n", client.NodeName(), client.ReplicasetName())
			client.Stop()
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
	tmpDir, err := ioutil.TempDir("", "pbm_")
	if err != nil {
		t.Fatalf("Cannot create temporary directory for TestClientDisconnect: %s", err)
	}
	defer os.RemoveAll(tmpDir) // Clean up
	log.Printf("Using %s as the temporary directory", tmpDir)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	defer testutils.CleanTempDir()
	d.StartAllAgents()

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

// This test checks if statuses are being set correctly
func TestRunBackupTwice(t *testing.T) {
	ndocs := 1000
	tmpDir := path.Join(os.TempDir(), "dump_test")
	os.RemoveAll(tmpDir)       // Cleanup before start. Don't check for errors. The path might not exist
	defer os.RemoveAll(tmpDir) // Clean up after testing.

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
	defer testutils.CleanTempDir()
	d.StartAllAgents()

	storageName := "local-filesystem"

	runBackup(t, d, storageName, ndocs)
	runBackup(t, d, storageName, ndocs)
}

func runBackup(t *testing.T, d *testGrpc.Daemon, storageName string, ndocs int) {
	// Genrate random data so we have something in the oplog
	oplogGeneratorStopChan := make(chan bool)
	s1Session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		log.Fatalf("Cannot connect to the DB: %s", err)
	}
	generateDataToBackup(t, s1Session, ndocs)
	go generateOplogTraffic(t, s1Session, ndocs, oplogGeneratorStopChan)

	// Also generate random data on shard 2
	s2Session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard2ReplsetName))
	if err != nil {
		log.Fatalf("Cannot connect to the DB: %s", err)
	}
	generateDataToBackup(t, s2Session, ndocs)
	go generateOplogTraffic(t, s2Session, ndocs, oplogGeneratorStopChan)

	backupNamePrefix := time.Now().UTC().Format(time.RFC3339)

	msg := &pb.StartBackup{
		BackupType:      pb.BackupType_BACKUP_TYPE_LOGICAL,
		CompressionType: pb.CompressionType_COMPRESSION_TYPE_GZIP,
		Cypher:          pb.Cypher_CYPHER_NO_CYPHER,
		OplogStartTime:  time.Now().UTC().Unix(),
		NamePrefix:      backupNamePrefix,
		Description:     "general_test_backup",
		StorageName:     storageName,
	}
	if err := d.MessagesServer.StartBackup(msg); err != nil {
		t.Fatalf("Cannot start backup: %s", err)
	}

	log.Infof("starting backup: %s\n", backupNamePrefix)

	d.MessagesServer.WaitBackupFinish()
	err = d.MessagesServer.StopOplogTail()
	if err != nil {
		t.Fatalf("Cannot stop the oplog tailer: %s", err)
	}

	close(oplogGeneratorStopChan)
	d.MessagesServer.WaitOplogBackupFinish()
}

func TestBackupWithNoOplogActivity(t *testing.T) {
	ndocs := 1000
	tmpDir := path.Join(os.TempDir(), "dump_test")
	os.RemoveAll(tmpDir)       // Cleanup before start. Don't check for errors. The path might not exist
	defer os.RemoveAll(tmpDir) // Clean up after testing.
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
	defer testutils.CleanTempDir()
	d.StartAllAgents()

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
	d.MessagesServer.WaitBackupFinish()
	d.MessagesServer.StopOplogTail()
	d.MessagesServer.WaitOplogBackupFinish()

	// Test list backups
	log.Debug("Testing backup metadata")
	mdFilename := backupNamePrefix + ".json"
	d.MessagesServer.WriteBackupMetadata(mdFilename)
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

	cleanupDBForRestore(t, s1Session)
}

func testRestoreWithMetadata(t *testing.T, d *testGrpc.Daemon, md *pb.BackupMetadata, storageName string) {
	if err := d.MessagesServer.RestoreBackUp(md, storageName, true); err != nil {
		t.Errorf("Cannot restore using backup metadata: %s", err)
	}
	log.Infof("Wating restore to finish")
	d.MessagesServer.WaitRestoreFinish()
}

func TestConfigServerClusterID(t *testing.T) {
	tmpDir := path.Join(os.TempDir(), "dump_test")
	os.RemoveAll(tmpDir)       // Cleanup before start. Don't check for errors. The path might not exist
	defer os.RemoveAll(tmpDir) // Clean up after testing.
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
	defer testutils.CleanTempDir()

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

func diag(params ...interface{}) {
	if testing.Verbose() {
		format := params[0].(string)
		if !strings.HasSuffix(format, "\n") {
			format += "\n"
		}
		fmt.Printf(format, params[1:]...)
	}
}

func getLastOplogDoc(filename string) (bson.M, error) {
	br, err := bsonfile.OpenFile(filename)
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
func getLastOplogDocFromS3(svc *s3.S3, bucket, backupNamePrefix, replicaset string, t *testing.T) (bson.M, error) {
	oplogFile := fmt.Sprintf("%s_%s.oplog", backupNamePrefix, replicaset)
	tmpOplogFile := path.Join(os.TempDir(), oplogFile)
	fh, err := os.Create(tmpOplogFile)
	if err != nil {
		t.Fatalf("Cannot create temporary rs1OplogFile %s to count the number of docs in the oplog: %s",
			tmpOplogFile, err)
	}
	defer os.Remove(tmpOplogFile)
	defer fh.Close()

	if _, err := awsutils.DownloadFile(svc, bucket, oplogFile, fh); err != nil {
		t.Fatalf("Cannot download rs oplog file %s from bucket %s: %s", oplogFile, bucket, err)
	}

	br, err := bsonfile.OpenFile(tmpOplogFile)
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

func randomBucket() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("pbm-test-bucket-%05d", rand.Int63n(99999))
}

func sortedReplicaNames(replicas map[string]*server.Client) []string {
	a := []string{}
	for key := range replicas {
		a = append(a, key)
	}
	sort.Strings(a)
	return a
}

func cleanupDBForRestore(t *testing.T, session *mgo.Session) {
	err := session.DB(dbName).C(colName).DropCollection()
	if err != nil {
		t.Errorf("Cannot cleanup database: %s", err)
	}
}

func generateDataToBackup(t *testing.T, session *mgo.Session, ndocs int) {
	// Don't check for error because the collection might not exist.
	session.DB(dbName).C(colName).DropCollection()
	session.DB(dbName).C(colName).EnsureIndexKey("number")
	session.Refresh()

	for i := 0; i < ndocs; i++ {
		err := session.DB(dbName).C(colName).Insert(bson.M{"number": i})
		if err != nil {
			t.Fatalf("Cannot insert data to back up: %s", err)
		}
	}
}

// ndocs is the number of documents created for the pre-loaded data.
// we must start counting from ndocs+1 to avoid duplicated numbers
func generateOplogTraffic(t *testing.T, session *mgo.Session, ndocs int, stop chan bool) {
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
				t.Fatalf("insert to test.test failed: %v", err.Error())
			}
		}
	}
}

func waitForFiles(svc *s3.S3, bucket, prefix string) error {
	count := 0
	for {
		exists, err := awsutils.BucketExists(svc, bucket)
		if err != nil {
			return err
		}
		if exists {
			break
		}
		time.Sleep(1 * time.Second)
		count++
		if count > 5 {
			return fmt.Errorf("Waited 5 seconds and bucket %s still doesn't exists", bucket)
		}
	}

	count = 0
	for {
		results, err := awsutils.ListObjects(svc, bucket)
		if err != nil {
			return err
		}
		// (1 dump + 1 oplog) x (1 config server + 2 rs) = 6 files
		if len(results.Contents) == 6 {
			break
		}
		time.Sleep(1 * time.Second)
		count++
		if count > 5 {
			return fmt.Errorf("Waited 5 seconds and bucket %s still doesn't have all the files (have %d files, want 6)", bucket, len(results.Contents))
		}
	}

	return nil
}
