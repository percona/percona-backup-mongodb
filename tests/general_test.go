package test_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/grpc/api"
	"github.com/percona/mongodb-backup/grpc/client"
	"github.com/percona/mongodb-backup/grpc/server"
	"github.com/percona/mongodb-backup/internal/restore"
	"github.com/percona/mongodb-backup/internal/testutils"
	pbapi "github.com/percona/mongodb-backup/proto/api"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"google.golang.org/grpc"
	"gopkg.in/mgo.v2/bson"
)

var port, apiPort string

const (
	dbName  = "test"
	colName = "test_col"
)

func TestMain(m *testing.M) {

	if port = os.Getenv("GRPC_PORT"); port == "" {
		port = "10000"
	}
	if apiPort = os.Getenv("GRPC_API_PORT"); apiPort == "" {
		apiPort = "10001"
	}

	os.Exit(m.Run())
}

func TestGlobal(t *testing.T) {
	var opts []grpc.ServerOption
	stopChan := make(chan interface{})
	wg := &sync.WaitGroup{}

	// Start the grpc server
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", port))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// This is the sever/agents gRPC server
	grpcServer := grpc.NewServer(opts...)
	messagesServer := server.NewMessagesServer()
	pb.RegisterMessagesServer(grpcServer, messagesServer)

	wg.Add(1)
	log.Printf("Starting agents gRPC server. Listening on %s", lis.Addr().String())
	runAgentsGRPCServer(grpcServer, lis, stopChan, wg)

	//
	apilis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", apiPort))
	if err != nil {
		t.Fatal(err)
	}

	// This is the server gRPC API
	apiGrpcServer := grpc.NewServer(opts...)
	apiServer := api.NewApiServer(messagesServer)
	pbapi.RegisterApiServer(apiGrpcServer, apiServer)

	wg.Add(1)
	log.Printf("Starting API gRPC server. Listening on %s", apilis.Addr().String())
	runAgentsGRPCServer(apiGrpcServer, apilis, stopChan, wg)

	// Let's start an agent
	clientOpts := []grpc.DialOption{grpc.WithInsecure()}

	clientServerAddr := fmt.Sprintf("127.0.0.1:%s", port)
	clientConn, err := grpc.Dial(clientServerAddr, clientOpts...)

	ports := []string{testutils.MongoDBPrimaryPort, testutils.MongoDBSecondary1Port, testutils.MongoDBSecondary2Port}

	clients := []*client.Client{}
	for i, port := range ports {
		di := testutils.DialInfoForPort(port)
		session, err := mgo.DialWithInfo(di)
		log.Printf("Connecting agent #%d to: %s\n", i, di.Addrs[0])
		if err != nil {
			t.Fatalf("Cannot connect to the MongoDB server %q: %s", di.Addrs[0], err)
		}
		session.SetMode(mgo.Eventual, true)

		agentID := fmt.Sprintf("PMB-%03d", i)

		dbConnOpts := client.ConnectionOptions{
			Host:           testutils.MongoDBHost,
			Port:           port,
			User:           di.Username,
			Password:       di.Password,
			ReplicasetName: di.ReplicaSetName,
		}

		client, err := client.NewClient(ctx, dbConnOpts, client.SSLOptions{}, clientConn)
		if err != nil {
			t.Fatalf("Cannot create an agent instance %s: %s", agentID, err)
		}
		clients = append(clients, client)
	}

	clientsList := messagesServer.Clients()
	if len(clientsList) != 3 {
		t.Errorf("Want 3 connected clients, got %d", len(clientsList))
	}

	clientsByReplicaset := messagesServer.ClientsByReplicaset()
	var firstClient *server.Client
	for _, client := range clientsByReplicaset {
		if len(client) == 0 {
			break
		}
		firstClient = client[0]
		break
	}

	status, err := firstClient.GetStatus()
	if err != nil {
		t.Errorf("Cannot get first client status: %s", err)
	}
	if status.BackupType != pb.BackupType_LOGICAL {
		t.Errorf("The default backup type should be 0 (Logical). Got backup type: %v", status.BackupType)
	}

	backupSource, err := firstClient.GetBackupSource()
	if err != nil {
		t.Errorf("Cannot get backup source: %s", err)
	}
	if backupSource == "" {
		t.Error("Received empty backup source")
	}
	log.Printf("Received backup source: %v", backupSource)

	backupSources, err := messagesServer.BackupSourceByReplicaset()
	if err != nil {
		t.Fatalf("Cannot get backup sources by replica set: %s", err)
	}

	// Genrate random data so we have something in the oplog
	sc := make(chan bool)
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		log.Fatalf("Cannot connect to the DB: %s", err)
	}
	generateDataToBackup(t, session)
	go generateOplogTraffic(t, session, sc)

	tmpDir, err := ioutil.TempDir("", "dump_test.")
	if err != nil {
		t.Fatalf("Cannot create temporary file for testing: %s", err)
	}

	// Start the backup. Backup just the first replicaset (sorted by name) so we can test the restore
	replNames := sortedReplicaNames(backupSources)
	replName := replNames[0]
	client := backupSources[replName]

	fmt.Printf("Replicaset: %s, Client: %s\n", replName, client.NodeName)
	if testing.Verbose() {
		log.Printf("Temp dir for backup: %s", tmpDir)
	}

	err = client.StartBackup(&pb.StartBackup{
		BackupType:      pb.BackupType_LOGICAL,
		DestinationType: pb.DestinationType_FILE,
		DestinationName: "test",
		DestinationDir:  tmpDir,
		CompressionType: pb.CompressionType_NO_COMPRESSION,
		Cypher:          pb.Cypher_NO_CYPHER,
		OplogStartTime:  time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("Cannot start backup: %s", err)
	}

	messagesServer.WaitBackupFinish()
	messagesServer.StopOplogTail()
	stopChan <- true
	messagesServer.WaitBackupFinish()

	fmt.Println("stoppping clients")
	for _, client := range clients {
		client.Stop()
	}

	cancel()
	messagesServer.Stop()
	close(stopChan)
	wg.Wait()

	testRestore(t, session, tmpDir)
}

func runAgentsGRPCServer(grpcServer *grpc.Server, lis net.Listener, stopChan chan interface{}, wg *sync.WaitGroup) {
	go func() {
		err := grpcServer.Serve(lis)
		if err != nil {
			log.Printf("Cannot start agents gRPC server: %s", err)
		}
		log.Println("Stopping server " + lis.Addr().String())
		wg.Done()
	}()

	go func() {
		<-stopChan
		log.Printf("Gracefuly stopping server at %s", lis.Addr().String())
		grpcServer.GracefulStop()
	}()
}

func testRestore(t *testing.T, session *mgo.Session, dir string) {
	if err := session.DB(dbName).C(colName).DropCollection(); err != nil {
		t.Fatalf("Cannot clean up %s.%s collection: %s", dbName, colName, err)
	}

	count, err := session.DB(dbName).C(colName).Find(nil).Count()
	if err != nil {
		t.Fatalf("Cannot count number of rows in the test collection %s: %s", colName, err)
	}

	if count > 0 {
		t.Fatalf("Invalid rows count in the test collection %s. Got %d, want 0", colName, count)
	}

	input := &restore.MongoRestoreInput{
		// this file was generated with the dump pkg
		Archive:  path.Join(dir, "test.dump"),
		DryRun:   false,
		Host:     testutils.MongoDBHost,
		Port:     testutils.MongoDBPrimaryPort,
		Username: testutils.MongoDBUser,
		Password: testutils.MongoDBPassword,
		Gzip:     false,
		Oplog:    false,
		Threads:  1,
		Reader:   nil,
	}

	r, err := restore.NewMongoRestore(input)
	if err != nil {
		t.Errorf("Cannot instantiate mongo restore instance: %s", err)
		t.FailNow()
	}

	if err := r.Start(); err != nil {
		t.Errorf("Cannot start restore: %s", err)
	}

	if err := r.Wait(); err != nil {
		t.Errorf("Error while trying to restore: %s", err)
	}

	count, err = session.DB(dbName).C(colName).Find(nil).Count()
	if err != nil {
		t.Fatalf("Cannot count number of rows in the test collection %q after restore: %s", colName, err)
	}

	if count < 100 {
		t.Fatalf("Invalid rows count in the test collection %s. Got %d, want 100", colName, count)
	}
}

func sortedReplicaNames(replicas map[string]*server.Client) []string {
	a := []string{}
	for key, _ := range replicas {
		a = append(a, key)
	}
	sort.Strings(a)
	return a
}

func generateDataToBackup(t *testing.T, session *mgo.Session) {
	// Don't check for error because the collection might not exist.
	session.DB(dbName).C(colName).DropCollection()

	for i := 0; i < 100; i++ {
		err := session.DB(dbName).C(colName).Insert(bson.M{"number": i})
		if err != nil {
			t.Fatalf("Cannot insert data to back up: %s", err)
		}
	}
}

func generateOplogTraffic(t *testing.T, session *mgo.Session, stop chan bool) {
	ticker := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-stop:
			ticker.Stop()
			return
		case <-ticker.C:
			// number + 10000 just to differentiate this data from the data generated
			// in generateDataToBackup() so we can test a dump restore + oplog apply
			number := rand.Int63n(9999) + 10000
			err := session.DB(dbName).C(colName).Insert(bson.M{"number": number})
			if err != nil {
				t.Logf("insert to test.test failed: %v", err.Error())
			}
		}
	}
}
