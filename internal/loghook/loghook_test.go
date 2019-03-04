package loghook

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/percona/percona-backup-mongodb/grpc/server"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	defaultGrpcPort        = 10000
	defaultShutdownTimeout = 5 // Seconds
	defaultBindIP          = "127.0.0.1"
)

// log package use an internal buffer that should be used ONLY durint the live of the Write method
// If we try to use it to capture the output, it will produce a data race.
// To avoid that, we need our own writer that copies the []bytes being written by Write
type logBuffer struct {
	c chan []byte
}

func (l *logBuffer) Write(b []byte) (n int, err error) {
	z := make([]byte, len(b))
	copy(z, b)
	l.c <- z
	return len(b), nil
}

func (l *logBuffer) Read(dest []byte) (int, error) {
	select {
	case b := <-l.c:
		copy(dest, b)
		return len(b), nil
	default:
	}
	return 0, io.EOF
}

func newLogBuffer() *logBuffer {
	return &logBuffer{
		c: make(chan []byte, 10),
	}
}

func TestReconnect(t *testing.T) {
	/*
		Run the server
	*/
	remoteLogBuffer := newLogBuffer()
	serverLogger := &logrus.Logger{
		Out: remoteLogBuffer,
		Formatter: &logrus.TextFormatter{
			FullTimestamp:          true,
			DisableLevelTruncation: true,
		},
		Hooks: make(logrus.LevelHooks),
		Level: logrus.DebugLevel,
	}
	clientID := "ABCD1234"
	logMessage := "I am the man with no name. Zapp Brannigan, at your service."
	grpcOpts := []grpc.ServerOption{}
	grpcServer := grpc.NewServer(grpcOpts...)
	messagesServer := server.NewMessagesServerWithClientLogging(os.TempDir(), 1, serverLogger)
	pb.RegisterMessagesServer(grpcServer, messagesServer)

	stopChan := make(chan interface{})
	wg := &sync.WaitGroup{}
	wg.Add(1)

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", defaultBindIP, defaultGrpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fmt.Printf("Starting agents gRPC server. Listening on %s\n", lis.Addr().String())
	runAgentsGRPCServer(grpcServer, lis, defaultShutdownTimeout, stopChan, wg)

	/*
		Run the logger client
	*/

	grpcClientOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	serverAddress := fmt.Sprintf("%s:%d", defaultBindIP, defaultGrpcPort)
	conn, err := grpc.Dial(serverAddress, grpcClientOpts...)
	if err != nil {
		t.Fatalf("Cannot connect to the gRPC server: %s", err)
	}

	localLogBuffer := newLogBuffer()
	log := &logrus.Logger{
		Out: localLogBuffer,
		Formatter: &logrus.TextFormatter{
			FullTimestamp:          true,
			DisableLevelTruncation: true,
		},
		Hooks: make(logrus.LevelHooks),
		Level: logrus.InfoLevel,
	}
	logHook, err := NewGrpcLogging(context.Background(), clientID, conn)
	if err != nil {
		log.Fatalf("Failed to create gRPC log hook: %v", err)
	}
	logHook.SetLevel(logrus.DebugLevel)
	log.AddHook(logHook)

	log.Error(logMessage)
	time.Sleep(1 * time.Second)

	wantMessage := fmt.Sprintf(`Client: %s, %s`, clientID, logMessage)
	gotRemoteMessage := make([]byte, 200)
	n, _ := remoteLogBuffer.Read(gotRemoteMessage)

	// n-2 to remove the last " and \n
	if !strings.HasSuffix(strings.TrimSpace(string(gotRemoteMessage[:n-2])), wantMessage) {
		t.Errorf("Received message is incorrect. Want suffix %q, got: %s",
			wantMessage,
			string(gotRemoteMessage[n-len(wantMessage)-2:n-2]))
	}

	stopChan <- true
	wg.Wait()
	lis.Close()

	log.Error(logMessage)
	time.Sleep(1 * time.Second)

	gotRemoteMessage = make([]byte, 200)
	n, err = remoteLogBuffer.Read(gotRemoteMessage)

	if err == nil {
		t.Errorf("Expected error. Read %d bytes from the remote log", n)
	}

	fmt.Println("Restarting the gRPC server ...")
	wg.Add(1)

	lis2, err := net.Listen("tcp", fmt.Sprintf("%s:%d", defaultBindIP, defaultGrpcPort))
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	remoteLogBuffer2 := newLogBuffer()
	serverLogger2 := &logrus.Logger{
		Out: remoteLogBuffer2,
		Formatter: &logrus.TextFormatter{
			FullTimestamp:          true,
			DisableLevelTruncation: true,
		},
		Hooks: make(logrus.LevelHooks),
		Level: logrus.DebugLevel,
	}

	grpcServer2 := grpc.NewServer(grpcOpts...)
	messagesServer2 := server.NewMessagesServerWithClientLogging(os.TempDir(), 1, serverLogger2)
	pb.RegisterMessagesServer(grpcServer2, messagesServer2)
	runAgentsGRPCServer(grpcServer2, lis2, defaultShutdownTimeout, stopChan, wg)

	time.Sleep(5 * time.Second)

	log.Error(logMessage) // first message will fail. Why?
	log.Error(logMessage)
	time.Sleep(2 * time.Second)

	gotRemoteMessage = make([]byte, 200)
	n, err = remoteLogBuffer2.Read(gotRemoteMessage)

	if err != nil {
		t.Fatalf("Log should have reconnect but, got error instead: %s", err)
	}
	if !strings.HasSuffix(strings.TrimSpace(string(gotRemoteMessage[:n-2])), wantMessage) {
		t.Errorf("Received message is incorrect. Want suffix %q, got: %s",
			wantMessage,
			string(gotRemoteMessage[n-len(wantMessage)-2:n-2]))
	}
}

func runAgentsGRPCServer(grpcServer *grpc.Server, lis net.Listener, shutdownTimeout int,
	stopChan chan interface{}, wg *sync.WaitGroup) {

	go func() {
		err := grpcServer.Serve(lis)
		if err != nil {
			log.Printf("Cannot start agents gRPC server: %s", err)
		}
		wg.Done()
	}()

	go func() {
		<-stopChan
		log.Printf("Gracefully stopping server at %s", lis.Addr().String())
		// Try to Gracefully stop the gRPC server.
		c := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			c <- struct{}{}
		}()

		// If after shutdownTimeout seconds the server hasn't stop, just kill it.
		select {
		case <-c:
			return
		case <-time.After(time.Duration(shutdownTimeout) * time.Second):
			log.Printf("Stopping server at %s", lis.Addr().String())
			grpcServer.Stop()
		}
	}()
}
