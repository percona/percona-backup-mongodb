package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"sync"
	"testing"

	// "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	// "github.com/grpc-ecosystem/go-grpc-middleware/tags"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/kr/pretty"
	"github.com/percona/percona-backup-mongodb/grpc/api"
	"github.com/percona/percona-backup-mongodb/grpc/server"
	"github.com/percona/percona-backup-mongodb/internal/utils"
	apipb "github.com/percona/percona-backup-mongodb/proto/api"
	pbapi "github.com/percona/percona-backup-mongodb/proto/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	yaml "gopkg.in/yaml.v2"
)

func TestDefaults(t *testing.T) {
	opts, err := processCliParams([]string{})
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		GrpcPort:             defaultGrpcPort,
		APIPort:              defaultAPIPort,
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              utils.Expand(defaultWorkDir),
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestOverrideDefaultsFromCommandLine(t *testing.T) {
	opts, err := processCliParams([]string{"--work-dir", os.TempDir(), "--api-port", "12345"})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		GrpcPort:             defaultGrpcPort,
		APIPort:              12345,
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              os.TempDir(),
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestOverrideDefaultsFromEnv(t *testing.T) {
	os.Setenv("PBM_COORDINATOR_API_PORT", "12346")
	opts, err := processCliParams([]string{"--work-dir", os.TempDir()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		GrpcPort:             defaultGrpcPort,
		APIPort:              12346,
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              os.TempDir(),
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestOverrideDefaultsFromConfigFile(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatalf("cannot create temp config file: %s", err)
	}

	defer os.Remove(tmpfile.Name())
	wantOpts := &cliOptions{
		configFile:           tmpfile.Name(),
		GrpcPort:             98765,
		APIPort:              12346,
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              os.TempDir(),
	}
	b, _ := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	opts, err := processCliParams([]string{"--config-file", tmpfile.Name()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestConfigfileEnvPrecedenceOverEnvVars(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatalf("cannot create temp config file: %s", err)
	}

	defer os.Remove(tmpfile.Name())
	wantOpts := &cliOptions{
		configFile:           tmpfile.Name(),
		GrpcPort:             98765,
		APIPort:              12346,
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              os.TempDir(),
	}
	b, _ := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	os.Setenv("PBM_COORDINATOR_API_PORT", "12347")

	opts, err := processCliParams([]string{"--config-file", tmpfile.Name()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestCommandLineArgsPrecedenceOverEnvVars(t *testing.T) {
	// env var name should be: app name _ param name (see Kingpin doc for DefaultEnvars())
	os.Setenv("PBM_COORDINATOR_API_PORT", "12346")
	opts, err := processCliParams([]string{"--work-dir", os.TempDir(), "--api-port", "12345"})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		GrpcPort:             defaultGrpcPort,
		APIPort:              12345, // command line args overrides env var value
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              os.TempDir(),
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestCommandLineArgsPrecedenceOverConfig(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatalf("cannot create temp config file: %s", err)
	}

	defer os.Remove(tmpfile.Name())
	wantOpts := &cliOptions{
		GrpcPort:             98765,
		APIPort:              12346,
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              os.TempDir(),
	}
	b, _ := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	wantOpts.APIPort = 12345
	opts, err := processCliParams([]string{
		"--config-file", tmpfile.Name(),
		"--work-dir", os.TempDir(),
		"--api-port", "12345",
	})
	opts.configFile = "" // It is not exported so, it doesn't exists in the config file
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	opts.configFile = "" // it is not exported so it doesn't exist in the config file

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestAuth(t *testing.T) {
	stopChan := make(chan interface{})
	wg := &sync.WaitGroup{}

	apilis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", "127.0.0.1", defaultAPIPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	messagesServer := server.NewMessagesServer(os.TempDir())

	validToken := "test-token"
	invalidToken := validToken + "some-extra-string"

	apiGrpcOpts := []grpc.ServerOption{}
	apiGrpcOpts = append(apiGrpcOpts,
		grpc.StreamInterceptor(grpc_auth.StreamServerInterceptor(buildAuth(validToken))),
		grpc.UnaryInterceptor(grpc_auth.UnaryServerInterceptor(buildAuth(validToken))),
	)

	apiServer := api.NewServer(messagesServer)
	apiGrpcServer := grpc.NewServer(apiGrpcOpts...)
	apipb.RegisterApiServer(apiGrpcServer, apiServer)

	wg.Add(1)
	runAgentsGRPCServer(apiGrpcServer, apilis, 1, stopChan, wg)

	/*
		Test with an invalid token / client interceptor
	*/
	grpcOpts := []grpc.DialOption{
		grpc.WithUnaryInterceptor(makeUnaryInterceptor(invalidToken)),
		grpc.WithStreamInterceptor(makeStreamInterceptor(invalidToken)),
		grpc.WithInsecure(),
	}

	serverAddress := fmt.Sprintf("%s:%d", "127.0.0.1", defaultAPIPort)
	clientConn, err := grpc.Dial(serverAddress, grpcOpts...)
	if err != nil {
		t.Fatalf("Cannot connect to the gRPC server: %s", err)
	}

	ctx := context.Background()

	apiClient := pbapi.NewApiClient(clientConn)
	if _, err := apiClient.LastBackupMetadata(ctx, &pbapi.LastBackupMetadataParams{}); err == nil {
		t.Errorf("Auth should have failled for unary interceptor. Got ok instead")
	}

	stream, err := apiClient.GetClients(ctx, &pbapi.Empty{})
	if err != nil {
		t.Errorf("Stream connection shouldn't fail")
	}
	if _, err := stream.Recv(); err == nil {
		t.Errorf("Auth should have failled for stream interceptor(2). Got ok instead")
	} else if err == io.EOF {
		t.Errorf("Auth should have failled for stream interceptor(3). Got io.EOF instead")
	}

	clientConn.Close()

	/*
		Test with a valid token
	*/
	grpcOpts = []grpc.DialOption{
		grpc.WithUnaryInterceptor(makeUnaryInterceptor(validToken)),
		grpc.WithStreamInterceptor(makeStreamInterceptor(validToken)),
		grpc.WithInsecure(),
	}

	serverAddress = fmt.Sprintf("%s:%d", "127.0.0.1", defaultAPIPort)
	clientConn, err = grpc.Dial(serverAddress, grpcOpts...)
	if err != nil {
		t.Fatalf("Cannot connect to the gRPC server: %s", err)
	}

	ctx = context.Background()

	apiClient = pbapi.NewApiClient(clientConn)
	_, err = apiClient.LastBackupMetadata(ctx, &pbapi.LastBackupMetadataParams{})
	if err != nil {
		t.Errorf("Auth should be valid. Got error: %s", err)
	}

	stream, err = apiClient.GetClients(ctx, &pbapi.Empty{})
	if err != nil {
		t.Errorf("Stream connection shouldn't fail")
	}
	if _, err := stream.Recv(); err != nil && err != io.EOF {
		t.Errorf("Stream auth shouldn't fail with a valid token. Got: %s", err)
	}

	clientConn.Close()
	stopChan <- true
	wg.Wait()
}

func TestAuthStream(t *testing.T) {
	stopChan := make(chan interface{})
	wg := &sync.WaitGroup{}

	apilis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", "127.0.0.1", defaultAPIPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	messagesServer := server.NewMessagesServer(os.TempDir())

	invalidToken := "invalid-token"
	validToken := "valid-token"

	apiGrpcOpts := []grpc.ServerOption{}
	apiGrpcOpts = append(apiGrpcOpts,
		grpc.StreamInterceptor(grpc_auth.StreamServerInterceptor(buildAuth(validToken))),
	)

	apiServer := api.NewServer(messagesServer)
	apiGrpcServer := grpc.NewServer(apiGrpcOpts...)
	apipb.RegisterApiServer(apiGrpcServer, apiServer)

	wg.Add(1)
	runAgentsGRPCServer(apiGrpcServer, apilis, 1, stopChan, wg)

	/*
		Test with an invalid token / client interceptor
	*/
	grpcOpts := []grpc.DialOption{
		grpc.WithStreamInterceptor(makeStreamInterceptor(invalidToken)),
		grpc.WithInsecure(),
	}

	serverAddress := fmt.Sprintf("%s:%d", "127.0.0.1", defaultAPIPort)
	clientConn, err := grpc.Dial(serverAddress, grpcOpts...)
	if err != nil {
		t.Fatalf("Cannot connect to the gRPC server: %s", err)
	}

	ctx := context.Background()

	apiClient := pbapi.NewApiClient(clientConn)

	stream, err := apiClient.GetClients(ctx, &pbapi.Empty{})
	if err != nil {
		t.Errorf("Failed to get the data stream: %s", err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Errorf("Auth should have failled. Got message instead")
	}

	clientConn.Close()

	/*
		Test with an valid token
	*/
	grpcOpts = []grpc.DialOption{
		grpc.WithStreamInterceptor(makeStreamInterceptor(validToken)),
		grpc.WithInsecure(),
	}

	clientConn, err = grpc.Dial(serverAddress, grpcOpts...)
	if err != nil {
		t.Fatalf("Cannot connect to the gRPC server: %s", err)
	}

	apiClient = pbapi.NewApiClient(clientConn)

	stream, err = apiClient.GetClients(ctx, &pbapi.Empty{})
	if err != nil {
		t.Errorf("Failed to get the data stream: %s", err)
	}
	_, err = stream.Recv()
	if err != nil && err != io.EOF {
		t.Errorf("Auth have failled: %s", err)
	}

	clientConn.Close()

	stopChan <- true
	wg.Wait()
}

func makeUnaryInterceptor(token string) func(ctx context.Context, method string, req interface{}, reply interface{},
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return func(ctx context.Context, method string, req interface{}, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		md := metadata.Pairs("authorization", "bearer "+token)
		ctx = metadata.NewOutgoingContext(ctx, md)
		err := invoker(ctx, method, req, reply, cc, opts...)
		return err
	}
}

func makeStreamInterceptor(token string) func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		md := metadata.Pairs("authorization", "bearer "+token)
		ctx = metadata.NewOutgoingContext(ctx, md)
		return streamer(ctx, desc, cc, method, opts...)
	}
}
