package main

import (
	"reflect"
	"sync"
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/grpc/client"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"google.golang.org/grpc"
)

func Test_main(t *testing.T) {
	tests := []struct {
		name string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			main()
		})
	}
}

func Test_run(t *testing.T) {
	type args struct {
		conn       *grpc.ClientConn
		mdbSession *mgo.Session
		clientID   string
		nodeType   pb.NodeType
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run(tt.args.conn, tt.args.mdbSession, tt.args.clientID, tt.args.nodeType)
		})
	}
}

func Test_processMessages(t *testing.T) {
	type args struct {
		rpcClient *client.Client
		wg        *sync.WaitGroup
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processMessages(tt.args.rpcClient, tt.args.wg)
		})
	}
}

func Test_processCliArgs(t *testing.T) {
	tests := []struct {
		name    string
		want    *cliOptios
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := processCliArgs()
			if (err != nil) != tt.wantErr {
				t.Errorf("processCliArgs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("processCliArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getgRPCOptions(t *testing.T) {
	type args struct {
		opts *cliOptios
	}
	tests := []struct {
		name string
		args args
		want []grpc.DialOption
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getgRPCOptions(tt.args.opts); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getgRPCOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getNodeType(t *testing.T) {
	type args struct {
		session *mgo.Session
	}
	tests := []struct {
		name    string
		args    args
		want    pb.NodeType
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getNodeType(tt.args.session)
			if (err != nil) != tt.wantErr {
				t.Errorf("getNodeType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getNodeType() = %v, want %v", got, tt.want)
			}
		})
	}
}
