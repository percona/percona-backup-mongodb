package test_test

import (
	"context"
	"io"

	pbapi "github.com/percona/percona-backup-mongodb/proto/api"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"google.golang.org/grpc/metadata"
)

// Mock stream interface to avoid connecting to the API server via TCP
// We just need to test the methods but the server is running locally
type mockBackupsMetadataStream struct {
	files map[string]*pb.BackupMetadata
}

func newMockBackupsMetadataStream() *mockBackupsMetadataStream {
	return &mockBackupsMetadataStream{
		files: make(map[string]*pb.BackupMetadata),
	}
}

func (m *mockBackupsMetadataStream) SendMsg(imsg interface{}) error {
	msg := imsg.(*pbapi.MetadataFile)
	m.files[msg.Filename] = msg.Metadata
	return nil
}

func (m *mockBackupsMetadataStream) Send(msg *pbapi.MetadataFile) error {
	m.files[msg.Filename] = msg.Metadata
	return nil
}

func (m *mockBackupsMetadataStream) Context() context.Context       { return context.TODO() }
func (m *mockBackupsMetadataStream) RecvMsg(im interface{}) error   { return nil }
func (m *mockBackupsMetadataStream) SetHeader(h metadata.MD) error  { return nil }
func (m *mockBackupsMetadataStream) SendHeader(h metadata.MD) error { return nil }
func (m *mockBackupsMetadataStream) SetTrailer(h metadata.MD)       {}

// Mock stream interface to avoid connecting to the API server via TCP
// We just need to test the methods but the server is running locally
type mockBackupStream struct {
	errs chan string
}

func newMockBackupStream() *mockBackupStream {
	return &mockBackupStream{
		errs: make(chan string, 100),
	}
}

func (m *mockBackupStream) SendMsg(msg interface{}) error {
	m.errs <- msg.(*pbapi.LastBackupError).GetError()
	return nil
}

func (m *mockBackupStream) Send(msg *pbapi.LastBackupError) error {
	return nil
}

func (m *mockBackupStream) Context() context.Context { return context.TODO() }
func (m *mockBackupStream) RecvMsg(im interface{}) error {
	select {
	case im = <-m.errs:
		return nil
	default:
		return io.EOF
	}
}
func (m *mockBackupStream) SetHeader(h metadata.MD) error  { return nil }
func (m *mockBackupStream) SendHeader(h metadata.MD) error { return nil }
func (m *mockBackupStream) SetTrailer(h metadata.MD)       {}
