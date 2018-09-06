package db

import (
	"crypto/tls"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
)

const testSSLDir = "../../docker/test/ssl"

var (
	testSSLPEMKey = filepath.Join(testSSLDir, "mongodb.pem")
	testSSLCACert = filepath.Join(testSSLDir, "rootCA.crt")
)

func TestNewDialInfo(t *testing.T) {
	// missing certificate files
	_, err := NewDialInfo(&Config{
		CertFile: "/does/not/exist",
		CAFile:   testSSLCACert,
	})
	if err == nil {
		t.Fatal("Expected an error for .NewDialInfo() on missing PEM key file")
	}
	_, err = NewDialInfo(&Config{
		CertFile: testSSLPEMKey,
		CAFile:   "/does/not/exist",
	})
	if err == nil {
		t.Fatal("Expected an error for .NewDialInfo() on missing CA certificate file")
	}

	// malformed certificates
	tmpFile, _ := ioutil.TempFile("", t.Name())
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())
	tmpFile.Write([]byte("this is not an x509 certificate"))
	_, err = NewDialInfo(&Config{
		CertFile: tmpFile.Name(),
		CAFile:   testSSLCACert,
	})
	if err == nil {
		t.Fatal("Expected an error from .NewDialInfo() on malformed PEM certificate file")
	}
	_, err = NewDialInfo(&Config{
		CertFile: testSSLPEMKey,
		CAFile:   tmpFile.Name(),
	})
	if err == nil {
		t.Fatal("Expected an error from .NewDialInfo() on malformed CA certificate file")
	}

	// test insecure mode
	di, err := NewDialInfo(&Config{
		CertFile: testSSLPEMKey,
		CAFile:   testSSLCACert,
		Insecure: true,
	})
	if err != nil {
		t.Fatalf("Failed to run .NewDialInfo(): %v", err.Error())
	}

	// test secure mode
	di, err = NewDialInfo(&Config{
		Host:     testutils.MongoDBHost + ":" + testutils.MongoDBPrimaryPort,
		Username: testutils.MongoDBUser,
		Password: testutils.MongoDBPassword,
		CertFile: testSSLPEMKey,
		CAFile:   testSSLCACert,
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to run .NewDialInfo(): %v", err.Error())
	}

	session, err := mgo.DialWithInfo(di)
	if err != nil {
		t.Fatalf("Failed to connect with dial info: %v", err.Error())
	}
	defer session.Close()
	if session.Ping() != nil {
		t.Fatalf("Failed to ping the connection: %v", err.Error())
	}
}

func TestValidateConnection(t *testing.T) {
	roots, err := loadCaCertificate(testSSLCACert)
	if err != nil {
		t.Fatalf("Could not load test root CA cert: %v", err.Error())
	}

	certificates, err := tls.LoadX509KeyPair(testSSLPEMKey, testSSLPEMKey)
	if err != nil {
		t.Fatalf("Cannot load key pair from '%s': %v", testSSLPEMKey, err)
	}

	host := testutils.MongoDBHost + ":" + testutils.MongoDBPrimaryPort
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{certificates},
		RootCAs:      roots,
	}
	conn, err := tls.Dial("tcp", host, tlsConfig)
	if err != nil {
		t.Fatalf("Failed to connect to '%s': %v", host, err.Error())
	}
	defer conn.Close()

	err = validateConnection(conn, tlsConfig, testutils.MongoDBHost)
	if err != nil {
		t.Fatalf("Failed to run .validateConnection(): %v", err.Error())
	}

	err = validateConnection(conn, tlsConfig, "this.should.fail")
	if err == nil || !(strings.HasPrefix(err.Error(), "x509: certificate is valid for ") && strings.HasSuffix(err.Error(), " not this.should.fail")) {
		t.Fatalf("Expected an error from .validateConnection(): %v", err.Error())
	}
}
