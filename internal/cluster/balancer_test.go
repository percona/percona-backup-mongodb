package cluster

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
)

// Make sure the balancer is started before the tests, then run tests
func TestMain(m *testing.M) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		fmt.Printf("Could not connect to mongos: %v", err.Error())
		os.Exit(1)
	}

	err = StartBalancer(session)
	if err != nil {
		fmt.Printf("Failed to run .StartBalancer(): %v", err.Error())
		session.Close()
		os.Exit(1)
	}
	session.Close()

	os.Exit(m.Run())
}

func TestGetBalancerStatus(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	status, err := GetBalancerStatus(session)
	if err != nil {
		t.Fatalf("Failed to run .GetBalancerStatus(): %v", err.Error())
	}
	if status == nil || status.Ok != 1 {
		t.Fatal("Got unexpected result from .GetBalancerStatus()")
	}
}

func TestIsBalancerEnabled(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	status, err := GetBalancerStatus(session)
	if err != nil {
		t.Fatalf("Failed to run .GetBalancerStatus(): %v", err.Error())
	}

	if !IsBalancerEnabled(status) {
		t.Fatal(".IsBalancerEnabled() should return true")
	}
}

func TestStopBalancer(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	err = StopBalancer(session)
	if err != nil {
		t.Fatalf("Failed to run .StopBalancer(): %v", err.Error())
	}

	status, err := GetBalancerStatus(session)
	if err != nil {
		t.Fatalf("Failed to run .GetBalancerStatus(): %v", err.Error())
	}

	if IsBalancerEnabled(status) {
		t.Fatal(".IsBalancerEnabled() should return false after .StopBalancer()")
	}

	// sometimes the balancer doesn't stop right away
	tries := 1
	maxTries := 60
	for tries < maxTries {
		status, err := GetBalancerStatus(session)
		if err != nil {
			t.Fatalf("Failed to run .GetBalancerStatus(): %v", err.Error())
		}
		if !IsBalancerRunning(status) {
			break
		}
		time.Sleep(time.Second)
		tries++
	}
	if tries >= maxTries {
		t.Fatal("The balancer did not stop running")
	}
}

func TestStopBalancerAndWait(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	err = StartBalancer(session)
	if err != nil {
		t.Fatalf("Failed to run .StartBalancer(): %v", err.Error())
	}

	err = StopBalancerAndWait(session, 10, time.Second)
	if err != nil {
		t.Fatalf("Failed to run .StopBalancerAndWait(): %v", err.Error())
	}

	status, err := GetBalancerStatus(session)
	if err != nil {
		t.Fatalf("Failed to run .GetBalancerStatus(): %v", err.Error())
	}

	if IsBalancerRunning(status) || IsBalancerEnabled(status) {
		t.Fatal("The balancer did not stop running")
	}
}

func TestStartBalancer(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	err = StartBalancer(session)
	if err != nil {
		t.Fatalf("Failed to run .StartBalancer(): %v", err.Error())
	}

	status, err := GetBalancerStatus(session)
	if err != nil {
		t.Fatalf("Failed to run .GetBalancerStatus(): %v", err.Error())
	}

	if !IsBalancerEnabled(status) {
		t.Fatal(".IsBalancerEnabled() should return true after .StartBalancer()")
	}
}
