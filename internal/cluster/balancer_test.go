package cluster

import (
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
)

func TestNewBalancer(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo(t))
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	b, err := NewBalancer(session)
	if err != nil {
		t.Fatalf("Could not run .NewBalancer(): %v", err.Error())
	} else if b.session == nil {
		t.Fatal("Got unexpected output from .NewBalancer()")
	}
}

func TestBalancerGetStatus(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo(t))
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	b, err := NewBalancer(session)
	if err != nil {
		t.Fatalf("Could not run .NewBalancer(): %v", err.Error())
	}

	status, err := b.getStatus()
	if err != nil {
		t.Fatalf("Failed to run .getStatus(): %v", err.Error())
	}
	if status == nil || status.Ok != 1 {
		t.Fatal("Got unexpected result from .getStatus()")
	}
}

func TestBalancerIsEnabled(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo(t))
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	b, err := NewBalancer(session)
	if err != nil {
		t.Fatalf("Could not run .NewBalancer(): %v", err.Error())
	}

	isEnabled, err := b.IsEnabled()
	if err != nil {
		t.Fatalf("Failed to run .IsEnabled(): %v", err.Error())
	} else if !isEnabled {
		t.Fatal(".IsEnabled() should return true")
	}
}

func TestBalancerStop(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo(t))
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	b, err := NewBalancer(session)
	if err != nil {
		t.Fatalf("Could not run .NewBalancer(): %v", err.Error())
	}

	err = b.Stop()
	if err != nil {
		t.Fatalf("Failed to run .Stop(): %v", err.Error())
	}

	isEnabled, err := b.IsEnabled()
	if err != nil {
		t.Fatalf(".IsEnabled() returned an error: %v", err.Error())
	} else if isEnabled {
		t.Fatal(".IsEnabled() should return false after .Stop()")
	}

	// sometimes the balancer doesn't stop right away
	tries := 1
	maxTries := 60
	for tries < maxTries {
		isRunning, err := b.IsRunning()
		if err != nil {
			t.Fatalf("Failed to run .IsRunning(): %v", err.Error())
		}
		if !isRunning {
			break
		}
		time.Sleep(time.Second)
		tries++
	}
	if tries >= maxTries {
		t.Fatal("The balancer did not stop running")
	}
}

func TestBalancerStopAndWait(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo(t))
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	b, err := NewBalancer(session)
	if err != nil {
		t.Fatalf("Could not run .NewBalancer(): %v", err.Error())
	}

	err = b.Start()
	if err != nil {
		t.Fatalf("Failed to run .Start(): %v", err.Error())
	}

	err = b.StopAndWait(10, time.Second)
	if err != nil {
		t.Fatalf("Failed to run .StopAndWait(): %v", err.Error())
	}

	isEnabled, err := b.IsEnabled()
	if err != nil {
		t.Fatalf("Failed to run .IsEnabled(): %v", err.Error())
	}

	isRunning, err := b.IsRunning()
	if err != nil {
		t.Fatalf("Failed to run .IsRunning(): %v", err.Error())
	}

	if isRunning || isEnabled {
		t.Fatal("The balancer did not stop running")
	}
}

func TestBalancerStart(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo(t))
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	b, err := NewBalancer(session)
	if err != nil {
		t.Fatalf("Could not run .NewBalancer(): %v", err.Error())
	}

	err = b.Start()
	if err != nil {
		t.Fatalf("Failed to run .Start(): %v", err.Error())
	}

	isEnabled, err := b.IsEnabled()
	if err != nil {
		t.Fatalf("Failed to run .IsEnabled(): %v", err.Error())
	} else if !isEnabled {
		t.Fatal(".IsEnabled() should return true after .Start()")
	}
}

func TestBalancerRestoreState(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo(t))
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	// start the balancer before the test
	b, err := NewBalancer(session)
	if err != nil {
		t.Fatalf("Could not run .NewBalancer(): %v", err.Error())
	}
	err = b.Start()
	if err != nil {
		t.Fatalf("Failed to run .Start(): %v", err.Error())
	}

	// create a new Balancer struct to test .RestoreState() after .Stop()
	b2, err := NewBalancer(session)
	if !b2.wasEnabled {
		t.Fatal("Balancer .wasEnabled bool should be true")
	}
	err = b2.Stop()
	if err != nil {
		t.Fatalf("Failed to run .Stop(): %v", err.Error())
	}
	err = b2.RestoreState()
	if err != nil {
		t.Fatalf("Failed to run .RestoreState(): %v", err.Error())
	}

	isEnabled, err := b.IsEnabled()
	if err != nil {
		t.Fatalf("Failed to run .IsEnabled(): %v", err.Error())
	} else if !isEnabled {
		t.Fatal(".IsEnabled() should return true after .Start()")
	}
}
