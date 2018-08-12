package cluster

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
	"github.com/percona/mongodb-backup/mdbstructs"
)

func TestHasReplsetMemberTags(t *testing.T) {
	memberConfig := mdbstructs.ReplsetConfigMember{
		Tags: map[string]string{"role": "backup"},
	}
	if !HasReplsetMemberTags(&memberConfig, map[string]string{"role": "backup"}) {
		t.Fatal(".HasReplsetMemberTags should have returned true")
	}
	if HasReplsetMemberTags(&memberConfig, map[string]string{"role": "not-backup"}) {
		t.Fatal(".HasReplsetMemberTags should have returned false")
	}
	if HasReplsetMemberTags(&memberConfig, map[string]string{
		"role": "backup",
		"does": "not-exist",
	}) {
		t.Fatal(".HasReplsetMemberTags should have returned false")
	}

	memberConfig = mdbstructs.ReplsetConfigMember{
		Tags: map[string]string{
			"role":    "backup",
			"another": "tag",
		},
	}
	if !HasReplsetMemberTags(&memberConfig, map[string]string{"another": "tag"}) {
		t.Fatal(".HasReplsetMemberTags should have returned true")
	}

	// test for the { role: "backup" } tag on the 2nd secondary
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to replset: %v", err.Error())
	}
	defer session.Close()

	config, err := GetConfig(session)
	if err != nil {
		t.Fatalf("Failed to run .GetConfig() on Replset struct: %v", err.Error())
	}

	for _, member := range config.Members {
		if member.Host == testSecondary2Host {
			if !HasReplsetMemberTags(member, map[string]string{"role": "backup"}) {
				t.Fatalf(".HasReplsetMemberTags() should have returned true for %v", testSecondary2Host)
			}
		}
	}
}

func TestGetConfig(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to replset: %v", err.Error())
	}
	defer session.Close()

	config, err := GetConfig(session)
	if err != nil {
		t.Fatalf("Failed to run .GetConfig() on Replset struct: %v", err.Error())
	} else if config.Name != testutils.MongoDBReplsetName {
		t.Fatalf("Got unexpected output from .GetConfig(), expected: %v, got %v", testutils.MongoDBReplsetName, config.Name)
	} else if len(config.Members) != 3 {
		t.Fatal("Unexpected number of replica set members in .GetConfig() result")
	}
}

func TestGetStatus(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to replset: %v", err.Error())
	}
	defer session.Close()

	status, err := GetStatus(session)
	if err != nil {
		t.Fatalf("Failed to run .GetStatus() on Replset struct: %v", err.Error())
	} else if status.Set != testutils.MongoDBReplsetName {
		t.Fatal("Got unexpected output from .GetStatus()")
	} else if len(status.Members) != 3 {
		t.Fatal("Unexpected number of replica set members in .GetStatus() result")
	}
}

func TestGetReplsetID(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to replset: %v", err.Error())
	}
	defer session.Close()

	config, err := GetConfig(session)
	if err != nil {
		t.Fatalf("Could not get config from replset: %v", err.Error())
	}

	if GetReplsetID(config) == nil {
		t.Fatal(".GetReplsetID() returned nil")
	}
}

func TestGetBackupSource(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to replset: %v", err.Error())
	}
	defer session.Close()

	config, err := GetConfig(session)
	if err != nil {
		t.Fatalf("Could not get config from replset: %v", err.Error())
	}

	status, err := GetStatus(session)
	if err != nil {
		t.Fatalf("Could not get status from replset: %v", err.Error())
	}

	source, err := GetBackupSource(config, status)
	if err != nil {
		t.Fatalf("Failed to run .GetBackupSource(): %v", err.Error())
	}
	if source.Host != testSecondary2Host {
		t.Fatal("Got unexpected output from .GetBackupSource()")
	}
}
