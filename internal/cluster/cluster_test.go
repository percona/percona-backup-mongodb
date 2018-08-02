package cluster

import (
	"testing"

	"github.com/percona/mongodb-backup/mdbstructs"
	"github.com/stretchr/testify/assert"
)

func TestGetIsMaster(t *testing.T) {
	isMaster, err := GetIsMaster(testSession.Clone())
	assert.NoError(t, err)
	assert.NotNil(t, isMaster)
	assert.Equal(t, 1, isMaster.Ok)
}

func TestIsReplset(t *testing.T) {
	assert.True(t, isReplset(&mdbstructs.IsMaster{SetName: "test"}))
	assert.False(t, isReplset(&mdbstructs.IsMaster{SetName: ""}))
}

func TestIsMongos(t *testing.T) {
	assert.True(t, isMongos(&mdbstructs.IsMaster{
		IsMaster: true,
		Msg:      "isdbgrid",
	}))
	assert.False(t, isMongos(&mdbstructs.IsMaster{IsMaster: true}))
}

func TestIsConfigServer(t *testing.T) {
	assert.True(t, isConfigServer(&mdbstructs.IsMaster{
		ConfigSvr: 2,
		SetName:   "csReplSet",
	}))
	assert.False(t, isConfigServer(&mdbstructs.IsMaster{SetName: "csReplSet"}))
}

func TestIsShardedCluster(t *testing.T) {
	assert.True(t, isShardedCluster(&mdbstructs.IsMaster{
		ConfigSvr: 2,
		SetName:   "csReplSet",
	}))
	assert.True(t, isShardedCluster(&mdbstructs.IsMaster{
		IsMaster: true,
		Msg:      "isdbgrid",
	}))
	assert.False(t, isShardedCluster(&mdbstructs.IsMaster{
		IsMaster: true,
		SetName:  "test",
	}))
}
