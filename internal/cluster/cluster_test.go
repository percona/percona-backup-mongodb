package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var testCluster *Cluster

func TestNew(t *testing.T) {
	testCluster = New(testSession.Clone())
	assert.NotNil(t, testCluster)
}

func TestGetIsMaster(t *testing.T) {
	isMaster, err := testCluster.getIsMaster()
	assert.NoError(t, err)
	assert.NotNil(t, isMaster)
	assert.Equal(t, 1, isMaster.Ok)
}
