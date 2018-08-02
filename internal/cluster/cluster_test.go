package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetIsMaster(t *testing.T) {
	isMaster, err := GetIsMaster(testSession.Clone())
	assert.NoError(t, err)
	assert.NotNil(t, isMaster)
	assert.Equal(t, 1, isMaster.Ok)
}
