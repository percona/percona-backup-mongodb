package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProfileFlag(t *testing.T) {
	t.Run("Main constructor is correct", func(t *testing.T) {
		pf := NewProfileFlagMain()
		assertMain(t, &pf)
	})

	t.Run("Value constructor with main", func(t *testing.T) {
		pf := NewProfileFlag("main")
		assertMain(t, &pf)
	})

	t.Run("Value constructor with name", func(t *testing.T) {
		pf := NewProfileFlag("test")
		assertName(t, "test", &pf)
	})

	t.Run("Value constructor with empty", func(t *testing.T) {
		pf := NewProfileFlag("")
		assertWildcard(t, &pf)
	})

	t.Run("Zero value", func(t *testing.T) {
		pf := ProfileFlag{}
		assertWildcard(t, &pf)
	})

	t.Run("Set with main", func(t *testing.T) {
		pf := ProfileFlag{}
		err := pf.Set("main")
		assert.NoError(t, err)
		assert.True(t, pf.IsSet())
		assertMain(t, &pf)
	})

	t.Run("Set with name", func(t *testing.T) {
		pf := ProfileFlag{}
		err := pf.Set("test")
		assert.NoError(t, err)
		assert.True(t, pf.IsSet())
		assertName(t, "test", &pf)
	})

	t.Run("Set with empty", func(t *testing.T) {
		pf := ProfileFlag{}
		err := pf.Set("")
		assert.Error(t, err)
	})
}

func assertMain(t *testing.T, pf *ProfileFlag) {
	assert.Equal(t, "main", pf.Name())
	assert.Equal(t, "", pf.Value())
	assert.Equal(t, pf.Name(), pf.String())
	assert.True(t, pf.IsMain())
}

func assertName(t *testing.T, name string, pf *ProfileFlag) {
	assert.True(t, pf.IsSet())
	assert.Equal(t, name, pf.Value())
	assert.Equal(t, pf.Name(), pf.String())
	assert.False(t, pf.IsMain())
}

func assertWildcard(t *testing.T, pf *ProfileFlag) {
	assert.False(t, pf.IsSet())
	assert.False(t, pf.IsMain())
	// Wildcard should immediately pass
	assert.NoError(t, pf.Validate(t.Context(), nil))
}
