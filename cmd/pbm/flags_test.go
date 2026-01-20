package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProfileFlag(t *testing.T) {
	t.Run("Default constructor is correct", func(t *testing.T) {
		pf := NewProfileFlagDefault()
		assertDefault(t, &pf)
	})

	t.Run("Wildcard constructor is correct", func(t *testing.T) {
		pf := NewProfileFlagWildcard()
		assertWildcard(t, &pf)
	})

	t.Run("new default", func(t *testing.T) {
		pf := NewProfileFlag("default")
		assertDefault(t, &pf)
	})

	t.Run("new empty string", func(t *testing.T) {
		pf := NewProfileFlag("")
		assertDefault(t, &pf)
	})

	t.Run("new wildcard", func(t *testing.T) {
		pf := NewProfileFlag("*")
		assertWildcard(t, &pf)
	})

	t.Run("new name", func(t *testing.T) {
		pf := NewProfileFlag("test")
		assert.Equal(t, "test", pf.DisplayName())
		assert.Equal(t, "test", pf.Value())
		assert.Equal(t, pf.Value(), pf.String())
		assert.False(t, pf.IsDefault())
		assert.False(t, pf.IsDefaultOrWildcard())
		assert.False(t, pf.IsWildcard())
	})
}

func assertDefault(t *testing.T, pf *ProfileFlag) {
	assert.Equal(t, "default", pf.DisplayName())
	assert.Equal(t, "", pf.Value())
	assert.Equal(t, pf.Value(), pf.String())
	assert.True(t, pf.IsDefault())
	assert.True(t, pf.IsDefaultOrWildcard())
	assert.False(t, pf.IsWildcard())
}

func assertWildcard(t *testing.T, pf *ProfileFlag) {
	assert.Equal(t, "*", pf.DisplayName())
	assert.Equal(t, "*", pf.Value())
	assert.Equal(t, pf.Value(), pf.String())
	assert.False(t, pf.IsDefault())
	assert.True(t, pf.IsDefaultOrWildcard())
	assert.True(t, pf.IsWildcard())
	// Wildcard should immediately pass
	assert.NoError(t, pf.Validate(t.Context(), nil))
}
