package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProfileFlag(t *testing.T) {
	t.Run("Default constructor is correct", func(t *testing.T) {
		pf := NewProfileFlagDefault()
		assert.False(t, pf.IsSet())
		assertDefault(t, &pf)
	})

	t.Run("Wildcard constructor is correct", func(t *testing.T) {
		pf := NewProfileFlagWildcard()
		assert.False(t, pf.IsSet())
		assertWildcard(t, &pf)
	})

	t.Run("value constructor with default", func(t *testing.T) {
		pf := NewProfileFlag("default")
		assert.False(t, pf.IsSet())
		assertDefault(t, &pf)
	})

	t.Run("value constructor with empty", func(t *testing.T) {
		pf := NewProfileFlag("")
		assert.False(t, pf.IsSet())
		assertDefault(t, &pf)
	})

	t.Run("value constructor with wildcard", func(t *testing.T) {
		pf := NewProfileFlag("*")
		assert.False(t, pf.IsSet())
		assertWildcard(t, &pf)
	})

	t.Run("value constructor with name", func(t *testing.T) {
		pf := NewProfileFlag("test")
		assert.False(t, pf.IsSet())
		assertName(t, "test", &pf)
	})

	t.Run("zero value", func(t *testing.T) {
		pf := ProfileFlag{}
		assert.False(t, pf.IsSet())
		assertDefault(t, &pf)
	})

	t.Run("Set with empty", func(t *testing.T) {
		pf := ProfileFlag{}
		err := pf.Set("")
		assert.NoError(t, err)
		assert.True(t, pf.IsSet())
		assertDefault(t, &pf)
	})

	t.Run("Set with default", func(t *testing.T) {
		pf := ProfileFlag{}
		err := pf.Set("default")
		assert.NoError(t, err)
		assert.True(t, pf.IsSet())
		assertDefault(t, &pf)
	})

	t.Run("Set with wildcard", func(t *testing.T) {
		pf := ProfileFlag{}
		err := pf.Set("*")
		assert.NoError(t, err)
		assert.True(t, pf.IsSet())
		assertWildcard(t, &pf)
	})

	t.Run("Set with name", func(t *testing.T) {
		pf := ProfileFlag{}
		err := pf.Set("test")
		assert.NoError(t, err)
		assert.True(t, pf.IsSet())
		assertName(t, "test", &pf)
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

func assertName(t *testing.T, name string, pf *ProfileFlag) {
	assert.Equal(t, name, pf.DisplayName())
	assert.Equal(t, name, pf.Value())
	assert.Equal(t, pf.Value(), pf.String())
	assert.False(t, pf.IsDefault())
	assert.False(t, pf.IsDefaultOrWildcard())
	assert.False(t, pf.IsWildcard())
}
