package main

import (
	"encoding/json"
	"testing"

	"github.com/percona/percona-backup-mongodb/pbm/lifecycle"
	"github.com/stretchr/testify/assert"
)

func TestValidateCleanupMode(t *testing.T) {
	tests := []struct {
		name       string
		olderThan  string
		lifecycle  bool
		wantErrMsg string
	}{
		{
			name:       "mode missing",
			wantErrMsg: "either --older-than or --lifecycle should be set",
		},
		{
			name:      "older than",
			olderThan: "30d",
		},
		{
			name:      "lifecycle",
			lifecycle: true,
		},
		{
			name:       "modes conflict",
			olderThan:  "30d",
			lifecycle:  true,
			wantErrMsg: "cannot use --older-than and --lifecycle at the same command",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCleanupMode(tt.olderThan, tt.lifecycle)
			if tt.wantErrMsg == "" {
				assert.NoError(t, err)
				return
			}

			assert.EqualError(t, err, tt.wantErrMsg)
		})
	}
}

func TestLifecycleResultUsesReportAbortState(t *testing.T) {
	report := &lifecycle.Report{
		Aborted:     true,
		AbortReason: "below minKeep",
	}
	result := lifecycleResult{Report: report, Msg: "Lifecycle cleanup aborted."}

	assert.Same(t, report, result.Report)
	assert.Equal(t, "Lifecycle cleanup aborted.", result.String())
	assert.Equal(t, report.String(), (lifecycleResult{Report: report}).String())
	assert.Empty(t, (lifecycleResult{}).String())

	b, err := json.Marshal(result)
	assert.NoError(t, err)
	var got struct {
		Report struct {
			Aborted     bool   `json:"aborted"`
			AbortReason string `json:"abortReason"`
		} `json:"report"`
		Msg string `json:"msg"`
	}
	assert.NoError(t, json.Unmarshal(b, &got))
	assert.True(t, got.Report.Aborted)
	assert.Equal(t, "below minKeep", got.Report.AbortReason)
	assert.Equal(t, "Lifecycle cleanup aborted.", got.Msg)

	var raw map[string]json.RawMessage
	assert.NoError(t, json.Unmarshal(b, &raw))
	assert.NotContains(t, raw, "aborted", "abort state should have one canonical location")
}

func TestLifecycleCommandSurface(t *testing.T) {
	app := newPbmApp()
	for _, cmd := range app.rootCmd.Commands() {
		assert.NotEqual(t, "lifecycle", cmd.Name(), "top-level lifecycle command must not be registered")
	}

	cleanup, _, err := app.rootCmd.Find([]string{"cleanup"})
	assert.NoError(t, err)
	if assert.NotNil(t, cleanup) {
		assert.NotNil(t, cleanup.Flags().Lookup("lifecycle"))
	}
}
