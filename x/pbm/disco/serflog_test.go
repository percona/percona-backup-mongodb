package disco

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestParseSerfLine(t *testing.T) {
	tests := []struct {
		line    string
		wantLvl zapcore.Level
		wantMsg string
	}{
		{"[TRACE] serf: rejected coordinate\n", zapcore.DebugLevel, "serf: rejected coordinate"},
		{"[DEBUG] memberlist: stream connection\n", zapcore.DebugLevel, "memberlist: stream connection"},
		{"[INFO] serf: EventMemberJoin: a1 10.0.0.1\n", zapcore.InfoLevel, "serf: EventMemberJoin: a1 10.0.0.1"},
		{"[WARN] memberlist: refuting a suspect message\n", zapcore.WarnLevel, "memberlist: refuting a suspect message"},
		{"[ERR] memberlist: failed to send ping\n", zapcore.ErrorLevel, "memberlist: failed to send ping"},
		{"[ERROR] serf: failed to rejoin\n", zapcore.ErrorLevel, "serf: failed to rejoin"},
		{"no tag here\n", zapcore.InfoLevel, "no tag here"},
		{"[NOPE] unknown tag\n", zapcore.InfoLevel, "[NOPE] unknown tag"},
		{"single\n", zapcore.InfoLevel, "single"},
	}

	for _, tt := range tests {
		lvl, msg := parseSerfLine(tt.line)
		if lvl != tt.wantLvl || msg != tt.wantMsg {
			t.Errorf("parseSerfLine(%q) = (%s, %q), want (%s, %q)",
				tt.line, lvl, msg, tt.wantLvl, tt.wantMsg)
		}
	}
}

func TestSerfStdLogLevelFilter(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	stdLog := newSerfStdLog(zap.New(core))

	stdLog.Printf("[DEBUG] memberlist: dropped")
	stdLog.Printf("[INFO] serf: dropped")
	stdLog.Printf("[WARN] memberlist: kept %d", 1)
	stdLog.Printf("[ERR] serf: kept %d", 2)

	got := logs.AllUntimed()
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(got), got)
	}
	if got[0].Level != zapcore.WarnLevel || got[0].Message != "memberlist: kept 1" {
		t.Errorf("entry 0 = %s %q, want warn %q", got[0].Level, got[0].Message, "memberlist: kept 1")
	}
	if got[1].Level != zapcore.ErrorLevel || got[1].Message != "serf: kept 2" {
		t.Errorf("entry 1 = %s %q, want error %q", got[1].Level, got[1].Message, "serf: kept 2")
	}
}
