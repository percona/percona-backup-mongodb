package agent

import "testing"

func TestAlwaysFail(t *testing.T) {
	t.Error("always fail")
}
