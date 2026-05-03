package defs

import "testing"

func TestIndexCommitQuorumCommandValue(t *testing.T) {
	tests := []struct {
		name string
		q    IndexCommitQuorum
		want any
	}{
		{name: "majority", q: IndexCommitQuorumMajority, want: string(IndexCommitQuorumMajority)},
		{name: "votingMembers", q: IndexCommitQuorumVotingMembers, want: string(IndexCommitQuorumVotingMembers)},
		{name: "numeric", q: "3", want: int32(3)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.q.CommandValue(); got != tt.want {
				t.Fatalf("CommandValue() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestValidateIndexCommitQuorum(t *testing.T) {
	tests := []struct {
		name    string
		value   IndexCommitQuorum
		wantErr bool
	}{
		{name: "empty", value: ""},
		{name: "majority", value: IndexCommitQuorumMajority},
		{name: "votingMembers", value: IndexCommitQuorumVotingMembers},
		{name: "positive integer", value: "3"},
		{name: "zero", value: "0", wantErr: true},
		{name: "negative integer", value: "-1", wantErr: true},
		{name: "unknown string", value: "whatever", wantErr: true},
		{name: "leading whitespace", value: " majority", wantErr: true},
		{name: "trailing whitespace", value: "majority ", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIndexCommitQuorum(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
