package config

import (
	"strconv"
	"strings"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

type IndexCommitQuorum string

const (
	IndexCommitQuorumMajority      IndexCommitQuorum = "majority"
	IndexCommitQuorumVotingMembers IndexCommitQuorum = "votingMembers"
)

const (
	DefaultRestoreIndexCommitQuorum = IndexCommitQuorumVotingMembers
	MaxIndexCommitQuorum            = 50
)

// CommandValue returns the value shape expected by MongoDB's createIndexes
// commitQuorum field. MongoDB accepts symbolic string values or an integer
// node count. PBM stores config and CLI values as text, so numeric text is
// converted to int32 at the command boundary.
func (q IndexCommitQuorum) CommandValue() any {
	if n, err := strconv.ParseInt(string(q), 10, 32); err == nil {
		return int32(n)
	}

	return string(q)
}

func ValidateIndexCommitQuorum(q IndexCommitQuorum) error {
	val := string(q)
	if strings.TrimSpace(val) != val {
		return errors.New("restore.indexCommitQuorum must not contain leading or trailing whitespace")
	}

	switch q {
	case "", IndexCommitQuorumMajority, IndexCommitQuorumVotingMembers:
		return nil
	}

	n, err := strconv.ParseInt(val, 10, 32)
	if err != nil || n <= 0 || n > MaxIndexCommitQuorum {
		return errors.Errorf("invalid restore.indexCommitQuorum %q", val)
	}

	return nil
}
