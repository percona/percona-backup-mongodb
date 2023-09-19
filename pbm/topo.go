package pbm

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func CheckTopoForBackup(cn *PBM, type_ BackupType) error {
	members, err := cn.ClusterMembers()
	if err != nil {
		return errors.WithMessage(err, "get cluster members")
	}

	ts, err := cn.ClusterTime()
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	agentList, err := cn.ListAgents()
	if err != nil {
		return errors.WithMessage(err, "list agents")
	}

	agents := make(map[string]map[string]AgentStat)
	for _, a := range agentList {
		if agents[a.RS] == nil {
			agents[a.RS] = make(map[string]AgentStat)
		}
		agents[a.RS][a.Node] = a
	}

	return collectTopoCheckErrors(members, agents, ts, type_)
}

type (
	ReplsetName = string
	NodeURI     = string
)

type topoCheckError struct {
	Replsets map[ReplsetName]map[NodeURI][]error
	Missed   []string
}

func (r topoCheckError) hasError() bool {
	return len(r.Missed) != 0
}

func (r topoCheckError) Error() string {
	if !r.hasError() {
		return ""
	}

	return fmt.Sprintf("no available agent(s) on replsets: %s", strings.Join(r.Missed, ", "))
}

func collectTopoCheckErrors(
	replsets []Shard,
	agentsByRS map[ReplsetName]map[NodeURI]AgentStat,
	ts primitive.Timestamp,
	type_ BackupType,
) error {
	rv := topoCheckError{
		Replsets: make(map[string]map[NodeURI][]error),
		Missed:   make([]string, 0),
	}

	for _, rs := range replsets {
		rsName, uri, _ := strings.Cut(rs.Host, "/")
		agents := agentsByRS[rsName]
		if len(agents) == 0 {
			rv.Missed = append(rv.Missed, rsName)
			continue
		}

		hosts := strings.Split(uri, ",")
		members := make(map[NodeURI][]error, len(hosts))
		anyAvail := false
		for _, host := range hosts {
			a, ok := agents[host]
			if !ok || a.Arbiter || a.Passive {
				continue
			}

			errs := []error{}
			if a.Err != "" {
				errs = append(errs, errors.New(a.Err))
			}
			if ok, estrs := a.OK(); !ok {
				for _, e := range estrs {
					errs = append(errs, errors.New(e))
				}
			}

			const maxReplicationLag uint32 = 35
			if ts.T-a.Heartbeat.T > maxReplicationLag {
				errs = append(errs, errors.New("stale"))
			}
			if err := FeatureSupport(a.MongoVersion()).BackupType(type_); err != nil {
				errs = append(errs, errors.WithMessage(err, "unsupported backup type"))
			}

			members[host] = errs
			if len(errs) == 0 {
				anyAvail = true
			}
		}

		rv.Replsets[rsName] = members

		if !anyAvail {
			rv.Missed = append(rv.Missed, rsName)
		}
	}

	if rv.hasError() {
		return rv
	}

	return nil
}
