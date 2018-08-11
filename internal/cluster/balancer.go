package cluster

import (
	"errors"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

// The amount of time in milliseconds to wait for a balancer
// command to run
var BalancerCmdTimeoutMs = 60000

// runBalancerCommand is a helper for running a
// balancerStart/balancerStop server command
func runBalancerCommand(session *mgo.Session, balancerCommand string) error {
	okResp := mdbstructs.OkResponse{}
	err := session.Run(bson.D{
		{balancerCommand, "1"},
		{"maxTimeMS", BalancerCmdTimeoutMs},
	}, &okResp)
	if err != nil {
		return err
	} else if okResp.Ok != 1 {
		return errors.New("got failed response from server")
	}
	return nil
}

// GetBalancerStatus returns a struct representing the result of
// the MongoDB 'balancerStatus' command. This command will only
// succeed on a session to a mongos process (as of MongoDB 3.6)
//
// https://docs.mongodb.com/manual/reference/command/balancerStatus/
//
func GetBalancerStatus(session *mgo.Session) (*mdbstructs.BalancerStatus, error) {
	balancerStatus := mdbstructs.BalancerStatus{}
	err := session.Run(bson.D{{"balancerStatus", "1"}}, &balancerStatus)
	return &balancerStatus, err
}

// IsBalancerEnabled returns a boolean reflecting if the balancer
// is enabled
func IsBalancerEnabled(status *mdbstructs.BalancerStatus) bool {
	return status.Mode == mdbstructs.BalancerModeFull
}

// IsBalancerRunning returns a boolean reflecting if the balancer
// is currently running
func IsBalancerRunning(status *mdbstructs.BalancerStatus) bool {
	return status.InBalancerRound
}

// StopBalancer performs a 'balancerStop' server command on
// the provided session
//
// https://docs.mongodb.com/manual/reference/command/balancerStop/
//
func StopBalancer(session *mgo.Session) error {
	return runBalancerCommand(session, "balancerStop")
}

// StartBalancer performs a 'balancerStart' server command on
// the provided session
//
// https://docs.mongodb.com/manual/reference/command/balancerStart/
//
func StartBalancer(session *mgo.Session) error {
	return runBalancerCommand(session, "balancerStart")
}
