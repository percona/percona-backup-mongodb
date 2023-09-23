package types

import "github.com/percona/percona-backup-mongodb/internal/defs"

type ConnectionStatus struct {
	AuthInfo AuthInfo `bson:"authInfo" json:"authInfo"`
}

type AuthInfo struct {
	Users     []AuthUser      `bson:"authenticatedUsers" json:"authenticatedUsers"`
	UserRoles []AuthUserRoles `bson:"authenticatedUserRoles" json:"authenticatedUserRoles"`
}

type AuthUser struct {
	User string `bson:"user" json:"user"`
	DB   string `bson:"db" json:"db"`
}

type AuthUserRoles struct {
	Role string `bson:"role" json:"role"`
	DB   string `bson:"db" json:"db"`
}

type BalancerStatus struct {
	Mode              defs.BalancerMode `bson:"mode" json:"mode"`
	InBalancerRound   bool              `bson:"inBalancerRound" json:"inBalancerRound"`
	NumBalancerRounds int64             `bson:"numBalancerRounds" json:"numBalancerRounds"`
	Ok                int               `bson:"ok" json:"ok"`
}

func (b *BalancerStatus) IsOn() bool {
	return b.Mode == defs.BalancerModeOn
}
