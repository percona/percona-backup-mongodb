package defs

// Command represents actions that could be done on behalf of the client by the agents
type Command string

const (
	CmdUndefined    Command = ""
	CmdBackup       Command = "backup"
	CmdRestore      Command = "restore"
	CmdReplay       Command = "replay"
	CmdCancelBackup Command = "cancelBackup"
	CmdResync       Command = "resync"
	CmdPITR         Command = "pitr"
	CmdDeleteBackup Command = "delete"
	CmdDeletePITR   Command = "deletePitr"
	CmdCleanup      Command = "cleanup"
)

func (c Command) String() string {
	switch c {
	case CmdBackup:
		return "Snapshot backup"
	case CmdRestore:
		return "Snapshot restore"
	case CmdReplay:
		return "Oplog replay"
	case CmdCancelBackup:
		return "Backup cancellation"
	case CmdResync:
		return "Resync storage"
	case CmdPITR:
		return "PITR incremental backup"
	case CmdDeleteBackup:
		return "Delete"
	case CmdDeletePITR:
		return "Delete PITR chunks"
	case CmdCleanup:
		return "Cleanup backups and PITR chunks"
	default:
		return "Undefined"
	}
}
