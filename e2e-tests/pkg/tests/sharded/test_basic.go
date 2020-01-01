package sharded

func (c *Cluster) BackupAndRestore() {
	checkData := c.DataChecker()

	bcpName := c.Backup()
	c.BackupWaitDone(bcpName)
	c.DeleteBallast()

	c.Restore(bcpName)
	checkData()
}
