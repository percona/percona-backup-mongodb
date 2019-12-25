package sharded

func (c *Cluster) BackupAndRestore() {
	c.GenerateBallastData(1e5)
	checkData := c.DataChecker()

	bcpName := c.Backup()
	c.DeleteData()

	c.Restore(bcpName)
	checkData()

	c.DeleteData()
}
