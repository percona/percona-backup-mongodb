package sharded

func (c *Cluster) DistributedTrxSnapshot() {
	c.DistributedTransactions(NewSnapshot(c), "test")
}
