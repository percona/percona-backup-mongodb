package sharded

func (c *Cluster) DistributedTrxSnapshot() {
	c.DistributedTransactions(NewSnapshot(c), "test")
}

func (c *Cluster) DistributedTrxPhysical() {
	c.DistributedTransactions(NewPhysical(c), "test")
}
