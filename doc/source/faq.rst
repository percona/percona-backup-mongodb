.. _faq:

**********************
FAQ 
**********************

What's the difference between PBM and ``mongodump``?
========================================================

Both |PBM| and ``mongodump`` are 'logical' backup solutions and have equal performance for non-sharded replica sets. However, as opposed to ``mongodump``, |PBM| allows you to achieve the following goals:

- make consistent backups and restores in sharded clusters
- restore your database to a specific point in time 
- run backups / restores on each replica set in parallel while ``mongodump`` runs in one process on ``mongos`` node. 
 

Why does |PBM| use UTC timezone instead of server local timezone?
==================================================================

``pbm-agents`` use UTC time zone by design. The reason behind this is to avoid user misunderstandings when replica set / cluster nodes are distributed geographically in different time zones. 

Can I restore a single collection with |PBM|?
=============================================================

No, |PBM| makes backups of and restores the whole state of a replica set / sharded cluster. 

If single-collection restores are your primary requirement and you are not using a sharded cluster, or the sharded cluster is only 2 or 3 shards, we recommend using ``-d`` and ``-c`` options with ``mongodump`` and/or ``mongorestore``. As ``mongodump``/``mongorestore`` connects directly to the primary (in a non-sharded replica set) or via a ``mongos`` node in a cluster, it sees the cluster as if it were a single node, making it simple. ``mongodump``/``mongorestore`` work in a single process, so if you aren’t reinserting to many shards, the lack of parallelization won't be too bad.

Can I backup specific shards in a cluster? 
===========================================

No, since this would result in backups with inconsistent timestamps across the cluster. Such backups would be invalid for restore.

|PBM| backs up the whole state of a sharded cluster and this guarantees data consistency during the restore. 

Do I need to stop the balancer for PITR restore?
================================================

Yes. The preconditions for both |PITR| restore and regular restore are the same:

1. In sharded cluster, stop the balancer
2. Make sure no writes are made to the database during restore. This ensures data consistency.
3. Disable |PITR| if it is enabled. This is because oplog slicing and restore are exclusive operations and cannot be run together. Note that oplog slices made after the restore and before the next backup snapshot become invalid. Make a fresh backup and re-enable |PITR|.



.. include:: .res/replace.txt