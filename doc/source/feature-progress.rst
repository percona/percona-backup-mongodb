.. _pbm.feature-progress:

Feature Progress
********************************************************************************

=========  =====================================================================
Available  Feature 
=========  =====================================================================
Yes        Oplog tailer
Yes        Oplog applier
Yes        AWS S3 streamer
Yes        AWS S3 streamer: Backup (upload)
No         AWS S3 streamer: Restore (download)
Yes        Mongodump Backup Method
Yes        Mongodump Backup Method: Backup
Yes        Mongodump Backup Method: Restore
Yes        Agent selection algorithm
Yes        SSL/TLS support
Yes        Replica Set Backup
Yes        Sharded Cluster Backup
Yes        Sharded Cluster Backup: Pausing of balancer at backup-time
Yes        Command-line management utility
No         Dockerhub images
No         Compression
Yes        Compression: Agent and CLI RPC communications
No         Compression: Backup data
Yes        Authorization of Agents and CLI
No         Encryption of backup data
No         Support for MongoDB SSL/TLS connections
No         Recovery from agent failures
No         Support for |hot-backup| for binary level backup
No         |hot-backup|: Backup
No         |hot-backup|: Restore
No         |hot-backup| Support for `WiredTiger Encryption`_
No         Support for more upload/transfer methods
No         Multiple sources of credentials (eg: file, Vault, Amazon KMS, etc.)
Yes        Restore from any Point-in-time
No         Support for incremental backups using oplogs
No         Prometheus metrics
=========  =====================================================================

.. |hot-backup| replace:: `Percona Server for MongoDB Hot Backup`_

.. include:: .res/url.txt
