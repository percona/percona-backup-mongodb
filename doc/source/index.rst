.. _pbm.index:

|pbm| Documentation
********************************************************************************

|pbm| is a distributed, low-impact solution for achieving consistent backups of
MongoDB Sharded Clusters and Replica Sets.

`Percona Server for MongoDB
<https://www.percona.com/software/mongo-database/percona-server-for-mongodb>`_
or MongoDB Community v3.6 or higher with `MongoDB Replication
<https://docs.mongodb.com/manual/replication/>`_ enabled.

.. note:: 

   The |pbm| project was inspired by (and intends to replace)
   `mongodb_consistent_backup`. This tool is not supported any longer.

.. rubric:: Contents

.. toctree::
   :maxdepth: 2

   architecture
   installation
   running
   docker
   contributing

.. _pbm.feature:

.. rubric:: Features

.. hlist::
   :columns: 2

   - Oplog tailer
   - Oplog applier
   - AWS S3 streamer
   - AWS S3 streamer: Backup (upload)
   - Mongodump Backup Method
   - Mongodump Backup Method: Backup
   - Mongodump Backup Method: Restore
   - Agent selection algorithm
   - SSL/TLS support
   - Replica Set Backup
   - Sharded Cluster Backup
   - Sharded Cluster Backup: Pausing of balancer at backup-time
   - Command-line management utility
   - Compression: Agent and CLI RPC communications
   - Authorization of Agents and CLI
   - Restore from any Point-in-time

.. rubric:: Contact Us

Use our Percona email address (mongodb-backup@percona.com) or the contact
form on the site (https://www.percona.com/about-percona/contact) to reach us.

.. include:: .res/replace.txt
.. include:: .res/url.txt
