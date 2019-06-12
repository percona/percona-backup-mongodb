.. _pbm.index:

|pbm| Documentation
********************************************************************************

|pbm| is a distributed, low-impact solution for achieving consistent backups of
|mongodb| Sharded Clusters and Replica Sets.

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
   release-notes
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

How to use |pbm|: going back in time (``pbmctl run restore``)
================================================================================

In a highly-available architecture, such as |mongodb| replication, there is no
need to make a backup in order to recover from a problem like a disk failure. If
you lose one node you can replace it by re-initializing from one of its replica
set peers.

*The only point of backups of a replica set is to go back in time*. For example,
a web application update was released on *Sunday, June 9th 23:00 EST* but, by
11:23 Monday, someone realizes that the update has wiped the historical data of
any user who logged in due to a bug.

Nobody likes to have downtime, but it's time roll back: what's the best backup
to use?

.. code-block:: bash
		
   $ pbmctl list backups

.. admonition:: Output

   .. code-block:: text

      |       Metadata file name      -     Description
      ------------------------------ - ----------------------------
      2019-06-10T07:04:14Z.json      - daily_bk_20190610_0300
      2019-06-09T07:03:50Z.json      - daily_bk_20190609_0300
      2019-06-08T07:04:21Z.json      - daily_bk_20190608_0300
      2019-06-07T07:04:18Z.json      - daily_bk_20190607_0300

The most recent daily backup would include 4 hours of damage caused by the bug.
Let's restore the one before that:

.. code-block:: bash

   $ pbmctl run restore 2019-06-09T07:03:50Z.json

Next time there is an application release, it might be best to make an extra backup
manually just before:

.. code-block:: bash

   $ pbmctl run backup --description "Pre-release v0.8.3 backup"

.. seealso::

   Typical use cases of |pbm|
      :ref:`pbm.running`

Contact Us
================================================================================

Use our Percona email address (mongodb-backup@percona.com) or the contact
form on the site (https://www.percona.com/about-percona/contact) to reach us.

.. include:: .res/replace.txt
.. include:: .res/url.txt
