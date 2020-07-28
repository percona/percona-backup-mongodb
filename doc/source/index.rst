.. _pbm.index:

|pbm| Documentation
********************************************************************************

|pbm| is a distributed, low-impact solution for achieving consistent backups of
|mongodb| sharded clusters and replica sets.

|pbm| supports `Percona Server for MongoDB
<https://www.percona.com/software/mongo-database/percona-server-for-mongodb>`_
and MongoDB Community v3.6 or higher with `MongoDB Replication
<https://docs.mongodb.com/manual/replication/>`_ enabled.

The |pbm| project inherited from and replaces `mongodb_consistent_backup`,
which is no longer actively developed or supported.

.. _pbm.feature:

.. rubric:: Features

.. hlist::
   :columns: 2

   - Backup and restore for both classic, non-sharded replica sets and clusters
   - Simple command-line management utility
   - Replica set and sharded cluster consistency through oplog capture
   - Distributed transaction consistency with MongoDB 4.2+
   - Simple, integrated-with-MongoDB authentication
   - No need to install a coordination service on a separate server.
   - Use any S3-compatible storage
   - Users with classic, locally-mounted remote filesystem backup servers can
     use 'file system' instead of 's3' storage type.

.. toctree::
   :maxdepth: 2

   intro
   architecture
   installation
   upgrading
   authentication
   config
   running
   point-in-time-recovery
   troubleshooting
   release-notes
   uninstalling
   contributing
   glossary

.. include:: .res/replace.txt
.. include:: .res/url.txt

----

.. rubric:: Contact Us

Bugs and feature requests can be opened as JIRA tickets at
https://jira.percona.com/projects/PBM/issues .

Contact us using the form on the
site (https://www.percona.com/about-percona/contact).
