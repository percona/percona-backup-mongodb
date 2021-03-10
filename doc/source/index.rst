.. _pbm.index:

|pbm| Documentation
********************************************************************************

|pbm| is a distributed, low-impact solution for achieving consistent backups of
|mongodb| sharded clusters and replica sets.

|pbm| supports `Percona Server for MongoDB
<https://www.percona.com/software/mongo-database/percona-server-for-mongodb>`_
and MongoDB Community v3.6 or higher with `MongoDB Replication
<https://docs.mongodb.com/manual/replication/>`_ enabled.

.. note::

   |PBM| doesn't work on standalone |mongodb| instances. This is because |PBM| requires an :term:`oplog <Oplog>` to guarantee backup consistency. Oplog is available on nodes with replication enabled.

   For testing purposes, you can deploy |PBM| on a single-node replica set. ( Specify the ``replication.replSetName`` option in the configuration file of the standalone server.)  

   .. seealso::

      MongoDB Documentation: Convert a Standalone to a Replica Set
         https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/

The |pbm| project is inherited from and replaces `mongodb_consistent_backup`,
which is no longer actively developed or supported.

.. _pbm.feature:

.. rubric:: Features

.. hlist::
   :columns: 2

   - Backup and restore for both classic non-sharded replica sets and sharded clusters
   - Simple command-line management utility
   - Replica set and sharded cluster consistency through oplog capture
   - Distributed transaction consistency with MongoDB 4.2+
   - Simple, integrated-with-MongoDB authentication
   - No need to install a coordination service on a separate server.
   - Use any S3-compatible storage
   - Users with classic, locally-mounted remote filesystem backup servers can
     use 'filesystem' instead of 's3' storage type.

Introduction
***********************************************

.. toctree::
   :maxdepth: 2

   intro
   pbm-components

Getting started
***********************************************

.. toctree::
   :maxdepth: 2

   installation   
   initial-setup
   

Usage
***********************************************

.. toctree::
   :maxdepth: 2  

   running
   point-in-time-recovery
   status

Details
***********************************************

.. toctree::
   :maxdepth: 1

   architecture
   authentication
   config
   storage-configuration

How to 
***********************************************

.. toctree::
   :maxdepth: 2
      
   schedule-backup
   Upgrade PBM <upgrading>
   Troubleshoot PBM <troubleshooting>   
   Remove PBM <uninstalling>

Reference
***********************************************
   
.. toctree::
   :maxdepth: 1

   pbm-commands
   configuration-options
   contributing
   glossary
   copyright
   trademark-policy

Release notes
***********************************************
   
.. toctree::
   :maxdepth: 1
   
   release-notes

.. include:: .res/replace.txt
.. include:: .res/url.txt

