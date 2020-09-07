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

   |PBM| doesn't work on standalone |mongodb| instances. This is because |PBM| requires an :term:`oplog` to guarantee backup consistency. Oplog is available in clusters and replica sets only.

   For testing purposes, you can deploy |PBM| on a single-node replica set. ( Specify the ``replication.replSetName`` in the configuration file of the standalone server.)  

   .. seealso::

      MongoDB Documentation: Convert a Standalone to a Replica Set
         https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/

The |pbm| project inherited from and replaces `mongodb_consistent_backup`,
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
     use 'file system' instead of 's3' storage type.

Introduction
***********************************************

.. toctree::
   :maxdepth: 2

   intro
   architecture

Installation and Upgrade
***********************************************

.. toctree::
   :maxdepth: 2

   installation
   upgrading

Getting Started
***********************************************  

.. toctree::
   :maxdepth: 2

   authentication
   config

Usage
***********************************************

.. toctree::
   :maxdepth: 2  

   running
   point-in-time-recovery

Troubleshooting 
***********************************************

.. toctree::
   :maxdepth: 2
      
   troubleshooting

Uninstall |PBM|
***********************************************

.. toctree::
   :maxdepth: 2
   
   uninstalling

Reference
***********************************************
   
.. toctree::
   :maxdepth: 1

   release-notes
   contributing
   glossary

.. include:: .res/replace.txt
.. include:: .res/url.txt

