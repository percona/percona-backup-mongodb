.. _pbm.architecture:

Architecture
********************************************************************************

.. contents::
   :local:

.. _pbm.architecture.agent:

|pbm-agent|
================================================================================

|pbm-agent| is a process that runs backup, restore, delete and other operations available with |PBM|.  

A |pbm-agent| instance must run on each
|mongod| instance. This includes replica set nodes that are currently secondaries
and config server replica set nodes in a sharded cluster.

An operation is triggered when the |pbm.app| CLI makes an update to the PBM Control collection. All ``pbm-agents`` monitor changes to the PBM control collections but only one |pbm-agent| in each replica set will be elected to execute an operation. The elections are done in a similar way replica set members elect a new primary.

The elected |pbm-agent| acquires a lock for an operation. This prevents mutually exclusive operations like backup and restore to be executed simultaneously.

When the operation is complete, the |pbm-agent| releases the lock and updates the PBM control collections.

.. _pbm.architecture.pbmctl:

PBM Command Line Utility (|pbm.app|)
================================================================================

|pbm.app| CLI is the command line tool with which you operate |PBM|. |pbm.app| provides the :command:`pbm` command that you will use manually in the shell. It will also
work as a command that can be executed in scripts (for example, by ``crond``).

The set of :ref:`pbm sub-commands <pbm-commands>` enables you to manage backups in your MongoDB environment.

|pbm.app| uses :ref:`PBM Control collections <pbm.architecture.pbm_control_collections>` to communicate with |pbm-agent| processes. It starts and monitors backup or restore operations by updating and reading the corresponding PBM control collections for operations, log, etc. Likewise, it modifies the PBM config by saving it in the PBM Control collection for config values.  

|pbm.app| does not have its own config and/or cache files. Setting the
|env-pbm-mongodb-uri| environment variable in your shell is a
configuration-like step that should be done for practical ease though. (Without
|env-pbm-mongodb-uri| the |opt-mongodb-uri| command line argument will need to
be specified each time.) 

To learn how to set the ``PBM_MONGODB_URI`` environment variable, see :ref:`set-mongodbURI-pbm.CLI`. For more information about MongoDB URI connection strings, see :ref:`pbm.auth`.

.. _pbm.architecture.pbm_control_collections:

PBM Control Collections
================================================================================

The config and state (current and historical) for backups is stored in
collections in the MongoDB cluster or non-sharded replica set itself. These are
put in the system ``admin`` db to keep them
cleanly separated from user db namespaces. 

In sharded clusters, this is the ``admin`` db of the config server replica set. In a non-sharded replica set, the PBM Control Collections are stored in 
``admin`` db of the replica set itself.

- *admin.pbmBackups* - Log / status of each backup
- *admin.pbmAgents* - Contains information about ``pbm-agents`` statuses and health
- *admin.pbmConfig* - Contains configuration information for |PBM|
- *admin.pbmCmd* - Is used to define and trigger operations
- *admin.pbmLock* - |pbm-agent| synchronization-lock structure
- *admin.pbmLockOp* - Is used to coordinate operations that are not mutually-exclusive such as make backup and delete backup.
- *admin.pbmLog* - Stores log information from all ``pbm-agents`` in the MongoDB environment. Available in |PBM| as of version 1.4.0
- *admin.pbmOpLog* - Stores :term:`operation IDs <OpID>`
- *admin.pbmPITRChunks* - Stores :term:`Point-in-Time Recovery` oplog slices
- *admin.pbmPITRState* - Contains current state of |PITR| incremental backups
- *admin.pbmRestores* - Contains restore history and the restore state for all replica sets
- *admin.pbmStatus* - Stores |PBM| status records

The |pbm.app| command line tool creates these collections as needed. You do not
have to maintain these collections, but you should not drop them unnecessarily
either. Dropping them during a backup will cause an abort of the backup.

Filling the config collection is a prerequisite to using |PBM| for executing
backups or restores. (See config page later.)
 
.. _pbm.architecture.remote_storage:

Remote Backup Storage
================================================================================

|PBM| saves your files to a directory. Conceptually in
the case of object store; actually if you are using filesystem-type remote
storage. Using |pbm-list|, a user can scan this directory to find existing
backups even if they never used |pbm.app| on their computer before.

The files are prefixed with the (UTC) starting time of the backup. For each
backup there is one metadata file. For each replica set, a backup includes the following:

- A mongodump-format compressed archive that is the dump of collections
- A (compressed) BSON file dump of the oplog covering the timespan of the backup.
    
The end time of the oplog slice(s) is the data-consistent point in time of a backup snapshot.

For details about supported backup storages, see :ref:`storage.config`.

.. include:: .res/replace.txt
