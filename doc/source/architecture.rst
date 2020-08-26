.. _pbm.architecture:

Architecture
********************************************************************************

|pbm| uses one |pbm-agent| process per |mongod| node. The :term:`PBM Control
collections` in the |mongodb| cluster or non-sharded replica set itself serve as
the central configuration, authentication and coordination
channel. Administrators observe and control the backups or restores with a
|pbm.app| CLI command that they can run from any host with the access to the
|mongodb| cluster.

A single |pbm-agent| is only involved with one cluster (or non-sharded replica
set). The |pbm.app| CLI tool can connect to any cluster it has network access
to, so it is possible for one user to list and launch backups or restores on
many clusters. 

.. contents::
   :local:

.. _pbm.architecture.agent:

|pbm-agent|
================================================================================

|pbm| requires one instance of |pbm-agent| to be attached locally to each
|mongod| instance. This includes replicaset nodes that are currently secondaries
and config server replicaset nodes in a cluster.

There is no |pbm-agent| config file. Some configuration is required for the
service script (e.g. ``systemd`` unit file) that will run it though. See
:ref:`pbm.installation.service_init_scripts`.

The |pbm-agent|'s backup and restore operations are triggered when it observes
updates made to the PBM control collections by the |pbm.app| command line
utility. In a method similar to the way replica set members elect a new primary,
the |pbm-agent| processes in the same replica set 'elect' the one to do the backup
or restore for that replica set.

.. _pbm.architecture.pbmctl:

PBM Command Line Utility (|pbm.app|)
================================================================================

|pbm.app| is the command you will use manually in the shell, and it will also
work as a command that can be executed in scripts (for example, by ``crond``).
It manages your backups through a set of sub-commands:

.. include:: .res/code-block/bash/pbm-help-output.txt

|pbm.app| modifies the PBM config by saving it in the PBM control collection for
config values. Likewise it starts and monitors backup or restore operations by
updating and reading other PBM control collections for operations, log, etc.

|pbm.app| does not have its own config and/or cache files per se. Setting the
|env-pbm-mongodb-uri| environment variable in your shell is a
configuration-like step that should be done for practical ease though. (Without
|env-pbm-mongodb-uri| the |opt-mongodb-uri| command line argument will need to
be specified each time.)

.. _pbm.architecture.pbm_control_collections:

PBM Control Collections
================================================================================

The config and state (current and historical) for backups is stored in
collections in the MongoDB cluster or non-sharded replica set itself. These are
put in the system ``admin`` db of the config server replica set to keep them
cleanly separated from user db namespaces. (In a non-sharded replicaset the
``admin`` db of the replica set itself is used.)

- *admin.pbmConfig*
- *admin.pbmCmd* (Used to define and trigger operations)
- *admin.pbmLock* (|pbm-agent| synchronization-lock structure)
- *admin.pbmBackup* (Log / status of each backup)

The |pbm.app| command line tool creates these collections as needed. You do not
have to maintain these collections, but you should not drop them unnecessarily
either. Dropping them during a backup will cause an abort of the backup.

Filling the config collection is a prerequisite to using PBM for executing
backups or restores. (See config page later.)
 
Remote Backup Storage
================================================================================

|PBM| saves your files to a directory. Conceptually in
the case of object store; actually if you are using filesystem-type remote
storage. Using |pbm-list|, a user can scan this directory to find existing
backups even if they never used |pbm.app| on their computer before.

The files are prefixed with the (UTC) starting time of the backup. For each
backup there is one metadata file. For each replicaset in the backup:

- A mongodump-format compressed archive that is the dump of collections
- A (compressed) BSON file dump of the oplog covering the timespan of the backup.
    
The end time of the oplog slice(s) is the data-consistent point in time of a backup snapshot.

.. include:: .res/replace.txt
