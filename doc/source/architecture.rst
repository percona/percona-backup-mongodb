.. _pbm.architecture:

Architecture
********************************************************************************

|pbm| uses one |pbm-agent| process per |mongod| node. The PBM control collections in the |mongodb| cluster or non-sharded replicaset itself serve as the central configuration, authentication and coordination channel. Administrators observe and control the backups or restores with a |pbm.app| CLI command that they can run from any host with the access to the |mongodb| cluster.

.. contents::
   :local:

.. _pbm.architecture.agent:

|pbm-agent|
================================================================================

|pbm| requires one instance of |pbm-agent| to be attached locally to each |mongod| instance. This  includes replicaset nodes that are currently secondaries and config server replicaset nodes in a cluster.

The |pbm-agent| activity is initiated by updates made to the PBM control collections by the |pbm.app| command line utility.  The |pbm-agent| detects if it is a good candidate to do the backup or restore operation and coordinates with the other nodes in the same replicaset to choose one that will perform the requested actions on behalf of the replicaset.
 
.. _pbm.architecture.pbmctl:

PBM Command Line Utility (|pbm.app|)
================================================================================

|pbm.app| is the tool you will use hands on. It is a normal CLI for users and a command that can be executed periodically in scripts (e.g. by crond). It manages your backups through a set of sub-commands:

.. include:: .res/code-block/bash/pbm-help-output.txt

|pbm.app| modifies config by saving it in the PBM control collection for config values. Likewise it starts and monitors backup or restore operations by updating and reading other PBM control collections for operations, log, etc.

PBM Control Collections
================================================================================

The config and state (current and historical) for backups is stored in collections in the MongoDB cluster or non-sharded replica set itself. These are put in the system "admin" db to keep them cleanly separated from user db namespaces. (In a cluster only the "admin" db on the configsvr replicaset, not the shards.)

- *admin.pbmConfig*
- *admin.pbmCmd*
- *admin.pbmOp*
- *admin.pbmBackup*

The |pbm.app| command line tool creates them as needed. You do not have to maintain these collections, but you should not drop them either.

A complete restore on a fresh mongod instance will not have any of these collections, so it will not have the PBM config information to find the backup storage. So for that sort of restore the config specifying the location and credentials to the backup storage must be reinserted as a first step (|pbm-config-file-set|). See the documentation for restore operations.

.. include:: .res/replace.txt
