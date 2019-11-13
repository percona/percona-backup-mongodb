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

It does not have a config file, but typically you would ensure that is always running when it should be by using a system init script. To supply it with the authentication information it requires that init script should include a |opt-mongodb-uri| option value or set the |env-pbm-mongodb-uri| environment variable.

The |pbm-agent| actions are initiated by updates made to the PBM control collections by the |pbm.app| command line utility.  The |pbm-agent| detects if it is a good candidate to do the backup or restore operation and coordinates with the other nodes in the same replicaset to choose one that will perform the requested actions on behalf of the replicaset.
 
.. _pbm.architecture.pbmctl:

PBM Command Line Utility (|pbm.app|)
================================================================================

|pbm.app| is the tool you will use hands on. It is a normal CLI for users and a command that can be executed periodically in scripts (e.g. by crond). It manages your backups through a set of sub-commands:

.. include:: .res/code-block/bash/pbm-help-output.txt

|pbm.app| modifies the PBM config by saving it in the PBM control collection for config values. Likewise it starts and monitors backup or restore operations by updating and reading other PBM control collections for operations, log, etc.

|pbm.app| does not have its own config and/or cache files per se. Setting the |env-pbm-mongodb-uri| environment variable in your shell resource files or sourcing that value in the scripts you run it with is very practical though. (Without that the |opt-mongodb-uri| option will need to be specified each time.)

PBM Control Collections
================================================================================

The config and state (current and historical) for backups is stored in collections in the MongoDB cluster or non-sharded replica set itself. These are put in the system "admin" db to keep them cleanly separated from user db namespaces. (In a cluster only the "admin" db on the configsvr replicaset, not the shards.)

- *admin.pbmConfig*
- *admin.pbmCmd* (Used to define and trigger operations)
- *admin.pbmOp* (|pbm-agent| synchronization-lock structure)
- *admin.pbmBackup* (Log / status of each backup)

The |pbm.app| command line tool creates them as needed. You do not have to maintain these collections, but you should not drop them unnecessarily either. Dropping them during a backup will cause an abort of the backup.

A fresh mongod instance does not have any of these collections. They are not needed for |pbm.app| to begin new backups or restores *except* the PBM config one because it holds the config including the location and credential information to open the remote backup storage. See the documentation for setting up or doing restores for how to initialise or reinitialise the config.
 
Remote Backup Storage
================================================================================

PBM saves your files to a directory &ndash; one in an S3-compatible store or on a filesystem. Using |pbm-list| a user can scan this directory to find existing backups even if they never used |pbm.app| on their computer before.

The files are prefixed with the (UTC) starting time of the backup. For each backup there will be one metadata file, and for each of the replicasets in the backup there will be one mongodump-format compressed archive that is the dump of collections plus (compressed) BSON file dump of the oplog covering the timespan of the backup. In a cluster backup the end times of the oplog slices are synchronized. The end time of the ollog slice(s) is the data-consistent point in time of a backup snapshot.

.. info:: 
   N.b. compared to using a S3-compatible object store the "filesystem" type of store has more work for the server administrator to do. Setting the PBM config to use a filesystem location is trivial, but there is no point for disaster recovery if the filesystem is just the local disk of the mongod node's server. The filesystem should be a remote backup server that shares a normal filesystem. The server admins need to ensure that shared remoted directory is mounted at the same local path on all servers that have mongod nodes.

Authentication
================================================================================

- The MongoDB cluster (or non-sharded replicaset) has MongoDB's own authentication. This can be whichever you have set, including use TLS options.
- Remote Storage:
  - S3-compatible object store server. Commonly this is authenticated to with an API key and a secret key. You are free to use whichever provider and whichever authenticaton methods it supports so long as they are supported by the golang API for AWS SDK (the S3 client library that PBM uses).
  - Filesystem type remote storage has no authentication for |pbm.app| or |pbm-agent| per se. It requires the server administrators to make the filesystem directory to writable for the |pbm-agent|.
- PBM has no authentication subsystem of its own &ndash; it uses MongoDB's. I.e. |pbm.app| and |pbm-agent| only require the valid MongoDB connection URI string for the PBM user. It accesses the authentication credentials to the remote storage in the PBM config collection in the MongoDB cluster (or non-sharded replicaset).

.. include:: .res/replace.txt
