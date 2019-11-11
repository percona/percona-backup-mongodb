.. _pbm.architecture:

Architecture
********************************************************************************

|pbm| uses one |pbm-agent| process per |mongod| node. The |mongodb| cluster or
non-sharded replicaset itself serves as the central configuration,
authentication and coordination channel. Administrators observe and control the
backups or restores with a |pbm.app| CLI command that they can run from any host
with the access to the |mongodb| cluster.

.. contents::
   :local:

.. _pbm.architecture.agent:

|pbm-agent|
================================================================================

An instance of |pbm-agent| must be attached to the localhost of each |mongod|
instance including replicaset nodes that are currently secondaries and (in case
of a sharded cluster) to the config server replicaset nodes. You control backup
agents (instances of |pbm-agent|) via the |pbm.app| command line utility.  The
|pbm-agent| detects if it is a good candidate to do the backup or restore
operations and coordinates with the other nodes in the same replicaset to
accomplish the requested actions on behalf of the replicaset, or to let another
|pbm-agent| do it.
 
.. _pbm.architecture.pbmctl:

PBM Command Line Utility (|pbm.app|)
================================================================================

|pbm.app| manages your backups through a set of sub-commands:

==============  ================================================================
Command         Description
==============  ================================================================
store set       Set up a backup store
store show      Show the backup store associated with the active replica set.
backup          Make a backup
restore         Restore a backup
list            List the created backups
==============  ================================================================

For each of these commands, you should supply the mongodb connection string as
the value of the |opt-mongodb-uri|:

.. include:: .res/code-block/bash/pbm-backup-mongodb-uri.txt

.. include:: .res/text/note-env-pbm-mongodb-uri.txt

.. include:: .res/replace.txt
