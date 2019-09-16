.. _pbm.architecture:

Architecture
********************************************************************************

|pbm| uses a distributed client-server architecture to perform backup and
restore actions. This architecture provides the maximum scalability and
flexibility.

.. contents::
   :local:

.. figure:: .res/graphics/mongodb-replica-set.png

   The architecture of |pbm|

.. .. _pbm.architecture.coordinator:
.. 
.. Backup Coordinator
.. ================================================================================
.. 
.. The |backup-coordinator| is a service that transparently handles communication
.. with backup agents (started with |pbm-agent|)and the backup control program
.. (|pbmctl|).
.. 
.. The main function of the |bc| is to gather information from the
.. |mongod| instances through the agents to determine which nodes should run
.. backup or restore and to establish consistent backup and restore points across
.. all shards.
.. 
.. The |backup-coordinator| listens on 2 TCP ports:
.. 
.. RPC
..    Port used for agent communications (Default: 10000/tcp)
.. API
..    Port used for CLI/API communications (Default: 10001/tcp)
.. 

.. _pbm.architecture.agent:

|pbm-agent|
================================================================================

Backup agents (instances of |pbm-agent|) receive commands from the |pbm.app|.

An instance of |pbm-agent| must be attached to each |mongod| instance in the
replica set in order to collect information about the instance and to forward it
to the |pbm.app|. |pbm.app| uses this information to determine the best agent to start a
backup or restore, to start or stop the balancer, and so on.

.. _pbm.architecture.pbmctl:

PBM Command Line Utility (|pbmctl|)
================================================================================

|pbm.app| manages your backups through a set of sub-commands:

==============  ================================================================
Command         Description
==============  ================================================================
store set       Set up a backup storage
store show      Show the backup storage associated with the active replica set.
backup          Make a backup
restore         Restore a backup
list            List the created backups
==============  ================================================================

For each of these commands, you should supply the mongodb connection string as
the value of the |opt-mongodb-uri|:

.. include:: .res/code-block/bash/pbm-backup-mongodb-uri.txt
   
If you store the connection string in the |env-pbm-mongodb-uri| environment
variable, you can omit the |opt-mongodb-uri| parameter:

.. code-block:: bash

   $ export PBM_MONGODB_URI="mongodb://172.17.0.3:27018"
   $ pbm backup

-----

.. include:: .res/replace.txt
