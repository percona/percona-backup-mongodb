.. _pbm.architecture:

Architecture
********************************************************************************

|pbm| uses a distributed client-server architecture to perform backup and
restore actions. This architecture provides the maximum scalability and
flexibility.

.. contents::
   :local:

.. _pbm.architecture.coordinator:

Backup Coordinator
================================================================================

The |backup-coordinator| is a daemon that handles communication with backup
agents and the backup control program.

The main function of the backup coordinator is to gather information from the
|mongodb| instances through the agents to determine which nodes should run
backup or restore and to establish consistent backup and restore points across
all shards.

The |backup-coordinator| listens on 2 TCP ports:

RPC
   Port used for agent communications (Default: 10000/tcp)
API
   Port used for CLI/API communications (Default: 10001/tcp)

.. _pbm.architecture.agent:

Agent
================================================================================

Backup agents receive commands from the coordinator and run them.

The agent runs locally (connected to 'localhost') on every |mongodb| instance
(|mongos| and config servers included) in order to collect information about
the instance and forward it to the coordinator. With that information, the
coordinator can determine the best agent to start a backup or restore, to
start/stop the balancer, and so on.

The agent requires outbound network access to the :ref:`backup coordinator
<pbm.architecture.coordinator>` RPC port.

.. _pbm.architecture.pbmctl:

PBM Control (|pbmctl|)
================================================================================

This program is a command line utility to send commands to the coordinator.
Currently, the available commands are:

==============  ================================================================
Command         Description
==============  ================================================================
list nodes      List all nodes (agents) connected to the coordinator
list backups    List all finished backups.
run backup      Start a new backup
run restore     Restore a backup
==============  ================================================================

The |pbmctl| utility requires outbound network access to the :ref:`backup
coordinator <pbm.architecture.coordinator>`.

.. include:: .res/replace.txt
