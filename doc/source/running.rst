.. _pbm.running:

Running |pbm|
********************************************************************************

.. contents::
   :local:

Please see :ref:`pbm.auth` if you have not already. This will explain the
MongoDB user that needs to be created, and the connection method used by |pbm|.

Initial Setup
================================================================================

1. Determine the right MongoDB connection string for the |pbm.app| CLI.
   (See :ref:`pbm.auth.mdb_conn_string`)
2. Use the |pbm.app| CLI to insert the config (especially the Remote Storage
   location and credentials information). See :ref:`pbm.config.initialize`
3. Start (or restart) the |pbm-agent| processes for all mongod nodes.


Start the |pbm-agent| processes
--------------------------------------------------------------------------------
After installing |pbm-agent| on the all the servers that have mongod nodes make
sure it is started one for each mongod node.

E.g. Imagine you put configsvr nodes (listen port 27019) colocated on the same
servers as the first shard's mongod nodes (listen port 27018, replica set name
"sh1rs") to save some hardware costs. In this server you would start two
|pbm-agent| processes, one connected to the shard
(e.g. "mongodb://username:password@localhost:27018/") and one to the configsvr
node (e.g. "mongodb://username:password@localhost:27019/").

It is best to use the packaged service scripts to run |pbm-agent|. But for
reference an example of how to do it manually is shown below. The output is
redirected to a file and the process is backgrounded. You can run it an shell
terminal temporarily if you want to observer and/or debug the startup from the
log messages.

.. code-block:: bash

   $ nohup pbm-agent --mongodb-uri "mongodb://username:password@localhost:27018/" > /data/mdb_node_xyz/pbm-agent.$(hostname -s).27018.log 2>&1 &

.. tip::
   
   Running as the mongod user and saving the log file in the same parent
   directory as the mongod node's data directory would be the most intuitive and
   convenient way. But if you want it can be another user, and the log file can
   be at location that files can be written to, or piped to logging service.

You can confirm the |pbm-agent| connected to its mongod and started OK by
confirming *"pbm agent is listening for the commands"* is printed to the log
file.

Running |pbm|
================================================================================

Running |pbm.app| Commands
================================================================================

|pbm.app| is the command line utility to control the backup system.

Configuring a Remote Store for Backup and Restore Operations
--------------------------------------------------------------------------------

This must done once, at installation time, before backups can be listed, made,
or restored. Please see :ref:`pbm.config`.

.. _pbm.running.backup.listing:

Example: Listing all backups
--------------------------------------------------------------------------------

.. include:: .res/code-block/bash/pbm-list-mongodb-uri.txt

.. admonition:: Sample output

   .. code-block:: text

      2019-09-10T07:04:14Z
      2019-09-09T07:03:50Z
      2019-09-08T07:04:21Z
      2019-09-07T07:04:18Z

.. _pbm.running.backup.starting: 

Example: Starting a backup
--------------------------------------------------------------------------------

.. include:: .res/code-block/bash/pbm-backup-mongodb-uri.txt

.. _pbm.running.backup.restoring: 

Example: Restoring a Backup
--------------------------------------------------------------------------------

To restore a backup that you have made using |pbm-backup| you should use the
|pbm-restore| command supplying the timestamp of the backup that you intend to
restore.

.. important::

   For PBM v1.0 (only) before running |pbm-restore| on a cluster stop the
   balancer.

.. important::

   Whilst the restore is running clients should be stopped from accessing the
   database. The data will naturally be incomplete whilst the restore is in
   progress, and writes they make will cause the final restored data to differ
   from the backed-up data. In a cluster's restore the simplest way would be to
   shutdown all mongos nodes.

.. include:: .res/code-block/bash/pbm-restore-mongodb-uri.txt

After a cluster's restore complete a cluster all mongos nodes will need to be
restarted to reload the sharding metadata.

Deleting backups
--------------------------------------------------------------------------------

Use the relevant native ``rm`` command for the storage. E.g. for S3-compatible
object stores use ``aws s3 rm <file name>``. For a filesystem-type storage, use
normal shell command ``rm``.

.. include:: .res/replace.txt
