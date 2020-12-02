.. _pbm.running:

Running |pbm|
********************************************************************************

.. contents::
   :local:

Please see :ref:`pbm.auth` if you have not already. This will explain the
MongoDB user that needs to be created, and the connection method used by |pbm|.

Initial setup
================================================================================

1. Determine the right MongoDB connection string for the |pbm.app| CLI.
   (See :ref:`pbm.auth.mdb_conn_string`) 
#. Use the |pbm.app| CLI to insert the config (especially the Remote Storage
   location and credentials information). See :ref:`pbm.config.initialize`
#. Start (or restart) the |pbm-agent| processes for all mongod nodes.


Start the |pbm-agent| processes
--------------------------------------------------------------------------------
After installing |pbm-agent| on all the servers that run ``mongod`` nodes, make
sure one instance of it is started for each ``mongod`` node. This also applies if you deployed several ``mongod`` nodes on the same server.

For example, your configsvr nodes (listen port 27019) run on the same servers as the first shard's mongod nodes (listen port 27018, replica set name
"sh1rs"). Then you should start two 
|pbm-agent| processes on these servers, one process is connected to the shard
(e.g. "mongodb://username:password@localhost:27018/") and another one to the configsvr
node (e.g. "mongodb://username:password@localhost:27019/").

It is best to use the packaged service scripts to run |pbm-agent|. After
adding the database connection configuration for them (see
:ref:`pbm.installation.service_init_scripts`), you can start the |pbm-agent|
service as below:

.. code-block:: bash

   $ sudo systemctl start pbm-agent
   $ sudo systemctl status pbm-agent

For reference an example of starting pbm-agent manually is shown below. The
output is redirected to a file and the process is backgrounded. Alternatively
you can run it on a shell terminal temporarily if you want to observe and/or
debug the startup from the log messages.

.. code-block:: bash

   $ nohup pbm-agent --mongodb-uri "mongodb://username:password@localhost:27018/" > /data/mdb_node_xyz/pbm-agent.$(hostname -s).27018.log 2>&1 &

.. tip::
   
   Running as the ``mongod`` user would be the most intuitive and convenient way.
   But if you want it can be another user.

When a message *"pbm agent is listening for the commands"* is printed to the
|pbm-agent| log file it confirms it connected to its mongod successfully.


.. _pbm-agent.log:

How to see the pbm-agent log
--------------------------------------------------------------------------------

With the packaged systemd service the log output to stdout is captured by
systemd's default redirection to systemd-journald. You can view it with the
command below. See `man journalctl` for useful options such as '--lines',
'--follow', etc.

.. code-block:: bash

   ~$ journalctl -u pbm-agent.service
   -- Logs begin at Tue 2019-10-22 09:31:34 JST. --
   Jan 22 15:59:14 akira-x1 systemd[1]: Started pbm-agent.
   Jan 22 15:59:14 akira-x1 pbm-agent[3579]: pbm agent is listening for the commands
   ...
   ...

If you started pbm-agent manually see the file you redirected stdout and stderr
to.

Running |pbm|
================================================================================
Provide the MongoDB URI connection string for |pbm.app|. This allows you to call |pbm.app| commands without the :option:`--mongodb-uri` flag.

Use the following command:

.. code-block:: bash
 
   export PBM_MONGODB_URI="mongodb://pbmuser:secretpwd@localhost:27018/"

For more information what connection string to specify, refer to :ref:`pbm.auth.pbm.app_conn_string` section.

Running |pbm.app| commands
================================================================================

|pbm.app| is the command line utility to control the backup system.

.. contents::
   :local:

Configuring a Remote Storage for Backup and Restore Operations
--------------------------------------------------------------------------------

This must be done once, at installation or re-installation time, before backups can
be listed, made, or restored. To configure remote storage, see :ref:`pbm.config` and :ref:`storage.config`.

.. _pbm.running.backup.listing:

Listing all backups
--------------------------------------------------------------------------------

.. include:: .res/code-block/bash/pbm-list-mongodb-uri.txt

.. admonition:: Sample output

   .. code-block:: text

      2020-07-10T07:04:14Z
      2020-07-09T07:03:50Z
      2020-07-08T07:04:21Z
      2020-07-07T07:04:18Z

.. _pbm.running.backup.starting: 

Starting a backup
--------------------------------------------------------------------------------

.. code-block:: bash

   pbm backup

.. rubric:: Starting a backup with compression

.. code-block:: bash

   pbm backup --compression=s2 

``s2`` is the default compression type. Other supported compression types are: ``gzip``,
``snappy``, ``lz4``, ``pgzip``.  The ``none`` value means no compression is done during
backup.

.. rubric:: Backup in sharded clusters 

.. important::

   For PBM v1.0 (only) before running |pbm-backup| on a cluster stop the
   balancer.

In sharded clusters, one of ``pbm-agent`` processes for every shard and the config server replica set writes backup snapshots and :term:`oplog slices <Oplog slice>` into the remote backup storage directly. To learn more about oplog slicing, see :ref:`pitr`.

The ``mongos`` nodes are not involved in the backup process.

The following diagram illustrates the backup flow.

.. image:: _images/pbm-backup-shard.png

|

Checking an in-progress backup
--------------------------------------------------------------------------------

Run the |pbm-list| command and you will see the running backup listed with a
'In progress' label. When that is absent the backup is complete.

.. _pbm.running.backup.restoring: 

Restoring a backup
--------------------------------------------------------------------------------

To restore a backup that you have made using |pbm-backup|, use the
|pbm-restore| command supplying the time stamp of the backup that you intend to
restore.

.. important::

   Consider these important notes on restore operation:

   1. |pbm| is designed to be a full-database restore tool. For versions earlier than 1.x, it performs a full all-databases, all collections restore and does not offer an option to restore only a subset of collections in the backup, as MongoDB's ``mongodump`` tool does.       
      As of versions 1.x and later, |pbm| replicates ``mongodump``'s behavior to only drop collections in the backup. It does not drop collections that are created new after the time of the backup and before the restore. Run a ``db.dropDatabase()`` manually in all non-system databases (i.e. all databases except "local", "config" and "admin") before running |pbm-restore| if you want to guarantee that the post-restore database only includes collections that are in the backup.
   3. Whilst the restore is running, prevents clients from accessing the database. The data will naturally be incomplete whilst the restore is in progress, and writes the clients make cause the final restored data to differ from the backed-up data. 
   4. If you enabled :term:`Point-in-Time Recovery`, disable it before running |pbm-restore|. This is because |PITR| incremental backups and restore are incompatible operations and cannot be run together.
   
.. code-block:: bash

   $ pbm restore 2019-06-09T07:03:50Z

.. versionadded:: 1.3.2 

   The |pbm| config includes the restore options to adjust the memory consumption by the |pbm-agent| in environments with tight memory bounds. This allows preventing out of memory errors during the restore operation. 

.. code-block:: yaml

   restore:
     batchSize: 500
     numInsertionWorkers: 10

The default values were adjusted to fit the setups with the memory allocation of 1GB and less for the agent. 

.. note:: 

  The lower the values, the less memory is allocated for the restore. However, the performance decreases too.

.. rubric:: Restoring a backup in sharded clusters

.. important::

   As preconditions for restoring a backup in a sharded cluster, complete the following steps:
  
   1. Stop the balancer.
   2. Shut down all ``mongos`` nodes to stop clients from accessing the database while restore is in progress. This ensures that the final restored data doesn’t differ from the backed-up data.
   3. Disable point-in-time recovery if it is enabled. To learn more about point-in-time recovery, see :ref:`pitr`.

Note that you can restore a sharded backup only into a sharded environment. It can be your existing cluster or a new one. To learn how to restore a backup into a new environment, see :ref:`pbm.restore-new-env`.

During the restore, ``pbm-agents`` write data to primary nodes in the cluster. The following diagram shows the restore flow.

.. image:: _images/pbm-restore-shard.png

|

After a cluster's restore is complete, restart all ``mongos`` nodes to reload the sharding metadata.


.. _pbm.restore-new-env:

.. rubric:: Restoring a backup into a new environment

To restore a backup from one environment to another, consider the following key points about the destination environment:

* Replica set names (both the config servers and the shards) in your new destination cluster and in the cluster that was backed up must be exactly the same.

* |PBM| configuration in the new environment must point to the same remote storage that is defined for the original environment, including the authentication credentials if it is an object store. Once you run ``pbm list`` and see the backups made from the original environment, then you can run the ``pbm restore`` command.

  Of course, make sure not to run ``pbm backup`` from the new environment whilst the |PBM| config is pointing to the remote storage location of the original environment.

.. _pbm.cancel.backup:

Canceling a backup
--------------------------------------------------------------------------------

You can cancel a running backup if, for example, you want to do
another maintenance of a server and don't want to wait for the large backup to finish first.

To cancel the backup, use the |pbm-cancel-backup| command.

.. code-block:: bash

  $ pbm cancel-backup
  Backup cancellation has started

After the command execution, the backup is marked as canceled in the |pbm-list| output:

.. code-block:: bash

  $ pbm list
  ...
  2020-04-30T18:05:26Z	Canceled at 2020-04-30T18:05:37Z
  
.. _pbm.backup.delete:

Deleting backups
--------------------------------------------------------------------------------

Use the |pbm-delete-backup| command to delete a specified backup or all backups
older than the specified time.

The command deletes the backup regardless of the remote storage used:
either S3-compatible or a filesystem-type remote storage.

.. note::

  You can only delete a backup that is not running (has the "done" or the "error" state). 

To delete a backup, specify the ``<backup_name>`` from the the |pbm-list|
output as an argument. 

.. include:: .res/code-block/bash/pbm-delete-backup.txt

By default, the |pbm-delete-backup| command asks for your confirmation
to proceed with the deletion. To bypass it, add the ``-f`` or
``--force`` flag.

.. code-block:: bash

  $ pbm delete-backup --force 2020-04-20T13:45:59Z

To delete backups that were created before the specified time, pass the ``--older-than`` flag to the |pbm-delete-backup|
command. Specify the timestamp as an argument
for the |pbm-delete-backup| command in the following format:

* ``%Y-%M-%DT%H:%M:%S`` (e.g. 2020-04-20T13:13:20) or
* ``%Y-%M-%D`` (e.g. 2020-04-20).

.. include:: .res/code-block/bash/pbm-delete-backup-older-than-timestamp.txt

.. include:: .res/replace.txt
