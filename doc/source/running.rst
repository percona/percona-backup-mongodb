.. _pbm.running:

Running |pbm|
********************************************************************************

This document provides examples of using |pbm.app| commands to operate your backup system. For detailed description of pbm commands, refer to :ref:`pbm-commands`.

.. contents::
   :local:

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

   $ pbm backup

.. rubric:: Starting a backup with compression

.. code-block:: bash

   $ pbm backup --compression=s2 

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

   1. |pbm| is designed to be a full-database restore tool. As of version <=1.x it performs a full all-databases, all collections restore and does not offer an option to restore only a subset of collections in the backup, as MongoDB's ``mongodump`` tool does. But to avoid surprising ``mongodump`` users, as of versions 1.x |pbm| replicates mongodump's behavior to only drop collections in the backup. It does not drop collections that are created new after the time of the backup and before the restore. Run a ``db.dropDatabase()`` manually in all non-system databases (i.e. all databases except "local", "config" and "admin") before running |pbm-restore|if you want to guarantee that the post-restore database only includes collections that are in the backup.
   2. Whilst the restore is running, prevents clients from accessing the database. The data will naturally be incomplete whilst the restore is in progress, and writes the clients make cause the final restored data to differ from the backed-up data. 
   3. If you enabled :term:`Point-in-Time Recovery`, disable it before running |pbm-restore|. This is because |PITR| incremental backups and restore are incompatible operations and cannot be run together.
   
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
another maintenance and don't want to wait for the large backup to finish first.

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
