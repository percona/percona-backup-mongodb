.. _pitr:

Point-in-Time Recovery
********************************************************************************

|PITR| is restoring a database up to a specific moment. |PITR| includes restoring the data from a backup snapshot and replaying all events that occurred to this data up to a specified moment from :term:`oplog slices <Oplog slice>`. |PITR| helps you prevent data loss during a disaster such as crashed database, accidental data deletion or drop of tables, unwanted update of multiple fields instead of a single one.

|PITR| is available in |PBM| starting from v1.3.0. |PITR| is enabled via the ``pitr.enabled`` config option.

.. code-block:: bash

   $ pbm config --set pitr.enabled=true

Incremental backups
================================================================================

When |PITR| is enabled, |pbm-agent| periodically saves consecutive slices of the :term:`oplog <Oplog>`. A method similar to the way replica set nodes elect a new primary is used to select the |pbm-agent| that saves the oplog slices. (Find more information in :ref:`pbm.architecture.agent`.)

By default, a slice covers a 10 minute span of oplog events. It can be shorter if |PITR| is disabled or interrupted by the start of a backup snapshot operation.

As of version 1.6.0, you can change the duration of an oplog span via the configuration file. Specify the new value (in minutes) for the ``pitr.oplogSpanMin`` option.

.. code-block:: bash

   $ pbm config --set pitr.oplogSpanMin=5

If you set the new duration when the |pbm-agent| is making an oplog slice, the slice’s span is updated right away.  

If the new duration is shorter, this triggers the |pbm-agent| to make a new slice with the updated span immediately. If the new duration is larger,  the |pbm-agent| makes the next slice with the updated span in its scheduled time.

The oplog slices are stored in the :file:`pbmPitr` subdirectory in the :ref:`remote storage defined in the config <storage.config>`. A slice name reflects the start and end time this slice covers. 

The |pbm-list| output includes the following information:

- Backup snapshots. As of version 1.4.0, it also shows the completion time
- Valid time ranges for recovery
- |PITR| status.

.. code-block:: bash

   $ pbm list

     2021-08-04T13:00:58Z [complete: 2021-08-04T13:01:23]
     2021-08-05T13:00:47Z [complete: 2021-08-05T13:01:11]
     2021-08-06T08:02:44Z [complete: 2021-08-06T08:03:09]
     2021-08-06T08:03:43Z [complete: 2021-08-06T08:04:08]
     2021-08-06T08:18:17Z [complete: 2021-08-06T08:18:41]

   PITR <off>:
     2021-08-04T13:01:24 - 2021-08-05T13:00:11
     2021-08-06T08:03:10 - 2021-08-06T08:18:29
     2021-08-06T08:18:42 - 2021-08-06T08:33:09

.. note::

   If you just enabled |PITR|, the time range list in the ``pbm list`` output is empty. It requires 10 minutes for the first chunk to appear in the list.  

Restore to the point in time
============================

A restore and |PITR| incremental backups are incompatible operations and cannot be run simultaneously. You must disable |PITR| before restoring a database:

.. code-block:: bash
   
   $ pbm config --set pitr.enabled=false

Run |pbm-restore| and specify the timestamp from the valid range:

.. code-block:: bash

   $ pbm restore --time="2020-12-14T14:27:04"

Restoring to the point in time requires both a backup snapshot and oplog slices that can be replayed on top of this backup. The timestamp you specify for the restore must  be within the time ranges in the PITR section of ``pbm list`` output. |PBM| automatically selects the most recent backup in relation to the specified timestamp and uses that as the base for the restore.

To illustrate this behavior, let’s use the ``pbm list`` output from the previous example. For timestamp ``2021-08-06T08:10:10``, 
the backup snapshot ``2021-08-06T08:02:44Z [complete: 2021-08-06T08:03:09]`` is used as the base for the restore as it is the most recent one.

If you :ref:`select a backup snapshot for the restore with the --base-snapshot option <select-backup>`, the timestamp for the restore must also be later than the selected backup. 

.. seealso::

   :ref:`pbm.running.backup.restoring`.

A restore operation changes the time line of oplog events. Therefore, all oplog slices made after the restore time stamp and before the last backup become invalid. After the restore is complete, make a new backup to serve as the starting point for oplog updates: 

.. code-block:: bash

   $ pbm backup

Re-enable |PITR| to resume saving oplog slices:  

.. code-block:: bash

   $ pbm config --set pitr.enabled=true

.. _select-backup:

.. rubric:: Selecting a backup snapshot for the restore

As of version 1.6.0, you can recover your database to the specific point in time using any backup snapshot, and not only the most recent one. Run the ``pbm restore`` command with the ``--base-snapshot=<backup_name>`` flag where you specify the desired backup snapshot. 

To restore from any backup snapshot, |PBM| requires continuous oplog. After the backup snapshot is made and |PITR| is re-enabled, it copies the oplog saved with the backup snapshot and creates oplog slices from the end time of the latest slice to the new starting point thus making the oplog continuous.


Delete backups
========================

As of version 1.6.0, backup snapshots and incremental backups (oplog slices) are deleted using separate commands: :command:`pbm delete-backup` and :command:`pbm delete-pitr`  respectively.

Running :command:`pbm delete-backup` deletes any backup snapshot but for the following ones:

-  A backup that can serve as the base for any point in time recovery and has |PITR| time ranges deriving from it
-  The most recent backup if |PITR| is enabled and there are no oplog slices following this backup yet.

To illustrate this, let’s take the following ``pbm list`` output:

.. code-block:: bash

   $ pbm list
   Backup snapshots:
   2021-07-20T03:10:59Z [complete: 2021-07-20T03:21:19]
   2021-07-21T22:27:09Z [complete: 2021-07-21T22:36:58]
   2021-07-24T23:00:01Z [complete: 2021-07-24T23:09:02]
   2021-07-26T17:42:04Z [complete: 2021-07-26T17:52:21]

   PITR <on>:
   2021-07-21T22:36:59-2021-07-22T12:20:23
   2021-07-24T23:09:03-2021-07-26T17:52:21

You can delete a backup ``2021-07-20T03:10:59Z`` since it has no time ranges for point-in-time recovery deriving from it. You cannot delete ``2021-07-21T22:27:09Z`` as it can be the base for recovery to any point in time from the ``PITR`` time range ``2021-07-21T22:36:59-2021-07-22T12:20:23``. Nor can you delete ``2021-07-26T17:42:04Z`` backup since there are no oplog slices following it yet. 


Running :command:`pbm delete-pitr` allows you to delete old and/or unnecessary slices and save storage space. You can either delete all chunks by passing the  ``--all`` flag. Or you can delete all slices that are made earlier than the specified time by passing the ``--older-than`` flag. In this case, specify the timestamp as an argument for :command:`pbm delete-pitr` in the following format:

* ``%Y-%M-%DT%H:%M:%S`` (for example, 2021-07-20T10:01:18) or
* ``%Y-%M-%D`` (2021-07-20).

.. code-block:: bash 

   $ pbm delete-pitr --older-than 2021-07-20T10:01:18

.. note::

   To enable point in time recovery from the most recent backup snapshot, |PBM| does not delete slices that were made after that snapshot. For example, if the most recent snapshot is ``2021-07-20T07:05:23Z [complete: 2021-07-21T07:05:44]`` and you specify the timestamp ``2021-07-20T07:05:44``, |PBM| deletes only slices that were made before ``2021-07-20T07:05:23Z``.


For |PBM| 1.5.0 and earlier versions, when you :ref:`delete a backup <pbm.backup.delete>`, all oplog slices that relate to this backup are deleted too. For example, you delete a backup snapshot 2020-07-24T18:13:09 while there is another snapshot
2020-08-05T04:27:55 created after it.  |pbm-agent| deletes only oplog slices that relate to 2020-07-24T18:13:09.

The same applies if you delete backups older than the specified time.

Note that when |PITR| is enabled, the most recent backup snapshot and oplog slices that relate to it are not deleted.


.. include:: .res/replace.txt
