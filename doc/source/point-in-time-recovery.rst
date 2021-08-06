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

A slice covers a 10 minute span of oplog events. It can be shorter if |PITR| is disabled or interrupted by the start of a backup snapshot operation.

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


.. rubric:: Delete a backup

When you :ref:`delete a backup <pbm.backup.delete>`, all oplog slices that relate to this backup will be deleted too. For example, you delete a backup snapshot 2020-07-24T18:13:09 while there is another snapshot 2020-08-05T04:27:55 created after it.  |pbm-agent| deletes only oplog slices that relate to 2020-07-24T18:13:09.

 The same applies if you delete backups older than the specified time.

.. note::

   When |PITR| is enabled, the most recent backup snapshot and oplog slices that relate to it won't be deleted.


.. include:: .res/replace.txt
