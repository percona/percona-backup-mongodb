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

   Backup snapshots:
  	2020-12-10T12:19:10Z [complete: 2020-12-10T12:23:50]
  	2020-12-14T10:44:44Z [complete: 2020-12-14T10:49:03]
  	2020-12-14T14:26:20Z [complete: 2020-12-14T14:34:39]
  	2020-12-17T16:46:59Z [complete: 2020-12-17T16:51:07]
   PITR <on>:
  	2020-12-14T14:26:40 - 2020-12-16T17:27:26
  	2020-12-17T16:47:20 - 2020-12-17T16:57:55

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

Restoring to the point in time requires both a backup snapshot and oplog slices that can be replayed on top of this backup. The time stamp you specify for the restore must meet the following conditions:

* It must be from the time ranges in the PITR section of ``pbm list`` output
* It must be later than the backup snapshot used for the restore. 

To illustrate this behavior, let’s use the ``pbm list`` output from the previous example. For timestamp ``2020-12-14T14:27:04``, 
the backup snapshot ``2020-12-14T14:26:20Z [complete: 2020-12-14T14:34:39]`` is used as the base for the restore as it is the most recent one.

.. seealso::

   :ref:`pbm.running.backup.restoring`.

A restore operation changes the time line of oplog events. Therefore, all oplog slices made after the restore time stamp and before the last backup become invalid. After the restore is complete, make a new backup to serve as the starting point for oplog updates: 

.. code-block:: bash

   $ pbm backup

Re-enable |PITR| to resume saving oplog slices:  

.. code-block:: bash

   $ pbm config --set pitr.enabled=true

As of version 1.6.0, you can recover your database to the specific point in time using any backup snapshot, and not only the most recent one. Run the ``pbm restore`` command with the ``--base-snapshot=<backup_name>`` flag where you specify the desired backup snapshot. 

To restore from any backup snapshot, |PBM| requires continuous oplog. After the backup snapshot is made and |PITR| is re-enabled, it copies the oplog saved with the backup snapshot and creates oplog slices from the end time of the latest slice to the new starting point thus making the oplog continuous.


.. rubric:: Delete a backup

When you :ref:`delete a backup <pbm.backup.delete>`, all oplog slices that relate to this backup will be deleted too. For example, you delete a backup snapshot 2020-07-24T18:13:09 while there is another snapshot 2020-08-05T04:27:55 created after it.  |pbm-agent| deletes only oplog slices that relate to 2020-07-24T18:13:09.

 The same applies if you delete backups older than the specified time.

.. note::

   When |PITR| is enabled, the most recent backup snapshot and oplog slices that relate to it won't be deleted.


.. include:: .res/replace.txt
