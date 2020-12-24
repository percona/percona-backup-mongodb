.. _pbm-commands:


|pbm.app| commands
**********************************************************************

``pbm CLI`` is the command line utility to control the backup system. This page describes |pbm.app| commands available in |PBM|.

For how to get started with |PBM|, see :ref:`initial-setup`.

:command:`pbm help`

Returns the help information about |pbm.app| commands.

:command:`pbm config`

Sets, changes or lists |PBM| configuration.

The command has the following syntax:

.. code-block:: bash

   $ pbm config [<flags>] [<key>]

The command accepts the following flags:

.. list-table:: 
   :header-rows: 1
   :widths: 30 70

   * - Flag
     - Description
   * - ``--force-resync``
     - Resync backup list with the current storage
   * - ``--list``
     - List current settings
   * - ``--file=FILE`` 
     - Upload the config information from a YAML file
   * - ``--set=SET``
     - Set a new config option value. Specify the option in the <key.name=value> format.
   
:command:`pbm backup`

Creates a backup snapshot and saves it in the remote backup storage. 

The command has the following syntax:

.. code-block:: bash

   $ pbm backup [<flags>]

For more information about using ``pbm backup``, see :ref:`pbm.running.backup.starting` 

The command accepts the following flags:

.. list-table:: 
   :header-rows: 1
   :widths: 30 70

   * - Flag
     - Description
   * - ``--compression``
     - Create a backup with compression. 
       Supported compression methods: ``gzip``, ``snappy``, ``lz4``, ``s2``, ``pgzip``. Default: ``s2``
       The ``none`` value means no compression is done during backup.
   
:command:`pbm restore`

Restores database from a specified backup / to a specified point in time. 

The command has the following syntax:

.. code-block:: bash

   $ pbm restore [<flags>] [<backup_name>]

For more information about using ``pbm restore``, see :ref:`pbm.running.backup.restoring`.

The command accepts the following flags:

.. list-table:: 
   :header-rows: 1
   :widths: 30 70

   * - Flag
     - Description
   * - ``--time=TIME``
     - Restores the database to the specified point in time. Available if :ref:`PITR` is enabled.
       
:command:`pbm cancel-backup`

Cancels a running backup. The backup is marked as canceled in the backup list.

:command:`pbm list`

Provides the list of backups. In versions 1.3.4 and earlier, the command listed all backups and their states. Backup states are the following:

- In progress - A backup is running
- Canceled - A backup was canceled
- Error - A backup was finished with an error
- No status means a backup is complete

As of version 1.4.0, only successfully completed backups are listed. To view currently running backup information, run `pbm status`.
  
When :ref:`PITR` is enabled, the ``pbm list`` also provides the list of valid time ranges for recovery and point-in-time recovery status. 

The command has the following syntax:

.. code-block:: bash

   $ pbm list [<flags>]

The command accepts the following flags:

.. list-table:: 
   :header-rows: 1
   :widths: 30 70

   * - Flag
     - Description
   * - ``--restore``
     - Shows last N restores.
   * - ``--size=0``
     - Shows last N backups.

:command:`pbm delete-backup`

Deletes the specified backup or all backups that are older than the specified time. The command deletes backups that are not running regardless of the remote backup storage being used.

The following is the command syntax:

.. code-block:: bash

   $ pbm delete-backup [<flags>] [<name>]

The command accepts the following flags:

.. list-table:: 
   :header-rows: 1
   :widths: 30 70

   * - Flag
     - Description
   * - ``--older-than=TIMESTAMP``
     - Deletes backups older than date / time in the format ``%Y-%M-%DT%H:%M:%S`` (e.g. 2020-04-20T13:13:20) or ``%Y-%M-%D`` (e.g. 2020-04-20)
   * - ``--force``
     - Forcibly deletes backups without asking for user's confirmation   

:command:`pbm version`

Shows the version of |PBM|.

The command accepts the following flags:

.. list-table:: 
   :header-rows: 1
   :widths: 30 70

   * - Flag
     - Description
   * - ``--short``
     - Shows only version info
   * - ``--commit``
     - Shows only git commit info

.. include:: .res/replace.txt
