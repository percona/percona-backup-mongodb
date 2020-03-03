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
sure one instance of it is started for each mongod node.

E.g. Imagine you put configsvr nodes (listen port 27019) colocated on the same
servers as the first shard's mongod nodes (listen port 27018, replica set name
"sh1rs"). In this server there should be two 
|pbm-agent| processes, one connected to the shard
(e.g. "mongodb://username:password@localhost:27018/") and one to the configsvr
node (e.g. "mongodb://username:password@localhost:27019/").

It is best to use the packaged service scripts to run |pbm-agent|. After
adding the database connection configuration for them (see
:ref: pbm.installation.service_init_scripts) you can start the |pbm-agent|
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
   
   Running as the mongod user would be the most intuitive and convenient way.
   But if you want it can be another user.

You can confirm the |pbm-agent| connected to its mongod and started OK by
confirming *"pbm agent is listening for the commands"* is printed to the log
file.

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

Running |pbm.app| Commands
================================================================================

|pbm.app| is the command line utility to control the backup system.

Configuring a Remote Store for Backup and Restore Operations
--------------------------------------------------------------------------------

This must be done once, at installation or re-installaton time, before backups can
be listed, made, or restored. Please see :ref:`pbm.config`.

.. _pbm.running.backup.listing:

Listing all backups
--------------------------------------------------------------------------------

.. include:: .res/code-block/bash/pbm-list-mongodb-uri.txt

.. admonition:: Sample output

   .. code-block:: text

      2019-09-10T07:04:14Z
      2019-09-09T07:03:50Z
      2019-09-08T07:04:21Z
      2019-09-07T07:04:18Z

.. _pbm.running.backup.starting: 

Starting a backup
--------------------------------------------------------------------------------

.. include:: .res/code-block/bash/pbm-backup-mongodb-uri.txt

.. important::

   For PBM v1.0 (only) before running |pbm-backup| on a cluster stop the
   balancer.

Checking an in-progress backup
--------------------------------------------------------------------------------

Run the |pbm-list| command and you will see the running backup listed with a
'In progress' label. When that is absent the backup is complete.

.. _pbm.running.backup.restoring: 

Restoring a Backup
--------------------------------------------------------------------------------

To restore a backup that you have made using |pbm-backup| you should use the
|pbm-restore| command supplying the timestamp of the backup that you intend to
restore.

.. important::

   For PBM v1.0 (only) before running |pbm-restore| on a cluster stop the
   balancer.

.. important::

   Whilst the restore is running, clients should be stopped from accessing the
   database. The data will naturally be incomplete whilst the restore is in
   progress, and writes they make will cause the final restored data to differ
   from the backed-up data. In a cluster's restore the simplest way would be to
   shutdown all mongos nodes.

.. important::

   |pbm| is designed to be a full-database restore tool. As of version <=1.x it
   will perform a full all-databases, all collections restore and does not
   offer an option to restore only a subset of collections in the backup, as
   MongoDB's mongodump tool does. But to avoid surprising mongodump users |pbm|
   as of now (versions 1.x) replicates mongodump's behaviour to only drop
   collections in the backup. It does not drop collections that are created new
   after the time of the backup and before the restore. Run a db.dropDatabase()
   manually in all non-system databases (i.e. all databases except "local",
   "config" and "admin") before running |pbm-restore| if you want to guarantee
   the post-restore database only includes collections that are in the backup.

.. include:: .res/code-block/bash/pbm-restore-mongodb-uri.txt

After a cluster's restore is complete all mongos nodes will need to be
restarted to reload the sharding metadata.

Deleting backups
--------------------------------------------------------------------------------

Use the relevant native ``rm`` command for the storage. E.g. for S3-compatible
object stores use ``aws s3 rm <file name>``. For a filesystem-type storage, use
normal shell command ``rm``.

.. include:: .res/replace.txt
