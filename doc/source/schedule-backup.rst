.. _schedule: 

Schedule backups
==============================

In |PBM| version 1.4.1 and earlier, we recommend using ``crond`` or similar services to schedule backup snapshots.

.. important::

   Before configuring ``crond``, make sure that you have :ref:`installed <install>` and :ref:`configured <initial-setup>` |PBM| to make backups in your database. Start a backup manually to verify this: :command:`pbm backup`. 

The recommended approach is to create a :file:`crontab` file in the :dir:`/ect/cron.d` directory and specify the command in it. This simplifies server administration especially if multiple users have access to it.

|pbm.app| CLI requires a valid MongoDB URI connection string to authenticate in MongoDB. Instead of specifying the MongoDB URI connection string as a command line argument, which is a potential security risk, we recommend creating an environment file and specify the ``export PBM_MONGODB_URI=$PBM_MONGODB_URI`` statement within.

As an example, let's configure to run backup snapshots on 23:30 every Sunday. 
The steps are the following:

1. Create an environment file. Let's name it :file:`pbm-cron`. 
   The path for this file on Debian and Ubuntu is :file:`/etc/default/pbm-cron`. For Red Hat Enterprise Linux and CentOS, the path is :file:`/etc/sysconfig/pbm-cron`. 

#. Specify the environment variable in :file:`pbm-cron`:
   
   .. code-block:: guess
   
      export PBM_MONGODB_URI="mongodb://pbmuser:secretpwd@localhost:27018?/replSetName=xxxx" 

#. Grant access to the :file:`pbm-cron` file for the user that will execute the ``cron`` task.
#. Create a ``crontab`` file. Let's name it ``pbm-backup``. 
#. Specify the command in the file:
   
   .. code-block:: guess
   
      30 23 * * sun <user-to-execute-cron-task> . /etc/default/pbm-cron; /usr/bin/pbm backup 

 Note the dot ``.`` before the environment file. It sources (includes) the environment file for the rest of the shell commands.

#. Verify that backups are running in :file:`/var/log/cron` or :file:`/var/log/syslog` logs:
   
   .. code-block:: guess
   
      $ grep CRON /var/log/syslog

.. admonition:: Backup storage cleanup

   Previous backups are not automatically removed from the backup storage. 
   You need to remove the oldest ones periodically to limit the amount of space used in the backup storage. 

   We recommend using the :command:`pbm delete backup --older-than <timestamp>` command. You can configure a ``cron`` task to automate backup deletion by specifying the following command in the :file:`crontab` file:

   .. code-block:: guess

      /usr/bin/pbm delete-backup -f --older-than $(date -d '-1 month' +\%Y-\%m-\%d)

   This command deletes backups that are older than 30 days. You can change the period by specifying a desired interval for the ``date`` function.

Schedule backups with |PITR| running
---------------------------------------------------

It is convenient to automate making backups on a schedule using ``crond`` if you enabled :ref:`pitr`. 

You can configure |PITR| and ``crond`` in any order. Note, however, that |PITR| will only start running after at least one full backup has been made.

 * Make a fresh backup manually. It will serve as the starting point for incremental backups
 * Enable point-in-time recovery
 * Configure ``crond`` to run backup snapshots on a schedule
    
 When it is time for another backup snapshot, |PBM| automatically disables |PITR| and re-enables it once the backup is complete.

.. include:: .res/replace.txt