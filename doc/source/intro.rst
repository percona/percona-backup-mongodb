.. _pbm.intro:

How |pbm| works
********************************************************************************

Even in a highly-available architecture, such as with |mongodb| replication, backups are still required even though losing one server is not fatal. Whether for a complete or partial data disaster, you can use PBM (Percona Backup for MongoDB) to go back in time to the best available backup snapshot.

|PBM| is a command line interface. It provides the set of commands to make backup and restore operations in your database. 

The following example illustrates how to use |PBM|.

For example, imagine your web application's update was released on Sunday, June 9th 23:00 EDT but, by 11:23 Monday, someone realizes that the update has a bug that is wiping the historical data of any user who logged in. Nobody likes to have downtime, but it's time to roll back: what's the best backup to use?

.. include:: .res/code-block/bash/pbm-list-mongodb-uri.txt

.. admonition:: Output

   .. code-block:: text

      $ pbm list
      2019-09-10T07:04:14Z
      2019-09-09T07:03:50Z
      2019-09-08T07:04:21Z
      2019-09-07T07:04:18Z

The most recent daily backup is 03:04 EDT (07:04 UTC), which would include 4 hours of damage caused by the bug.
Let's restore the one before that:

.. include:: .res/code-block/bash/pbm-restore-mongodb-uri.txt

To be on the safe side, it might be best to make an extra backup manually before the next application release:

.. include:: .res/code-block/bash/pbm-backup-mongodb-uri.txt

Find the full set of commands available in |PBM| in :ref:`pbm-commands`.

.. include:: .res/code-block/bash/pbm-help-output.txt

.. include:: .res/replace.txt
