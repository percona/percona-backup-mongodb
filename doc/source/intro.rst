.. _pbm.intro:

|pbm| Intro
********************************************************************************

.. rubric:: How to use |pbm|: going back in time (|pbm-restore|)

Even in a highly-available architecture, such as with |mongodb| replication, backups are still required even though losing one server suddenly is not fatal. Whether for a complete or partial data disaster you can use PBM (Percona Backup for MongoDB) to go back in time to the best available backup snapshot.

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

Next time there is an application release, it might be best to make an extra backup
manually just before:

.. include:: .res/code-block/bash/pbm-backup-mongodb-uri.txt

Percona Backup for MongoDB is an uncomplicated command-line tool by design. The full set of commands:

.. include:: .res/code-block/bash/pbm-help-output.txt

.. include:: .res/replace.txt
