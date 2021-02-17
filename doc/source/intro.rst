.. _pbm.intro:

How |pbm| works
********************************************************************************

Even in a highly-available architecture, such as with |mongodb| replication, backups are still required even though losing one server is not fatal. Whether for a complete or partial data disaster, you can use PBM (Percona Backup for MongoDB) to go back in time to the best available backup snapshot.

|PBM| is a command line interface. It provides the set of commands to make backup and restore operations in your database. 

The following example illustrates how to use |PBM|.

For example, imagine your web application's update was released on February 2nd 23:00 EDT but, by 11:23 the next day, someone realizes that the update has a bug that is wiping the historical data of any user who logged in. Nobody likes to have downtime, but it's time to roll back: what's the best backup to use?

.. code-block:: bash

   $ pbm list

.. admonition:: Output

   .. code-block:: text

      Backup snapshots:
        2021-02-03T08:08:15Z [complete: 2021-02-03T08:08:35]
        2021-02-05T13:55:55Z [complete: 2021-02-05T13:56:15]
        2021-02-07T13:57:58Z [complete: 2021-02-07T13:58:17]
        2021-02-09T14:06:06Z [complete: 2021-02-09T14:06:26]
        2021-02-11T14:22:41Z [complete: 2021-02-11T14:23:01]

The most recent daily backup is 09:22 EDT (14:22 UTC), which would include 4 hours of damage caused by the bug.
Let's restore the one before that:

.. code-block:: bash

   $ pbm restore 2021-02-09T14:06:06Z 

To be on the safe side, it might be best to make an extra backup manually before the next application release:

.. code-block:: bash

   $ pbm backup

Find the full set of commands available in |PBM| in :ref:`pbm-commands`.

.. include:: .res/replace.txt
