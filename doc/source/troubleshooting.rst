.. _pbm.troubleshooting:

==============================================
Troubleshooting |pbm|
==============================================

|pbm| provides troubleshooting tools to operate data backups.

.. contents::
   :local:
   :depth: 1

pbm-speed-test
==============================================

|pbm-speed-test| allows field-testing compression and backup upload speed. You
can use it:

* to check performance before starting a backup;
* to find out what slows down the running backup.

By default, |pbm-speed-test| operates with fake semi random data documents. To
run |pbm-speed-test| on a real collection, provide a valid :ref:`MongoDB connection URI string <pbm.auth.mdb_conn_string>` for the ``--mongodb-uri`` flag.

Run :command:`pbm-speed-test` for the full set of available commands.

Compression test
----------------------------------------------

.. code-block:: bash

   $ pbm-speed-test compression --compression=s2 --size-gb 10
   Test started ....
   10.00GB sent in 8s.
   Avg upload rate = 1217.13MB/s.


:command:`pbm-speed-test compression` uses the compression library from the config
file and sends a fake semi random data document (1 GB by default) to the
black hole storage. (Use the ``pbm config`` command to change the compression library). 

To test compression on a real collection, pass the
``--sample-collection`` flag with the <my_db.my_collection> value.

Run |pbm-speed-test-compression-help| for the full set of supported flags:

.. code-block:: bash

  $ pbm-speed-test compression --help
  usage: pbm-speed-test compression

  Run compression test

  Flags:
        --help                     Show context-sensitive help (also try
                                   --help-long and --help-man).
        --mongodb-uri=MONGODB-URI  MongoDB connection string
    -c, --sample-collection=SAMPLE-COLLECTION  
                                   Set collection as the data source
    -s, --size-gb=SIZE-GB          Set data size in GB. Default 1
        --compression=s2           Compression type
                                   <none>/<gzip>/<snappy>/<lz4>/<s2>/<pgzip>


Upload speed test
----------------------------------------------------

.. code-block:: bash

   $ pbm-speed-test storage --compression=s2
   Test started 
   1.00GB sent in 1s.
   Avg upload rate = 1744.43MB/s.

|pbm-speed-test-storage| sends the semi random data (1 GB by default) to the
remote storage defined in the config file. Pass the ``--size-gb`` flag to change the
data size. 

To run the test with the real collection's data instead of the semi random data,
pass the ``--sample-collection`` flag with the <my_db.my_collection> value.

Run |pbm-speed-test-storage-help| for the full set of available flags:

.. code-block:: bash

   $ pbm-speed-test storage --help
   usage: pbm-speed-test storage

   Run storage test

   Flags:
         --help                     Show context-sensitive help (also try --help-long and --help-man).
         --mongodb-uri=MONGODB-URI  MongoDB connection string
     -c, --sample-collection=SAMPLE-COLLECTION  
                                    Set collection as the data source
     -s, --size-gb=SIZE-GB          Set data size in GB. Default 1
         --compression=s2           Compression type <none>/<gzip>/<snappy>/<lz4>/<s2>/<pgzip>

Backup progress logs
============================================================================

If you have a large backup you can track backup progress in |pbm-agent| logs. A line is appended every 
minute showing bytes copied vs. total size for the current collection.

.. include:: .res/code-block/bash/pbm-agent-backup-progress-log.txt

.. include:: .res/replace.txt
