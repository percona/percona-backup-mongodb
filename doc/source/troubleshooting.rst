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

The full set of commands:

.. include:: .res/code-block/bash/pbm-speed-test-help.txt

By default, |pbm-speed-test| operates with fake semi random data documents. To
run |pbm-speed-test| on a real collection, you require a connection to the
|mongodb|. See :ref:`pbm.auth.mdb_conn_string` for details.    

Compression test
----------------------------------------------

.. include:: .res/code-block/bash/pbm-speed-test-compression.txt

|pbm-speed-test-compression| uses the compression library from the config
file and sends a fake semi random data document (1 GB by default) to the
black hole storage. (Use the ``pbm config`` command to change the compression library). 
After the test, the data is removed.

To test compression on a real collection, pass the
``--sample-collection`` flag with the <my_db.my_collection> value.

Run |pbm-speed-test-compression-help| for the full set of supported flags:

.. include:: .res/code-block/bash/pbm-speed-test-compression-help.txt

Upload speed test
----------------------------------------------------

.. include:: .res/code-block/bash/pbm-speed-test-storage.txt

|pbm-speed-test-storage| sends the semi random data (1 GB by default) to the
remote storage defined in the config file. Pass the ``--size-gb`` flag to change the
data size. 

Run |pbm-speed-test-storage-help| for the full set of available flags:

.. include:: .res/code-block/bash/pbm-speed-test-storage-help.txt

Backup progress logs
============================================================================

Track backup progress in |pbm-agent| logs. Log messages are written
every minute.

.. include:: .res/code-block/bash/pbm-agent-backup-progress-log.txt

.. include:: .res/replace.txt
