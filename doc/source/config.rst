.. _pbm.config:

|pbm| config in a Cluster (or Non-sharded Replicaset)
********************************************************************************

The config information is stored in a single document of the *admin.pbmConfig*
collection. That single copy is shared by all the |pbm-agent| processes in a
cluster (or non-sharded replicaset), and can be read or updated using the
|pbm.app| tool.

In short: you can see the whole config by running
*db.getSiblingDB("admin").pbmConfig.findOne()*. But you don't have to use the
mongo shell; the |pbm.app| CLI has a "config" subcommand to read and update it.

As of v1.0 or v1.1 the config only contains the remote storage information.

.. _pbm.config.initialize:

Insert the whole |pbm| Config from a YAML file
================================================================================

If you are initializing a cluster or non-sharded replicaset for the first time
it is simplest to write the whole config as YAML file and use the
|pbm-config-file-set| method to upload all the values in one command.

.. include:: .res/code-block/bash/pbm-config-file-set.txt

Execute whilst connecting to config server replicaset if it is
cluster. Otherwise just connect to the non-sharded replica set as normal. (See
:ref:`pbm.auth.mdb_conn_string` if you are not familiar with MongoDB connection
strings yet.)

Run |pbm-config-list| to see the whole config. (Sensitive fields such as keys
will be redacted.)

.. _pbm.config.example_yaml:

Example config files
================================================================================

.. rubric:: S3-compatible remote storage

Amazon Simple Storage Service

.. include:: .res/code-block/yaml/example-amazon-s3-storage.yaml

Minio

.. include:: .res/code-block/yaml/example-minio-s3-storage.yaml

.. rubric:: Remote Filesystem Server Storage

This storage must be a remote fileserver mounted to a local directory. It is the
responsibility of the server administrators to guarantee that the same remote
directory is mounted at exactly the same local path on all servers in the
MongoDB cluster or non-sharded replicaset.

.. warning::
   PBM uses the directory as if it was any normal directory, and does not
   attempt to confirm it is mounted from a remote server.
   If the path is accidentally a normal, local directory errors will eventually
   occur, most likely during a restore attempt. This will happen because
   |pbm-agent| processes of other nodes in the same replicaset can't access
   backup archive files in a normal local directory on another server.
   
.. include:: .res/code-block/yaml/example-local-file-system-store.yaml

.. rubric:: Local Filesystem Storage

This cannot be used except if you have a single-node replicaset. (See warning
note above as to why). We recommend using any object store you might be already
familiar with for testing. If you don't have an object store yet we recommend
using Minio for testing as it has simple setup. If you plan to use a remote
filesytem-type backup server please see "Remote Filesystem Server Storage"
above.

.. rubric:: Accessing or updating single config values

You can set a single value at time. For nested values use dot-concatenated key
names as shown in the following example:

.. code-block:: bash

   $ pbm config --set storage.s3.bucket="operator-testing"

To list a single value you can specify just the key name by itself and the value
will be returned (if set)

.. code-block:: bash

   $ pbm config storage.s3.bucket
   operator-testing
   $ pbm config storage.s3.INVALID-KEY
   Error: unable to get config key: invalid config key

.. include:: .res/replace.txt
