.. _pbm.config:

|pbm| config in a Cluster (or Non-sharded Replica set)
********************************************************************************

The configuration information is stored in a single document of the *admin.pbmConfig*
collection. That single copy is shared by all the |pbm-agent| processes in a
cluster (or non-sharded replica set), and can be read or updated using the
|pbm.app| tool.

You can see the whole config by running
*db.getSiblingDB("admin").pbmConfig.findOne()*. But you don't have to use the
mongo shell; the |pbm.app| CLI has a "config" subcommand to read and update it.

As of v1.0 or v1.1, the config contains the remote storage information.
Starting from v1.3.0, it also includes the :ref:`pitr` configuration.

.. rubric:: S3 compatible storage

|PBM| should work with other S3-compatible storages but was only tested with the following ones:

- `Amazon Simple Storage Service <https://docs.aws.amazon.com/s3/index.html>`_, 
- `Google Cloud Storage <https://cloud.google.com/storage>`_, 
- `MinIO <https://min.io/>`_.

.. rubric:: Remote Filesystem Server Storage

This storage must be a remote fileserver mounted to a local directory. It is the
responsibility of the server administrators to guarantee that the same remote
directory is mounted at exactly the same local path on all servers in the
MongoDB cluster or non-sharded replica set.

.. warning::
   |PBM| uses the directory as if it was any normal directory, and does not
   attempt to confirm it is mounted from a remote server.
   If the path is accidentally a normal local directory, errors will eventually
   occur, most likely during a restore attempt. This will happen because
   |pbm-agent| processes of other nodes in the same replica set can't access
   backup archive files in a normal local directory on another server.

.. rubric:: Local Filesystem Storage

This cannot be used except if you have a single-node replica set. (See the warning
note above as to why). We recommend using any object store you might be already
familiar with for testing. If you don't have an object store yet, we recommend
using MinIO for testing as it has simple setup. If you plan to use a remote
filesytem-type backup server, please see the "Remote Filesystem Server Storage"
above.

.. _pbm.config.initialize:

Insert the whole |pbm| config from a YAML file
================================================================================

If you are initializing a cluster or non-sharded replica set for the first time, it is simplest to write the whole config as YAML file and use the
|pbm-config-file-set| method to upload all the values in one command.

.. include:: .res/code-block/bash/pbm-config-file-set.txt

Execute whilst connecting to config server replica set if it is a
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

GCS

.. include:: .res/code-block/yaml/example-gcs-s3-storage.yaml

MinIO

.. include:: .res/code-block/yaml/example-minio-s3-storage.yaml

.. rubric:: Remote filesystem server storage

.. include:: .res/code-block/yaml/example-local-file-system-store.yaml

.. _pbm.storage.config.options:

Configuration options
================================================================================

S3 storage options
-------------------------------------------------------------------------------

.. list-table::
   :widths: 30 10 20 40
   :header-rows: 1

   * - Option
     - Type
     - Mandatory
     - Description
   * - ``storage.s3.provider``
     - string
     - NO
     - The storage provider's name. 
       Supported values: aws, gcs
   * - ``storage.s3.bucket``
     - string
     - YES
     - The name of the storage :term:`bucket <Bucket>`. See the `AWS Bucket naming rules <https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules>`_ and `GCS bucket naming guidelines <https://cloud.google.com/storage/docs/naming-buckets#requirements>`_ for bucket name requirements.
   * - ``storage.s3.region``
     - string
     - YES (for AWS and GCS)
     - The location of the storage bucket. 
       Use the `AWS region list <https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region>`_ and `GCS region list <https://cloud.google.com/storage/docs/locations>`_ to define the bucket region.
   * - ``storage.s3.prefix``
     - string
     - NO
     - The path to the data directory on the bucket. If undefined, backups are stored in the bucket root directory.
   * - ``storage.s3.endpointUrl``
     - string
     - YES (for MinIO and GCS)
     - The URL to access the bucket.
       The default value for GCS is ``https://storage.googleapis.com``
   * - ``storage.s3.credentials.access-key-id``
     - string
     - YES
     - Your access key to the storage bucket
   * - ``storage.s3.credentials.secret-access-key``
     - string
     - YES
     - The key to sign your programmatic requests to the storage bucket 

Filesystem storage options
-------------------------------------------------------------------------------

.. list-table::
   :widths: 30 10 20 40
   :header-rows: 1

   * - Option
     - Type
     - Mandatory
     - Description
   * - ``storage.filesystem.path``
     - string
     - YES
     - The path to the backup directory

.. _pbm.config.update:

Accessing or updating single config values
================================================================================

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
