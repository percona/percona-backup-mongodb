.. _storage.config:

Remote backup storage 
*********************************************************************************

.. contents::
   :local:

|PBM| supports the following types of remote backup storages:

* S3-compatible storage
* Filesystem type storage

.. rubric:: S3 compatible storage

|PBM| should work with other S3-compatible storages but was only tested with the following ones:

- `Amazon Simple Storage Service <https://docs.aws.amazon.com/s3/index.html>`_ 
- `Google Cloud Storage <https://cloud.google.com/storage>`_
- `MinIO <https://min.io/>`_
  
Starting from v1.3.2, |PBM| supports :term:`server-side encryption <Server-side encryption>` for :term:`S3 buckets <Bucket>` with customer managed keys stored in |AWS KMS|.

.. seealso::

   `Protecting Data Using Server-Side Encryption with CMKs Stored in AWS Key Management Service (SSE-KMS) <https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html>`_

.. rubric:: Remote Filesystem Server Storage

This storage must be a remote fileserver mounted to a local directory. It is the
responsibility of the server administrators to guarantee that the same remote
directory is mounted at exactly the same local path on all servers in the
MongoDB cluster or non-sharded replica set.

.. warning::
   |PBM| uses the directory as if it were any normal directory, and does not
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

.. _pbm.config.example_yaml:

Example config files
================================================================================

Provide the remote backup storage configuration as a YAML config file. The following are the examples of config fles for supported remote storages. For how to insert the config file, see :ref:`pbm.config.initialize`. 

.. rubric:: S3-compatible remote storage

Amazon Simple Storage Service

.. include:: .res/code-block/yaml/example-amazon-s3-storage.yaml

GCS

.. include:: .res/code-block/yaml/example-gcs-s3-storage.yaml

MinIO

.. include:: .res/code-block/yaml/example-minio-s3-storage.yaml

.. rubric:: Remote filesystem server storage

.. include:: .res/code-block/yaml/example-local-file-system-store.yaml

For the description of configuration options, see :ref:`pbm.config.options`.

.. include:: .res/replace.txt     