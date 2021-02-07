.. _pbm.config.options:

Configuration file options
********************************************************************************

This page describes configuration file options available in |PBM|. For how to use configuration file, see :ref:`pbm.config`.

.. contents::
   :local:

.. _pbm.storage.config.options:

Remote backup storage options
============================================================================

|PBM| supports two types of remote storages: S3-compatible storages and filesystem. |PBM| should work with other S3-compatible storages but was only tested with the following ones: 

- `Amazon Simple Storage Service <https://docs.aws.amazon.com/s3/index.html>`_, 
- `Google Cloud Storage <https://cloud.google.com/storage>`_, 
- `MinIO <https://min.io/>`_.

.. option:: storage.type
   
   :type: string
   :required: YES

   Remote backup storage type. Supported values: s3, filesystem.

S3 type storage options
-------------------------------------------------------------------------------

.. code-block:: yaml

   storage:
     type: s3
     s3:
       region: <string>
       bucket: <string>
       prefix: <string>
       credentials:
         access-key-id: <your-access-key-id-here>
         secret-access-key: <your-secret-key-here>
       serverSideEncryption:
         sseAlgorithm: aws:kms
         kmsKeyID: <your-kms-key-here>

.. option:: storage.s3.provider
   
   :type: string
   :required: NO
   
   The storage provider's name. Supported values: aws, gcs
   
.. option:: storage.s3.bucket
  
   :type: string
   :required: YES

   The name of the storage :term:`bucket <Bucket>`. See the `AWS Bucket naming rules <https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules>`_ and `GCS bucket naming guidelines <https://cloud.google.com/storage/docs/naming-buckets#requirements>`_ for bucket name requirements

.. option:: storage.s3.region

   :type: string
   :required: YES (for AWS and GCS)
  
   The location of the storage bucket. 
   Use the `AWS region list <https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region>`_ and `GCS region list <https://cloud.google.com/storage/docs/locations>`_ to define the bucket region
   
.. option:: storage.s3.prefix

   :type: string
   :required: NO
   
   The path to the data directory on the bucket. If undefined, backups are stored in the bucket root directory

.. option:: storage.s3.endpointUrl

   :type: string
   :required: YES (for MinIO and GCS)

   The URL to access the bucket. The default value for GCS is ``https://storage.googleapis.com``

.. option:: storage.s3.credentials.access-key-id

   :type: string
   :required: YES
   
   Your access key to the storage bucket
   
.. option:: storage.s3.credentials.secret-access-key

  :type: string
  :required: YES
  
  The key to sign your programmatic requests to the storage bucket 

.. option:: storage.s3.uploadPartSize
    
   :type: int
   :required: NO
  
   The size of data chunks in bytes to be uploaded to the storage bucket. Default: 10MB
       
   |PBM| automatically increases the ``uploadPartSize`` value if the size of the file to be uploaded exceeds the max allowed file size. (The max allowed file size is calculated with the default values of uploadPartSize * `maxUploadParts <https://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#pkg-constants>`_ and is appr. 97,6 GB).

   The ``uploadPartSize`` value is printed in the :ref:`pbm-agent log <pbm-agent.log>`.

   By setting this option, you can manually adjust the size of data chunks if |PBM| failed to do it for some reason. The defined ``uploadPartSize`` value overrides the default value and is used for calculating the max allowed file size

.. rubric:: Server-side encryption options

.. option:: serverSideEncryption.sseAlgorythm
   
   :type: string
  
   The key management mode used for server-side encryption 

   Supported value: ``aws:kms``
   
.. option:: serverSideEncryption.kmsKeyID
     
   :type: string
  
   Your customer-managed key

Filesystem storage options
-------------------------------------------------------------------------------

.. code-block:: yaml

   storage:
     type: filesystem
     filesystem:
       path: <string>

.. option:: storage.filesystem.path

   :type: string
   :required: YES
   
   The path to the backup directory

.. _pitr.config:

Point-in-time recovery options
=================================================================================

.. code-block:: yaml

   pitr:
     enabled: <boolean> 

.. option:: pitr.enabled
  
   :type: boolean
   
   Enables point-in-time recovery

.. _restore.config:

Backup restore options
=================================================================================

.. code-block:: yaml

   restore:
     batchSize: <int>
     numInsertionWorkers: <int>
     preserveUUID: <bool>

.. option:: batchSize
   
   :type: int
   :default: 500

   The number of documents to buffer. 

.. option:: numInsertionWorkers 

   :type: int
   :default: 10

   The number of workers that add the documents to buffer.

.. option:: preserveUUID
   :type: bool
   :default: false

   Preserve collection UUID from backup source.
       
.. include:: .res/replace.txt      