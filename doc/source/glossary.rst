==========
 Glossary
==========

.. glossary::

  ACID
     Set of properties that guarantee database transactions are
     processed reliably. Stands for :term:`Atomicity`,
     :term:`Consistency`, :term:`Isolation`, :term:`Durability`.

  Amazon S3
     Amazon S3 (Simple Storage Service) is an object storage service provided through a web service interface offered by Amazon Web Services.

  Atomicity
     Atomicity means that database operations are applied following a
     "all or nothing" rule. A transaction is either fully applied or not
     at all.

  Blob
    A blob stands for Binary Large Object, which includes objects such as images and multimedia files. In other words these are various data files that you store in Microsoft’s data storage platform. Blobs are organized in :term:`containers <Container>` which are kept in Azure Blob storage under your storage account.

  Bucket
     A bucket is a container on the s3 remote storage that stores backups.

  Collection
     A collection is the way data is organized in MongoDB. It is analogous to a table in relational databases.

  Consistency
     In the context of backup and restore, consistency means that the data restored will be consistent in a given point in time. Partial or incomplete writes to disk of atomic operations (for example, to table and index data structures separately) won't be served to the client after the restore. The same applies to multi-document transactions, that started but didn't complete by the time the backup was finished.

  Container 
    A container is like a directory in Azure Blob storage that contains a set of :term:`blobs <Blob>`.

  Durability
     Once a transaction is committed, it will remain so.

  GCP
     GCP (Google Cloud Platform) is the set of services, including storage service, that runs on Google Cloud infrastructure.

  Isolation
     The Isolation requirement means that no transaction can interfere
     with another.

  Jenkins
     `Jenkins <http://www.jenkins-ci.org>`_ is a continuous integration
     system that we use to help ensure the continued quality of the
     software we produce. It helps us achieve the aims of:

     * no failed tests in trunk on any platform,
     * aid developers in ensuring merge requests build and test on all platforms,
     * no known performance regressions (without a damn good explanation).

  MinIO
     MinIO is a cloud storage server compatible with :term:`Amazon S3`, released under Apache License v2.

  Oplog
     Oplog (operations log) is a fixed-size collection that keeps a rolling record of all operations that modify data in the database. 

  Oplog slice
     A compressed bundle of :term:`oplog <Oplog>` entries stored in the Oplog Store database in MongoDB. The oplog size captures an approximately 10-minute frame. For a snapshot, the oplog size is defined by the time that the slowest replica set member requires to perform mongodump.    

  OpID
    A unique identifier of an operation such as backup, restore, resync. When a pbm-agent starts processing an operation, it acquires a lock and an opID. This prevents processing the same operation twice (for example, if there are network issues in distributed systems). Using opID as a log filter allows viewing logs for an operation in progress.

  pbm-agent
     A ``pbm-agent`` is a :term:`PBM <Percona Backup for MongoDB>` process running on the mongod node for backup and restore operations. A pbm-agent instance is required for every mongod node (including replica set secondary members and config server replica set nodes).   

  pbm CLI
     Command-line interface for controlling the backup system. PBM CLI can connect to several clusters so that a user can manage backups on many clusters.

  PBM Control collections
     PBM Control collections are :term:`collections <Collection>` with config, authentication data and backup states. They are stored in the admin db  in the cluster or non-sharded replica set and serve as the communication channel between :term:`pbm-agent` and :term:`pbm CLI`. :term:`pbm CLI` creates a new pbmCmd document for a new operation. :term:`pbm-agents <pbm-agent>` monitor it and update as they process the operation.

  Percona Backup for MongoDB
     Percona Backup for MongoDB (PBM) is a low-impact backup solution for MongoDB non-sharded replica sets and clusters. It supports both :term:`Percona Server for MongoDB` and MongoDB Community Edition. 

  Percona Server for MongoDB 
     Percona Server for MongoDB is a drop-in replacement for MongoDB Community Edition with enterprise-grade features.

  Point-in-Time Recovery
     Point-in-Time Recovery is restoring the database up to a specific moment in time. The data is restored from the backup snapshot and then events that occurred to the data are replayed from oplog. 

  Replica set
     A replica set is a group of mongod nodes that host the same data set.

  S3 compatible storage   
     This is the storage that is built on the :term:`S3 <Amazon S3>` API.
 
  Server-side encryption
     Server-side encryption is the encryption of data by the remote storage server as it receives it. The data is encrypted when it is written to S3 bucket and decrypted when you access the data. 
