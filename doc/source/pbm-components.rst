.. _pbm-components:

****************
|PBM| components
****************


|pbm| consists of the following components:

- :term:`pbm-agent` is a process running on every ``mongod`` node within the cluster or a replica set that performs backup and restore operations. 
- :term:`PBM CLI <pbm CLI>` is a command-line utility that instructs pbm-agents to perform an operation. 

  A single |pbm-agent| is only involved with one cluster (or non-sharded replica set). The |pbm.app| CLI utility can connect to any cluster it has network access to, so it is possible for one user to list and launch backups or restores on many clusters. 

- :term:`PBM Control collections` are special collections in MongoDB that store the configuration data and backup states. Both |pbm.app| CLI and |pbm-agent| use PBM Control collections to check backup status in MongoDB and communicate with each other. 
- Remote backup storage is where |pbm| saves backups. It can be either an :term:`S3 compatible storage` or a filesystem-type storage.

The following diagram illustrates how |PBM| components communicate with MongoDB. 

.. image:: _images/pbm-architecture.png
   :width: 400
   :align: center
   :alt: PBM components 

To learn more about |PBM| architecture, see :ref:`pbm.architecture`.

.. include:: .res/replace.txt