.. _pbm.running:

Backing Up and Restoring Data
********************************************************************************

.. contents::
   :local:

Running |pbm|
================================================================================

|pbm| uses the |mongodb| cluster (or non-sharded replica set) as the store for
its own config.

Once a user for PBM backups is created you can use the |pbm.app| CLI to connect
to and operate on the cluster from any server that can access the |mongodb|
cluster.

On every server that has a mongod node, you must run a |pbm-agent| process
connecting to that |mongod|. This includes config server nodes if you have a
cluster.

After installing the binaries the following steps are needed before you can
start using |pbm.app| CLI.

#. Create a *mongodb* user for the |pbm.app| and |pbm-agent| instances to
   connect to the cluster with. See :ref:`pbm.running.mongodb-authentication`
   section for more information.
#. Set the location and credentials for the remote storage the backups will be stored in. See :ref:`pbm.running.storages-yml-file` for more information.
#. Start |pbm-agent| processes for every |mongod| process.

.. _pbm.running.storage.setting-up:

Configuring a Remote Store for Backup and Restore Operations
--------------------------------------------------------------------------------

For running the backup (|pbm-backup|) and restore (|pbm-restore|) operations,
you need to set up a place where the backups will be stored and retrieved. To
configure the store you need to use a YAML file with a predefined structure.

.. include:: .res/code-block/bash/pbm-store-set-config-mongodb-uri.txt

In |version|, |pbm.app| supports the following types of store:

- |amazon-s3|
- |minio|
- local file system

|pbm.app| is associated with a store that you supply in the YAML
format as the value of the |pbm-config-file-set| command. You do not need
to supply the store information for any subsequent operations.

.. include:: .res/code-block/bash/pbm-agent-mongodb-uri.txt
   
.. include:: .res/text/note-env-pbm-mongodb-uri.txt

.. _pbm.running.storages-yml-file:

Store Configuration File
--------------------------------------------------------------------------------

The store configuration file is a YAML file that contains all
required options that pertain to one store. In |version|, 
|amazon-s3| compatible remote stores and the local file system are supported.

The following example demonstrates the settings of an |amazon-s3| store
identified by `s3-us-west`.

.. rubric:: |amazon-s3| Store

To set up an |amazon-s3| store in |config-filename-example|, give it a name as the top
level element. Set the ``type`` sub element to `s3`. The ``s3`` element, which
is a sibling to the ``type`` element, set the essential parameters: `region`,
`bucket`, and `credentials`.

.. admonition:: Example of an |amazon-s3| storage in the |config-filename-example| file

   .. include:: .res/code-block/yaml/example-amazon-s3-storage.yaml

.. seealso::

   More information about |amazon-s3|
      https://aws.amazon.com/s3/

.. rubric:: |minio| Storage

|minio| is an |amazon-s3| compatible object storage. |minio| uses the same settings
as the |amazon-s3| storage including the ``type`` element: ``type: s3``. What
makes the |minio| type distinct is the ``EndpointURL`` element included into the
``s3`` element.

.. seealso::

   More information about |minio|
      https://min.io/

.. rubric:: Local File System

To use the local file system for storing backups, set the ``type``
element to *filesystem* and specify a local directory as the value of
the `path`` element as follows:

.. admonition:: Example of a local file system store in the |config-filename-example| file.

   .. include:: .res/code-block/yaml/example-local-file-system-store.yaml


Running |pbm-agent|
================================================================================

On every |mongod| instance (and config servers) in your cluster, you need to
start an agent that will receive commands from the |pbm.app|.

|pbm-agent| is started with |opt-mongodb-uri| option that you use to provide a
connection string to the |mongodb| instance. 

.. admonition:: Example

   .. include:: .res/code-block/bash/pbm-agent-mongodb-uri.txt

If you `MongoDB Authentication`_ is enabled you specify ``--mongodb-user`` and
``--mongodb-password`` options with |pbm-agent| to provide the
credentials:

.. seealso::

   Running the agent if |mongodb| authentication is enabled on the |mongodb| host
      :ref:`pbm.running.mongodb-authentication`

.. _pbm.running.mongodb-authentication:

|mongodb| Authentication
================================================================================

If `MongoDB Authentication`_ is enabled the backup agent must be provided
credentials for a |mongodb| user with the `backup
<https://docs.mongodb.com/manual/reference/built-in-roles/#backup>`__, `restore
<https://docs.mongodb.com/manual/reference/built-in-roles/#restore>`__ and
`clusterMonitor
<https://docs.mongodb.com/manual/reference/built-in-roles/#clusterMonitor>`__
built-in auth roles. This user must exist on every database node and it should
not be used by other applications.

An example of the ``createUser`` command (must be run via the 'mongo' shell on a
``PRIMARY`` member):

.. include:: .res/code-block/mongo/db-createuser.txt

Running |pbm.app| Commands
================================================================================

|pbm.app| is the command line utility to control the backup system. Before you
can work with backups, make sure to set the remote store.

If ``pbm store show --mongodb-uri="...."`` returns an empty or incomplete remote
store configuration please follow the instructions in the
:ref:`pbm.running.storage.setting-up` section:

.. include: .res/code-block/bash/pbm-store-set-config-mongodb-uri.txt

The connection string should point to the config server replica set if a
cluster. For a non-sharded replicaset, the connection string should be just to
that replicaset.

.. contents::
   :local:

.. _pbm.running.backup.listing:

Example: Listing all backups
--------------------------------------------------------------------------------

.. include:: .res/code-block/bash/pbm-list-mongodb-uri.txt

.. admonition:: Sample output

   .. code-block:: text

      2019-09-10T07:04:14Z
      2019-09-09T07:03:50Z
      2019-09-08T07:04:21Z
      2019-09-07T07:04:18Z

.. _pbm.running.backup.starting: 

Example: Starting a backup
--------------------------------------------------------------------------------

.. include:: .res/code-block/bash/pbm-backup-mongodb-uri.txt

.. _pbm.running.backup.restoring: 

Example: Restoring a Backup
--------------------------------------------------------------------------------

To restore a backup that you have made using |pbm-backup| you should use the
|pbm-restore| command supplying the name of the backup that you intend to
restore.

.. important::

   Before running |pbm-restore| on a cluster it is important that the balancer be
   stopped on |mongos|. 

   .. code-block:: guess

      > db.stopBalancer()

.. include:: .res/code-block/bash/pbm-restore-mongodb-uri.txt

The instance that you will restore your backup to may already
have data. After running |pbm-restore|, the instance will
have both its existing data and the data from the backup. To make sure
that your data are consistent, either clean up the target instance or
use an instance without data.

.. warning::

   The data may be inconsistent on the node where you restore the
   backup to (usually, a primary node) if this node steps down or a
   different primary is elected.

.. include:: .res/replace.txt
