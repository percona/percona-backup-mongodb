.. _pbm.running:

Backing Up and Restoring Data
********************************************************************************

.. contents::
   :local:

Running |pbm|
================================================================================

One |pbm.app| instance is needed per deployment. You start |pbm.app| as follows:

.. code-block:: bash

   $ pbm [<flags>] <command> [<args> ...]

By default, |pbm.app| listens for agents on port 10000.

.. _pbm.running.storage.setting-up:

Setting Up Storages for Backup and Restore Operations
--------------------------------------------------------------------------------

For running the backup (|pbm-backup|) and restore (|pbm-restore|) operations,
you need to set up a place where the backups will be stored and retrieved. To
configure the storage you need to use a |yaml| file with a predefined structure.

.. include:: .res/code-block/bash/pbm-store-set-config-mongodb-uri.txt

In |version|, |pbm.app| supports the following types of storage:

- |amazon-s3|
- |minio|

|pbm.app| is associated with a storage that you supply in the |yaml|
format as the value of the |pbm-storage-set| command. You do not need
to supply the storage information for any subsequent operations.

.. include:: .res/code-block/bash/pbm-agent-mongodb-uri.txt

.. _pbm.running.storages-yml-file:

Storage Configuration File
--------------------------------------------------------------------------------

The storage configuration file is a |yaml| file that contains all
required options that pertain to one storage. In |version|, only
|amazon-s3| compatible remote storages are supported.

The following example demonstrates the settings of an |amazon-s3| storage
identified by `s3-us-west`.

.. rubric:: |amazon-s3| Storage

To set up an |amazon-s3| storage in |storages-yml|, give it a name as the top
level element. Set the ``type`` sub element to `s3`. The ``s3`` element, which
is a sibling to the ``type`` element, set the esential parameters: `region`,
`bucket`, and `credentials`.

.. admonition:: Example of an |amazon-s3| storage in the |storages-yml| file

   .. code-block:: yaml

      s3-us-west:
         type: s3
         s3:
            region: us-west-2
            bucket: pbm-test-bucket-69835
            credentials:
               access-key-id: <your-access-key-id-here>
               secret-access-key: <your-secret-key-here>

.. seealso::

   More information about |amazon-s3|
      https://aws.amazon.com/s3/

.. rubric:: |minio| Storage

|minio| is an |amazon-s3| compatible object storage. You use the same settings
as for an |amazon-s3| storage including the ``type`` element: ``type: s3``. What
makes the |minio| type distict is the ``EndpointURL`` element included into the
``s3`` element.

.. admonition:: Example of a |minio| Storage in the |storages-yml| File

   .. code-block:: yaml

      minio-storage:
         type: s3
	 s3:
	    region: us-west-2
	    bucket: a-different-bucket
	    EndpointURL: <the-minio-endpoint-here>,
	    credentials:
               access-key-id: <your-access-key-id-here>
               secret-access-key: <your-secret-key-here>

.. seealso::

   More information about |minio|
      https://min.io/


Running |pbm-agent|
================================================================================

On every |mongod| instance (and config servers) in your cluster, you need to
start an agent that will receive commands from the |pbm.app|.

|pbm-agent| is started with |opt-mongodb-uri| option that you use to provide a
connection string to the |mongodb| instance. 

.. admonition:: Example

   .. code-block:: bash

      $ pbm-agent --mongodb-uri="mongodb://172.17.0.3:27018"

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

.. code-block:: guess

   > use admin;
   > db.createUser({user: "pbmAgent",
                    pwd: "securePassw0rd",
                    roles: [{db: "admin", role: "backup" },
		            {db: "admin", role: "readWrite" },
                            {db: "admin", role: "clusterMonitor" },
                            { db: "admin", role: "restore" }],
                    authenticationRestrictions: [{ clientSource: ["127.0.0.1"]}]})

Running |pbm.app| Commands
--------------------------------------------------------------------------------

|pbm.app| is the command line utility to control the backup system. Before you
can work with backups, make sure to set the remote store:

.. include:: .res/code-block/bash/pbm-store-set-config-mongodb-uri.txt

Command Examples
================================================================================

.. contents::
   :local:

Listing all backups
--------------------------------------------------------------------------------

.. code-block:: bash

   $ pbm list

.. admonition:: Sample output

   .. code-block:: text

      2019-09-10T07:04:14Z
      2019-09-09T07:03:50Z
      2019-09-08T07:04:21Z
      2019-09-07T07:04:18Z

Starting a backup
--------------------------------------------------------------------------------

.. include:: .res/code-block/bash/pbm-backup-mongodb-uri.txt

While the |pbm-backup| operation is running, neither rename nor drop
collections or indexes from the backup lest the backup operation should fail.

.. _pbm.running.backup.listing:

Listing all completed backups
--------------------------------------------------------------------------------

.. include:: .res/code-block/bash/pbm-backup-mongodb-uri.txt

.. admonition:: Sample output:

   .. code-block:: bash

      Backup History:

      2019-09-10T19:04:14Z

.. _pbm.running.backup.restoring: 

Restoring a Backup
--------------------------------------------------------------------------------

To restore a backup that you have made using |pbm-backup| you should use the
|pbm-restore| command supplying the name of the backup that you intend to
restore.

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

-----

.. include:: .res/replace.txt
