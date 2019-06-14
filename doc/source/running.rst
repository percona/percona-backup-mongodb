.. _pbm.running:

Backing Up and Restoring Data
********************************************************************************

.. contents::
   :local:

.. _pbm.running.backup-coordinator:

Running the Backup Coordinator
================================================================================

The |bc| can be executed on any server since it does not need a connection to a
|mongod| instance. One |bc| is needed per deployment.

To start the |bc| run |pbm-coordinator| as follows:

.. code-block:: bash

   $ pbm-coordinator --work-dir=<directory to store metadata>
n
If the |opt-work-dir| option is not specified, it will use the default
|work-dir|.  By default, the |bc| listens for agents on port 10000.

Setting Up Storages for Backup and Restore Operations
--------------------------------------------------------------------------------

For running the backup (`pbmctl run backup`) and restore (`pbmctl run restore`)
operations, you need to set up a storage. This is a location known to |pbmctl|
where the backup data are stored. All storages must be listed in the
|storages-yml| file. You can use the name of the storage without
having to supply its connection parameters on the command line. 

.. code-block:: bash

   $ pbmctl run backup --description="PBM Backup" --storage=localfs

In |version|, |pbm| supports the following types of storage:

- |amazon-s3|
- |minio|
- A directory in the file system

|pbmctl| itself does not maintain the list of storages. |storages-yml| is
associated with an instance of |pbm-agent| started on a member of the replica set. |pbmctl| retrieves the storage by name when either
running the backup or restore operation. You specify where the
|storages-yml| file is located by using its configuration file or via the
``--storage-config`` parameter.

.. code-block:: bash

   $ pbm-agent --storage-config="./storages.yml"

When using a configuration file, |pbm-agent| must be started with the
``--config-file`` parameter. Note that the configuration file for |pbm-agent|
and |storages-yml| use the |yaml| format.

.. code-block:: bash

   $ pbm-agent --config-file "conf.yml"

.. rubric:: Generating an Example of a Configuration File for |pbm-agent|

You do not have to create a configuration file for |pbm-agent| from scratch. Run
``pbm-agent --generate-sample-config`` to obtain a configuration file with
default settings.  In the configuration file, set the value of the
``storages_config`` element to point to |storages-yml|.

.. code-block:: yaml

   server_address: 127.0.0.1:10000
   server_compressor: ""
   storages_config: "storages.yml"
   mongodb_conn_options:
      host: 127.0.0.1
      port: "27018"
      reconnect_delay: 10

.. _pbm.running.storages-yml-file:

|storages-yml| File
--------------------------------------------------------------------------------

The |storages-yml| is a |yaml| file that contains all locations, or storages,
where data can backed up to or restored from. At top level, each storage is
represented by a unique name. The nested elements specify the type and the
required settings for |pbm| to be able to use the given location.

The following example demonstrates the settings of an |amazon-s3| storage
identified by `s3-us-west`.

.. note::

   It is only required that each storage name be unique in |storages-yml|. You
   can use any names composed of any characters as long as they are allowed by
   the |YAML| format and are accepted on the command line as parameter values.

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

   To use this storage with |pbmctl-backup| use its name as the value of the
   |opt-storage| parameter:

   .. code-block:: bash

      $ pbmctl run backup --description="Backup to S3 storage" --storage s3-us-west

.. seealso::

   More information about |amazon-s3|
      https://aws.amazon.com/s3/

.. rubric:: |minio| Storage

|minio| is an |amazon-s3| compatible object storage. You use the same settings
as for an |amazon-s3| storage including the ``type`` element: ``type: s3``. What
makes the |minio| type distict is the ``EndpointURL`` element inclueded into the
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

   To use this storage with |pbmctl-backup| use its name as the value of the
   |opt-storage| parameter:

   .. code-block:: bash

      $ pbmctl run backup --description="Backup to MinIO storage" --storage minio-storage

.. seealso::

   More information about |minio|
      https://min.io/

.. rubric:: Local Storage

Setting up a local directory only involves giving it a unique name, setting the
``type`` sub element to `filesystem` and providing the absolute path (``path``
element) to a directory of your choice under the ``filesystem`` element.

.. admonition:: Example of a local storage in the |storages-yml| file

   .. code-block:: yaml

      localfs:
      type: filesystem
      filesystem:
         path: /tmp/pbm-data/

   To use this storage with |pbmctl-backup| use its name as the value of the |opt-storage| parameter:

   .. code-block:: bash

      $ pbmctl run backup --description="Backup to local file system" --storage localfs

Running |pbm-agent|
================================================================================

On every |mongod| instance (and config servers) in your cluster, you need to start an agent that
will receive commands from the |bc|.

By default, the agent connects to |mongodb| using host **127.0.0.1** and port
**27017**. 

.. admonition:: Example

   .. code-block:: bash

      $ pbm-agent --server-address=172.16.0.2:10000 \
      --mongodb-port=27017 \


If you `MongoDB Authentication`_ is enabled you ``--mongodb-user`` and
``--mongodb-password`` options with :program:`pbm-agent` to provide
the credentials:

.. admonition:: Example

   .. code-block:: bash

      $ pbm-agent --server-address=172.16.0.2:10000 \
      --mongodb-port=27017 \
      --mongodb-username=pbmAgent \
      --mongodb-password=s3cur#p@%Sw0rd \

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
                            {db: "admin", role: "clusterMonitor" },
                            { db: "admin", role: "restore" }],
                    authenticationRestrictions: [{ clientSource: ["127.0.0.1"]}]})

Running |pbmctl| Commands
--------------------------------------------------------------------------------

|pbmctl| is the command line utility to control the backup system. Since
n|pbmctl| needs to connect to the |bc|, you need to specify the |bc|
`IP:Port`. The default values are `127.0.0.1:10001`. If you are running |pbmctl|
from the same server where the coordinator is running, you can ommit the
|opt-server-address| option.
  
Command Examples
================================================================================

.. contents::
   :local:

Listing all connected agents
--------------------------------------------------------------------------------

.. code-block:: bash

   $ pbmctl --server-address=127.0.0.1:10001 list nodes

.. admonition:: Sample output

   .. code-block:: text

      |    Node ID                Cluster ID                 Node Type                Node Name
      ----------------   ------------------------   --------------------------   ----------------------
      localhost:17000    - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOS           - 127.0.0.1:17000
      127.0.0.1:17001    - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:17001
      127.0.0.1:17002    - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:17002
      127.0.0.1:17003    - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:17003
      127.0.0.1:17004    - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:17004
      127.0.0.1:17005    - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:17005
      127.0.0.1:17006    - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:17006
      127.0.0.1:17007    -                          - NODE_TYPE_MONGOD_CONFIGSVR - 127.0.0.1:17007

Starting a backup
--------------------------------------------------------------------------------

.. code-block:: bash

   $ pbmctl run backup --description "Test backup 01" --storage=./storages.yml

While the ``pbmctl run backup`` operation is running, neither rename nor drop
collections or indexes from the backup lest the backup operation should fail.

.. _pbm.running.backup.listing:

Listing all completed backups
--------------------------------------------------------------------------------

.. code-block:: bash

   $ pbmctl --server-address=127.0.0.1:10001 list backups

.. admonition:: Sample output:

   .. code-block:: bash

      Metadata file name             -         Description
      ------------------------------ - --------------------------------------------
      2018-12-18T19:04:14Z.json      - Test backup 01

.. _pbm.running.backup.restoring: 

Restoring a Backup
--------------------------------------------------------------------------------

To restore a backup that you have made using ``pbmctl run backup`` you should
use the ``pbmctl run restore`` command. This command takes two required
parameters: the storage name and the metadata file. 

.. code-block:: bash

   $ pbmctl run restore --storage=localfs datestamp.json

The instance that you intend to restore your backup to may already
have data. After running ``pbmctl run restore``, the instance will
have both its existing data and the data from the backup. To make sure
that your data are consistent, either clean up the target instance or
use an instance without data.

.. warning::

   The data may be inconsistent on the node where you restore the
   backup to (usually, a primary node) if this node steps down or a
   different primary is elected.

.. rubric:: Storage

The storage name refers to a storage defined in the |storages-yml| file on one
of the members of the shard replica set. In the following example, a member of
the shard replica set is on 127.0.0.1:27018.

.. admonition:: Registering a member of the shard replica set with |pbm-agent|

   .. code-block:: bash

      $ pbm-agent --mongodb-port=12018 --storage-config=./storages.yml

When |pbm-agent| starts successfully, the new node appears on the list of nodes.

.. code-block:: bash

   $ pbmctl list nodes

.. admonition:: Output showing all nodes in the cluster

   Note that the node type of *127.0.0.1:27018* is **NODE_TYPE_MONGOD_SHARDSVR**.

   .. code-block:: text
		   
      |     Node ID                 Cluster ID                   Node Type               Node Name          Replicaset Name      Running DB/Oplog backup
      --------------------   ------------------------   --------------------------   -----------------   ---------------------   -----------------------
      127.0.0.1:27017      - 5cb864db62350b10f654e592 - NODE_TYPE_MONGOD_CONFIGSVR - 127.0.0.1:27017   - pbmdocconf            -       No / No 
      127.0.0.1:27018      - 5cb864db62350b10f654e592 - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:27018   - pbmdocshard           -       No / No 
      127.0.0.1:27019      - 5cb864db62350b10f654e592 - NODE_TYPE_MONGOS           - 127.0.0.1:27019   -                       -       No / No 

Check the registered storages by using ``pbmctl list storage`` command:

.. code-block:: bash

   $ pbmctl list storage

.. admonition:: Output

   .. code-block:: text

      |   Available Storages:
      -------------------------------------------------------------------------------------------------
      Name         : localfs
      MatchClients : 127.0.0.1:27018,
      DifferClients: 
      Storage configuration is valid.
      Type: filesystem
      |   Path       : /home/pbm-user/pbm-storage/+ 
      -------------------------------------------------------------------------------------------------

.. rubric:: Metadata File

The metadata file is created automatically as a result of running ``pbm run
backup``. This is a JSON file which contains the essential information about one
backup operation. Every time a backup operation completes successfully, |pbm|
automatically creates a metadata file. Its name contains the date and time stamp
when the backup operation occurred.

When restoring you choose which backup to restore by specifying the name of the
associated metadata file. All metadata files are stored in the working directory
of |pbm-coordinator|. Also, a copy of the metadata file is stored along with the
backed up files.

To determine which metadata file is associated with a given backup use
:ref:`pbmctl list backups <pbm.running.backup.listing>`

.. include:: .res/replace.txt
