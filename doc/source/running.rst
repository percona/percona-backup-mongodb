.. _pbm.running:

Running
********************************************************************************

.. contents::
   :local:

.. _pbm.running.backup-coordinator:

Running the Backup Coordinator
================================================================================

The |bc| can be executed in any server since it does not need a connection to a
|mongodb| instance. One |bc| is needed per deployment.

To start the |bc| run |pbm-coordinator| as follows:

.. code-block:: bash

   $ pbm-coordinator --work-dir=<directory to store metadata>

If the |opt-work-dir| option is not specified, it will use the default
|work-dir|.  By default, the |bc| listens for agents on port 10000.

Running the Agent
================================================================================

On every |mongodb| instance, you need to start an agent that will receive
commands from the |bc|.

By default, the agent connects to |mongodb| using host **127.0.0.1** and port
**27017**.

.. admonition:: Example

   .. code-block:: bash

      $ pbm-agent --server-address=172.16.0.2:10000 \
		  --backup-dir=/data/backup \
		  --mongodb-port=27017 \
		  --mongodb-user=pbmAgent \
		  --mongodb-password=securePassw0rd \
		  --pid-file=/tmp/pbm-agent.pid

.. seealso::

   Running the agent if |mongodb| authentication is enabled on the |mongodb| host
      :ref:`pxb.running.mongodb-authentication`

.. _pxb.running.mongodb-authentication:

|mongodb| Authentication
================================================================================

If `MongoDB Authentication`_ is enabled the backup agent must be provided
credentials for a |mongodb| user with the
`backup <https://docs.mongodb.com/manual/reference/built-in-roles/#backup>`__,
`restore <https://docs.mongodb.com/manual/reference/built-in-roles/#restore>`__
and
`clusterMonitor  <https://docs.mongodb.com/manual/reference/built-in-roles/#clusterMonitor>`__
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

Running |pbmctl| commands
--------------------------------------------------------------------------------

|pbmctl| is the command line utility to control the backup system. Since
|pbmctl| needs to connect to the |bc|, you need to specify the |bc|
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

   $ pbmctl run backup --description "Test backup 01"

Listing all the completed backups
--------------------------------------------------------------------------------

.. code-block:: bash

   $ pbmctl --server-address=127.0.0.1:10001 list backups

.. admonition:: Sample output:

   .. code-block:: bash

      Metadata file name             -         Description
      ------------------------------ - --------------------------------------------
      2018-12-18T19:04:14Z.json      - Test backup 01

Restoring a backup
--------------------------------------------------------------------------------

.. code-block:: bash

   $ pbmctl run restore 2018-12-18T19:04:14Z.json

.. include:: .res/replace.txt
