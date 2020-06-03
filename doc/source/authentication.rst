.. _pbm.auth:

Authentication
********************************************************************************

|pbm| has no authentication and authorization subsystem of its own - it uses
MongoDB's, i.e. |pbm.app| and |pbm-agent| only require a valid MongoDB
connection URI string for the PBM user.

For the S3-compatible remote storage authentication config, see
:ref:`pbm.config`.

.. _pbm.auth.create_pbm_user:

Create the PBM user
===================

To run |pbm| a user must be created in the ``admin`` db that has the role
`grants` as shown below.

.. include:: .res/code-block/mongo/db-createuser.txt

User name and password values and other options of the createUser command can be
set as you require so long as the roles shown above are granted.

This user must be created on every replicaset, i.e. it must be created on the
shard replicasets as well as the config server replicaset.

.. note::

   In a cluster run `db.getSiblingDB("config").shards.find({}, {"host": true,
   "_id": false})` to list all the host+port lists for the shard
   replicasets. The replicaset name at the *front* of these "host" strings will
   have to be placed as a "/?replicaSet=xxxx" argument in the parameters part
   of the connection URI (see below).

.. _pbm.auth.mdb_conn_string:

MongoDB connection strings - A Reminder (or Primer)
===================================================

|pbm| uses `MongoDB Connection URI
<https://docs.mongodb.com/manual/reference/connection-string/>`_ strings to open
MongoDB connections. Neither |pbm.app| or |pbm-agent| accept legacy-style
command-line arguments for ``--host``, ``--port``, ``--user``, ``--password``,
etc. as the ``mongo`` shell or ``mongodump`` command does.

.. include:: .res/code-block/bash/pbm-agent-mongodb-conn-string-examples.txt

.. include:: .res/code-block/bash/pbm-cli-mongodb-conn-string-examples.txt

The connection URI above is the format that MongoDB drivers accept universally
since approximately the release time of MongoDB server v3.6. The ``mongo`` shell
`accepts it too since v4.0
<https://docs.mongodb.com/v4.0/mongo/#mongodb-instance-on-a-remote-host>`_. Using
a v4.0+ mongo shell is a recommended way to debug connection URI validity from
the command line.

The `MongoDB Connection URI
<https://docs.mongodb.com/manual/reference/connection-string/>`_ specification
includes several non-default options you may need to use. For example the TLS
certificates/keys needed to connect to a cluster or non-sharded replicaset with
network encryption enabled are "tls=true" plus "tlsCAFile" and/or
"tlsCertificateKeyFile" (see `tls options
<https://docs.mongodb.com/manual/reference/connection-string/#tls-options>`_).

.. admonition:: Technical note

   As of v1.0 the driver used by |pbm| is the official v1.1 `mongo-go-driver
   <https://docs.mongodb.com/ecosystem/drivers/driver-compatibility-reference/#go-driver-compatibility>`_.

The |pbm-agent| connection string
---------------------------------

|pbm-agent| processes should connect to their localhost mongod with a standalone
type of connection.

The |pbm.app| connection string
-------------------------------

The |pbm.app| CLI will ultimately connect to the replica set with the
:ref:`PBM control collections <pbm.architecture.pbm_control_collections>`.

- In a non-sharded replica set it is simply that replica set.
- In a cluster it is the config server replica set.

You do not necessarily have to provide that connection string. If you provide
a connection to any live node (shard, configsvr, or non-sharded replicaset
member), it will automatically determine the right hosts and establish a new
connection to those instead.

.. tip:: 
   
   When running |pbm.app| from an unsupervised script, we recommend using a
   replica set connection string. A standalone-style connection string will fail if that ``mongod`` host happens to be down temporarily.

.. include:: .res/replace.txt
