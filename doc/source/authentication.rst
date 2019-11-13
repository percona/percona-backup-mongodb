.. _pbm.auth:

When using |pbm| you use only the following for authentication and authorization (a.k.a. access control):
- MongoDB authentication and authorization, and
- S3-compatible Object Store service authentication

For the S3-compatible service authentication see the pbm.confg_ page.

.. _pbm.auth.create_pbm_user:

Create the PBM user
================================================================================

To run |pbm| a user must be created in the "admin" db that has the role grants as shown below. This user must created on every replicaset, i.e. it must be created on the shard replicasets as well as the config server replicaset.

An example of the ``createUser`` command you would execute in the 'mongo' shell.
``PRIMARY`` member):

.. include:: .res/code-block/mongo/db-createuser.txt

User name and password values and other options of the createUser command can be chosen as you like so long as the roles shown above are granted.

.. _pbm.auth.mdb_conn_string:

MongoDB connection strings &ndash; A Reminder (or Primer)
================================================================================

|pbm| uses [MongoDB Connection URI](https://docs.mongodb.com/manual/reference/connection-string/) strings to open MongoDB connections. Its programs do not have separate command-line arguments for host, port, replicaset, user, password, etc.

.. include:: .res/code-block/bash/pbm-agent-mongodb-conn-string-examples.txt

.. include:: .res/code-block/bash/pbm-cli-mongodb-conn-string-examples.txt

This is the format that Mongo driver-using apps can more-or-less use universally since the time MongoDB server v3.6 was released. The mongo shell accepts it too since v4.0 ([doc link](https://docs.mongodb.com/v4.0/mongo/#mongodb-instance-on-a-remote-host)). Using a v4.0+ mongo shell is a recommended way to debug from the command line if your connection URI is valid or not.

The [MongoDB Connection URI](https://docs.mongodb.com/manual/reference/connection-string/) specification includes several non-default options you may need to use. For example the TLS certificates/keys needed to connect to a cluster or non-sharded replicaset with network encryption enabled are "tls=true" plus "tlsCAFile" and/or "tlsCertificateKeyFile" (see [tls options](https://docs.mongodb.com/manual/reference/connection-string/#tls-options)).

|pbm-agent| processes should connect to their mongod with a standalone type of connection. Although the |pbm.app| client will sometimes work with a standalone type of connection this is only when it is to the primary node. It is better practice to use a replicaset connection string so the primary is always automatically selected.

Code note: As of v1.0 the driver used by |pbm| is the official v1.1 [mongo-go-driver](https://docs.mongodb.com/ecosystem/drivers/driver-compatibility-reference/#go-driver-compatibility)r.

The |pbm.app| connection string
--------------------------------------------------------------------------------

The |pbm.app| CLI should connect to the replica set with the PBM control collections.

- In a non-sharded replica set it is simply that replica set.
- In a cluster it is the config server replica set.

To make sure |pbm.app| always automatically connects to the current primary node use a replica set connection string.

.. include:: .res/replace.txt
