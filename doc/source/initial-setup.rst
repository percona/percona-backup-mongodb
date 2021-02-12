.. _initial-setup:

Initial setup   
****************************************************************

After you’ve installed |pbm-agent| on every server with a ``mongod`` node that is not an arbiter node, perform initial setup to run |PBM|.

The setup steps are the following:

1.	Configure authentication in MongoDB
#. Use ``pbm CLI`` to insert the config information for remote backup storage
#.	Start |pbm-agent| process
 	
.. contents::
   :local:

Configure authentication in MongoDB
================================================================================

|pbm| has no authentication and authorization subsystem of its own - it uses
the one of MongoDB. This means that to authenticate |pbm|, you need to create a corresponding ``pbm`` user in the ``admin`` database and set a valid MongoDB connection URI string for both |pbm-agent| and |pbm.app|.

.. _pbm.auth.create_pbm_user:

Create the ``pbm`` user
---------------------------------------

First create the role that allows any action on any resource. 

.. code-block:: javascript

   db.getSiblingDB("admin").createRole({ "role": "pbmAnyAction",
         "privileges": [
            { "resource": { "anyResource": true },
              "actions": [ "anyAction" ]
            }
         ],
         "roles": []
      });

Then create the user and assign the role you created to it. 

.. code-block:: javascript

   db.getSiblingDB("admin").createUser({user: "pbmuser",
          "pwd": "secretpwd",
          "roles" : [
             { "db" : "admin", "role" : "readWrite", "collection": "" },
             { "db" : "admin", "role" : "backup" },
             { "db" : "admin", "role" : "clusterMonitor" },
             { "db" : "admin", "role" : "restore" },
             { "db" : "admin", "role" : "pbmAnyAction" }
          ]
       });

You can specify username and password values and other options of the ``createUser`` command as you require so long as the roles shown above are granted.

Create the ``pbm`` user on every replica set. In a sharded cluster, this means every shard replica set and the config server replica set.

.. note::

   In a cluster run `db.getSiblingDB("config").shards.find({}, {"host": true,
   "_id": false})` to list all the host+port lists for the shard
   replica sets. The replica set name at the *front* of these "host" strings will
   have to be placed as a "/?replicaSet=xxxx" argument in the parameters part
   of the connection URI (see below).

Set the MongoDB connection URI for ``pbm-agent``
------------------------------------------------------------------

A |pbm-agent| process connects to its localhost ``mongod`` node with a standalone type of connection. To set the MongoDB URI connection string means to configure a service init script (:file:`pbm-agent.service` systemd unit file) that runs a |pbm-agent|.

The :file:`pbm-agent.service` systemd unit file includes the environment file. You set the MongoDB URI connection string for the  "PBM_MONGODB_URI" variable within the environment file.

The environment file for Debian and Ubuntu is :file:`/etc/default/pbm-agent`. For Redhat and CentOS, it is :file:`/etc/sysconfig/pbm-agent`. 

Edit the environment file and specify MongoDB connection URI string for the ``pbm`` user to the local ``mongod`` node. For example, if ``mongod`` node listens on port 27018, the MongoDB connection URI string will be the following:

.. code-block:: bash

   PBM_MONGODB_URI="mongodb://pbmuser:secretpwd@localhost:27018"

Configure the service init script for every |pbm-agent|. 

.. hint:: How to find the environment file

   The path to the environment file is specified in the :file:`pbm-agent.service` systemd unit file. 

   In Ubuntu and Debian, the :file:`pbm-agent.service` systemd unit file is at the path :file:`/lib/systemd/system/pbm-agent.service`. 

   In Red Hat and CentOS, the path to this
   file is :file:`/usr/lib/systemd/system/pbm-agent.service`.
       
   .. admonition:: Example of pbm-agent.service systemd unit file  

      .. include:: .res/code-block/bash/systemd-unit-file.txt    

.. seealso::

   More information about standard MongoDB connection strings
      :ref:`pbm.auth`

.. _set-mongodbURI-pbm.CLI: 

Set the MongoDB connection URI for ``pbm CLI``
------------------------------------------------------------------

Provide the MongoDB URI connection string for |pbm.app| in your shell. This allows you to call |pbm.app| commands without the :option:`--mongodb-uri` flag.

Use the following command:

.. code-block:: bash
 
   export PBM_MONGODB_URI="mongodb://pbmuser:secretpwd@localhost:27018?/replSetName=xxxx"

For more information what connection string to specify, refer to :ref:`pbm.auth.pbm.app_conn_string` section.

Configure remote backup storage
==================================

The easiest way to provide remote backup storage configuration is to specify it in a YAML config file and upload this file to |PBM| using ``pbm CLI``.

The storage configuration itself is out of scope of the present document. We assume that you have configured one of the :ref:`supported remote backup storages <storage.config>`.

1. Create a config file (e.g. :file:`pbm_config.yaml`).
2. Specify the storage information within. 
   
   The following is the sample configuration for Amazon AWS:
   
   .. include:: .res/code-block/yaml/example-amazon-s3-storage.yaml
   
   This is the sample configuration for filesystem storage:

   .. include:: .res/code-block/yaml/example-local-file-system-store.yaml

   .. important::

      When using a filesystem storage, verify that the user running |PBM| is the owner of the backup folder.

      .. code-block:: bash
      
         $ sudo chown pbm:pbm <backup_directory>

   See more examples in :ref:`pbm.config.example_yaml`.

3. Insert the config file
   
   .. code-block:: bash

      $ pbm config --file pbm_config.yaml 

   For a sharded cluster, run this command whilst connecting to config server replica set. Otherwise connect to the non-sharded replica set as normal. 

To learn more about |PBM| configuration, see :ref:`pbm.config`.
 
Start the |pbm-agent| process
==================================

Start |pbm-agent| on every server with the ``mongod`` node installed. It is best to use the packaged service scripts to run |pbm-agent|. 

.. code-block:: bash

   $ sudo systemctl start pbm-agent
   $ sudo systemctl status pbm-agent

E.g. Imagine you put configsvr nodes (listen port 27019) colocated on the same
servers as the first shard's mongod nodes (listen port 27018, replica set name
"sh1rs"). In this server there should be two 
|pbm-agent| processes, one connected to the shard
(e.g. "mongodb://username:password@localhost:27018/") and one to the configsvr
node (e.g. "mongodb://username:password@localhost:27019/").

For reference, the following is an example of starting |pbm-agent| manually. The
output is redirected to a file and the process is backgrounded. Alternatively
you can run it on a shell terminal temporarily if you want to observe and/or
debug the startup from the log messages.

.. code-block:: bash

   $ nohup pbm-agent --mongodb-uri "mongodb://username:password@localhost:27018/" > /data/mdb_node_xyz/pbm-agent.$(hostname -s).27018.log 2>&1 &

.. tip::
   
   Running as the ``mongod`` user would be the most intuitive and convenient way.
   But if you want it can be another user.

.. _pbm-agent.log:

How to see the pbm-agent log
--------------------------------------------------------------------------------

With the packaged systemd service the log output to stdout is captured by
systemd's default redirection to systemd-journald. You can view it with the
command below. See `man journalctl` for useful options such as '--lines',
'--follow', etc.

.. code-block:: bash

   ~$ journalctl -u pbm-agent.service
   -- Logs begin at Tue 2019-10-22 09:31:34 JST. --
   Jan 22 15:59:14 akira-x1 systemd[1]: Started pbm-agent.
   Jan 22 15:59:14 akira-x1 pbm-agent[3579]: pbm agent is listening for the commands
   ...
   ...

If you started pbm-agent manually, see the file you redirected stdout and stderr
to.

When a message *"pbm agent is listening for the commands"* is printed to the
|pbm-agent| log file, it confirms that it has connected to its ``mongod`` node successfully.


.. include:: .res/replace.txt
