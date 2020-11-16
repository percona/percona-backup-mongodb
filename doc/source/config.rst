.. _pbm.config:

|pbm| configuration in a cluster (or non-sharded replica set)
********************************************************************************

The configuration information is stored in a single document of the *admin.pbmConfig*
collection. That single copy is shared by all the |pbm-agent| processes in a
cluster (or non-sharded replica set), and can be read or updated using the
|pbm.app| tool.

You can see the whole config by running
*db.getSiblingDB("admin").pbmConfig.findOne()*. But you don't have to use the
mongo shell; the |pbm.app| CLI has a "config" subcommand to read and update it.

|PBM| config contains the following settings:

- :ref:`storage.config` configuration is available as of v1.0 or v1.1
- :ref:`pitr` configuration is available as of v1.3.0
- :ref:`restore.config` are available as of v1.3.2  

Run |pbm-config-list| to see the whole config. (Sensitive fields such as keys
will be redacted.)

.. _pbm.config.initialize:

Insert the whole |pbm| config from a YAML file
================================================================================

If you are initializing a cluster or non-sharded replica set for the first time, it is simplest to write the whole config as YAML file and use the
|pbm-config-file-set| method to upload all the values in one command.

.. include:: .res/code-block/bash/pbm-config-file-set.txt

Execute whilst connecting to config server replica set if it is a
cluster. Otherwise just connect to the non-sharded replica set as normal. (See
:ref:`pbm.auth.mdb_conn_string` if you are not familiar with MongoDB connection
strings yet.) For more information about available config file options, see :ref:`pbm.config.options`.

.. _pbm.config.update:

Accessing or updating single config values
================================================================================

You can set a single value at time. For nested values use dot-concatenated key
names as shown in the following example:

.. code-block:: bash

   $ pbm config --set storage.s3.bucket="operator-testing"

To list a single value you can specify just the key name by itself and the value
will be returned (if set)

.. code-block:: bash

   $ pbm config storage.s3.bucket
   operator-testing
   $ pbm config storage.s3.INVALID-KEY
   Error: unable to get config key: invalid config key

.. include:: .res/replace.txt
