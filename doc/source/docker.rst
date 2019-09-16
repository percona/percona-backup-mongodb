.. _pbm.docker:

Docker
********************************************************************************

To build the |docker| images (requires 'docker' or 'docker-ce'):

.. code-block:: bash

   $ make docker-build

.. note::

   Official Dockerhub images are coming soon!

.. contents::
   :local:

.. _pbm.docker.backup-coordinator.starting:

Starting Backup Coordinator
================================================================================

The working directory must be owned by the user passed to ``docker`` via the
``--user`` option:

.. code-block:: bash

   $ docker run --user=X
   $ docker run -d \
   --restart=always \
   --user=$(id -u) \
   --name=percona-backup-mongodb \
   -e PBM_COORDINATOR_GRPC_PORT=10000 \
   -e PBM_COORDINATOR_API_PORT=10001 \
   -e PBM_COORDINATOR_WORK_DIR=/data \
   -p 10000-10001:10000-10001 \
   -v /data/mongodb-backup:/data \
   perconalab/percona-backup-mongodb \
   pbm


Viewing |bc| logs
================================================================================

.. code-block:: bash

   $ docker logs percona-backup-mongodb

Stopping |bc|
================================================================================

.. code-block:: bash

   $ docker stop percona-backup-mongodb

Starting the Agent
================================================================================

.. code-block:: bash

   $ mkdir -m 0700 -p /data/mongodb-backup-agent
   $ docker run -d \
   --restart=always \
   --user=$(id -u) \
   --name=mongodb-backup-agent \
   -e PBM_AGENT_SERVER_ADDRESS=172.16.0.2:10000 \
   -e PBM_AGENT_BACKUP_DIR=/data \
   -e PBM_AGENT_MONGODB_HOST=172.16.0.2 \
   -e PBM_AGENT_MONGODB_PORT=27017 \
   -e PBM_AGENT_MONGODB_USERNAME=backupUser \
   -e PBM_AGENT_MONGODB_PASSWORD=$eQrP@sw8rD \
   -e PBM_AGENT_STORAGE_CONFIG=/data/storage-config.yml \
   -e PBM_AGENT_MONGODB_REPLICASET=rs1 \
   -v /data/mongodb-backup-agent1:/data \
   perconalab/percona-backup-mongodb \
   pbm-agent

Viewing the Agent Logs
================================================================================

.. code-block:: bash

   $ docker logs mongodb-backup-agent

Stopping the Agent
================================================================================

.. code-block:: bash

   $ docker stop mongodb-backup-agent

Passing Environment Variables When Creating a Container
================================================================================

By using the ``-e`` option, you can set any environment variables when creating
your docker container. The following example sets a bunch of environment
variables in ``docker run``. Make sure to repeat ``-e`` for each
environment variable.

.. code-block:: bash

   $ docker run --user=X
   --restart=always \
   --user=$(id -u) \
   --name=percona-backup-mongodb \
   ...
   -e PBM_AGENT_SERVER_ADDRESS=192.168.88.3:10000 \
   -e PBM_AGENT_BACKUP_DIR=/data \
   -e PBM_AGENT_MONGODB_HOST=192.168.88.3 \
   -e PBM_AGENT_MONGODB_PORT=27019 \
   -e PBM_AGENT_MONGODB_USERNAME=user \
   -e PBM_AGENT_MONGODB_PASSWORD=pass \
   -p 10000-10001:10000-10001 \
   -v /data/percona-mongodb-backup:/data \
   perconalab/percona-backup-mongodb \
   pbm-coordinator
   
In general, you can use ``-e`` to pass any supported options as environment
variables and change the configuration either of |pbm| or
|pbm-agent|. Several rules are applied to the name of an environment variable
bound to an option:

- The name of the environment variable starts with the ``PBM_`` or
  ``PBM_AGENT`` prefix.
- Only long options (those start with two dashes ":code:`--`") can be used in
  environment variables.
- The dash delimiter (:code:`-`) in options is replaced with the underscore
  (:code:`_`).

For example, to pass |opt-config-file| as an environment variable with ``docker
run`` when creating a container for |pbm-agent|, use the following syntax:

.. code-block:: text
   
   -e PBM_AGENT_CONFIG_FILE

To see the full list of supported options run |pbm-help| or
|pbm-agent-help| accordingly.

.. include:: .res/replace.txt
