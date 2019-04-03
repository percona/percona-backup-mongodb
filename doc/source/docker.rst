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
   $ mkdir -m 0700 -p /data/mongodb-backup-coordinator
   $ docker run -d \
      --restart=always \
      --user=$(id -u) \
      --name=mongodb-backup-coordinator \
      -e PBM_COORDINATOR_GRPC_PORT=10000 \
      -e PBM_COORDINATOR_API_PORT=10001 \
      -e PBM_COORDINATOR_WORK_DIR=/data \
      -p 10000-10001:10000-10001 \
      -v /data/mongodb-backup-coordinator:/data \
      mongodb-backup-coordinator

Viewing |bc| logs
================================================================================

.. code-block:: bash

   $ docker logs mongodb-backup-coordinator

Stopping |bc|
================================================================================

.. code-block:: bash

   $ docker stop mongodb-backup-coordinator

Starting the Agent
================================================================================

The |bc| :ref:`must be started <pbm.docker.backup-coordinator.starting>` before
the agent.

.. code-block:: bash

   $ mkdir -m 0700 -p /data/mongodb-backup-agent
   $ docker run -d \
		--restart=always \
		--user=$(id -u) \
		--name=mongodb-backup-agent \
		-e PBM_AGENT_SERVER_ADDRESS=172.16.0.2:10000 \
		-e PBM_AGENT_BACKUP_DIR=/data \
		-e PBM_AGENT_MONGODB_PORT=27017 \
		-e PBM_AGENT_MONGODB_USER=pbmAgent \
		-e PBM_AGENT_MONGODB_PASSWORD=securePassw0rd \
		-v /data/mongodb-backup-agent:/data \
		mongodb-backup-agent

Viewing the Agent Logs
================================================================================

.. code-block:: bash

   $ docker logs mongodb-backup-agent

Stopping the Agent
================================================================================

.. code-block:: bash

   $ docker stop mongodb-backup-agent

.. include:: .res/replace.txt
