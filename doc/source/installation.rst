.. _pbm.installation:
.. _install:

Installing |pbm|
********************************************************************************

.. contents::
   :local:

|percona| provides and supports installation packages for |pbm| in the *deb* and
*rpm* formats that you can install by using ``apt`` or ``yum`` or other
interfaces to your package management system.

The `Percona Software and Platform Lifecycle <https://www.percona.com/services/policies/percona-software-platform-lifecycle#mongodb>`_ page lists Linux distributions for which |pbm| installation packages are available.

The recommended approach is to install |pbm|  from Percona repositories using the  |percona-release| tool.

If you want a full control over the installation, you may build and install |pbm| from source code.

Regardless of the installation method you choose, the following tools are at
your disposal after the installation completes:

===============  ===============================================================
Tool             Purpose
===============  ===============================================================
pbm              Command-line interface for controlling the backup system
pbm-agent        An agent for running backup/restore actions on a database host
===============  ===============================================================

|

Install |pbm-agent| on every server that has mongod nodes in the
MongoDB cluster (or non-sharded replica set). You can install |pbm.app| CLI 
on any or all servers or desktop computers you wish to use it from, so long as
those computers aren't network-blocked from accessing the MongoDB cluster.

Installing from Percona repositories
================================================================================

To install |pbm|, first install |percona-release| tool using the package manager of your operating system. This is a repository management tool that automatically configures the required repository for you.  

Follow the instructions in `Percona Software repositories documentation <https://www.percona.com/doc/percona-repo-config/percona-release.html>`_ to install |percona-release|.

Enable the repository. As of version 1.3.0, |pbm| packages are stored in the *pbm* repository.

.. code-block:: bash

   $ sudo percona-release enable pbm release

Installing |pbm| Using ``apt``
---------------------------------------------

.. code-block:: bash

   $ sudo apt-get update
   $ sudo apt-get install percona-backup-mongodb

Installing |pbm| Using ``yum``
---------------------------------------------

.. code-block:: bash

   $ sudo yum install percona-backup-mongodb

Building from source code
================================================================================

Building the project requires:

- Go 1.11 or above
- make
- git

.. seealso::

   Installing and setting up Go tools
      https://golang.org/doc/install

To build the project (from the project dir):

.. code-block:: bash

   $ go get -d github.com/percona/percona-backup-mongodb
   $ cd "$(go env GOPATH)/src/github.com/percona/percona-backup-mongodb"
   $ make build

After :program:`make` completes, you can find |pbm.app| and |pbm-agent| binaries
in the :dir:`./bin` directory:

.. code-block:: bash

   $ cd bin
   $ pbm version

By running :program:`pbm version`, you can verify if |pbm| has been built correctly and is ready for use.

.. admonition:: Output

   .. code-block:: guess

      Version:   [pbm version number]
      Platform:  linux/amd64
      GitCommit: [commit hash]
      GitBranch: master
      BuildTime: [time when this version was produced in UTC format]
      GoVersion: [Go version number]

After |pbm| is successfully installed on your system, you have |pbm.app|
and |pbm-agent| programs available. See :ref:`initial-setup` for guidelines how to set up |PBM|.  


.. include:: .res/replace.txt
