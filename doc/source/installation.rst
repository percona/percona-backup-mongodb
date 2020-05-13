.. _pbm.installation:
.. _install:

Installing |pbm|
********************************************************************************

.. contents::
   :local:

|percona| provides and supports installation packages for |pbm| in the *deb* and
*rpm* formats that you can install by using ``apt`` or ``yum`` or other
interfaces to your package management system.

For your convenience, we recommend that you install the |percona-release| utility
which makes it easy to install any |percona| product on your system.

You may also build and install |pbm| from source code in case you require a
fully controlled installation method.

Regardless of the installation method you choose, the following tools are at
your disposal after the installation completes:

===============  ===============================================================
Tool             Purpose
===============  ===============================================================
pbm              Command-line interface for controlling the backup system
pbm-agent        An agent for running backup/restore actions on a database host
===============  ===============================================================

You should install |pbm-agent| on every server that has mongod nodes in the
MongoDB cluster (or non-sharded replica set). The |pbm.app| CLI can be installed
on any or all servers or desktop computers you wish to use it from, so long as
those computers aren't network-blocked from accessing the MongoDB cluster.

.. seealso::

   More information about |percona-release|
      https://www.percona.com/doc/percona-repo-config/percona-release.html

Prerequisites
================================================================================

It is recommended to install |pbm| from official |percona| repositories by using
the |percona-release| utility.

.. code-block:: bash

   $ percona-release enable tools

|pbm| is available for installation from your package management system when you
enable the *tools* repository.

.. seealso:: 

   Configuring |percona| repositories
      https://www.percona.com/doc/percona-repo-config/index.html

Installing |pbm| Using ``apt``
================================================================================

|percona| provides packages for the following systems that use the
:program:`apt` to interface the package management system:

* Debian 8 ("jessie")
* Debian 9 ("stretch")
* Debian 10 ("buster")
* Ubuntu 16.04 LTS (Xenial Xerus)
* Ubuntu 18.04 LTS (Bionic Beaver)
* Ubuntu 20.04 LTS (Focal Fossa)

.. code-block:: bash

   $ apt update
   $ apt install percona-backup-mongodb

Installing |pbm| Using ``yum``
================================================================================

|percona| provides packages for the following systems that use the
:program:`yum` to interface the package management system:

* Red Hat Enterprise Linux / CentOS 6 (current stable release)
* Red Hat Enterprise Linux / CentOS 7 (current stable release)
* Red Hat Enterprise Linux / CentOS 8 (current stable release)  

.. code-block:: bash

   $ yum update
   $ yum install percona-backup-mongodb

Building from source code
================================================================================

Building the project requires:

- Go 1.11 or above
- make

.. seealso::

   Installing and setting up Go tools
      https://golang.org/doc/install

To build the project (from the project dir):

.. code-block:: bash

   $ go get -d github.com/percona/percona-backup-mongodb
   $ cd "$(go env GOPATH)/src/github.com/percona/percona-backup-mongodb"
   $ make

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

|pbm| services and location of configuration files
--------------------------------------------------------------------------------

After |pbm| is successfully installed on your system, you have |pbm.app|
and |pbm-agent| programs on your system.

.. _pbm.installation.service_init_scripts:

Configuring service init scripts
================================================================================

The MongoDB connection URI string to the local mongod node should be set in the
environment file that the `pbm-agent.service` systemd unit file includes.

With the current systemd unit file (see below), this means setting the
"PBM_MONGODB_URI" environment variable in :file:`/etc/default/pbm-agent` (for
Debian and Ubuntu) or :file:`/etc/sysconfig/pbm-agent` (for Red Hat or CentOS).

-----

The :ref:`pbm.running` section, explains in detail how to start |pbm-agent| and
provides examples how to use |pbm.app| commands.

.. hint::

   In Ubuntu and Debian :file:`pbm-agent.service` is located in the
   :file:`/lib/systemd/system/` directory. In Red Hat and CentOS, this
   file is found in :file:`/usr/lib/systemd/system/pbm-agent.service`.

.. include:: .res/code-block/bash/systemd-unit-file.txt

.. seealso::

   More information about standard MongoDB connection strings
      :ref:`pbm.auth`



.. include:: .res/replace.txt
