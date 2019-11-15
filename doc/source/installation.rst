.. _pbm.installation:

.. contents::
   :local:

Installation
********************************************************************************

|percona| provides and supports installation packages for |pbm| in the *deb* and
*rpm* formats that you can install by using ``apt`` or ``yum`` or other
interfaces to your package management system.

For your convenience, we recommend that you install the |percona-release| utility
which makes it easy to install any |percona| product on your system.

You may also build and install |pbm| from source code in case you require a
fully controlled installation method.

Regardless of the installation method you choose, the following tools are at your disposal after the installation completes:

===============  ===============================================================
Tool             Purpose
===============  ===============================================================
pbm              Command-line interface for controlling the backup system
pbm-agent        An agent for running backup/restore actions on a database host
===============  ===============================================================

You should install |pbm-agent| on every server that has mongod nodes in the MongoDB cluster (or non-sharded replica set). The |pbm.app| CLI can be installed on any or all servers or desktop computers you wish to use it from, so long as those computers aren't network-blocked from accessing the MongoDB cluster.

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
* Ubuntu 14.04 LTS (Trusty Tahr)
* Ubuntu 16.04 LTS (Xenial Xerus)
* Ubuntu 17.10 (Artful Aardvark)
* Ubuntu 18.04 LTS (Bionic Beaver)

.. code-block:: bash

   $ apt update
   $ apt install percona-backup-mongodb

Installing |pbm| Using ``yum``
================================================================================

|percona| provides packages for the following systems that use the
:program:`yum` to interface the package management system:

* Red Hat Enterprise Linux / CentOS 6 (current stable release)
* Red Hat Enterprise Linux / CentOS 7 (current stable release)

.. code-block:: bash

   $ yum update
   $ yum install percona-backup-mongodb

Building from source code
================================================================================

Building the project requires:

- Go 1.11 or above
- make

To build the project (from the project dir):

.. code-block:: bash

   $ go get -d github.com/percona/percona-backup-mongodb
   $ cd $GOPATH/src/github.com/percona/percona-backup-mongodb
   $ make

|pbm| services and location of configuration files
--------------------------------------------------------------------------------

After |pbm| is successfully installed on your system, you have |pbm.app|
and |pbm-agent| programs on your system.

.. _pbm.installation.service_init_scripts:

Configuring service init scripts
================================================================================

Some configuration is required for the service script (e.g. systemd unit file) that will run the |pbm-agent| processes.

- The MongoDB connection URI string to the local mongod node. (See pbm.auth_ for an explanation of standard MongoDB connection strings if you need.)
- A filepath to save log output to. |pbm-agent|'s log output comes straight to stdout and the service script just redirects it (and stderr) to this path.

.. seealso::

   |pbm| stores
      :ref:`pbm.config.storage.setting-up`


.. include:: .res/replace.txt
