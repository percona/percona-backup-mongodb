.. _pbm.installation:

Installation
********************************************************************************

|percona| provides and supports installation packages for |pbm| in the *deb* and
*rpm* formats that you can install by using ``apt`` or ``yum`` or other
interfaces to your package management system.

For your convenience, we recommend that you install the |percona-release| utility
which makes it easy to install any |percona| product on your system.

.. important::

   Make sure to install the latest version of |percona-release|.

You may also build and install |pbm| from source code in case you require a
fully controlled installation method.

Regardless of the installation method you choose, the following tools are at your
disposal after the installation completes:

===============  ===============================================================
Tool             Purpose
===============  ===============================================================
pbm              Command-line interface for controlling the backup system
pbm-agent        An agent for running backup/restore actions on a database host
===============  ===============================================================

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

.. contents::
   :local:

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
- upx (optional)

To build the project (from the project dir):

.. code-block:: bash

   $ go get -d github.com/percona/percona-backup-mongodb
   $ cd $GOPATH/src/github.com/percona/percona-backup-mongodb
   $ make

|pbm| services and location of configuration files
--------------------------------------------------------------------------------

After |pbm| is successfully installed on your system, you have |pbm.app|
and |pbm-agent| programs on your system.

The |pbm| sample configuration files are placed into the :file:`/etc`
directory:

- :file:`/etc/pbm-agent.conf`
- :file:`/etc/pbm-agent-storage.conf`

.. seealso::

   |pbm| storages
      :ref:`pbm.running.storage.setting-up`

.. Unit tests
.. --------------------------------------------------------------------------------
.. 
.. The testing launches a |mongodb| cluster in |docker| containers. ``docker`` and
.. |docker-compose| are required.
.. 
.. .._rubric:: To run the tests (may require 'sudo')
.. 
.. .._code-block:: bash
.. 
..    $ make test-full
.. 
.. .._ rubric:: To tear-down the test (and containers, data, etc)
.. 
.. .._code-block:: bash
.. 
..    $ make test-full-clean
.. 

-----

.. include:: .res/replace.txt
