.. _pbm.building:

Building
********************************************************************************

Building the project requires:

- Go 1.11 or above
- make
- upx (optional)

To build the project (from the project dir):

.. code-block:: bash

   $ go get -d github.com/percona/percona-backup-mongodb
   $ cd $GOPATH/src/github.com/percona/percona-backup-mongodb
   $ make

A successful build outputs the following binaries: 

===============  ===============================================================
Binary           Purpose
===============  ===============================================================
pbmctl           Command-line interface for controlling the backup system
pbm-agent        Agent for running backup/restore actions on a database host
pbm-coordinator  Server for coordinating backup system actions
===============  ===============================================================

Unit Tests
--------------------------------------------------------------------------------

The testing launches a |mongodb| cluster in |docker| containers. ``docker`` and
``docker-compose`` are required.

.. rubric:: To run the tests (may require 'sudo')

.. code-block:: bash

   $ make test-full

.. rubric:: To tear-down the test (and containers, data, etc)

.. code-block:: bash

   $ make test-full-clean

.. include:: .res/replace.txt
