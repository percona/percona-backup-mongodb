.. _pbm.upgrade:

********************************************************************************
Upgrading |PBM|
********************************************************************************

.. contents::
   :local:
   :depth: 2

Similar to installing, the recommended and most convenient way to upgrade |PBM| is from the |percona| repository. 

You can upgrade |PBM| to the **latest version** or to a **specific version**. Since all packages of |PBM| are stored in the same repository, the following steps apply to both upgrade scenarios:

1. Enable |percona| repository.
#. Stop |pbm-agent|. 
#. Install new version packages (the old ones are automatically removed).
#. Start |pbm-agent|.
   
.. _important_notes:   

.. rubric:: Important notes
 
1. Backward compatibility between data backup and restore is supported for upgrades within one major version only (e.g. from 1.1.x to 1.2.y). When you upgrade |PBM| over several major versions (e.g. from 1.0.x to 1.2.y), we recommend to make a backup right after the upgrade.
2. Upgrade |PBM| on all nodes where it is installed.
      
Enable |percona| repository
===========================

Install the |percona-release| utility or update it to the latest version as described in `Percona Software Repositories Documentation <https://www.percona.com/doc/percona-repo-config/percona-release.html#installation>`_.

Enable the repository running the command as root or via |sudo|

.. code-block:: bash

   $ sudo percona-release enable tools  

.. note:: 

   For ``apt``-based systems, run :command:`apt-get update` to update the local cache.

Upgrade |PBM| using ``apt``
================================================================================

.. important:: 

   Run all commands as root or via |sudo|.

Upgrade to the latest version
--------------------------------------------------------------------------------

1. Stop |pbm-agent|

   .. code-block:: bash

      $ sudo systemctl stop pbm-agent

#. Install new packages 
   
   .. code-block:: bash
   
      apt-get install percona-backup-mongodb

#. Start |pbm-agent|

   .. code-block:: bash

      $ sudo systemctl start pbm-agent

Upgrade to a specific version
--------------------------------------------------------------------------------

1. List available options: 


   .. code-block:: bash
   
      $ sudo apt-cache madison percona-backup-mongodb
   
   .. admonition:: Sample output
   
      .. include:: .res/text/apt-versions-list.txt

#. Stop |pbm-agent|: 

   .. code-block:: bash
   
      $ sudo systemctl stop pbm-agent  

#. Install a specific version packages. For example, to upgrade to |PBM| 1.1.3, run the following command:   

   .. code-block:: bash
   
      $ sudo apt-get install percona-backup-mongodb=1.1.3-1.stretch

#. Start |pbm-agent|: 

   .. code-block:: bash
   
      $ sudo systemctl start pbm-agent

Upgrade |PBM| using ``yum``
================================================================================ 

.. important:: 

   Run all commands as root or via |sudo|.

Upgrade to the latest version
--------------------------------------------------------------------------------
                  
1. Stop |pbm-agent|

   .. code-block:: bash

      $ sudo systemctl stop pbm-agent

#. Install new packages 
   
   .. code-block:: bash
   
      $ sudo yum install percona-backup-mongodb

#. Start |pbm-agent|

   .. code-block:: bash

      $ sudo systemctl start pbm-agent

Upgrade to a specific version 
--------------------------------------------------------------------------------

1. List available versions
   
   .. code-block:: bash
   
      $ sudo yum list percona-backup-mongodb --showduplicates

   .. admonition:: Sample output

      .. include:: .res/text/yum-versions-list.txt

#. Stop |pbm-agent|: 
 
   .. code-block:: bash
   
      $ sudo systemctl stop pbm-agent

#. Install a specific version packages. For example, to upgrade |PBM| to version 1.1.3, use the following command: 
   
   .. code-block:: bash
   
      $ sudo yum install percona-backup-mongodb-1.1.3-1.el7

#. Start |pbm-agent|: 

   .. code-block:: bash
   
      $ sudo systemctl start pbm-agent


      


.. include:: .res/replace.txt           
