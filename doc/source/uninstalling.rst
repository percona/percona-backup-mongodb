.. _pbm.uninstalling:

Uninstalling |pbm|
********************************************************************************

To uninstall |pbm| perform the following steps:

1. Check no backups are currently in progress in the output of |pbm-list|.
#. Before the next 2 steps make sure you know where the remote backup storage
   is, so you can delete backups made by |pbm|. If it is S3-compatible object
   storage you will need to use another tool such as Amazon AWS's "aws s3",
   Minio's ``mc``, the web AWS Management Console, etc. to do that once |pbm| is
   uninstalled.
#. Uninstall the |pbm-agent| and |pbm.app| executables. If you installed using a
   package manager, see :ref:`pbm.installation` for relevant package names and
   commands for your OS distribution.
#. Drop the :ref:`PBM control collections <pbm.architecture.pbm_control_collections>`.
#. Drop the PBM user. If this is a cluster the dropUser command will need to be
   run on each shard as well as in the config server replica set.
#. (Optional) Delete the backups from the remote backup storage.

.. include:: .res/replace.txt
