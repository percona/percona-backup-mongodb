.. _pbm-status:


Checking status of |PBM| 
******************************************************

As of version 1.4.0, you can check the status of |pbm| running in your MongoDB environment. This helps you identify 

To check |PBM| status, run the following command:

.. code-block:: bash

   $ pbm status

The output of the :command:`pbm status` command provides the information about:

- Your MongoDB deployment and ``pbm-agents`` running in it: to what ``mongod`` node each agent is connected, the |pbm| version it runs and the agent's state
- The currently running backups / restores, if any
- Backups stored in the remote backup storage: backup name, completion time, size and status (complete, canceled, failed)
- :term:`Point-in-Time Recovery` status (enabled or disabled).
- Valid time ranges for point-in-time recovery and the data size

.. admonition:: Sample output

   .. code-block:: text
   
      Cluster:
      ========
      config:
        - config/localhost:27027: pbm-agent v1.3.2 OK
        - config/localhost:27028: pbm-agent v1.3.2 OK
        - config/localhost:27029: pbm-agent v1.3.2 OK
      rs1:
        - rs1/localhost:27018: pbm-agent v1.3.2 OK
        - rs1/localhost:27019: pbm-agent v1.3.2 OK
        - rs1/localhost:27020: pbm-agent v1.3.2 OK
      rs2:
        - rs2/localhost:28018: pbm-agent v1.3.2 OK
        - rs2/localhost:28019: pbm-agent v1.3.2 OK
        - rs2/localhost:28020: pbm-agent v1.3.2 OK
        
      PITR incremental backup:
      ========================
      Status [OFF]

      Currently running:
      ==================
      (none)

      Backups:
      ========
      S3 us-east-1 https://storage.googleapis.com/backup-test
         Snapshots:
           2020-12-16T10:36:52Z 491.98KB [complete: 2020-12-16T10:37:13]
           2020-12-15T12:59:47Z 284.06KB [complete: 2020-12-15T13:00:08]
           2020-12-15T11:40:46Z 0.00B [canceled: 2020-12-15T11:41:07]
           2020-12-11T16:23:55Z 284.82KB [complete: 2020-12-11T16:24:16]
           2020-12-11T16:22:35Z 284.04KB [complete: 2020-12-11T16:22:56]
           2020-12-11T16:21:15Z 283.36KB [complete: 2020-12-11T16:21:36]
           2020-12-11T16:19:54Z 281.73KB [complete: 2020-12-11T16:20:15]
           2020-12-11T16:19:00Z 281.73KB [complete: 2020-12-11T16:19:21]
           2020-12-11T15:30:38Z 287.07KB [complete: 2020-12-11T15:30:59]
      PITR chunks:
           2020-12-16T10:37:13 - 2020-12-16T10:43:26 44.17KB


.. include:: .res/replace.txt
