.. _pbm.roadmap:

Roadmap
********************************************************************************

Continuous oplog capture and Point-in-time restore
--------------------------------------------------------------------------------

- Add an optional mode of |pbm-backup| that causes |pbm-agent| to continually add new oplog entries (minus "noop" etc.) after the snapshot is created.
  - Minor user interaction requirement: Will require another subcommand to allow users to stop the oplog capture
- Add an optional argument to |pbm-restore| that accepts an exact timestamp to restore to, if there is suitably captured oplog. The restore operation will restore the closest snapshot before that that time then replay the oplogs for each replicaset to the requested time.

.. Restore on new hosts
.. --------------------------------------------------------------------------------
.. 
.. - When backup is restored on new hosts the snapshot contains system collections with host definitions (replicaset hosts, configsvr connection strings, etc.) and config file items (net.bind IP addresses etc.) that are incompatible for the new location. Development goal is to provide a way for users to fix these in one step.

.. include:: .res/replace.txt
