# Percona Backup for MongoDB
[![Go Report Card](https://goreportcard.com/badge/github.com/percona/percona-backup-mongodb)](https://goreportcard.com/report/github.com/percona/percona-backup-mongodb) [![codecov](https://codecov.io/gh/percona/percona-backup-mongodb/branch/master/graph/badge.svg?token=TiuOmTfp2p)](https://codecov.io/gh/percona/percona-backup-mongodb) [![CLA assistant](https://cla-assistant.percona.com/readme/badge/percona/percona-backup-mongodb)](https://cla-assistant.percona.com/percona/percona-backup-mongodb)

![PBM logo](doc/source/images/backup-mongo.jpeg)

Percona Backup for MongoDB (PBM) is a distributed, low-impact solution for achieving
consistent backups of MongoDB sharded clusters and replica sets. Percona Backup for MongoDB supports Percona Server for MongoDB and MongoDB Community Edition v3.6 and higher.

For more information about PBM components and how to use it, see
[Percona Backup for MongoDB documentation](https://www.percona.com/doc/percona-backup-mongodb)

Percona Backup for MongoDB includes the following **features**:

- Backup and restore for both classic non-sharded replica sets and sharded clusters
- Point-in-Time recovery
- Simple command-line management utility
- Replica set and sharded cluster consistency through oplog capture
- Distributed transaction consistency with MongoDB 4.2+
- Simple, integrated-with-MongoDB authentication
- No need to install a coordination service on a separate server
- Use of any S3-compatible storage
- Support of locally-mounted remote filesystem backup servers.

## Architecture

Percona Backup for MongoDB consists of the following components:

- **pbm-agent** is a process running on every mongod node within the cluster or a replica set that performs backup and restore operations.
- **pbm** CLI is a command-line utility that instructs pbm-agents to perform an operation.
- **PBM Control collections** are special collections in MongoDB that store the configuration data and backup states
- Remote backup storage as either s3-compatible or filesystem type storage

![Architecture](https://github.com/percona/pbm-docs/blob/main/source/_images/pbm-architecture.png)

[Read more about PBM architecture](https://www.percona.com/doc/percona-backup-mongodb/architecture.html#pbm-architecture).

## Installation

You can install Percona Backup for MongoDB in the following ways:
- from Percona repository (recommended)
- build from source code

Find the installation instructions in the [official documentation](https://www.percona.com/doc/percona-backup-mongodb/installation.html)

Alternatively, you can [run Percona Backup for MongoDB as a Docker container](https://hub.docker.com/r/percona/percona-backup-mongodb).

## Submit Bug Report / Feature Request

If you find a bug in Percona Backup for MongoDB, you can submit a report to the project's [JIRA issue tracker](https://jira.percona.com/projects/PBM).

As a general rule of thumb, please try to create bug reports that are:

- Reproducible. Include steps to reproduce the problem.
- Specific. Include as much detail as possible: which version, what environment, etc.
- Unique. Do not duplicate existing tickets.
- Scoped to a Single Bug. One bug per report.

When submitting a bug report or a feature, please attach the following information:

- The output of the [`pbm status`](https://docs.percona.com/percona-backup-mongodb/status.htm) command
- The output of the [`pbm logs`](https://docs.percona.com/percona-backup-mongodb/running.html#pbm-logs) command. Use the following filters:

   ```sh
   $ pbm logs -x -s D -t 0
   ```

>**NOTE** : When reporting an issue with a certain event or a node, you can use the following filters to receive a more specific data set:

>```bash
>#Logs per node
>$ pbm logs -x -s D -t 0 -n replset/host:27017
>#Logs per event
>$ pbm logs -x -s D -t 0 -e restore/2020-10-06T11:45:14Z
>```


## Licensing

Percona is dedicated to **keeping open source open**. Wherever possible, we strive to include permissive licensing for both our software and documentation. For this project, we are using the Apache License 2.0 license. 

## How to get involved

We encourage contributions and are always looking for new members that are as dedicated to serving the community as we are.

The [Contributing Guide](https://github.com/percona/percona-backup-mongodb/blob/main/CONTRIBUTING.md) contains the guidelines how you can contribute.

## Contact

You can reach us:
* on [Forums](https://forums.percona.com) and [Discord](https://discord.gg/mQEyGPkNbR)
* by [Email](mailto:mongodb-backup@percona.com)
