# Percona Backup for MongoDB
[![Go Report Card](https://goreportcard.com/badge/github.com/percona/percona-backup-mongodb)](https://goreportcard.com/report/github.com/percona/percona-backup-mongodb) [![codecov](https://codecov.io/gh/percona/percona-backup-mongodb/branch/master/graph/badge.svg?token=TiuOmTfp2p)](https://codecov.io/gh/percona/percona-backup-mongodb) [![CLA assistant](https://cla-assistant.percona.com/readme/badge/percona/percona-backup-mongodb)](https://cla-assistant.percona.com/percona/percona-backup-mongodb)

Percona Backup for MongoDB is a distributed, low-impact solution for achieving consistent backups of MongoDB Sharded Clusters and Replica Sets.

The project was inspired by *(and intends to replace)* the [Percona-Lab/mongodb_consistent_backup](https://github.com/Percona-Lab/mongodb_consistent_backup) tool.

1. [Feature Progress](#feature-progress)
1. [Architecture](#architecture)
    1. [Coordinator](#coordinator)
    1. [Agent](#agent)
    1. [PBM Control (pbmctl)](#pbm-control-pbmctl)
    1. [Running](#running)
        1. [Running the Coordinator](#running-the-coordinator)
        1. [Running the Agent](#running-the-agent)
            1. [MongoDB Authentication](#mongodb-authentication)
        1. [Running pbmctl commands](#running-pbmctl-commands)
            1. [Command Examples](#command-examples)
1. [Requirements](#requirements)
1. [Installing](#installing)
    1. [CentOS/RedHat](#centosredhat)
    1. [Debian/Ubuntu](#debianubuntu)
    1. [Mac OSX](#mac-osx)
    1. [From Source](#from-source)
        1. [Unit Tests](#unit-tests)
1. [Docker](#docker)
    1. [Build Docker images](#build-docker-images)
    1. [Run Docker containers](#run-docker-containers)
        1. [Coordinator](#coordinator1)
            1. [Start Coordinator](#start-coordinator)
            1. [View Coordinator logs](#view-coordinator-logs)
            1. [Stop Coordinator](#stop-coordinator)
        1. [Agent](#agent1)
            1. [Start Agent](#start-agent)
            1. [View Agent logs](#view-agent-logs)
            1. [Stop Agent](#stop-agent)
1. [Submit Bug Report / Feature Request](#submit-bug-report--feature-request)
1. [Contact](#contact)

## Feature Progress
- [x] Oplog tailer
- [x] Oplog applier
- [x] AWS S3 streamer
    - [x] Backup *(upload)* 
    - [ ] Restore *(download)*
- [x] Mongodump Backup Method
    - [x] Backup
    - [x] Restore
- [x] Agent selection algorithm
- [x] SSL/TLS support
- [x] Replica Set Backup
- [x] Sharded Cluster Backup
    - [x] Pausing of balancer at backup-time
- [x] Command-line management utility
- [x] Compression
    - [x] Agent and CLI RPC communications
    - [x] Backup data - Gzip
    - [ ] Backup data - LZ4 and Snappy
- [ ] Dockerhub images
- [ ] Authorization of Agents and CLI
- [ ] Encryption of backup data
- [ ] Support for MongoDB SSL/TLS connections
- [ ] Recovery from agent failures
- [ ] Support for [Percona Server for MongoDB Hot Backup](https://www.percona.com/doc/percona-server-for-mongodb/LATEST/hot-backup.html) for binary-level backup
    - [ ] Backup
    - [ ] Restore
    - [ ] Support for [WiredTiger Encryption](https://www.percona.com/blog/2018/11/01/wiredtiger-encryption-at-rest-percona-server-for-mongodb/)
- [ ] Support for more upload/transfer methods
- [ ] Multiple sources of credentials *(eg: file, Vault, Amazon KMS, etc)*
- [ ] Restore from any Point-in-time
    - [ ] Support for incremental backups using oplogs
- [ ] Prometheus metrics

# Architecture

Percona Backup for MongoDB uses a distributed client/server architecture to perform backup/restore actions. This architecture model was chosen to provide maximum scalability/flexibility.

![MongoDB Replica Set](mongodb-replica-set.png)

## Coordinator

The backup coordinator is a daemon that handles communication with backup agents and the backup control program.

The main function of the backup coordinator is to gather information from the MongoDB instances through the agents to determine which nodes should run the backup/restore and to establish consistent backup/restore points across all shards.

The Coordinator listens on 2 x TCP ports:
1. **RPC** - Port used for agent communications *(Default: 10000/tcp)*
2. **API** - Port used for CLI/API communications *(Default: 10001/tcp)*

## Agent

Backup Agents are in charge of receiving commands from the coordinator and run them.

The agent must run locally *(connected to 'localhost')* on every MongoDB instance *(mongos and config servers included)* in order to collect information about the instance and forward it to the coordinator. With that information, the coordinator can determine the best agent to start a backup or restore, to start/stop the balancer, etc.

The agent requires outbound network access to the [Coordinator](#Coordinator) RPC port.



### Defining backup storages

Before running the agent, you need to create a yaml file with the available storages configuration for each backup agent. Each storage has a name that will be used to identify the storage, the type (local file system or S3) and the storage definition parameters.

**Example**

```
s3-us-west:
  type: s3
  s3:
    region: us-west-2
    endpointUrl: https://minio
    bucket: bucket name
    credentials:
      access-key-id: <something>
      secret-access-key: <something>
local-filesystem:
  type: filesystem
  filesystem:
    path: path/to/the/backup/dir
```



## PBM Control (pbmctl)

This program is a command line utility to send commands to the coordinator.
Currently, the available commands are:  
- **list nodes**: List all nodes (agents) connected to the coordinator
- **list backups**: List all finished backups.
- **run backup**: Start a new backup
- **run restore**: Restore a backup

The pbmctl utility requires outbound network access to the [Coordinator](#Coordinator) API port.

## Running

### Running the Coordinator

The backup coordinator can be executed in any server since it doesn't need a connection to a MongoDB instance. One Coordinator is needed per deployment.

To start the coordinator run:

```
$ pbm-coordinator --work-dir=<directory to store metadata>
```
If `--work-dir` is not specified, it will use the default `${HOME}/percona-backup-mongodb`.
By default, the coordinator will listen for agents on port 10000.

### Running the Agent

On every MongoDB instance, you need to start an agent that will receive commands from the coordinator.

By default the agent will connect to MongoDB using the host '127.0.0.1' and the port 27017. See [MongoDB Authentication](#MongoDB-Authentication) below if Authentication is enabled on the MongoDB host.

Example:
```
$ pbm-agent --server-address=172.16.0.2:10000 \
            --backup-dir=/data/backup \
            --mongodb-port=27017 \
            --mongodb-user=pbmAgent \
            --mongodb-password=securePassw0rd \
            --storages-config=/path/to/storages.yaml \
            --pid-file=/tmp/pbm-agent.pid
```

#### MongoDB Authentication

If [MongoDB Authentication](https://docs.mongodb.com/manual/core/authentication/) is enabled the backup agent must be provided credentials for a MongoDB user with the ['backup'](https://docs.mongodb.com/manual/reference/built-in-roles/#backup), ['restore'](https://docs.mongodb.com/manual/reference/built-in-roles/#restore) and ['clusterMonitor'](https://docs.mongodb.com/manual/reference/built-in-roles/#clusterMonitor) built-in auth roles. This user must exist on every database node and it should not be used by other applications.

Example *'createUser'* command *(must be ran via the 'mongo' shell on a PRIMARY member)*:

```
> use admin;
> db.createUser({
    user: "pbmAgent",
    pwd: "securePassw0rd",
    roles: [
        { db: "admin", role: "backup" },
        { db: "admin", role: "clusterMonitor" },
        { db: "admin", role: "restore" }
    ],
    authenticationRestrictions: [
        { clientSource: ["127.0.0.1"] }
    ]
})
```

### Running pbmctl commands

`pbmctl` is the command line utility to control the backup system. 
Since it needs to connect to the coordinator you need to specify the coordinator `ip:port`. The defaults are `127.0.0.1:10001` so, if you are running `pbmctl` from the same server where the coordinator is running, you can ommit the `--server-address` parameter.  

#### Command Examples

##### List all connected agents
```
$ pbmctl --server-address=127.0.0.1:10001 list nodes
```
Sample output:
```
          Node ID                            Cluster ID                   Node Type                   Node Name
------------------------------------   ------------------------   --------------------------   ------------------------------
localhost:17000                      - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOS           - 127.0.0.1:17000
127.0.0.1:17001                      - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:17001
127.0.0.1:17002                      - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:17002
127.0.0.1:17003                      - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:17003
127.0.0.1:17004                      - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:17004
127.0.0.1:17005                      - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:17005
127.0.0.1:17006                      - 5c1942acbf27f9aceccb3c2f - NODE_TYPE_MONGOD_SHARDSVR  - 127.0.0.1:17006
127.0.0.1:17007                      -                          - NODE_TYPE_MONGOD_CONFIGSVR - 127.0.0.1:17007
```

##### Start a backup
```
$ pbmctl run backup --description "Test backup 01" --storage s3-us-west
```

##### List all the completed backups
```
$ pbmctl --server-address=127.0.0.1:10001 list backups
```
Sample output:
```
       Metadata file name     -         Description
------------------------------ - ---------------------------------------------------------------------------
2018-12-18T19:04:14Z.json      - Test backup 01
```

##### Restore a backup
```
$ pbmctl run restore 2018-12-18T19:04:14Z.json --storage s3-us-west
```

##### List available storages

```
pbmctl list storages
```

Sample output:

```
Available Storages:
-------------------------------------------------------------------------------------------------
Name         : local-filesystem
MatchClients : 127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003,127.0.0.1:17004,127.0.0.1:17005,127.0.0.1:17006,127.0.0.1:17007,karl-hp-omen:17000,
DifferClients:
Storage configuration is valid.
Type: filesystem
  Path       : /tmp/dump_test/
-------------------------------------------------------------------------------------------------
Name         : s3-us-west
MatchClients : 127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003,127.0.0.1:17004,127.0.0.1:17005,127.0.0.1:17006,127.0.0.1:17007,karl-hp-omen:17000,
DifferClients:
Storage configuration is valid.
Type: s3
  Region      : us-west-2
  Endpoint URI:
  Bucket      : pbm-test-bucket-69835
-------------------------------------------------------------------------------------------------
```



# Requirements

1. [Percona Server for MongoDB](https://www.percona.com/software/mongo-database/percona-server-for-mongodb) or MongoDB Community 3.6 and above
    1. [MongoDB Replication](https://docs.mongodb.com/manual/replication/) enabled

# Installing

Releases include RPM/Debian-based packages *(recommended)* and binary tarballs. The packages contain all 3 x Percona Backup for MongoDB binaries.

## CentOS/RedHat
*Note: replace 'v0.2.1' with desired release name from [Releases Page](https://github.com/percona/percona-backup-mongodb/releases)*

```
$ rpm -Uvh https://github.com/percona/percona-backup-mongodb/releases/download/v0.2.1/percona-backup-mongodb_0.2.1_linux_amd64.rpm
Retrieving https://github.com/percona/percona-backup-mongodb/releases/download/v0.2.1/percona-backup-mongodb_0.2.1_linux_amd64.rpm
Preparing...                          ################################# [100%]
Updating / installing...
   1:percona-backup-mongodb-0.2.1-1   ################################# [100%]
```

## Debian/Ubuntu
*Note: replace 'v0.2.1' with desired release name from [Releases Page](https://github.com/percona/percona-backup-mongodb/releases)*
```
$ wget -q https://github.com/percona/percona-backup-mongodb/releases/download/v0.2.1/percona-backup-mongodb_0.2.1_linux_amd64.deb
$ dpkg -i percona-backup-mongodb_0.2.1_linux_amd64.deb                   
Selecting previously unselected package percona-backup-mongodb.
(Reading database ... 6977 files and directories currently installed.)
Preparing to unpack percona-backup-mongodb_0.2.1_linux_amd64.deb ...
Unpacking percona-backup-mongodb (0.2.1) ...
Setting up percona-backup-mongodb (0.2.1) ...
```

## Mac OSX
Use *'darwin'* binary tarballs from [Releases Page](https://github.com/percona/percona-backup-mongodb/releases)

## From Source

Building the project requires:
1. Go 1.11 or above
1. make
1. upx *(optional)*

To build the project *(from the project dir)*:
```
$ go get -d github.com/percona/percona-backup-mongodb
$ cd $GOPATH/src/github.com/percona/percona-backup-mongodb
$ make
```

A successful build outputs binaries: 
1. **pbmctl**: A command-line interface for controlling the backup system
1. **pbm-agent**: An agent that executes backup/restore actions on a database host
1. **pbm-coordinator**: A server that coordinates backup system actions

### Unit Tests

The testing launches a MongoDB cluster in Docker containers. *'docker'* and *'docker-compose'* is required.

To run the tests *(may require 'sudo')*:
```
$ make test-full
```

To tear-down the test *(and containers, data, etc)*:
```
$ make test-full-clean
```

# Docker

*Note: Official Dockerhub images coming soon!*

## Build Docker images

To build the Docker images *(requires 'docker' or 'docker-ce')*:
```
$ make docker-build
```

## Run Docker containers

### Coordinator

#### Start Coordinator
*Note: work dir must be owned by the passed to 'docker run --user=X'*
```
$ mkdir -m 0700 -p /data/mongodb-backup-coordinator
$ docker run -d \
    --restart=always \
    --user=$(id -u) \
    --name=mongodb-backup-coordinator \
    -e PBM_COORDINATOR_GRPC_PORT=10000 \
    -e PBM_COORDINATOR_API_PORT=10001 \
    -e PBM_COORDINATOR_WORK_DIR=/data \
    -p 10000-10001:10000-10001 \
    -v /data/mongodb-backup-coordinator:/data \
mongodb-backup-coordinator
```

#### View Coordinator logs
```
$ docker logs mongodb-backup-coordinator
```

#### Stop Coordinator
```
$ docker stop mongodb-backup-coordinator
```

### Agent

#### Start Agent
*Note: the [Coordinator](#start-coordinator) must be started before the agent!*
```
$ mkdir -m 0700 -p /data/mongodb-backup-agent
$ docker run -d \
    --restart=always \
    --user=$(id -u) \
    --name=mongodb-backup-agent \
    -e PBM_AGENT_SERVER_ADDRESS=172.16.0.2:10000 \
    -e PBM_AGENT_BACKUP_DIR=/data \
    -e PBM_AGENT_MONGODB_PORT=27017 \
    -e PBM_AGENT_MONGODB_USER=pbmAgent \
    -e PBM_AGENT_MONGODB_PASSWORD=securePassw0rd \
    -v /data/mongodb-backup-agent:/data \
mongodb-backup-agent
```

#### View Agent logs
```
$ docker logs mongodb-backup-agent
```

#### Stop Agent
```
$ docker stop mongodb-backup-agent
```

# Submit Bug Report / Feature Request
If you find a bug in Percona Backup for MongoDB, you can submit a report to the project's [JIRA issue tracker](https://jira.percona.com/projects/PBM).

Your first step should be to search the existing set of open tickets for a similar report. If you find that someone else has already reported your problem, then you can upvote that report to increase its visibility.

If there is no existing report, submit a report following these steps:

1. Sign in to Percona JIRA. You will need to create an account if you do not have one.
2. Go to the Create Issue screen and select the relevant project.
3. Fill in the fields of Summary, Description, Steps To Reproduce, and Affects Version to the best you can. If the bug corresponds to a crash, attach the stack trace from the logs.

As a general rule of thumb, please try to create bug reports that are:

- Reproducible. Include steps to reproduce the problem.
- Specific. Include as much detail as possible: which version, what environment, etc.
- Unique. Do not duplicate existing tickets.
- Scoped to a Single Bug. One bug per report.

# Contact

* Percona - [Email](mailto:mongodb-backup@percona.com), [Contact Us](https://www.percona.com/about-percona/contact)
