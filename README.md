# Percona Backup for MongoDB
[![Go Report Card](https://goreportcard.com/badge/github.com/percona/percona-backup-mongodb)](https://goreportcard.com/report/github.com/percona/percona-backup-mongodb) [![codecov](https://codecov.io/gh/percona/percona-backup-mongodb/branch/master/graph/badge.svg?token=TiuOmTfp2p)](https://codecov.io/gh/percona/percona-backup-mongodb) [![CLA assistant](https://cla-assistant.percona.com/readme/badge/percona/percona-backup-mongodb)](https://cla-assistant.percona.com/percona/percona-backup-mongodb)

Percona Backup for MongoDB is a distributed, low-impact solution for achieving consistent backups of MongoDB Sharded Clusters and Replica Sets.

The project was inspired by *(and intends to replace)* the [Percona-Lab/mongodb_consistent_backup](https://github.com/Percona-Lab/mongodb_consistent_backup) tool.

## Feature Progress:
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
- [x] Command-line management utility
- [ ] Dockerhub images
- [ ] Authorization of Agents and CLI
- [ ] Compression
    - [ ] Backup data
    - [ ] Agent and CLI RPCs
- [ ] Encryption of backup data
- [ ] Recovery from agent failures
- [ ] Support for [Percona Server for MongoDB Hot Backup](https://www.percona.com/doc/percona-server-for-mongodb/LATEST/hot-backup.html) for binary-level backup
    - [ ] Backup
    - [ ] Restore
    - [ ] Support for [WiredTiger Encryption](https://www.percona.com/blog/2018/11/01/wiredtiger-encryption-at-rest-percona-server-for-mongodb/)
- [ ] Multiple sources of credentials *(eg: file, Vault, Amazon KMS, etc)*
- [ ] Restore from any Point-in-time
    - [ ] Support for incremental backups using oplogs
- [ ] Prometheus metrics

# Requirements

1. [Percona Server for MongoDB](https://www.percona.com/software/mongo-database/percona-server-for-mongodb) or MongoDB Community 3.6 and above
    1. [MongoDB Replication](https://docs.mongodb.com/manual/replication/) enabled

# Architecture

Percona Backup for MongoDB uses a distributed client/server architecture to perform backup/restore actions. This architecture model was chosen to provide maximum scalability/flexibility.

## Coordinator

The backup coordinator is a daemon that handles communication with backup agents and the backup control program.

The main function of the backup coordinator is to gather information from the MongoDB instances through the agents to determine which nodes should run the backup/restore and to establish consistent backup/restore points across all shards.

The Coordinator listens on 2 x TCP ports:
1. **RPC** - Port used for agent communications *(Default: 10000/tcp)*
2. **API** - Port used for CLI/API communications *(Default: 10001/tcp)*

## Agent

Backup Agents are in charge of receiving commands from the coordinator and run them.

The agent must run locally *(connected to 'localhost')* on every MongoDB instance *(mongos and config servers included)* in order to collect information about the instance and forward it to the coordinator. With that information, the coordinator can determine the best agent to start a backup or restore, to start/stop the balancer, etc.

The agent requires outbound network access to the Coordinator RPC port.

## PBM Control (pmbctl)

This program is a command line utility to send commands to the coordinator.
Currently, the available commands are:  
- **list nodes**: List all nodes (agents) connected to the coordinator
- **list backups**: List all finished backups.
- **run backup**: Start a new backup
- **run restore**: Restore a backup

## Running

### Running the Coordinator

The backup coordinator can be executed in any server since it doesn't need a connection to a MongoDB instance.
To start the coordinator just run:

```
./pbm-coordinator --work-dir=<directory to store metadata>
```
If `--work-dir` is not specified, it will use the default `${HOME}/percona-backup-mongodb`.
By default, the coordinator will listen for agents on port 10000.

### Running the Agent

On every MongoDB instance, you need to start an agent that will receive commands from the coordinator.

In most situations the agent must connect to MongoDB using the host 'localhost' *(127.0.0.1)* and the port 27017. See [MongoDB Authentication](#MongoDB-Authentication) below if Authentication is enabled on the host.

Example:
```
./pbm-agent --mongodb-user=pbmAgent \
            --mongodb-password=securePassw0rd \
            --mongodb-host=127.0.0.1 \
            --mongodb-port=27017 \
            --replicaset=rs0 \
            --server-address=10.10.10.10:10000 \
            --backup-dir=/data/backup \
            --pid-file=/tmp/pbm-agent.pid
```

#### MongoDB Authentication

If [MongoDB Authentication](https://docs.mongodb.com/manual/core/authentication/) is enabled the backup agent must be provided credentials for a MongoDB user with the ['backup'](https://docs.mongodb.com/manual/reference/built-in-roles/#backup), ['restore'](https://docs.mongodb.com/manual/reference/built-in-roles/#restore) and ['clusterMonitor'](https://docs.mongodb.com/manual/reference/built-in-roles/#clusterMonitor) built-in auth roles. This user must exist on every database node and it should not be used by other applications.

Example *'createUser'* command *(must be ran via a 'mongo' shell via a PRIMARY member)*:

```
> use admin;
> db.createUser({
    _id: "pbmAgent",
    pwd: "securePassw0rd",
    roles: [
        { db: "admin", role: "backup"},
        { db: "admin", role: "clusterMonitor},
        { db: "admin", role: "restore"}
    ],
    authenticationRestrictions: [
        { clientSource: "127.0.0.1" }
    ]
})
```

### Running commands

`pmbctl` is the command line utility to control the backup system. 
Since it needs to connect to the coordinator you need to specify the coordinator `ip:port`. The defaults are `127.0.0.1:10000` so, if you are running `pmbctl` from the same server where the coordinator is running, you can ommit the `--server-address` parameter.  
  
#### Examples
##### List all connected agents
```
pmbctl --server-address=127.0.0.1:10000 list nodes
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
pbmctl run backup --description "Test backup 01"
```

##### List all the completed backups
```
pmbctl --server-address=127.0.0.1:10000 list backups
```
Sample output:
```
       Metadata file name     -         Description
------------------------------ - ---------------------------------------------------------------------------
2018-12-18T19:04:14Z.json      - Test backup 01
```

##### Restore a backup
```
pbmctl run restore 2018-12-18T19:04:14Z.json
```

# Contributing



# Building

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

## Unit Tests

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

#### Create Coordinator
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

#### Create Agent
*Note: the [Coordinator](#create-coordinator) must be started before the agent!*
```
$ mkdir -m 0700 -p /data/mongodb-backup-agent
$ docker run -d \
    --restart=always \
    --user=$(id -u) \
    --name=mongodb-backup-agent \
    -e PBM_AGENT_BACKUP_DIR=/data \
    -e PBM_AGENT_SERVER_ADDRESS=172.16.0.2:10000 \
    -e PBM_AGENT_MONGODB_USER=pbmAgent \
    -e PBM_AGENT_MONGODB_PASSWORD=securePassw0rd \
    -e PBM_AGENT_MONGODB_REPLICASET=rs \
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

# Contact

* Percona - [Email](mailto:mongodb-backup@percona.com), [Contact Us](https://www.percona.com/about-percona/contact)