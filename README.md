# Percona Backup for MongoDB
[![codecov](https://codecov.io/gh/percona/percona-backup-mongodb/branch/master/graph/badge.svg?token=TiuOmTfp2p)](https://codecov.io/gh/percona/percona-backup-mongodb)
[![CLA assistant](https://cla-assistant.percona.com/readme/badge/percona/percona-backup-mongodb)](https://cla-assistant.percona.com/percona/percona-backup-mongodb)

Progress:
- [x] Oplog tailer
- [x] Oplog applier
- [x] S3 streamer
- [x] Mongodump backup
- [x] Mongodump restore
- [x] Agent selection
- [x] Replica Set Backup
- [x] Sharded Cluster Backup

## Building

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

## Testing

The integration testing launches a MongoDB cluster in Docker containers. *'docker'* and *'docker-compose'* is required.

To run the tests *(may require 'sudo')*:
```
$ make test-full
```

To tear-down the test *(and containers, data, etc)*:
```
$ make test-full-clean
```

## Run in Docker

### Build Docker images

To build the Docker images:
```
$ make docker-build
```

### Coordinator

#### Create Coordinator
*Note: data volume must be owned as unix UID 100*
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

#### Read Coordinator Logs
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
    -e PBM_AGENT_MONGODB_USER=usern@m3 \
    -e PBM_AGENT_MONGODB_PASSWORD=password123456 \
    -e PBM_AGENT_MONGODB_REPLICASET=rs \
    -v /data/mongodb-backup-agent:/data \
mongodb-backup-agent
```

#### Read Agent Logs
```
$ docker logs mongodb-backup-agent
```

#### Stop Agent
```
$ docker stop mongodb-backup-agent
```
