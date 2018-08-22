# MongoDB backup tool
[![codecov](https://codecov.io/gh/percona/mongodb-backup/branch/master/graph/badge.svg?token=TiuOmTfp2p)](https://codecov.io/gh/percona/mongodb-backup)

Progress:
- [x] Oplog tailer
- [ ] S3 streamer [WIP]

## Building

Building the project requires:
1. Go 1.10 or above
1. Makefile

To build the project:
```
make
```

A successful build outputs binaries: *mongodb-backup-admin*, *mongodb-backup-agent* and *mongodb-backupd*.

## Testing

The integration testing launches a MongoDB cluster in Docker containers. *'docker'* and *'docker-compose'* is required.

To run the tests *(may require 'sudo')*:
```
make test-full
```

To tear-down the test *(and containers, data, etc)*:
```
make test-full-clean
```
