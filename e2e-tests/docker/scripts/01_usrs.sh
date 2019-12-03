#!/usr/bin/env bash

MONGO_USER="dba"
MONGO_PASS="test1234"
MONGO_BACKUP_USER="backupUser"
MONGO_BACKUP_PASS="test1234"

sleep 30

mongo localhost:27017/admin --quiet --eval "db.createUser({ user: \"${MONGO_USER}\", pwd: \"${MONGO_PASS}\", roles: [ \"root\", \"userAdminAnyDatabase\", \"clusterAdmin\" ] });"
mongo --auth localhost:27017/admin --quiet --eval "db.createUser({ user: \"${MONGO_BACKUP_USER}\", pwd: \"${MONGO_BACKUP_PASS}\", roles: [ { db: \"admin\", role: \"backup\" }, { db: \"admin\", role: \"clusterMonitor\" }, { db: \"admin\", role: \"restore\" } ] });"
