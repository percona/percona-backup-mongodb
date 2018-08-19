#!/bin/bash

cp /mongod.key /tmp/mongod.key
cp /mongod.pem /tmp/mongod.pem
cp /rootCA.crt /tmp/mongod-rootCA.crt
chmod 400 /tmp/mongod.key /tmp/mongod.pem /tmp/mongod-rootCA.pem

trap "[ -d /data/backup ] && rm -rf /data/backup/* 2>/dev/null" SIGINT SIGTERM

/usr/bin/mongod \
	--keyFile=/tmp/mongod.key \
	--bind_ip=0.0.0.0 \
	--dbpath=/data/db \
	--sslMode=preferSSL \
	--sslCAFile=/tmp/mongod-rootCA.crt \
	--sslPEMKeyFile=/tmp/mongod.pem \
	$*
