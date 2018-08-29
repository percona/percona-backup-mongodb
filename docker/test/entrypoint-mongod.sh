#!/bin/bash

cp /mongod.key /tmp/mongod.key
cp /mongod.pem /tmp/mongod.pem
cp /rootCA.crt /tmp/mongod-rootCA.crt
chmod 400 /tmp/mongod.key /tmp/mongod.pem /tmp/mongod-rootCA.pem

/usr/bin/mongod \
	--keyFile=/tmp/mongod.key \
	--bind_ip=127.0.0.1 \
	--profile=2 \
	--dbpath=/data/db \
	--sslMode=preferSSL \
	--sslCAFile=/tmp/mongod-rootCA.crt \
	--sslPEMKeyFile=/tmp/mongod.pem \
	$*
