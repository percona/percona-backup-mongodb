#!/bin/bash

cp /mongod.pem /tmp/mongod.pem
cp /rootCA.crt /tmp/mongod-rootCA.crt
chmod 400 /tmp/mongod.pem /tmp/mongod-rootCA.pem

/usr/bin/mongod \
	--bind_ip=0.0.0.0 \
	--dbpath=/data/db \
	--sslMode=preferSSL \
	--sslCAFile=/tmp/mongod-rootCA.crt \
	--sslPEMKeyFile=/tmp/mongod.pem \
	$*
