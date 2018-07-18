#!/bin/bash

tries=1
max_tries=15
sleep_secs=5

cp /rootCA.crt /tmp/rootCA.crt
cp /client.pem /tmp/client.pem
chmod 400 /tmp/rootCA.crt /tmp/client.pem

MONGO_FLAGS="--quiet --ssl --sslCAFile=/tmp/rootCA.crt --sslPEMKeyFile=/tmp/client.pem"
MONGODB_IP=127.0.0.1
MONGODB_PRIMARY_HOST=${MONGODB_IP}:${TEST_MONGODB_PRIMARY_PORT}
sleep $sleep_secs
while [ $tries -lt $max_tries ]; do
	/usr/bin/mongo ${MONGO_FLAGS} \
		--port=${TEST_MONGODB_PRIMARY_PORT} \
		--eval='rs.initiate({
			_id: "'${TEST_MONGODB_RS}'",
			version: 1,
			members: [
				{ _id: 0, host: "'${MONGODB_IP}':'${TEST_MONGODB_PRIMARY_PORT}'", priority: 10 },
				{ _id: 1, host: "'${MONGODB_IP}':'${TEST_MONGODB_SECONDARY1_PORT}'", priority: 1 },
				{ _id: 2, host: "'${MONGODB_IP}':'${TEST_MONGODB_SECONDARY2_PORT}'", priority: 1 }
			]})' | tee /tmp/init-result.json
	if [ $? == 0 ]; then
	  grep -q '"ok" : 1' /tmp/init-result.json
	  [ $? == 0 ] && break
	fi
	echo "# INFO: retrying rs.initiate() in $sleep_secs secs (try $tries/$max_tries)"
	sleep $sleep_secs
	tries=$(($tries + 1))
done
if [ $tries -ge $max_tries ]; then
	echo "# ERROR: reached max tries $max_tries, exiting"
	exit 1
fi
echo "# INFO: replset is initiated"

sleep $sleep_secs
tries=1
while [ $tries -lt $max_tries ]; do
	ISMASTER=$(/usr/bin/mongo ${MONGO_FLAGS} \
		--port=${TEST_MONGODB_PRIMARY_PORT} \
		--eval='printjson(db.isMaster().ismaster)' 2>/dev/null)
	[ "$ISMASTER" == "true" ] && break
	echo "# INFO: retrying db.isMaster() check in $sleep_secs secs (try $tries/$max_tries)"
	sleep $sleep_secs
	tries=$(($tries + 1))
done
if [ $tries -ge $max_tries ]; then
	echo "# ERROR: reached max tries $max_tries, exiting"
	exit 1
fi
echo "# INFO: replset has primary ${MONGODB_PRIMARY_HOST}"
