#!/bin/bash

tries=1
max_tries=15
sleep_secs=3

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


tries=1
while [ $tries -lt $max_tries ]; do
	/usr/bin/mongo ${MONGO_FLAGS} \
		--port=${TEST_MONGODB_CONFIGSVR_PORT} \
		--eval='rs.initiate({
			_id: "csReplSet",
			configsvr: true,
			version: 1,
			members: [
				{ _id: 0, host: "'${MONGODB_IP}':'${TEST_MONGODB_CONFIGSVR_PORT}'" },
			]})' | tee /tmp/init-result.json
	if [ $? == 0 ]; then
	  grep -q '"ok" : 1' /tmp/init-result.json
	  [ $? == 0 ] && break
	fi
	echo "# INFO: retrying rs.initiate() for configsvr in $sleep_secs secs (try $tries/$max_tries)"
	sleep $sleep_secs
	tries=$(($tries + 1))
done
if [ $tries -ge $max_tries ]; then
	echo "# ERROR: reached max tries $max_tries, exiting"
	exit 1
fi
echo "# INFO: sharding configsvr is initiated"


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


tries=1
while [ $tries -lt $max_tries ]; do
	ADDSHARD=$(/usr/bin/mongo ${MONGO_FLAGS} \
		--port=${TEST_MONGODB_MONGOS_PORT} \
		--eval='printjson(sh.addShard("'${TEST_MONGODB_RS}'/127.0.0.1:'${TEST_MONGODB_PRIMARY_PORT}',127.0.0.1:'${TEST_MONGODB_SECONDARY1_PORT}',127.0.0.1:'${TEST_MONGODB_SECONDARY2_PORT}'").ok)' 2>/dev/null)
	[ "$ADDSHARD" == "1" ] && break
	echo "# INFO: retrying sh.addShard() check in $sleep_secs secs (try $tries/$max_tries)"
	sleep $sleep_secs
	tries=$(($tries + 1))
done
if [ $tries -ge $max_tries ]; then
	echo "# ERROR: reached max tries $max_tries, exiting"
	exit 1
fi
echo "# INFO: cluster has 1 shard: ${TEST_MONGODB_RS}"
