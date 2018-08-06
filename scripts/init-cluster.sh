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
				{ _id: 2, host: "'${MONGODB_IP}':'${TEST_MONGODB_SECONDARY2_PORT}'", priority: 0, hidden: true }
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
		--port=${TEST_MONGODB_CONFIGSVR1_PORT} \
		--eval='rs.initiate({
				_id: "'${TEST_MONGODB_CONFIGSVR_RS}'",
				configsvr: true,
				version: 1,
				members: [
					{ _id: 0, host: "'${MONGODB_IP}':'${TEST_MONGODB_CONFIGSVR1_PORT}'" }
				]
		})'
	[ $? == 0 ] && break
	echo "# INFO: retrying rs.initiate() for configsvr in $sleep_secs secs (try $tries/$max_tries)"
	sleep $sleep_secs
	tries=$(($tries + 1))
done
if [ $tries -ge $max_tries ]; then
	echo "# ERROR: reached max tries $max_tries, exiting"
	exit 1
fi
echo "# INFO: sharding configsvr is initiated"


for MONGODB_PORT in ${TEST_MONGODB_PRIMARY_PORT} ${TEST_MONGODB_CONFIGSVR1_PORT}; do
	tries=1
	while [ $tries -lt $max_tries ]; do
		ISMASTER=$(/usr/bin/mongo ${MONGO_FLAGS} \
			--port=${MONGODB_PORT} \
			--eval='printjson(db.isMaster().ismaster)' 2>/dev/null)
		[ "$ISMASTER" == "true" ] && break
		echo "# INFO: retrying db.isMaster() check on 127.0.0.1:${MONGODB_PORT} in $sleep_secs secs (try $tries/$max_tries)"
		sleep $sleep_secs
		tries=$(($tries + 1))
	done
	if [ $tries -ge $max_tries ]; then
		echo "# ERROR: reached max tries $max_tries, exiting"
		exit 1
	fi
done
echo "# INFO: all replsets have primary"


for MONGODB_PORT in ${TEST_MONGODB_PRIMARY_PORT} ${TEST_MONGODB_CONFIGSVR1_PORT}; do
	tries=1
	while [ $tries -lt $max_tries ]; do
		/usr/bin/mongo ${MONGO_FLAGS} \
			--port=${MONGODB_PORT} \
			--eval='db.createUser({
				user: "'${TEST_MONGODB_ADMIN_USERNAME}'",
				pwd: "'${TEST_MONGODB_ADMIN_PASSWORD}'",
				roles: [
					{ db: "admin", role: "root" }
				]
			})' \
			admin
		if [ $? == 0 ]; then
			/usr/bin/mongo ${MONGO_FLAGS} \
				--username=${TEST_MONGODB_ADMIN_USERNAME} \
				--password=${TEST_MONGODB_ADMIN_PASSWORD} \
				--port=${MONGODB_PORT} \
				--eval='db.createUser({
					user: "'${TEST_MONGODB_USERNAME}'",
					pwd: "'${TEST_MONGODB_PASSWORD}'",
					roles: [
						{ db: "admin", role: "backup" },
						{ db: "admin", role: "clusterMonitor" },
						{ db: "admin", role: "restore" },
						{ db: "test", role: "readWrite" }
					]
				})' \
				admin
			[ $? == 0 ] && break
		fi
		echo "# INFO: retrying db.createUser() on 127.0.0.1:${MONGODB_PORT} in $sleep_secs secs (try $tries/$max_tries)"
		sleep $sleep_secs
		tries=$(($tries + 1))
	done
done
echo "# INFO: all replsets have auth user(s)"


tries=1
shard=${TEST_MONGODB_RS}'/127.0.0.1:'${TEST_MONGODB_PRIMARY_PORT}',127.0.0.1:'${TEST_MONGODB_SECONDARY1_PORT}
while [ $tries -lt $max_tries ]; do
	ISMASTER=$(/usr/bin/mongo ${MONGO_FLAGS} \
		--username=${TEST_MONGODB_ADMIN_USERNAME} \
		--password=${TEST_MONGODB_ADMIN_PASSWORD} \
		--port=${TEST_MONGODB_MONGOS_PORT} \
		--eval='printjson(sh.addShard("'$shard'").ok)' \
		admin 2>/dev/null)
	[ $? == 0 ] && [ "$ISMASTER" == "1" ] && break
	echo "# INFO: retrying sh.addShard() check in $sleep_secs secs (try $tries/$max_tries)"
	sleep $sleep_secs
	tries=$(($tries + 1))
done
if [ $tries -ge $max_tries ]; then
	echo "# ERROR: reached max tries $max_tries, exiting"
	exit 1
fi
echo "# INFO: cluster has 1 shard: $shard"
