#!/bin/bash

max_tries=45
sleep_secs=1

cp /rootCA.crt /tmp/rootCA.crt
cp /client.pem /tmp/client.pem
chmod 400 /tmp/rootCA.crt /tmp/client.pem

MONGO_FLAGS="--quiet --ssl --sslCAFile=/tmp/rootCA.crt --sslPEMKeyFile=/tmp/client.pem"
MONGODB_IP=127.0.0.1

sleep $sleep_secs

/usr/bin/mongo --version


## Shard 1
tries=1
while [ $tries -lt $max_tries ]; do
	/usr/bin/mongo ${MONGO_FLAGS} \
		--port=${TEST_MONGODB_S1_PRIMARY_PORT} \
		--eval='rs.initiate({
			_id: "'${TEST_MONGODB_S1_RS}'",
			version: 1,
			members: [
				{ _id: 0, host: "'${MONGODB_IP}':'${TEST_MONGODB_S1_PRIMARY_PORT}'", priority: 10 },
				{ _id: 1, host: "'${MONGODB_IP}':'${TEST_MONGODB_S1_SECONDARY1_PORT}'", priority: 1 },
				{ _id: 2, host: "'${MONGODB_IP}':'${TEST_MONGODB_S1_SECONDARY2_PORT}'", priority: 0, hidden: true, tags: { role: "backup" } }
			]})' | tee /tmp/init-result.json
	if [ $? == 0 ]; then
	  grep -q '"ok" : 1' /tmp/init-result.json
	  [ $? == 0 ] && rm -vf /tmp/init-result.json && break
	fi
	echo "# INFO: retrying rs.initiate() on ${TEST_MONGODB_S1_RS} in $sleep_secs secs (try $tries/$max_tries)"
	sleep $sleep_secs
	tries=$(($tries + 1))
done
if [ $tries -ge $max_tries ]; then
	echo "# ERROR: reached max tries $max_tries for ${TEST_MONGODB_S1_RS}, exiting"
	exit 1
fi
echo "# INFO: replset ${TEST_MONGODB_S1_RS} is initiated"


## Shard 2
tries=1
while [ $tries -lt $max_tries ]; do
	/usr/bin/mongo ${MONGO_FLAGS} \
		--port=${TEST_MONGODB_S2_PRIMARY_PORT} \
		--eval='rs.initiate({
			_id: "'${TEST_MONGODB_S2_RS}'",
			version: 1,
			members: [
				{ _id: 0, host: "'${MONGODB_IP}':'${TEST_MONGODB_S2_PRIMARY_PORT}'", priority: 10 },
				{ _id: 1, host: "'${MONGODB_IP}':'${TEST_MONGODB_S2_SECONDARY1_PORT}'", priority: 1 },
				{ _id: 2, host: "'${MONGODB_IP}':'${TEST_MONGODB_S2_SECONDARY2_PORT}'", priority: 0, hidden: true, tags: { role: "backup" } }
			]})' | tee /tmp/init-result.json
	if [ $? == 0 ]; then
	  grep -q '"ok" : 1' /tmp/init-result.json
	  [ $? == 0 ] && rm -vf /tmp/init-result.json && break
	fi
	echo "# INFO: retrying rs.initiate() on ${TEST_MONGODB_S2_RS} in $sleep_secs secs (try $tries/$max_tries)"
	sleep $sleep_secs
	tries=$(($tries + 1))
done
if [ $tries -ge $max_tries ]; then
	echo "# ERROR: reached max tries $max_tries for ${TEST_MONGODB_S2_RS}, exiting"
	exit 1
fi
echo "# INFO: replset ${TEST_MONGODB_S2_RS} is initiated"


## Configsvr replset
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


for MONGODB_PORT in ${TEST_MONGODB_S1_PRIMARY_PORT} ${TEST_MONGODB_S2_PRIMARY_PORT} ${TEST_MONGODB_CONFIGSVR1_PORT}; do
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


for MONGODB_PORT in ${TEST_MONGODB_S1_PRIMARY_PORT} ${TEST_MONGODB_S2_PRIMARY_PORT} ${TEST_MONGODB_CONFIGSVR1_PORT}; do
	tries=1
	while [ $tries -lt $max_tries ]; do
		/usr/bin/mongo ${MONGO_FLAGS} \
			--port=${MONGODB_PORT} \
			--eval='db.createUser({
				user: "'${TEST_MONGODB_ADMIN_USERNAME}'",
				pwd: "'${TEST_MONGODB_ADMIN_PASSWORD}'",
				roles: [
					{ db: "admin", role: "root" }
				],
				authenticationRestrictions: [
					{ 
						clientSource: ["127.0.0.1"]
					}
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
						{ db: "config", role: "read" },
						{ db: "test", role: "readWrite" }
					],
					authenticationRestrictions: [
						{
							clientSource: ["127.0.0.1"]
						}
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


shard1=${TEST_MONGODB_S1_RS}'/127.0.0.1:'${TEST_MONGODB_S1_PRIMARY_PORT}',127.0.0.1:'${TEST_MONGODB_S1_SECONDARY1_PORT}
shard2=${TEST_MONGODB_S2_RS}'/127.0.0.1:'${TEST_MONGODB_S2_PRIMARY_PORT}',127.0.0.1:'${TEST_MONGODB_S2_SECONDARY1_PORT}
for shard in $shard1 $shard2; do
	tries=1
	while [ $tries -lt $max_tries ]; do
		ADDSHARD=$(/usr/bin/mongo ${MONGO_FLAGS} \
			--username=${TEST_MONGODB_ADMIN_USERNAME} \
			--password=${TEST_MONGODB_ADMIN_PASSWORD} \
			--port=${TEST_MONGODB_MONGOS_PORT} \
			--eval='printjson(sh.addShard("'$shard'").ok)' \
			admin 2>/dev/null)
		[ $? == 0 ] && [ "$ADDSHARD" == "1" ] && break
		echo "# INFO: retrying sh.addShard() check for '$shard' in $sleep_secs secs (try $tries/$max_tries)"
		sleep $sleep_secs
		tries=$(($tries + 1))
	done
	if [ $tries -ge $max_tries ]; then
		echo "# ERROR: reached max tries $max_tries for '$shard', exiting"
		exit 1
	fi
	echo "# INFO: added shard: $shard"
done

tries=1
while [ $tries -lt $max_tries ]; do
	ENABLESHARDING=$(/usr/bin/mongo ${MONGO_FLAGS} \
		--username=${TEST_MONGODB_ADMIN_USERNAME} \
		--password=${TEST_MONGODB_ADMIN_PASSWORD} \
		--port=${TEST_MONGODB_MONGOS_PORT} \
		--eval='sh.enableSharding("test").ok' \
		admin 2>/dev/null)
	[ $? == 0 ] && [ "$ENABLESHARDING" == "1" ] && break
	echo "# INFO: retrying sh.enableSharding(\"test\") check in $sleep_secs secs (try $tries/$max_tries)"
	sleep $sleep_secs
	tries=$(($tries + 1))
done
if [ $tries -ge $max_tries ]; then
	echo "# ERROR: reached max tries $max_tries for '$shard', exiting"
	exit 1
fi
echo "# INFO: \"test\" database is now sharded"

tries=1
while [ $tries -lt $max_tries ]; do
	SHARDCOL=$(/usr/bin/mongo ${MONGO_FLAGS} \
		--username=${TEST_MONGODB_ADMIN_USERNAME} \
		--password=${TEST_MONGODB_ADMIN_PASSWORD} \
		--port=${TEST_MONGODB_MONGOS_PORT} \
		--eval='sh.shardCollection("test.test", {_id: 1}).ok' \
		admin 2>/dev/null)
	[ $? == 0 ] && [ "$ENABLESHARDING" == "1" ] && break
	echo "# INFO: retrying sh.shardCollection(\"test.test\", {_id: 1}) check in $sleep_secs secs (try $tries/$max_tries)"
	sleep $sleep_secs
	tries=$(($tries + 1))
done
if [ $tries -ge $max_tries ]; then
	echo "# ERROR: reached max tries $max_tries for '$shard', exiting"
	exit 1
fi
echo "# INFO: \"test.test\" collection is now sharded"
