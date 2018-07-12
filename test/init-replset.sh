#!/bin/bash

tries=1
max_tries=15
sleep_secs=5

sleep $sleep_secs
while [ $tries -lt $max_tries ]; do
	/usr/bin/mongo --quiet \
		--host ${TEST_MONGODB_URI} \
		--eval 'rs.initiate({
			_id: "'${TEST_PSMDB_RSNAME}'",
			version: 1,
			members: [
				{ _id: 0, host: "'${TEST_MONGODB_URI}'" }
			]})'
	[ $? == 0 ] && break
	echo "# INFO: retrying rs.initiate() in $sleep_secs secs (try $tries/$max_tries)"
	sleep $sleep_secs
	tries=$(($tries + 1))
done
if [ $tries -ge $max_tries ]; then
	echo "# ERROR: reached max tries $max_tries, exiting"
	exit 1
fi

sleep $sleep_secs
tries=1
while [ $tries -lt $max_tries ]; do
	ISMASTER=$(/usr/bin/mongo --quiet \
		--host ${TEST_MONGODB_URI} \
		--eval 'printjson(db.isMaster().ismaster)' 2>/dev/null)
	[ "$ISMASTER" == "true" ] && break
	echo "# INFO: retrying db.isMaster() check in $sleep_secs secs (try $tries/$max_tries)"
	sleep $sleep_secs
	tries=$(($tries + 1))
done
if [ $tries -ge $max_tries ]; then
	echo "# ERROR: reached max tries $max_tries, exiting"
	exit 1
fi
