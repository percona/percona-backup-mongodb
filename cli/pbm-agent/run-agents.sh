#!/bin/bash

export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export GOCACHE=off
export GOLANG_DOCKERHUB_TAG=1.10-stretch

export TEST_PSMDB_VERSION=latest
export TEST_MONGODB_ADMIN_USERNAME=admin
export TEST_MONGODB_ADMIN_PASSWORD=admin123456
export TEST_MONGODB_USERNAME=test
export TEST_MONGODB_PASSWORD=123456
export TEST_MONGODB_HOST="127.0.0.1"

export TEST_MONGODB_S1_RS=rs1
export TEST_MONGODB_S2_RS=rs2

export TEST_MONGODB_S1_PORT1=17001
export TEST_MONGODB_S1_PORT2=17002
export TEST_MONGODB_S1_PORT3=17003

export TEST_MONGODB_S2_PORT1=17004 # Primary
export TEST_MONGODB_S2_PORT2=17005 # Secondary 1
export TEST_MONGODB_S2_PORT3=17006 # Secondary 2

export TEST_MONGODB_CONFIGSVR_RS=csReplSet

export TEST_MONGODB_CONFIGSVR1_PORT=17007
export TEST_MONGODB_CONFIGSVR2_PORT=17008
export TEST_MONGODB_CONFIGSVR3_PORT=17009

export TEST_MONGODB_MONGOS_PORT=17000

run_agents() {
    ports=$1
    replicaset=$2

    pids=""
    for port in $ports
    do
        pidfile=/tmp/agent.${port}.pid
        logfile=/tmp/agent.${port}.log
        backupdir=/tmp/backup.${port}
        rm -rf ${backupdir}
        mkdir -p ${backupdir}

        echo "./pbm-agent --mongodb-user=${TEST_MONGODB_USERNAME} \\"
        echo "    --mongodb-password=${TEST_MONGODB_PASSWORD} \\"
        echo "    --mongodb-host=${TEST_MONGODB_HOST} \\"
        echo "    --mongodb-port=${port} \\"
        echo "    --replicaset=${replicaset} \\"
        echo "    --backup-dir=${backupdir} \\"
        echo "    --pid-file=${pidfile} &> ${logfile} &"

        ./pbm-agent --mongodb-user=${TEST_MONGODB_USERNAME} \
            --mongodb-password=${TEST_MONGODB_PASSWORD} \
            --mongodb-host=${TEST_MONGODB_HOST} \
            --mongodb-port=${port} \
            --replicaset=${replicaset} \
            --backup-dir=${backupdir} \
            --pid-file=${pidfile} &> ${logfile} &
        pid=$!
        pids="$pids $pid"
        if [ "$?" != "0" ]; then
            echo "Started agent on mongodb port $port, pid: $pid, pid-file: $pidfile"
        fi
    done
    echo "To kill all agents run:"
    echo "kill${pids}"
}

go build

echo "Starting agents on first shard"
run_agents "$TEST_MONGODB_S1_PORT1 $TEST_MONGODB_S1_PORT2 $TEST_MONGODB_S1_PORT3" "$TEST_MONGODB_S1_RS"

echo "Starting agents on second shard"
run_agents "$TEST_MONGODB_S2_PORT1 $TEST_MONGODB_S2_PORT2 $TEST_MONGODB_S2_PORT3" "$TEST_MONGODB_S2_RS"

echo "Starting agents on config servers"
run_agents "$TEST_MONGODB_CONFIGSVR1_PORT"

echo "Starting agents on mongos"
run_agents "$TEST_MONGODB_MONGOS_PORT"

