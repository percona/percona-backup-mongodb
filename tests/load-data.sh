#!/bin/bash
N=${1:-5}
SLEEP=0
for ((i=0;i<=N;i++))
do
    echo "Generating $i"
    sleep $SLEEP
    mgodatagen --host 127.0.0.1 --port 17001 --username admin --password admin123456 --append -f big.json
    mgodatagen --host 127.0.0.1 --port 17004 --username admin --password admin123456 --append -f small.json
    SLEEP=10
done
