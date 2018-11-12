FROM percona/percona-server-mongodb:3.6

ADD release/pmb-agent /usr/local/bin/pmb-agent
ADD docker/node/entrypoint.sh /entrypoint.sh
ADD docker/node/start-pmb-agent.sh /start-pmb-agent.sh

#USER mongodb
#ENTRYPOINT /entrypoint.sh
