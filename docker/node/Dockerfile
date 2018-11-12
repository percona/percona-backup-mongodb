FROM percona/percona-server-mongodb:3.6

USER root

ADD docker/node/entrypoint.sh /entrypoint.sh
ADD docker/node/start-pmb-agent.sh /start-pmb-agent.sh
ADD release/pmb-agent /usr/local/bin/pmb-agent

RUN mkdir -p /data/pmb && chown -R mongodb /data/pmb

USER mongodb
ENTRYPOINT ["/entrypoint.sh"]
CMD []
