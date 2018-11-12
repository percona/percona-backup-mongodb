FROM percona/percona-server-mongodb:3.6

ADD docker/node/entrypoint.sh /entrypoint.sh
ADD docker/node/start-pmb-agent.sh /start-pmb-agent.sh
ADD release/pmb-agent /usr/local/bin/pmb-agent

USER root
RUN mkdir -p /data/pmb && chown -R mongodb /data/pmb

USER mongodb
ENTRYPOINT ["/entrypoint.sh"]
CMD []
