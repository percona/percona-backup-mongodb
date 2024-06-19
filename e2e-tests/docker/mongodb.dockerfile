ARG MONGODB_VERSION=4.4
ARG MONGODB_IMAGE=percona/percona-server-mongodb
FROM ${MONGODB_IMAGE}:${MONGODB_VERSION}-multi
USER root
COPY e2e-tests/docker/keyFile /opt/keyFile
RUN chown mongodb /opt/keyFile && chmod 400 /opt/keyFile && mkdir -p /home/mongodb/ && chown mongodb /home/mongodb
USER mongodb
