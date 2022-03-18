ARG MONGODB_VERSION=4.2
FROM golang:1.16
ARG MONGODB_VERSION=4.2
ENV VERSION=${MONGODB_VERSION:-4.2}
RUN mkdir /opt/backups
RUN chown 1001 /opt/backups
RUN wget -qO - https://www.mongodb.org/static/pgp/server-${VERSION}.asc | apt-key add - && \
	echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/$VERSION main" | tee /etc/apt/sources.list.d/mongodb-org-$VERSION.list && \
	apt-get update && apt-get install -y --no-install-recommends \
	libfaketime \
	iproute2 \
	libkrb5-dev \
	mongodb-org \
	vim \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /opt/pbm
COPY . .
RUN make install-tests

USER 1001
