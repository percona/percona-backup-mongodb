FROM golang:1.16
RUN wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add - && \
	echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/4.4 main" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list && \
	apt-get update && apt-get install -y --no-install-recommends \
	libfaketime \
	iproute2 \
	libkrb5-dev \
	mongodb-org \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /opt/pbm
COPY . .
RUN make install-tests

USER 1001
