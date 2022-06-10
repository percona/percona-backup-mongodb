FROM golang:1.18
WORKDIR /opt/pbm-test
ARG TESTS_BCP_TYPE
ENV TESTS_BCP_TYPE=${TESTS_BCP_TYPE}

COPY . .

RUN go install -mod=vendor ./e2e-tests/cmd/pbm-test

CMD ["pbm-test"]