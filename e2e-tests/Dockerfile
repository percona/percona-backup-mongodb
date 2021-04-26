FROM golang:1.15
WORKDIR /opt/pbm-test

COPY . .

RUN go install -mod=vendor ./e2e-tests/cmd/pbm-test

CMD ["pbm-test"]