FROM golang:1.12
WORKDIR /opt/pbm

COPY . .

RUN make install

USER nobody

CMD ["pbm"]