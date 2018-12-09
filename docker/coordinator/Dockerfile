FROM alpine

ADD pmb-coordinator /usr/local/bin/pmb-coordinator

RUN adduser -h /data -S pmb-coordinator
USER pmb-coordinator

ENTRYPOINT ["pmb-coordinator"]
CMD []
