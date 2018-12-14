FROM alpine

ADD pbm-coordinator /usr/local/bin/pbm-coordinator

RUN adduser -h /data -S pbm-coordinator
USER pbm-coordinator

ENTRYPOINT ["pbm-coordinator"]
CMD []
