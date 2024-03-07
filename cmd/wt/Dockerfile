FROM oraclelinux:8 AS wt-build
WORKDIR /build

RUN dnf config-manager --set-enabled ol8_codeready_builder && dnf update && \
dnf install git cmake gcc gcc-c++ snappy-devel zlib-devel libzstd-devel lz4-devel

RUN git clone -b mongodb-7.0 --depth=1 https://github.com/wiredtiger/wiredtiger.git .

RUN cmake . \
-DHAVE_BUILTIN_EXTENSION_LZ=1 \
-DHAVE_BUILTIN_EXTENSION_SNAPPY=1 \
-DHAVE_BUILTIN_EXTENSION_ZLIB=1 \
-DHAVE_BUILTIN_EXTENSION_ZSTD=1 \
-DENABLE_SHARED=1 \
-DENABLE_STATIC=1 \
-DWT_STANDALONE_BUILD=0 \
-DCMAKE_BUILD_TYPE=Release && \
make && make install

FROM oraclelinux:8 as pbm-build
WORKDIR /build

RUN dnf config-manager --set-enabled ol8_codeready_builder && dnf update && \
dnf install make golang snappy-devel zlib-devel libzstd-devel lz4-devel

COPY --from=wt-build /usr/local/lib64/libwiredtiger.so* /usr/lib/
COPY --from=wt-build /usr/local/include/wiredtiger.h /usr/include/

COPY . .

RUN make build-pbm build-agent

FROM percona/percona-server-mongodb:7.0-multi as psmdb
FROM oraclelinux:8

RUN dnf update && dnf install snappy zlib libzstd lz4 && dnf clean all

COPY --from=wt-build /usr/local/lib64/libwiredtiger.so* /usr/lib64/
COPY --from=pbm-build /build/bin/pbm* /bin/
COPY --from=psmdb /bin/mongod /bin/

CMD ["pbm-agent"]
