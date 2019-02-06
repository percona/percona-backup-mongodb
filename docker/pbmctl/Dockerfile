FROM centos:7
MAINTAINER Percona Development <info@percona.com>

# check repository package signature in secure way
RUN export GNUPGHOME="$(mktemp -d)" \
        && gpg --keyserver ha.pool.sks-keyservers.net --recv-keys 430BDF5C56E7C94E848EE60C1C4CBDCDCD2EFD2A \
        && gpg --export --armor 430BDF5C56E7C94E848EE60C1C4CBDCDCD2EFD2A > ${GNUPGHOME}/RPM-GPG-KEY-Percona \
        && rpmkeys --import ${GNUPGHOME}/RPM-GPG-KEY-Percona /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7 \
        && curl -L -o /tmp/percona-release.rpm http://www.percona.com/downloads/percona-release/redhat/0.1-6/percona-release-0.1-6.noarch.rpm \
        && rpmkeys --checksig /tmp/percona-release.rpm \
        && yum install -y /tmp/percona-release.rpm \
        && rm -rf "$GNUPGHOME" /tmp/percona-release.rpm

RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm \
        && yum install -y Percona-Server-MongoDB-36-tools awscli \
        && yum clean all \
        && rm -rf /var/cache/yum

ADD https://raw.githubusercontent.com/Percona-Lab/percona-server-mongodb-operator/master/backup-restorer.sh /backup-restorer.sh
RUN chmod +rx /backup-restorer.sh

ADD pbmctl /usr/local/bin/pbmctl

RUN mkdir /data && adduser -d /data -r pbmctl && chown -R pbmctl /data
USER pbmctl
WORKDIR /data

ENTRYPOINT ["pbmctl"]
CMD []
