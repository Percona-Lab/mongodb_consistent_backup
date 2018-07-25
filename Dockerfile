FROM centos:centos7
MAINTAINER Tim Vaillancourt <tim.vaillancourt@percona.com>

RUN yum install -y https://www.percona.com/redir/downloads/percona-release/redhat/latest/percona-release-0.1-6.noarch.rpm && \
	yum install -y Percona-Server-MongoDB-36-tools zbackup && yum clean all

ADD build/rpm/RPMS/x86_64/mongodb_consistent_backup-*.el*.x86_64.rpm /
RUN yum localinstall -y /mongodb_consistent_backup-*.el*.x86_64.rpm && \
	yum clean all && rm -f /mongodb_consistent_backup-*.el*.x86_64.rpm

USER mongodb_consistent_backup
ENTRYPOINT ["mongodb-consistent-backup"]
CMD ["--help"]
