FROM centos:centos7
MAINTAINER Tim Vaillancourt <tim.vaillancourt@percona.com>
RUN yum install -y https://www.percona.com/redir/downloads/percona-release/redhat/latest/percona-release-0.1-4.noarch.rpm && \
	yum install -y Percona-Server-MongoDB-32-tools
COPY bin/mongodb-consistent-backup /usr/bin/mongodb-consistent-backup
ENTRYPOINT ["mongodb-consistent-backup"]
CMD ["--help"]
