FROM centos:centos7
MAINTAINER Tim Vaillancourt <tim.vaillancourt@percona.com>
RUN yum install -y https://www.percona.com/redir/downloads/percona-release/redhat/latest/percona-release-0.1-4.noarch.rpm && \
	yum install -y Percona-Server-MongoDB-32-tools && yum clean all && \
	curl -Lo /usr/bin/mongodb-consistent-backup https://github.com/Percona-Lab/mongodb_consistent_backup/releases/download/0.4.0/mongodb-consistent-backup.el7.x86_64 && \
	chmod +x /usr/bin/mongodb-consistent-backup
ENTRYPOINT ["mongodb-consistent-backup"]
CMD ["--help"]
