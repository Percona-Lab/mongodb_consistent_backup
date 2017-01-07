FROM centos:centos7
MAINTAINER Tim Vaillancourt <tim.vaillancourt@percona.com>
COPY . /usr/src/mongodb_consistent_backup
RUN yum install -y https://www.percona.com/redir/downloads/percona-release/redhat/latest/percona-release-0.1-4.noarch.rpm && \
	yum install -y Percona-Server-MongoDB-32-tools python python-virtualenv make gcc libffi-devel openssl-devel git && \
	cd /usr/src/mongodb_consistent_backup && \
	touch VERSION && make && make PREFIX=/usr install && \
	rm -rf /usr/src/mongodb_consistent_backup && \
	yum remove -y python-virtualenv make gcc openssl-devel git && \
	yum autoremove -y && yum clean all
ENTRYPOINT ["mongodb-consistent-backup"]
CMD ["--help"]
