FROM centos:centos7
MAINTAINER Tim Vaillancourt <tim.vaillancourt@percona.com>

RUN yum install -y https://repo.percona.com/centos/7/RPMS/noarch/percona-release-0.1-8.noarch.rpm epel-release \
	&& rpm --import /etc/pki/rpm-gpg/PERCONA-PACKAGING-KEY \
	&& percona-release disable all \
	&& percona-release enable percona release \
	&& yum install -y Percona-Server-MongoDB-34-tools zbackup \
	&& yum clean all

ADD build/rpm/RPMS/x86_64/mongodb_consistent_backup*.el7.x86_64.rpm /
RUN yum localinstall -y /mongodb_consistent_backup*.el7.x86_64.rpm && \
	yum clean all && rm -f /mongodb_consistent_backup*.el7.x86_64.rpm

USER mongodb_consistent_backup
ENTRYPOINT ["mongodb-consistent-backup"]
CMD ["--help"]
