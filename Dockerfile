FROM centos:centos7
MAINTAINER Tim Vaillancourt <tim.vaillancourt@percona.com>
RUN curl -Lo /usr/bin/mongodb-consistent-backup https://github.com/Percona-Lab/mongodb_consistent_backup/releases/download/0.3.3/mongodb_consistent_backup.centos7_amd64 && \
	chmod +x /usr/bin/mongodb-consistent-backup
ENTRYPOINT ["mongodb-consistent-backup"]
CMD ["--help"]
