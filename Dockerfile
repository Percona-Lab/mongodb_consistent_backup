FROM centos:centos7
MAINTAINER Tim Vaillancourt <tim.vaillancourt@percona.com>
COPY bin/mongodb-consistent-backup /usr/bin/mongodb-consistent-backup
ENTRYPOINT ["mongodb-consistent-backup"]
CMD ["--help"]
