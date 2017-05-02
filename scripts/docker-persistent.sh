#!/bin/sh
#
# Script for running mongodb_consistent_backup under Docker
# with persistent data container for data, config and logs.
#
# See: scripts/docker-persistent.example.conf for an example
# config to pass to this script during backup. 'log_dir' and
# 'backup.location' variables are auto-set by this script.
#
# Run backup:
#     $ scripts/docker-persistent.sh backup scripts/docker-persistent.example.conf
#     # Loading config file scripts/docker-persistent.example.conf into container
#     # Running Docker image: timvaillancourt/mongodb_consistent_backup:latest
#     [2017-04-27 11:22:04,255] [INFO] [MainProcess] [Main:init:127] Starting mongodb-consistent-backup version 1.0.0 (git commit: d780ad545b603d3a2f807e1813f1de407e81f1ba)
#     ...
#     ...
#
# List backups (in persistent Docker volume):
#     $ scripts/docker-persistent.sh list
#     /mongodb_consistent_backup/data/default/20170427_1122
#     /mongodb_consistent_backup/data/default/20170427_1123
#
# Get a backup:
#     $ scripts/docker-persistent.sh get /mongodb_consistent_backup/data/default/20170427_1122
#     $ ls -ald ./20170427_112
#     drwxr-xr-x. 3 root root 59 Apr 27 13:22 ./20170427_1122
#

ACTION=$1
[ -z $1 ] && echo "Usage: $0 [backup|list|get] [action flags]" && exit 1

BACKUP_DIR=/mongodb_consistent_backup
BACKUP_CNF=$BACKUP_DIR/mongodb-consistent-backup.conf
BACKUP_IMAGE=mongodb_consistent_backup
BACKUP_DATA_IMAGE=mongodb_consistent_backup-data
MCB_FLAGS="-c $BACKUP_CNF -L $BACKUP_DIR/logs -l $BACKUP_DIR/data"
DOCKER_IMAGE=timvaillancourt/mongodb_consistent_backup:latest

if [ "$ACTION" = "backup" ]; then
  CNF=$2
  [ -z $2 ]  && echo "Usage: $0 backup [mongodb-consistent-backup config file]" && exit 1

  docker ps -a | grep -q "$BACKUP_DATA_IMAGE"
  if [ $? -gt 0 ]; then
    echo "# Creating persistent volume for Docker image: $DOCKER_IMAGE"
    docker create -v $BACKUP_DIR --name $BACKUP_DATA_IMAGE $DOCKER_IMAGE
  fi
  
  if [ -f $CNF ]; then
      echo "# Loading config file $CNF into container"
      docker cp $CNF ${BACKUP_DATA_IMAGE}:${BACKUP_CNF}
  
      echo "# Running Docker image: $DOCKER_IMAGE"
      docker run -i --name $BACKUP_IMAGE --rm --volumes-from $BACKUP_DATA_IMAGE $DOCKER_IMAGE $MCB_FLAGS
  else
      echo "# Config file $CNF does not exist!"
      exit 1
  fi
elif [ "$ACTION" = "list" ]; then
  echo "# Listing backups in $BACKUP_DATA_IMAGE"
  docker run -it --name $BACKUP_IMAGE --rm --volumes-from $BACKUP_DATA_IMAGE --entrypoint /bin/find $DOCKER_IMAGE $BACKUP_DIR/data -maxdepth 2 -type d -name "[0-9]*_*"
elif [ "$ACTION" = "get" ]; then
  DIR=$2
  [ -z $DIR ] && echo "Usage: $0 get [backup path]" && exit 1
  docker cp $BACKUP_DATA_IMAGE:$DIR .
fi
