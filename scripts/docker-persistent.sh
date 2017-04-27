#!/bin/sh
#
# Script for running mongodb_consistent_backup under Docker
# with persistent data container for data, config and logs.
#
# See: scripts/docker-persistent.example.conf for an example
# config to pass to this script during backup. Make sure
# 'log_dir' and 'backup.location' are not changed!


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
  docker run -it --rm --volumes-from $BACKUP_DATA_IMAGE --entrypoint /bin/find $DOCKER_IMAGE $BACKUP_DIR/data -maxdepth 2 -type d -name "[0-9]*_*"
elif [ "$ACTION" = "get" ]; then
  DIR=$2
  [ -z $DIR ] && echo "Usage: $0 get [backup path]" && exit 1
  docker cp $BACKUP_DATA_IMAGE:$DIR .
fi
