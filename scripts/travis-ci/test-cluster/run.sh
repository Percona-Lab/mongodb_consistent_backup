#!/bin/bash

set -e
set -x

MONGO_VERSION=${1:-3.2}

pushd $(dirname $0)
	export MONGO_VERSION=${MONGO_VERSION}

	echo "# Starting instances with docker-compose"
	docker-compose up -d mongo-mongos

	echo "# Waiting 10 seconds"
	sleep 10
	
	echo "# Initiating csReplSet (config server set)"
	docker-compose run --rm mongo-cs-1 mongo --port 27017 --quiet --eval 'rs.initiate({
	  _id: "csReplSet",
	  configsvr: true,
	  members: [
	    { _id: 0, host: "mongo-cs-1:27017" },
	    { _id: 1, host: "mongo-cs-2:27017" }
	  ]
	})'
	
	echo "# Initiating rs0"
	docker-compose run --rm mongo-rs0-1 mongo --port 27017 --quiet --eval 'rs.initiate({
	  _id: "rs0",
	  members: [
	    { _id: 0, host: "mongo-rs0-1:27017" },
	    { _id: 1, host: "mongo-rs0-2:27017", priority: 0 }
	  ]
	})'
	
	echo "# Waiting 10 seconds"
	sleep 10
	
	echo "# Adding shard rs0"
	set -e
	TRIES=0
	while [ $TRIES -le 5 ]; do
  	  docker-compose run --rm mongo-mongos mongo --port 27017 --quiet --eval 'sh.addShard("rs0/mongo-rs0-1:27017,mongo-rs0-2:27017")'
	  [ $? = 0 ] && break
	  echo "# Retrying adding shard rs0"
	  TRIES=$(($TRIES + 1))
	  sleep 3
	done
	set +e

        echo "# Starting mongodb_consistent_backup, cluster mode (in docker)"
        docker-compose up  --abort-on-container-exit mongodb_consistent_backup-cluster

        echo "# Starting mongodb_consistent_backup, replset-only mode (in docker)"
        docker-compose up  --abort-on-container-exit mongodb_consistent_backup-replset

        echo "# Stopping instances with docker-compose"
        docker-compose down
popd
