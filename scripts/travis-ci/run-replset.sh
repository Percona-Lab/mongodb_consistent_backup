#!/bin/bash

set -e
set -x

MONGO_VERSION=${1:-3.2}
MCB_EXTRA="${@:2}"

pushd $(dirname $0)
	source $PWD/func.sh

	export MONGO_VERSION=${MONGO_VERSION}
	export DATA_MONGOD_FLAGS=
	export MONGOS_CONFIGDB=
	export CONFIGSVR_FLAGS=
	export MCB_EXTRA=${MCB_EXTRA}

	echo "# Starting instances with docker-compose"
	docker-compose up -d mongo-rs0-1
	docker-compose up -d mongo-rs0-2
	docker-compose up -d mongo-rs0-3

	echo "# Waiting 10 seconds"
	sleep 10
	
	echo "# Initiating rs0"
	doMongo mongo-rs0-1 mongo-rs0-1:27017 'rs.initiate({
	  _id: "rs0",
	  members: [
	    { _id: 0, host: "mongo-rs0-1:27017" },
	    { _id: 1, host: "mongo-rs0-2:27017" },
	    { _id: 2, host: "mongo-rs0-3:27017", priority: 0 }
	  ]
	})'
	
	echo "# Waiting 10 seconds"
	sleep 10
	
        echo "# Starting mongodb_consistent_backup, replset-only mode (in docker)"
        docker-compose up --abort-on-container-exit backup-replset

        echo "# Stopping instances with docker-compose"
        docker-compose down
popd
