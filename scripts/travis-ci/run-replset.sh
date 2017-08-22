#!/bin/bash

set -e
set -x

MONGO_VERSION=${1:-3.2}

pushd $(dirname $0)
	export MONGO_VERSION=${MONGO_VERSION}

	echo "# Starting instances with docker-compose"
	docker-compose up -d mongo-rs0-1
	docker-compose up -d mongo-rs0-2
	docker-compose up -d mongo-rs0-3

	echo "# Waiting 10 seconds"
	sleep 10
	
	echo "# Initiating rs0"
	docker-compose run --rm mongo-rs0-1 /usr/bin/mongo mongo-rs0-1:27017 --quiet --eval 'rs.initiate({
	  _id: "rs0",
	  members: [
	    { _id: 0, host: "mongo-rs0-1:27017" },
	    { _id: 1, host: "mongo-rs0-2:27017" },
	    { _id: 3, host: "mongo-rs0-3:27017", priority: 0 }
	  ]
	})'
	
	echo "# Waiting 10 seconds"
	sleep 10
	
        echo "# Starting mongodb_consistent_backup, replset-only mode (in docker)"
        docker-compose up --abort-on-container-exit backup-replset

        echo "# Stopping instances with docker-compose"
        docker-compose down
popd
