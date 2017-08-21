#!/bin/bash

set -e
set -x

pushd $(dirname $0)
	echo "# Starting instances with docker-compose"
	docker-compose up -d
	
	echo "# Initiating csReplSet (config server set)"
	docker-compose run --rm mongo-csReplSet-1 mongo mongo-csReplSet-1:27019 --quiet --eval 'rs.initiate({
	  _id: "csReplSet",
	  configsvr: true,
	  members: [
	    { _id: 0, host: "mongo-csReplSet-1:27019" },
	    { _id: 1, host: "mongo-csReplSet-2:27029" }
	  ]
	})'
	
	echo "# Initiating rs0"
	docker-compose run --rm mongo-rs0-1 mongo mongo-rs0-1:27017 --quiet --eval 'rs.initiate({
	  _id: "rs0",
	  members: [
	    { _id: 0, host: "mongo-rs0-1:27017" },
	    { _id: 1, host: "mongo-rs0-2:27027", priority: 0 }
	  ]
	})'
	
	echo "# Waiting 15 seconds"
	sleep 15
	
	echo "# Adding shard rs0"
	docker-compose run --rm mongo-mongos mongo mongo-mongos:27018 --quiet --eval 'sh.addShard("rs0/mongo-rs0-1:27017,mongo-rs0-2:27027")'
popd
