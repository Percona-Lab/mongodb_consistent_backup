#!/bin/bash

set -e
set -x

MONGO_VERSION=${1:-3.2}
CONFIGSVR_TYPE=${2:-CSRS}
MCB_EXTRA="${@:3}"

pushd $(dirname $0)
	source $PWD/func.sh

	export MONGO_VERSION=${MONGO_VERSION}
	export MCB_EXTRA=${MCB_EXTRA}

	export CONFIGSVR_REPLSET=csReplSet
	if [ "${CONFIGSVR_TYPE}" == "CSRS" ]; then
		export CONFIGSVR_FLAGS="--replSet ${CONFIGSVR_REPLSET}"
		export MONGOS_CONFIGDB="${CONFIGSVR_REPLSET}/mongo-cs-1:27017,mongo-cs-2:27017"
		echo "# Using CSRS-based config servers: ${MONGOS_CONFIGDB}"
	else
		export CONFIGSVR_FLAGS=
		export MONGOS_CONFIGDB="mongo-cs-1:27017,mongo-cs-2:27017"
		echo "# Using SCCC-based config servers: ${MONGOS_CONFIGDB}"
	fi

	echo "# Starting instances with docker-compose"
	docker-compose up -d mongo-mongos

	echo "# Waiting 10 seconds"
	sleep 10
	
	if [ "${CONFIGSVR_TYPE}" == "CSRS" ]; then
		echo "# Initiating csReplSet (config server set)"
		doMongo mongo-mongos mongo-cs-1:27017 'rs.initiate({
		  _id: "csReplSet",
		  configsvr: true,
		  members: [
		    { _id: 0, host: "mongo-cs-1:27017" },
		    { _id: 1, host: "mongo-cs-2:27017" }
		  ]
		})'
	fi
	
	echo "# Initiating rs0"
	doMongo mongo-mongos mongo-s-rs0-1:27017 'rs.initiate({
	  _id: "rs0",
	  members: [
	    { _id: 0, host: "mongo-s-rs0-1:27017" },
	    { _id: 1, host: "mongo-s-rs0-2:27017", priority: 0 }
	  ]
	})'
	
	echo "# Waiting 10 seconds"
	sleep 10
	
	echo "# Adding shard rs0"
  	doMongo mongo-mongos mongo-mongos:27017 'sh.addShard("rs0/mongo-s-rs0-1:27017,mongo-s-rs0-2:27017")'

        echo "# Starting mongodb_consistent_backup, cluster mode (in docker)"
        docker-compose up --abort-on-container-exit backup-cluster

        echo "# Stopping instances with docker-compose"
        docker-compose down
popd
