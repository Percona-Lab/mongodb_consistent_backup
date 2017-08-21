#!/bin/bash

set -e

VERSION=${1:-3.2}
NAME=${2:-test-cluster}


IP=$(/sbin/ip route|awk '/default/ { print $3 }')
FULL_NAME="${NAME}-${VERSION}"
DOCKER_TAG=percona/percona-server-mongodb:${VERSION}

function doMongo() {
	local PORT=$1
	local CMD=$2
	local MAX_TRIES=10

	set +e
	local TRIES=0
	local EXIT=-1
	local RETURN=
	while [ $EXIT != 0 ] && [ $TRIES -le $MAX_TRIES ]; do
		RETURN=$(mongo --port ${PORT} --quiet --eval "$CMD" || true)
		echo ${RETURN} | grep -q '"ok" : 1'
		EXIT=$?
		TRIES=$((${TRIES} + 1))
		sleep 3
	done
	echo $RETURN
	set -e
}

echo "# Doing cleanup"
docker rm -f ${FULL_NAME}-r0-1 ${FULL_NAME}-r0-2 ${FULL_NAME}-csReplSet-1 ${FULL_NAME}-csReplSet-2 ${FULL_NAME}-mongos 2>/dev/null || true
trap "docker rm -f ${FULL_NAME}-r0-1 ${FULL_NAME}-r0-2 ${FULL_NAME}-csReplSet-1 ${FULL_NAME}-csReplSet-2 ${FULL_NAME}-mongos 2>/dev/null" SIGINT SIGTERM

echo "# Starting instances"
docker run --name=${FULL_NAME}-r0-1 -d -p 27017:27017 ${DOCKER_TAG} --port=27017 --replSet=rs0 --dbpath /data/db
docker run --name=${FULL_NAME}-r0-2 -d -p 27027:27027 ${DOCKER_TAG} --port=27027 --replSet=rs0 --dbpath /data/db
docker run --name=${FULL_NAME}-csReplSet-1 -d -p 27019:27019 ${DOCKER_TAG} --port=27019 --replSet=csReplSet --configsvr --dbpath /data/db
docker run --name=${FULL_NAME}-csReplSet-2 -d -p 27029:27029 ${DOCKER_TAG} --port=27029 --replSet=csReplSet --configsvr --dbpath /data/db

docker ps -a
sleep 5

echo "# Setup replication"
doMongo 27019 "rs.initiate({ _id: \"csReplSet\", configsvr: true, members: [{ _id: 0, host: \"${IP}:27019\" }, { _id: 1, host: \"${IP}:27029\" }]})"
doMongo 27017 "rs.initiate({ _id: \"rs0\", members: [{ _id: 0, host: \"${IP}:27017\" }, { _id: 1, host: \"${IP}:27027\", priority: 0 }]})"

sleep 5

echo "# Start mongos"
docker run --name=${FULL_NAME}-mongos -d -p 27018:27018 ${DOCKER_TAG} /usr/bin/mongos --port=27018 --configdb=csReplSet/${IP}:27019,${IP}:27029

docker ps -a
sleep 5

echo "# Add shard"
doMongo 27018 "sh.addShard(\"rs0/${IP}:27017,${IP}:27027\")"
