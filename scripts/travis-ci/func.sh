#!/bin/bash

function doMongo() {
	set +x
	set +e
	local service=$1
	local host=$2
	local cmd=$3
	local tries=0
	while [ $tries -le 5 ]; do
  	  bash -x -c "docker-compose run --rm ${service} /usr/bin/mongo ${host} --quiet --eval '${cmd}'"
	  [ $? = 0 ] && break
	  echo "# Retrying mongo command to ${host}"
	  tries=$(($tries + 1))
	  sleep 3
	done
	set -e
	set -x
}
