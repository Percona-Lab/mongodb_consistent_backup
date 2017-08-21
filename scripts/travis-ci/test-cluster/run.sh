#!/bin/bash

set -e
set -x

MONGO_VERSION=${1:-3.2}

pushd $(dirname $0)
	sed s/'{{MONGO_VERSION}}'/$MONGO_VERSION/g docker-compose.yml.tmpl >docker-compose.yml

	echo "# Starting mongodb_consistent_backup (in docker)"
	docker-compose up mongodb_consistent_backup
popd
