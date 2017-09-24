#!/usr/bin/env bash
BASEDIR=$(dirname "$0")

docker-compose -f $BASEDIR/docker-compose/sqoop-thirdpartytest-db-services.yml up -d "$@"
