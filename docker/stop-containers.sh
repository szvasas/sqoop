#!/usr/bin/env bash
docker-compose -f ./docker-compose/docker-compose.yml stop
docker-compose -f ./docker-compose/docker-compose.yml rm -f
