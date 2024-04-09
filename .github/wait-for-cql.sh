#!/usr/bin/env bash

service=$1

source .github/attempt.sh

echo "Waiting for CQL to be ready in service ${service}"
attempt 'docker compose -f docker-compose-tests.yml exec ${service} bash -c "cqlsh -e '"'"'describe cluster'"'"'" > /dev/null'
