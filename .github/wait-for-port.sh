#!/usr/bin/env bash

port=$1

source .github/attempt.sh

echo "Waiting for successful HTTP status code on port ${port}"
attempt 'curl -s "http://127.0.0.1:$port" > /dev/null'
