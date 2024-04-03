#!/usr/bin/env bash

port=$1

source .github/insist.sh

echo "Waiting for successful HTTP status code on port ${port}"
insist 'curl -s "http://127.0.0.1:$port" > /dev/null'
