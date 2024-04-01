#!/usr/bin/env bash

port=$1
echo "Waiting for port ${port}"
attempts=0
max_attempts=30
while ! curl -s "http://127.0.0.1:$port" > /dev/null && [[ $attempts -lt $max_attempts ]] ; do
    attempts=$((attempts+1))
    sleep 1;
    echo "waiting... (${attempts}/${max_attempts})"
done
