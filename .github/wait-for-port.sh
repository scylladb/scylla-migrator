#!/usr/bin/env bash

port=$1
echo "Waiting for port ${port}"
attempts=0
max_attempts=60
while ! curl -s "http://127.0.0.1:$port" > /dev/null ; do
    [[ $attempts -ge $max_attempts ]] && echo "Failed!" && exit 1
    attempts=$((attempts+1))
    sleep 1;
    echo "waiting... (${attempts}/${max_attempts})"
done
