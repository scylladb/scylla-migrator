#!/usr/bin/env sh

# Create table
aws \
  --endpoint-url http://localhost:8000 \
  dynamodb create-table \
    --table-name Example \
    --attribute-definitions AttributeName=id,AttributeType=S \
    --key-schema AttributeName=id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=100,WriteCapacityUnits=100

# Add items in parallel
# Change 40000 into 400 below for a faster demo (10,000 items instead of 1,000,000)
seq 1 40000 | xargs --max-procs=8 --max-args=1 ./create-25-items.sh
