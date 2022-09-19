#!/bin/bash

set -e
set -x

#workaround for number exceptions, once new sbt will be used + 2.12 scala below won't be needed
export TERM=xterm-color 

git submodule update --init --recursive

TMPDIR="$PWD"/tmpexec
mkdir -p "$TMPDIR"
trap "rm -rf $TMPDIR" EXIT
pushd spark-cassandra-connector
sbt -Djava.io.tmpdir="$TMPDIR" ++2.12.10 assembly
popd
pushd spark-dynamodb
sbt ++2.12.10 assembly
popd
pushd spark-kinesis
sbt ++2.12.10 assembly
popd

if [ ! -d "./lib" ]; then
    mkdir lib
fi

cp ./spark-cassandra-connector/connector/target/scala-2.12/spark-cassandra-connector-assembly-*.jar ./lib
cp ./spark-dynamodb/target/scala-2.12/spark-dynamodb-assembly-*.jar ./lib
cp ./spark-kinesis/target/scala-2.12/spark-streaming-kinesis-asl-assembly-*.jar ./lib

sbt -Djava.io.tmpdir="$TMPDIR" ++2.12.10 assembly
