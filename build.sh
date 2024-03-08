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
sbt -Djava.io.tmpdir="$TMPDIR" ++2.11.12 assembly
popd
pushd spark-kinesis
sbt assembly
popd

if [ ! -d "./migrator/lib" ]; then
    mkdir migrator/lib
fi

cp ./spark-cassandra-connector/connector/target/scala-2.11/spark-cassandra-connector-assembly-*.jar ./migrator/lib
cp ./spark-kinesis/target/scala-2.11/spark-streaming-kinesis-asl-assembly-*.jar ./migrator/lib

sbt -Djava.io.tmpdir="$TMPDIR" migrator/assembly
