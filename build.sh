#!/bin/bash

#workaround for number exceptions, once new sbt will be used + 2.12 scala below won't be needed
export TERM=xterm-color

set -e
set -x

git submodule update --init --recursive

TMPDIR="$PWD"/tmpexec
mkdir -p "$TMPDIR"
trap "rm -rf $TMPDIR" EXIT
pushd spark-cassandra-connector
sbt -Djava.io.tmpdir="$TMPDIR" -Dscala-2.11=true assembly
popd

if [ ! -d "./lib" ]; then
    mkdir lib
fi

cp ./spark-cassandra-connector/spark-cassandra-connector/target/full/scala-2.11/spark-cassandra-connector-assembly-*.jar ./lib

sbt -Djava.io.tmpdir="$TMPDIR" assembly
