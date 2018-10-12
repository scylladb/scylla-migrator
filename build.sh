#!/bin/bash

set -e
set -x

git submodule update --init --recursive

pushd spark-cassandra-connector
sbt -Dscala-2.11=true assembly
popd

if [ ! -d "./lib" ]; then
    mkdir lib
fi

cp ./spark-cassandra-connector/spark-cassandra-connector/target/full/scala-2.11/spark-cassandra-connector-assembly-*.jar ./lib

sbt assembly
