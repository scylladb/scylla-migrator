#!/bin/bash
docker buildx build --file Dockerfile --output type=local,dest=./migrator/target/scala-2.13 .
