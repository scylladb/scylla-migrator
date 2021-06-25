#!/bin/bash

set -e
set -x

#workaround for number exceptions, once new sbt will be used + 2.12 scala below won't be needed
export TERM=xterm-color

TMPDIR="$PWD"/tmpexec
mkdir -p "$TMPDIR"
trap "rm -rf $TMPDIR" EXIT

sbt -Djava.io.tmpdir="$TMPDIR" assembly
