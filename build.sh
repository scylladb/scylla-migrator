#!/bin/bash

set -e
set -x

export TERM=xterm-color

sbt -mem 8192 migrator/assembly
