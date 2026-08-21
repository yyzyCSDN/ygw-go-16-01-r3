#!/bin/sh
set -eu
docker build -f benzhi.Dockerfile -t relaydock:local .
