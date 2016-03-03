#!/bin/sh -ex

# source this, or run with ". " (dot space) notation, e.g.:
#
#    . docker-local-init.sh
#

docker-machine start docker-dart || echo 'docker-dart is already running... skipping startup'
sleep 2
eval "$(docker-machine env docker-dart)"
export DOCKER_HOST=tcp://localhost:2376
