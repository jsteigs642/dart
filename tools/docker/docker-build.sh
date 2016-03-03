#!/bin/sh -e

source ./docker-local-init.sh
TYPE=${1}
DOCKER_IMAGE=${2}

pushd ../../ > /dev/null


docker build -f tools/docker/Dockerfile-${TYPE} -t ${DOCKER_IMAGE} .

REMOVABLE=$(docker images -f "dangling=true" -q)
[[ -n "${REMOVABLE}" ]] && docker rmi ${REMOVABLE}


popd > /dev/null
