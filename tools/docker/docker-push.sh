#!/bin/sh

DOCKER_IMAGE=${2}

pushd ../../ > /dev/null


$(aws ecr get-login)
docker push ${DOCKER_IMAGE}


popd > /dev/null
