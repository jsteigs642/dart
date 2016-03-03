#!/bin/sh -e

[[ -z "${DART_CONFIG}" ]] && echo "environment variable needs to be set: DART_CONFIG" && exit 1
source ../util.sh
source ./docker-local-init.sh

pushd ../../ > /dev/null


echo "reading configuration: ${DART_CONFIG}"
OLD_IFS=${IFS}
IFS=
CONFIG="$(DART_ROLE=tool dart_conf)"
DOCKER_IMAGE_FLASK=$(dart_conf_value "${CONFIG}" "$.cloudformation_stacks.web.boto_args.Parameters[@.ParameterKey is 'FlaskWorkerDockerImage'][0].ParameterValue")
DOCKER_IMAGE_NGINX=$(dart_conf_value "${CONFIG}" "$.cloudformation_stacks.web.boto_args.Parameters[@.ParameterKey is 'NginxWorkerDockerImage'][0].ParameterValue")
IFS=${OLD_IFS}

HOST_IP=$(ifconfig en0 | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*')

docker run \
    --name dart-flask \
    --add-host=container_host:${HOST_IP} \
    -e "DART_ROLE=web" \
    -e "DART_CONFIG=${DART_CONFIG}" \
    -e "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
    -e "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
    -d \
    -P \
    ${DOCKER_IMAGE_FLASK}

docker run --name dart-nginx -d -p 8080:8080 --volumes-from dart-flask ${DOCKER_IMAGE_NGINX}


popd > /dev/null
