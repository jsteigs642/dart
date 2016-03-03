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
DOCKER_IMAGE_WORKER_ENGINE=$(dart_conf_value "${CONFIG}" "$.cloudformation_stacks.'engine-worker'.boto_args.Parameters[@.ParameterKey is 'EngineWorkerDockerImage'][0].ParameterValue")
DOCKER_IMAGE_WORKER_TRIGGER=$(dart_conf_value "${CONFIG}" "$.cloudformation_stacks.'trigger-worker'.boto_args.Parameters[@.ParameterKey is 'TriggerWorkerDockerImage'][0].ParameterValue")
DOCKER_IMAGE_WORKER_SUBSCRIPTION=$(dart_conf_value "${CONFIG}" "$.cloudformation_stacks.'subscription-worker'.boto_args.Parameters[@.ParameterKey is 'SubscriptionWorkerDockerImage'][0].ParameterValue")
IFS=${OLD_IFS}

$(aws ecr get-login)
set -x
docker push ${DOCKER_IMAGE_FLASK}
docker push ${DOCKER_IMAGE_WORKER_ENGINE}
docker push ${DOCKER_IMAGE_WORKER_TRIGGER}
docker push ${DOCKER_IMAGE_WORKER_SUBSCRIPTION}
set +x


popd > /dev/null
