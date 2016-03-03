#!/bin/sh -e

[[ -z "${DART_CONFIG}" ]] && echo "environment variable needs to be set: DART_CONFIG" && exit 1
source ../util.sh
source ./docker-local-init.sh

pushd ../../ > /dev/null


echo "reading configuration: ${DART_CONFIG}"
OLD_IFS=${IFS}
IFS=
CONFIG="$(DART_ROLE=tool dart_conf)"
IMPALA_DOCKER_REPO_BASE_URL=$(dart_conf_value "${CONFIG}" "$.engines.emr_engine.options.impala_docker_repo_base_url")
IMPALA_VERSION=$(dart_conf_value "${CONFIG}" "$.engines.emr_engine.options.impala_version")
IFS=${OLD_IFS}

set -x
docker push ${IMPALA_DOCKER_REPO_BASE_URL}/impala-state-store:${IMPALA_VERSION}
docker push ${IMPALA_DOCKER_REPO_BASE_URL}/impala-catalog:${IMPALA_VERSION}
docker push ${IMPALA_DOCKER_REPO_BASE_URL}/impala-server:${IMPALA_VERSION}
set +x


popd > /dev/null
