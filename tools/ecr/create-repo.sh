#!/bin/sh -e

[[ -z "${DART_CONFIG}" ]] && echo "environment variable needs to be set: DART_CONFIG" && exit 1
source ../util.sh
REPO_NAME=${1}

pushd ../../ > /dev/null


echo "reading configuration: ${DART_CONFIG}"
OLD_IFS=${IFS}
IFS=
CONFIG="$(DART_ROLE=tool dart_conf)"
REPO_PREFIX=$(dart_conf_value "${CONFIG}" "$.ecr.repo_prefix")
REPO_POLICY_FILE=$(dart_conf_value "${CONFIG}" "$.ecr.policy_file")
IFS=${OLD_IFS}

aws ecr create-repository --repository-name ${REPO_PREFIX}/${REPO_NAME}
aws ecr set-repository-policy --repository-name ${REPO_PREFIX}/${REPO_NAME} --policy-text file://${REPO_POLICY_FILE}


popd > /dev/null
