#!/bin/sh -e

REPO=${1}
REPO_POLICY_FILE=${2}

pushd ../../ > /dev/null


aws ecr create-repository --repository-name ${REPO}
aws ecr set-repository-policy --repository-name ${REPO} --policy-text file://${REPO_POLICY_FILE}


popd > /dev/null
