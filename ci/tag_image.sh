#!/bin/sh

set -e

export DOCKER_IMAGE="${CI_REGISTRY_IMAGE}:${CI_COMMIT_REF_SLUG}-${CI_PIPELINE_ID}"
export TAGGED_IMAGE="$CI_REGISTRY_IMAGE:${CI_COMMIT_TAG}"
echo "{\"auths\":{\"$CI_REGISTRY\":{\"username\":\"$CI_REGISTRY_USER\",\"password\":\"$CI_REGISTRY_PASSWORD\"},\"https://index.docker.io/v1/\":{\"auth\":\"$HUB_BASE64\"}}}" > /kaniko/.docker/config.json
echo "FROM $DOCKER_IMAGE TO $TAGGED_IMAGE"
echo "FROM  ${DOCKER_IMAGE}" | /kaniko/executor --context $CI_PROJECT_DIR --dockerfile /dev/stdin --destination ${TAGGED_IMAGE}
echo "Docker image -- $TAGGED_IMAGE"
