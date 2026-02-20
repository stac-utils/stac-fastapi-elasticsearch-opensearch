#!/bin/sh

set -e

export DOCKER_IMAGE="${CI_REGISTRY_IMAGE}:${CI_COMMIT_REF_SLUG}-${CI_PIPELINE_ID}"
echo "Docker image -- $DOCKER_IMAGE"
echo "{\"auths\":{\"$CI_REGISTRY\":{\"username\":\"$CI_REGISTRY_USER\",\"password\":\"$CI_REGISTRY_PASSWORD\"},\"https://index.docker.io/v1/\":{\"auth\":\"$HUB_BASE64\"}}}" > /kaniko/.docker/config.json
/kaniko/executor --build-arg=GITLAB_SSH_KEY  --context $CI_PROJECT_DIR --dockerfile $CI_PROJECT_DIR/Dockerfile --destination $DOCKER_IMAGE;
echo "Docker image -- $DOCKER_IMAGE"
