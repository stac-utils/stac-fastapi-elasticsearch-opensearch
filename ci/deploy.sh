#!/bin/sh

set -e

if [ -z "$1" ]; then
    echo "Environment name is required. Possible options are: 'production-creo', 'production-lta'..."
    exit 1
fi


ENV=$1;

# Download operations repo and prepare for changes.
apk add --no-cache git
if [ -z "$CI_COMMIT_TAG" ]; then export APP_VERSION="${CI_COMMIT_REF_SLUG}-${CI_PIPELINE_ID}"; else APP_VERSION="${CI_COMMIT_TAG}"; fi
if [ -z "$CI_COMMIT_TAG" ]; then export DOCKER_IMAGE="$CI_REGISTRY_IMAGE:${CI_COMMIT_REF_SLUG}-${CI_PIPELINE_ID}"; else DOCKER_IMAGE="$CI_REGISTRY_IMAGE:${CI_COMMIT_TAG}"; fi
git remote set-url origin https://oauth2:"${GITLAB_PUSH_TOKEN}"@gitlab.cloudferro.com/stac/operations.git
git config --global user.email "gitlab@cloudferro.com"
git config --global user.name "GitLab CI/CD"
git clone https://oauth2:"${GITLAB_PUSH_TOKEN}"@gitlab.cloudferro.com/stac/operations.git
cd operations
git checkout -B master

# Change deployment configuration operations.
sed -i "s~'registry.cloudferro.com/stac/sfeos.*~'${DOCKER_IMAGE}',~" environments/"${ENV}"/stac-fastapi-os/main.jsonnet
git add environments/"${ENV}"/stac-fastapi-os/main.jsonnet
git commit -m "[${ENV}] stac-fastapi-os ${APP_VERSION}"
git push origin master
