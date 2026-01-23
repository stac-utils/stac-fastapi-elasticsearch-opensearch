#!/bin/sh

set -e

if [ -z "$1" ]; then
    echo "Environment name is required. Possible options are: 'prod/waw3-2-general-01', 'staging/waw3-2-general-01-staging', 'dev/waw3-2-general-01-staging'..."
    exit 1
fi

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

# Iterate over arguments/envs
for var in "$@"
do
  # Check for special deployments (DEV QA)
  if [  $var == "dev/waw3-2-general-01-staging-qa"  ]; then
    ENV="dev/waw3-2-general-01-staging"
    DEPLOYMENT_NAME="stac-fastapi-os-qa"
  elif [  $var == "dev/waw3-2-general-01-staging-qa2"  ]; then
    ENV="dev/waw3-2-general-01-staging"
    DEPLOYMENT_NAME="stac-fastapi-os-qa2"
  else
    ENV=$var
    DEPLOYMENT_NAME="stac-fastapi-os"
  fi

  # Change deployment configuration operations.
  sed -i "s~'registry.cloudferro.com/stac/sfeos.*~'${DOCKER_IMAGE}',~" environments/"${ENV}"/"${DEPLOYMENT_NAME}"/main.jsonnet
  git add environments/"${ENV}"/"${DEPLOYMENT_NAME}"/main.jsonnet
  git commit -m "[${ENV}] ${DEPLOYMENT_NAME} ${APP_VERSION}"
done

git push origin master
