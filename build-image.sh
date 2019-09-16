#!/bin/bash

set -e

ORG=${ORG:-hsldevcom}
DOCKER_TAG=develop
DOCKER_IMAGE=$ORG/transitlog-hfp-sink:${DOCKER_TAG}
DOCKER_IMAGE_LATEST=$ORG/transitlog-hfp-sink:develop

docker build -t $DOCKER_IMAGE .

docker tag $DOCKER_IMAGE $DOCKER_IMAGE_LATEST
docker login
docker push $DOCKER_IMAGE_LATEST
