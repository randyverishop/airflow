#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -exuo pipefail

DOCKERHUB_IMAGE=airflow
DOCKERHUB_USER=potiuk

TRAVIS_TAG=${TRAVIS_TAG:=}

IMAGE_TAG=${TRAVIS_PULL_REQUEST_BRANCH:=latest}

TRAVIS_CI_IMAGE=${DOCKERHUB_USER}/${DOCKERHUB_IMAGE}:airflow-travis-ci
CURRENT_CI_IMAGE=${DOCKERHUB_USER}/${DOCKERHUB_IMAGE}:current-ci

# Pull the image for image currently built if it exists
docker pull ${TRAVIS_CI_IMAGE} || true

DOCKERFILE_PATH=scripts/ci/Dockerfile

echo "Enabling cache"
CACHE_SPEC="--cache-from ${TRAVIS_CI_IMAGE}"

# Build the image using cached versions if present - both the branch/tag version and latest build can be used
# as cache source - docker will choose appropriate cache based on hashes of the files changed
docker build --pull --build-arg AIRFLOW_TAG=${TRAVIS_TAG} -f ${DOCKERFILE_PATH} -t ${CURRENT_CI_IMAGE} \
        ${CACHE_SPEC} .

echo "Build finished"
