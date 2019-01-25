#!/usr/bin/env bash

#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

set -x

DIRNAME=$(cd "$(dirname "$0")"; pwd)
AIRFLOW_ROOT="$DIRNAME/../.."

# Fix file permissions
sudo chown -R airflow.airflow . $HOME/.cache $HOME/.wheelhouse/ $HOME/.cache/pip

if [[ -d $HOME/.minikube ]]; then
    sudo chown -R airflow.airflow $HOME/.kube $HOME/.minikube
fi

workon ${VIRTUALENV}

if [[ ${BACKEND} == MYSQL ]]; then
  export AIRFLOW__CORE__SQL_ALCHEMY_CONN=mysql://root@mysql/airflow
  export AIRFLOW__CELERY__RESULT_BACKEND=db+mysql://root@mysql/airflow
elif [[ ${BACKEND} == POSTGRES ]]; then
  export AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:airflow@postgres/airflow
  esport AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://postgres:airflow@postgres/airflow
elif [[ ${BACKEND} == SQLITE ]]; then
  export AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:///${HOME}/airflow.db
  export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
fi

cd ${AIRFLOW_ROOT} && $PIP --version && tox --version

if [[ -z ${KUBERNETES_VERSION} ]];
then
  ${DIRNAME}/setup/setup-env.sh
  ${DIRNAME}/setup/setup-kdc.sh
  if [[ ${BACKEND} == MYSQL ]]; then
    ${DIRNAME}/setup/setup-mysql.sh
  fi
  ${DIRNAME}/run-tests.sh []
  codecov -e TOXENV=${VIRTUALENV}_${BACKEND}_${ENV}
else
#  # This script runs inside a container, the path of the kubernetes certificate
#  # is /home/travis/.minikube/client.crt but the user in the container is `airflow`
#  if [ ! -d /home/travis ]; then
#    sudo mkdir -p /home/travis
#  fi
#  sudo ln -s /home/airflow/.minikube /home/travis/.minikube
#
#  tox -e $TOX_ENV -- tests.contrib.minikube \
#                     --with-coverage \
#                     --cover-erase \
#                     --cover-html \
#                     --cover-package=airflow \
#                     --cover-html-dir=airflow/www/static/coverage \
#                     --with-ignore-docstrings \
#                     --rednose \
#                     --with-timer \
#                     -v \
#                     --logging-level=DEBUG
    echo
fi
