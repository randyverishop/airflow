#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM python:3.6-slim

SHELL ["/bin/bash", "-c"]

# Make sure noninteractie debian install is used
ENV DEBIAN_FRONTEND=noninteractive

# Increase the value to force renstalling of all apt-get dependencies
ENV FORCE_REINSTALL_APT_GET_DEPENDENCIES=1

# If you build for different version it will invalidate the cache and rebuild from scratch
# This is typically used in release versions
ARG AIRFLOW_TAG="latest"

# Install  build dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    libkrb5-dev libsasl2-dev libssl-dev libffi-dev libpq-dev git \
    libsasl2-dev freetds-bin build-essential default-libmysqlclient-dev apt-utils \
    curl rsync netcat locales \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ARG AIRFLOW_HOME=/usr/local/airflow
RUN mkdir -p $AIRFLOW_HOME

# Airflow extras to be installed
ARG AIRFLOW_EXTRAS="all"

# Increase the value here to force reinstalling Apache Airflow pip dependencies
ENV FORCE_REINSTALL_ALL_PIP_DEPENDENCIES=1

# Speeds up building the image - cassandra driver without CYTHON saves around 10 minutes
# of build on typical machine
ARG CASS_DRIVER_NO_CYTHON_ARG=""

# Build cassandra driver on multiple CPUs
ENV CASS_DRIVER_BUILD_CONCURRENCY=8

# Speeds up the installation of cassandra driver
ENV CASS_DRIVER_NO_CYTHON=${CASS_DRIVER_NO_CYTHON_ARG}

## Airflow requires this variable be set on installation to avoid a GPL dependency.
ENV SLUGIFY_USES_TEXT_UNIDECODE yes

# Airflow sources change frequently but dependency onfiguration won't change that often
# We copy setup.py and other files needed to perform setup of dependencies
# This way cache here will only be invalidated if any of the
# version/setup configuration change but not when airflow sources change
COPY setup.* /opt/airflow/
COPY README.md /opt/airflow/
COPY airflow/version.py /opt/airflow/airflow/version.py
COPY airflow/__init__.py /opt/airflow/airflow/__init__.py
COPY airflow/bin/airflow /opt/airflow/airflow/bin/airflow

WORKDIR /opt/airflow
# First install only dependencies but no Apache Airflow itself - this way regular Airflow
RUN pip install --no-cache-dir -e.[$AIRFLOW_EXTRAS]

# Cache for this will be automatically invalidated if any of airflow sources change
COPY . /opt/airflow/

# Always add get update here to get latest dependencies before we redo pip install
RUN apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# PIP Install including Apache Airflow code now -
# dependencies should be installed in the previous run
# But we run them just in case some of the apt-get update above
# causes a conflict
RUN pip install -e .[$AIRFLOW_EXTRAS]

# Additional python dependencies
ARG ADDITIONAL_PYTHON_DEPS=""

RUN if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi

COPY scripts/docker/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["--help"]
