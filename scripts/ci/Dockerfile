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
#

# WARNING: THIS DOCKERFILE IS NOT INTENDED FOR PRODUCTION USE OR DEPLOYMENT. AT
#          THIS POINT, THIS IS ONLY INTENDED FOR USE IN AUTOMATED TESTS.

# Pleas run the `docker build` with the context of whole airflow project
FROM ubuntu:18.04

SHELL ["/bin/bash", "-c"]

USER root

# Increase the value to force renstalling of all apt-get dependencies
ENV FORCE_REINSTALL_APT_GET_DEPENDENCIES=1

ARG AIRFLOW_TAG

# If you build for different version it will invalidate the cache here and rebuild from scratch
# This is typically used in release versions
ENV AIRFLOW_TAG=${AIRFLOW_TAG:-"latest"}

ENV DEBIAN_FRONTEND noninteractive
ENV LANGUAGE C.UTF-8
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV LC_CTYPE C.UTF-8
ENV LC_MESSAGES C.UTF-8

# add a simple script that can auto-detect the appropriate JAVA_HOME value
# based on whether the JDK or only the JRE is installed
RUN { \
    echo '#!/bin/sh'; \
    echo 'set -e'; \
    echo; \
    echo 'dirname "$(dirname "$(readlink -f "$(which javac || which java)")")"'; \
  } > /usr/local/bin/docker-java-home \
  && chmod +x /usr/local/bin/docker-java-home

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        lsb-release mysql-server libmysqlclient-dev libsasl2-dev mysql-client \
        bzip2 unzip apt-transport-https xz-utils mlocate wget curl gcc \
        g++ \
    && apt-get clean

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-8-jdk  \
    && [ "$JAVA_HOME" = "$(docker-java-home)" ] \
    && apt-get clean

RUN /var/lib/dpkg/info/ca-certificates-java.postinst configure

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python-pip python3-pip virtualenvwrapper \
    && apt-get clean

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git-all tig tmux vim less curl gnupg2 software-properties-common unzip sudo \
        ldap-utils mysql-server mysql-client \
        default-libmysqlclient-dev postgresql-client sqlite3 libkrb5-dev krb5-user \
        openssh-client openssh-server python-selinux sasl2-bin libsasl2-2 \
        libsasl2-dev libsasl2-modules locales \
    && apt-get clean

RUN echo 'deb http://deb.nodesource.com/node_8.x bionic main' > /etc/apt/sources.list.d/nodesource.list
RUN echo 'deb-src http://deb.nodesource.com/node_8.x bionic main' >> /etc/apt/sources.list.d/nodesource.list

RUN sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8

# Install python 3.6 and 3.5 for airflow's compatibility,
# python-dev and necessary libraries to build all python packages
RUN add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install  -y --no-install-recommends \
       python3.6 python3.6-dev python3.5 python3.5-dev python-dev \
       build-essential autoconf libtool libkrb5-dev \
    && apt-get clean

ENV HADOOP_VERSION 2.6.0
ENV HADOOP_DISTRO=cdh
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_DISTRO}
ENV HIVE_HOME=/opt/hive
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

RUN  mkdir ${HADOOP_HOME} && \
     mkdir ${HIVE_HOME}  && \
     mkdir /opt/minicluster  && \
     mkdir -p /user/hive/warehouse && \
     chmod -R 777 ${HIVE_HOME} && \
     chmod -R 777 /user/

# Install Hadoop
# --absolute-names is a work around to avoid this issue https://github.com/docker/hub-feedback/issues/727
RUN cd /opt && \
    wget -q https://archive.cloudera.com/cdh5/cdh/5/hadoop-${HADOOP_VERSION}-cdh5.11.0.tar.gz && \
    tar xzf hadoop-${HADOOP_VERSION}-cdh5.11.0.tar.gz --absolute-names --strip-components 1 -C $HADOOP_HOME && \
    rm hadoop-${HADOOP_VERSION}-cdh5.11.0.tar.gz

# Install Hive
RUN cd /opt && \
    wget -q https://archive.cloudera.com/cdh5/cdh/5/hive-1.1.0-cdh5.11.0.tar.gz && \
    tar xzf hive-1.1.0-cdh5.11.0.tar.gz --strip-components 1 -C $HIVE_HOME && \
    rm hive-1.1.0-cdh5.11.0.tar.gz

# Install MiniCluster
RUN cd /opt && \
    wget -q https://github.com/bolkedebruin/minicluster/releases/download/1.1/minicluster-1.1-SNAPSHOT-bin.zip && \
    unzip minicluster-1.1-SNAPSHOT-bin.zip -d /opt && \
    rm minicluster-1.1-SNAPSHOT-bin.zip

# Setup un-privileged user with passwordless sudo access.
RUN groupadd -r airflow && useradd -m -r -g airflow -G sudo airflow
RUN echo 'airflow   ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers

RUN pip install --no-cache --upgrade setuptools virtualenvwrapper \
   && pip3 install --no-cache --upgrade setuptools virtualenvwrapper

USER airflow

RUN source /usr/share/virtualenvwrapper/virtualenvwrapper.sh \
    && mkvirtualenv -p /usr/bin/python3.6 airflow36  \
    && mkvirtualenv -p /usr/bin/python3.5 airflow35  \
    && mkvirtualenv -p /usr/bin/python2.7 airflow27

# Note. Increase this number to force rebuilding to the latest dependencies
ENV REINSTALL_AIRFLOW_DEPENDENCIES=1

ENV AIRFLOW_SOURCES=/workspace/

WORKDIR ${AIRFLOW_SOURCES}
#
# The Dockerfile should be run with context set to the
# main Airflow directory
#
COPY setup.* ${AIRFLOW_SOURCES}
COPY airflow/version.py ${AIRFLOW_SOURCES}/airflow/version.py
COPY airflow/__init__.py ${AIRFLOW_SOURCES}/airflow/__init__.py
COPY airflow/bin/airflow ${AIRFLOW_SOURCES}/airflow/bin/airflow

RUN sudo chown -R airflow.airflow ${AIRFLOW_SOURCES}

ENV PIP_FLAGS="--no-use-pep517"

# Speed up the installation of cassandra driver
ENV CASS_DRIVER_BUILD_CONCURRENCY=8
ENV CASS_DRIVER_NO_CYTHON=1

RUN . /usr/share/virtualenvwrapper/virtualenvwrapper.sh \
    && workon airflow27 \
    && pip install ${PIP_FLAGS} -e .[devel_ci]

RUN . /usr/share/virtualenvwrapper/virtualenvwrapper.sh \
    && workon airflow36 \
    && pip install ${PIP_FLAGS} -e .[devel_ci]

RUN . /usr/share/virtualenvwrapper/virtualenvwrapper.sh \
    && workon airflow35 \
    && pip install ${PIP_FLAGS} -e .[devel_ci]

COPY . ${AIRFLOW_SOURCES}

RUN sudo chown -R airflow.airflow ${AIRFLOW_SOURCES}

# Always add-get update/upgrade here to get latest dependencies before we redo pip install
RUN sudo apt-get update \
    && sudo apt-get upgrade -y --no-install-recommends \
    && sudo apt-get clean && sudo rm -rf /var/lib/apt/lists/*

RUN . /usr/share/virtualenvwrapper/virtualenvwrapper.sh \
    && workon airflow27 \
    && pip install ${PIP_FLAGS} -e .[devel_ci]

RUN . /usr/share/virtualenvwrapper/virtualenvwrapper.sh \
    && workon airflow36 \
    && pip install ${PIP_FLAGS} -e .[devel_ci]

RUN . /usr/share/virtualenvwrapper/virtualenvwrapper.sh \
    && workon airflow35 \
    && pip install ${PIP_FLAGS} -e .[devel_ci]

WORKDIR /home/airflow

ENV PATH "$PATH/opt/hive/bin"

EXPOSE 8080
