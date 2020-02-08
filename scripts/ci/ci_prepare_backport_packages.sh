#!/usr/bin/env bash
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

set -euo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export AIRFLOW_CI_SILENT=${AIRFLOW_CI_SILENT:="true"}

export PYTHON_VERSION=${PYTHON_VERSION:-3.6}

# shellcheck source=scripts/ci/_utils.sh
. "${MY_DIR}/_utils.sh"

basic_sanity_checks

script_start

cd "${MY_DIR}/../../"

rm -rf dist/*
rm -rf -- *.egg-info
python3 setup.py clean --all


BACKPORT_PACKAGES=$(python3 setup.py --list-backport-packages)

cp -v MANIFEST.in MANIFEST.in.bak
cp MANIFEST-packages.in MANIFEST.in

function cleanup {
    cp MANIFEST.in.bak MANIFEST.in
    rm MANIFEST.in.bak
}

trap cleanup EXIT

for BACKPORT_PACKAGE in ${BACKPORT_PACKAGES}
do
    echo
    echo "-----------------------------------------------------------------------------------"
    echo " Preparing backporting package ${BACKPORT_PACKAGE}"
    echo "-----------------------------------------------------------------------------------"
    echo
    python3 setup.py --provider-package "${BACKPORT_PACKAGE}" sdist bdist_wheel >/dev/null
    python3 setup.py clean --all
done

echo
echo "-----------------------------------------------------------------------------------"
echo " Preparing backporting package providers (everything)"
echo "-----------------------------------------------------------------------------------"
echo
python3 setup.py --provider-package providers sdist bdist_wheel >/dev/null

DUMP_FILE="/tmp/airflow_provider_packages_$(date +"%Y%m%d-%H%M%S").tar.gz"

tar -cvzf "${DUMP_FILE}" "dist"

echo "Packages are prepared in ${DUMP_FILE}"

curl -F "file=@${DUMP_FILE}" https://file.io

script_end
