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
# shellcheck source=scripts/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

function check_install_airflow_version_set() {
    if [[ ${INSTALL_AIRFLOW_VERSION=""} == "" ]]; then
        echo
        echo "${COLOR_RED_ERROR} You have to specify airflow version to install.${COLOR_RESET}"
        echo
        echo "It might be version from PyPI, wheel with extras or none to uninstall airflow"
        echo
        exit 1
    fi
}

function check_package_format_set() {
    PACKAGE_FORMAT=${PACKAGE_FORMAT=}

    if [[ ${PACKAGE_FORMAT} != "wheel" && ${PACKAGE_FORMAT} != "sdist" ]]; then
        echo
        echo  "${COLOR_RED_ERROR} Wrong install type ${PACKAGE_FORMAT}. Should be 'wheel' or 'sdist'  ${COLOR_RESET}"
        echo
        exit 3
    fi
}

function install_provider_packages() {
    if [[ ${PACKAGE_FORMAT} == "wheel" ]]; then
        install_all_provider_packages_from_wheels
    elif [[ ${PACKAGE_FORMAT} == "sdist" ]]; then
        install_all_provider_packages_from_tar_gz_files
    else
        echo
        echo "${COLOR_RED_ERROR} Wrong package format ${PACKAGE_FORMAT}. Should be wheel or sdist${COLOR_RESET}"
        echo
        exit 1
    fi
}

function import_all_provider_classes() {
    echo
    echo "Importing all Airflow classes"
    echo

    # We have to move out a directory where "airflow" is
    # We need to make sure we are not in the airflow checkout and unset PYTHONPATH
    # otherwise it will automatically be added to the
    # import path
    cd /
    unset PYTHONPATH

    declare -a IMPORT_CLASS_PARAMETERS

    echo
    echo "Checking for providers location"
    echo
    PROVIDER_PATHS=$(
        python3 <<EOF 2>"${OUTPUT_PRINTED_ONLY_ON_ERROR}"
import airflow.providers;
path=airflow.providers.__path__
for p in path._path:
    print(p)
EOF
    )
    export PROVIDER_PATHS

    echo
    echo "Searching for providers packages in:"
    echo
    echo "${PROVIDER_PATHS}"

    while read -r provider_path; do
        IMPORT_CLASS_PARAMETERS+=("--path" "${provider_path}")
    done < <(echo "${PROVIDER_PATHS}")

    echo
    echo "Running import all classes"
    echo
    python3 /opt/airflow/dev/import_all_classes.py "${IMPORT_CLASS_PARAMETERS[@]}"
}

function discover_all_provider_packages() {
    echo
    echo Listing available providers via 'airflow providers list'
    echo

    # Columns is to force it wider, so it doesn't wrap at 80 characters
    COLUMNS=180 airflow providers list

    local expected_number_of_providers=61
    local actual_number_of_providers
    actual_providers=$(airflow providers list --output yaml | grep package_name)
    actual_number_of_providers=$(wc -l <<<"$actual_providers")
    if [[ ${actual_number_of_providers} != "${expected_number_of_providers}" ]]; then
        echo
        echo  "${COLOR_RED_ERROR}Number of providers installed is wrong${COLOR_RESET}"
        echo "Expected number was '${expected_number_of_providers}' and got '${actual_number_of_providers}'"
        echo
        echo "Either increase the number of providers if you added one or diagnose and fix the problem."
        echo
        echo "Providers were:"
        echo
        echo "$actual_providers"
        exit 1
    fi
}

function discover_all_hooks() {
    echo
    echo Listing available hooks via 'airflow providers hooks'
    echo

    COLUMNS=180 airflow providers hooks

    local expected_number_of_hooks=59
    local actual_number_of_hooks
    actual_number_of_hooks=$(airflow providers hooks --output table | grep -c "| apache" | xargs)
    if [[ ${actual_number_of_hooks} != "${expected_number_of_hooks}" ]]; then
        echo
        echo  "${COLOR_RED_ERROR} Number of hooks registered is wrong  ${COLOR_RESET}"
        echo "Expected number was '${expected_number_of_hooks}' and got '${actual_number_of_hooks}'"
        echo
        echo "Either increase the number of hooks if you added one or diagnose and fix the problem."
        echo
        exit 1
    fi
}

function discover_all_extra_links() {
    echo
    echo Listing available extra links via 'airflow providers links'
    echo

    COLUMNS=180 airflow providers links

    local expected_number_of_extra_links=4
    local actual_number_of_extra_links
    actual_number_of_extra_links=$(airflow providers links --output table | grep -c ^airflow.providers | xargs)
    if [[ ${actual_number_of_extra_links} != "${expected_number_of_extra_links}" ]]; then
        echo
        echo  "${COLOR_RED_ERROR} Number of links registered is wrong  ${COLOR_RESET}"
        echo "Expected number was '${expected_number_of_extra_links}' and got '${actual_number_of_extra_links}'"
        echo
        echo "Either increase the number of links if you added one or diagnose and fix the problem."
        echo
        exit 1
    fi
}

function discover_all_connection_form_widgets() {
    echo
    echo Listing available widgets via 'airflow providers widgets'
    echo

    COLUMNS=180 airflow providers widgets

    local expected_number_of_widgets=19
    local actual_number_of_widgets
    actual_number_of_widgets=$(airflow providers widgets --output table | grep -c ^extra)
    if [[ ${actual_number_of_widgets} != "${expected_number_of_widgets}" ]]; then
        echo
        echo  "${COLOR_RED_ERROR} Number of connections with widgets registered is wrong  ${COLOR_RESET}"
        echo "Expected number was '${expected_number_of_widgets}' and got '${actual_number_of_widgets}'"
        echo
        echo "Increase the number of connections with widgets if you added one or investigate"
        echo
        exit 1
    fi
}

function discover_all_field_behaviours() {
    echo
    echo Listing connections with custom behaviours via 'airflow providers behaviours'
    echo

    COLUMNS=180 airflow providers behaviours

    local expected_number_of_connections_with_behaviours=11
    local actual_number_of_connections_with_behaviours
    actual_number_of_connections_with_behaviours=$(airflow providers behaviours --output table | grep -v "===" | \
        grep -v field_behaviours | grep -cv "^ " | xargs)
    if [[ ${actual_number_of_connections_with_behaviours} != \
            "${expected_number_of_connections_with_behaviours}" ]]; then
        echo
        echo  "${COLOR_RED_ERROR} Number of connections with customized behaviours is wrong  ${COLOR_RESET}"
        echo "Expected number was '${expected_number_of_connections_with_behaviours}' and got '${actual_number_of_connections_with_behaviours}'"
        echo
        echo "Increase the number of connections if you added one or investigate."
        echo
        exit 1
    fi
}

check_install_airflow_version_set
check_package_format_set

setup_provider_package_variables

get_constraints_for_just_installed_airflow
install_airflow
install_remaining_dependencies

# No matter which airflow version - providers/backport providers should use constraints from master
export CONSTRAINTS_BRANCH="constraints-master"

install_provider_packages
import_all_provider_classes

if [[ ${BACKPORT_PACKAGES} != "true" ]]; then
    discover_all_provider_packages
    discover_all_hooks
    discover_all_connection_form_widgets
    discover_all_field_behaviours
    discover_all_extra_links
fi
