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
"""Manages all providers."""
import fnmatch
import json
import logging
import os
from collections import OrderedDict
from functools import lru_cache
from typing import Dict, Set, Tuple

import jsonschema
import pkg_resources
import yaml

try:
    import importlib.resources as importlib_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources


log = logging.getLogger(__name__)


def _create_validator():
    """Creates JSON schema validator from the provider.yaml.schema.json"""
    schema = json.loads(importlib_resources.read_text('airflow', 'provider.yaml.schema.json'))
    cls = jsonschema.validators.validator_for(schema)
    validator = cls(schema)
    return validator


class ProvidersManager:
    """Manages all provider packages."""

    def __init__(self):
        # Keeps list of providers keyed by module name and value is Tuple: version, provider_info
        self._provider_directory: Dict[str, Tuple[str, Dict]] = OrderedDict()
        self._provider_list: [Tuple[str, str, Dict]] = []
        self._provider_set: Set[str] = set()
        self._validator = _create_validator()
        # Local source folders are loaded first. They should take precedence over the package ones for
        # Development purpose. In production provider.yaml files are not present in the 'airflow" directory
        # So there is no risk we are going to override package provider accidentally. This can only happen
        # in case of local development
        self.__find_all_airflow_builtin_providers_from_local_sources()
        self.__find_all_providers_from_packages()
        self.__creates_provider_directory()

    def __creates_provider_directory(self):
        """
        Creates provider_directory as sorted (by package_name) OrderedDict.

        Duplicates are removed from "package" providers in case corresponding "folder" provider is found.
        The "folder" providers are from local sources (packages do not contain provider.yaml files),
        so if someone has airflow installed from local sources, the providers are imported from there
        first so, provider information should be taken from there.
        :return:
        """
        self._provider_list.sort(key=lambda provider_list_element: provider_list_element[0])
        for package_name, version, provider_info in self._provider_list:
            self._provider_directory[package_name] = (version, provider_info)

    def __find_all_providers_from_packages(self) -> None:
        """
        Finds all providers by scanning packages installed. The list of providers should be returned
        via the 'apache_airflow_provider' entrypoint as a dictionary conforming to the
        'airflow/provider.yaml.schema.json' schema.

        The providers are updated in the list of found providers.

        """
        for entry_point in pkg_resources.iter_entry_points('apache_airflow_provider'):
            package_name = entry_point.dist.project_name
            version = entry_point.dist.version
            try:
                provider_info = entry_point.load()()
            except pkg_resources.VersionConflict as e:
                log.warning(
                    "The provider package %s could not be registered because of version conflict : %s",
                    package_name,
                    e,
                )
                continue
            self._validator.validate(provider_info)
            provider_info_package_name = provider_info['package-name']
            if package_name != provider_info_package_name:
                raise Exception(
                    f"The package '{package_name}' from setuptools and "
                    f"{provider_info_package_name} do not match. Please make sure they are"
                    f"aligned"
                )
            if package_name not in self._provider_set:
                self._provider_set.add(package_name)
                self._provider_list.append((package_name, version, provider_info))
            else:
                log.warning(
                    "The providers for package '%s' could not be registered because providers for that "
                    "package name have already been registered",
                    package_name,
                )

    def __find_all_airflow_builtin_providers_from_local_sources(self) -> None:
        """
        Finds all built-in airflow providers if airflow is run from the local sources.
        It finds `provider.yaml` files for all such providers and registers the providers using those.

        This 'provider.yaml' scanning takes precedence over scanning packages installed because
        in case you have both sources and packages installed, the providers will be loaded from
        the "airflow" sources rather than from the packages.
        """
        import airflow.providers

        try:
            for path in airflow.providers.__path__:
                self.__add_provider_info_from_local_source_files_on_path(path)
        except Exception as e:  # noqa pylint: disable=broad-except
            log.warning("Error when loading 'provider.yaml' files from airflow sources: %s", e)

    def __add_provider_info_from_local_source_files_on_path(self, path) -> None:
        """
        Finds all the provider.yaml files in the directory specified.
        :param path: path where to look for provider.yaml files
        """
        root_path = path
        for folder, _, files in os.walk(path):
            for filename in fnmatch.filter(files, "provider.yaml"):
                self.__add_provider_info_from_local_source_file(filename, folder, root_path)

    def __add_provider_info_from_local_source_file(self, filename, folder, root_path) -> None:
        """
        Parses found provider.yaml file and adds found provider to the list of all providers.
        :param filename: name of the provider.yaml file
        :param folder: folder where the provider.yaml file is
        :param root_path: root path where we started to search ("airflow/providers" folder from the
                          local sources
        """
        try:
            with open(os.path.join(folder, filename)) as provider_yaml_file:
                provider_info = yaml.safe_load(provider_yaml_file.read())
            self._validator.validate(provider_info)
            package_name = "apache-airflow-providers" + folder[len(root_path) :].replace(os.sep, "-")
            version = provider_info['versions'][0]
            if package_name not in self._provider_set:
                self._provider_set.add(package_name)
                self._provider_list.append((package_name, version, provider_info))
            else:
                log.warning(
                    "The providers for package '%s' could not be registered because providers for that "
                    "package name have already been registered",
                    package_name,
                )
        except Exception as e:  # noqa pylint: disable=broad-except
            log.warning("Error when loading '%s/%s': %s", folder, filename, e)

    @property
    def providers(self):
        """Returns information about available providers."""
        return self._provider_directory


# After we move to python 3.9 + we can get rid of this and use @cache from the functools
cache = lru_cache(maxsize=None)


@cache
def get_providers_manager() -> ProvidersManager:  # noqa
    """Returns singleton instance of ProvidersManager"""
    return ProvidersManager()
