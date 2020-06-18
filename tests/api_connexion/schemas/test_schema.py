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

import os
import unittest
from glob import glob
from importlib import import_module

from marshmallow import Schema

ROOT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)
)


class TestForceStrictSchema(unittest.TestCase):

    def test_enforce_strict_schema(self):
        """
        All schemas should use strict = True.

        We can delete it after migration to marshmallow 3.0:
        https://marshmallow.readthedocs.io/en/stable/upgrading.html#schemas-are-always-strict
        """
        schema_dir = f"{ROOT_DIR}/airflow/api_connexion/schemas/*"
        for filepath in glob(schema_dir):
            if "event_log" not in filepath:
                continue

            name, _, _ = os.path.basename(filepath).partition(".")
            mod = import_module(f"airflow.api_connexion.schemas.{name}")
            schemas = (o for o in mod.__dict__.values() if isinstance(o, Schema))
            invalid_schema = [schema for schema in schemas if not schema.strict]
            self.assertEqual({}, invalid_schema)
