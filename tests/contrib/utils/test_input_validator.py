# -*- coding: utf-8 -*-
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
#
import unittest

from airflow import AirflowException
from airflow.contrib.utils.input_validator import InputValidationMixin
from airflow.models import BaseOperator


class MockOperator(BaseOperator, InputValidationMixin):

    REQUIRED_ATTRIBUTES = ['req']
    OPTIONAL_NOT_EMPTY_ATTRIBUTES = ['opt']

    def __init__(self, req, opt, *args, **kwargs):
        self.req = req
        self.opt = opt
        self._validate_inputs()
        super(MockOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        pass


class InputValidatorTest(unittest.TestCase):

    def test_missing_required_attribute(self):
        with self.assertRaises(AirflowException) as cm:
            MockOperator(req=None, opt='', task_id="id")
        self.assertIn("The required parameter 'req' is missing", str(cm.exception))

    def test_empty_required_attribute(self):
        with self.assertRaises(AirflowException) as cm:
            MockOperator(req={}, opt='', task_id="id")
        self.assertIn("The required parameter 'req' is empty", str(cm.exception))

    def test_empty_optional_attribute(self):
        with self.assertRaises(AirflowException) as cm:
            MockOperator(req="req", opt='', task_id="id")
        self.assertIn("The optional parameter 'opt' is empty", str(cm.exception))

    def test_missing_optional_attribute(self):
        try:
            MockOperator(req="req", opt=None, task_id="id")
        except AirflowException:
            self.fail("Should not throw exception for missing optional attributes")
