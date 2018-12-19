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
from airflow import AirflowException


class InputValidationMixin(object):

    REQUIRED_ATTRIBUTES = []
    OPTIONAL_NOT_EMPTY_ATTRIBUTES = []

    def _validate_inputs(self):
        for attr_name in self.REQUIRED_ATTRIBUTES:
            attr = getattr(self, attr_name)
            if attr is None:
                raise AirflowException("The required parameter '{}' is missing".format(attr_name))
            if not attr:
                raise AirflowException("The required parameter '{}' is empty".format(attr_name))
        for attr_name in self.OPTIONAL_NOT_EMPTY_ATTRIBUTES:
            attr = getattr(self, attr_name)
            if attr is None:
                # attribute is optional so None is OK
                continue
            if not attr:
                raise AirflowException("The optional parameter '{}' is empty".format(attr_name))
