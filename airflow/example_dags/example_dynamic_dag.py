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

# [START faq_dynamic_dag]
from airflow import DAG


def create_dag(dag_id):
    """
    A function returning a DAG object.
    """

    return DAG(dag_id)


for i in range(10):
    dag_id = f'foo_{i}'
    globals()[dag_id] = DAG(dag_id)

    # or better, call a function that returns a DAG object!
    other_dag_id = f'bar_{i}'
    globals()[other_dag_id] = create_dag(other_dag_id)
# [END faq_dynamic_dag]
