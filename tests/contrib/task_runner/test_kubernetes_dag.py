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
import os
import unittest
from datetime import datetime

from airflow import models, AirflowException

from subprocess import check_call

try:
    check_call(["/usr/local/bin/kubectl", "get", "pods"])
except Exception as e:
    if os.environ.get('KUBERNETES_VERSION'):
        raise e
    else:
        raise unittest.SkipTest(
            "Kubernetes integration tests require a minikube cluster;"
            "Skipping tests {}".format(e)
        )

AIRFLOW_MAIN_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir, os.pardir)
)

DEFAULT_DATE = datetime(2015, 1, 1)

CONTRIB_OPERATORS_EXAMPLES_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "contrib", "example_dags"
)


class KubernetesDagTestCase(unittest.TestCase):
    def test_run_dag(self):
        dag_id = 'example_kubernetes'
        dag_folder = CONTRIB_OPERATORS_EXAMPLES_DAG_FOLDER
        dag_bag = models.DagBag(dag_folder=dag_folder, include_examples=False)
        dag = dag_bag.get_dag(dag_id)
        if dag is None:
            raise AirflowException(
                "The Dag {} could not be found. It's either an import problem or the dag {} was not "
                "symlinked to the DAGs folder. The content of the {} folder is {}".format(
                    dag_id, dag_id + ".py", dag_folder, os.listdir(dag_folder)
                )
            )
        dag.clear(reset_dag_runs=True)
        dag.run(ignore_first_depends_on_past=True, verbose=True)
