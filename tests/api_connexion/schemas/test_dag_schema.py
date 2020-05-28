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

import unittest

from airflow.api_connexion.schemas.dag_schema import DAGCollection, DAGCollectionSchema, DAGSchema
from airflow.models import DagModel, DagTag
from airflow.utils.session import provide_session
from tests.test_utils.db import clear_db_dags


class TestDagSchema(unittest.TestCase):
    def setUp(self) -> None:
        clear_db_dags()

    def tearDown(self) -> None:
        clear_db_dags()

    @provide_session
    def test_serialzie(self, session):
        dag_model = DagModel(
            dag_id="test_dag_id",
            root_dag_id="test_root_dag_id",
            is_paused=True,
            is_subdag=False,
            fileloc="/root/airflow/dags/my_dag.py",
            owners="airflow1,airflow2",
            description="The description",
            schedule_interval="5 4 * * *",
            tags=[DagTag(name="tag-1"), DagTag(name="tag-2")],
        )
        session.add(dag_model)
        session.commit()
        dag_model = session.query(DagModel).first()
        deserialized_dag = DAGSchema().dump(dag_model)
        self.assertEqual(
            deserialized_dag,
            {
                "dag_id": "test_dag_id",
                "description": "The description",
                "fileloc": "/root/airflow/dags/my_dag.py",
                "is_paused": True,
                "is_subdag": False,
                "owners": ["airflow1", "airflow2"],
                "root_dag_id": "test_root_dag_id",
                "schedule_interval": {"__type": "CronExpression", "value": "5 4 * * *"},
                "tags": [{"name": "tag-1"}, {"name": "tag-2"}],
            },
        )


class TestDAGCollectionSchema(unittest.TestCase):
    def test_serialize(self):
        dag_model_a = DagModel(dag_id="test_dag_id_a",)
        dag_model_b = DagModel(dag_id="test_dag_id_b",)
        schema = DAGCollectionSchema()
        instance = DAGCollection(dags=[dag_model_a, dag_model_b], total_entries=2)
        self.assertEqual(
            {
                "dags": [
                    {
                        "dag_id": "test_dag_id_a",
                        "description": None,
                        "fileloc": None,
                        "is_paused": None,
                        "is_subdag": None,
                        "owners": [],
                        "root_dag_id": None,
                        "schedule_interval": None,
                        "tags": [],
                    },
                    {
                        "dag_id": "test_dag_id_b",
                        "description": None,
                        "fileloc": None,
                        "is_paused": None,
                        "is_subdag": None,
                        "owners": [],
                        "root_dag_id": None,
                        "schedule_interval": None,
                        "tags": [],
                    },
                ],
                "total_entries": 2,
            },
            schema.dump(instance),
        )
