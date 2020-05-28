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

import pytest

from airflow.models import DagModel
from airflow.utils.session import provide_session
from airflow.www import app
from tests.test_utils.db import clear_db_dags


class TestBaseDagEndpoints(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app, _ = app.create_app(testing=True)  # type:ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore
        super().setUp()
        clear_db_dags()

    def tearDown(self) -> None:
        clear_db_dags()


class TestGetDags(TestBaseDagEndpoints):
    @provide_session
    def test_should_response_200(self, session):
        dag_model = DagModel(dag_id="test_dag_id", schedule_interval="5 4 * * *")
        session.add(dag_model)
        session.commit()
        result = session.query(DagModel).all()
        assert len(result) == 1
        response = self.client.get("/api/v1/dags")
        assert response.status_code == 200
        self.assertEqual(
            {
                "dags": [
                    {
                        "dag_id": "test_dag_id",
                        "description": None,
                        "fileloc": None,
                        "is_paused": True,
                        "is_subdag": False,
                        "owners": [],
                        "root_dag_id": None,
                        "schedule_interval": {"__type": "CronExpression", "value": "5 4 * * *"},
                        "tags": [],
                    }
                ],
                "total_entries": 0,
            },
            response.json,
        )


class TestGetDag(TestBaseDagEndpoints):
    @provide_session
    def test_should_response_200(self, session):
        dag_model = DagModel(dag_id="test_dag_id", schedule_interval="5 4 * * *")
        session.add(dag_model)
        session.commit()

        response = self.client.get("/api/v1/dags/test_dag_id")
        assert response.status_code == 200

        self.assertEqual(
            {
                "dag_id": "test_dag_id",
                "description": None,
                "fileloc": None,
                "is_paused": True,
                "is_subdag": False,
                "owners": [],
                "root_dag_id": None,
                "schedule_interval": {"__type": "CronExpression", "value": "5 4 * * *"},
                "tags": [],
            },
            response.json,
        )

    @unittest.skip(reason="Not implemented yet")
    def test_should_response_404(self):
        response = self.client.get("/api/v1/dags/non_exists")
        assert response.status_code == 404

        self.assertEqual(
            {
                "dag_id": "test_dag_id",
                "description": None,
                "fileloc": None,
                "is_paused": True,
                "is_subdag": False,
                "owners": [],
                "root_dag_id": None,
                "schedule_interval": {"__type": "CronExpression", "value": "5 4 * * *"},
                "tags": [],
            },
            response.json,
        )


class TestPatchDag:
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self, http_client):
        response = http_client.patch("/api/v1/dags/1")
        assert response.status_code == 200
