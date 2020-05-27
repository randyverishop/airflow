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
import datetime
import unittest

import pytest

from airflow.models import DagModel, DagTag
from airflow.utils.session import provide_session
from airflow.www import app
from tests.test_utils.db import clear_db_dags


class TestGetDag(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app, _ = app.create_app(testing=True)

    def setUp(self) -> None:
        self.client = self.app.test_client()
        super().setUp()
        clear_db_dags()

    def tearDown(self) -> None:
        clear_db_dags()

    @provide_session
    def test_should_response_200(self, session):
        dag_model = DagModel(
            dag_id="test_dag_id",
            root_dag_id="test_root_dag_id",
            is_paused=True,
            is_subdag=False,
            fileloc="/root/airflow/dags/my_dag.py",
            owners="airflow1,airflow2",
            description="The description",
            schedule_interval=datetime.timedelta(hours=3),
            tags=[DagTag(name="tag-1"), DagTag(name="tag-2")],
        )
        session.add(dag_model)
        session.commit()
        result = session.query(DagModel).all()
        assert len(result) == 1
        response = self.client.get("/api/v1/dags")
        assert response.status_code == 200
        assert response.json == {'dag_model': [], 'total_entries': 0}


class TestGetDags:
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self, http_client):
        response = http_client.get("/api/v1/dags/1")
        assert response.status_code == 200


class TestPatchDag:
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self, http_client):
        response = http_client.patch("/api/v1/dags/1")
        assert response.status_code == 200
