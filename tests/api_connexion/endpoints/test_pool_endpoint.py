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

from airflow.models import Pool
from airflow.utils.session import provide_session
from airflow.www import app
from tests.test_utils.db import clear_db_pools


class TestBasePoolEndpoints(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app, _ = app.create_app(testing=True)  # type:ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore
        super().setUp()
        clear_db_pools()

    def tearDown(self) -> None:
        clear_db_pools()


class TestDeletePool(TestBasePoolEndpoints):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.delete("/api/v1/pools/1")
        assert response.status_code == 204


class TestGetPool(TestBasePoolEndpoints):
    @provide_session
    def test_should_response_200(self, session):
        session.add(Pool(pool="test-api-pool", slots=32))
        session.commit()

        response = self.client.get("/api/v1/pools/test-api-pool")
        assert response.status_code == 200

        self.assertEqual(
            {'name': 'test-api-pool', 'occupied_slots': 0, 'queued_slots': 0, 'slots': 32}, response.json
        )


class TestGetPools(TestBasePoolEndpoints):
    @provide_session
    def test_should_response_200(self, session):
        response = self.client.get("/api/v1/pools")
        session.add(Pool(pool="test-api-pool"))
        session.commit()
        assert response.status_code == 200

        self.assertEqual(
            {
                "pools": [{"name": "default_pool", "occupied_slots": 0, "queued_slots": 0, "slots": 128}],
                "total_entries": 1,
            },
            response.json,
        )


class TestPatchPool(TestBasePoolEndpoints):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.patch("/api/v1/pools/1")
        assert response.status_code == 200


class TestPostPool(TestBasePoolEndpoints):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.post("/api/v1/pool")
        assert response.status_code == 200
