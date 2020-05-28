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

from airflow.api_connexion.schemas.pool_schema import PoolCollection, PoolCollectionSchema, PoolSchema
from airflow.models import Pool
from airflow.utils.session import provide_session
from tests.test_utils.db import clear_db_pools


class TestPoolSchema(unittest.TestCase):
    def setUp(self) -> None:
        clear_db_pools()

    def tearDown(self) -> None:
        clear_db_pools()

    @provide_session
    def test_serialzie(self, session):
        pool = Pool(pool="test-api-pool", slots=32)
        session.add(pool)
        session.commit()
        pool_model = session.query(Pool).first()
        deserialized_pool = PoolSchema().dump(pool_model)
        self.assertEqual(
            {'name': 'default_pool', 'occupied_slots': 0, 'queued_slots': 0, 'slots': 128},
            deserialized_pool
        )


class TestDAGCollectionSchema(unittest.TestCase):
    def test_serialize(self):
        pool_a = Pool(pool="test-api-pool-a", slots=32)
        pool_b = Pool(pool="test-api-pool-b", slots=64)
        schema = PoolCollectionSchema()
        instance = PoolCollection(pools=[pool_a, pool_b], total_entries=2)
        self.assertEqual(
            {
                "pools": [
                    {"name": "test-api-pool-a", "occupied_slots": 0, "queued_slots": 0, "slots": 32},
                    {"name": "test-api-pool-b", "occupied_slots": 0, "queued_slots": 0, "slots": 64},
                ],
                "total_entries": 2,
            },
            schema.dump(instance),
        )
