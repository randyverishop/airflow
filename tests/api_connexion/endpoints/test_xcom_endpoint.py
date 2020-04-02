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

import pytest


class TestDeleteXcomValues:
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self, http_client):
        response = http_client.delete(
            "/dags/TEST_DAG_ID}/taskInstances/TEST_TASK_ID/2005-04-02T21:37:42Z/xcomValues/XCOM_KEY"
        )
        assert response.status_code == 204


class TestGetXcomValue:
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self, http_client):
        response = http_client.get(
            "/dags/TEST_DAG_ID}/taskInstances/TEST_TASK_ID/2005-04-02T21:37:42Z/xcomValues/XCOM_KEY"
        )
        assert response.status_code == 200


class TestGetXcomValues:
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self, http_client):
        response = http_client.get(
            "/dags/TEST_DAG_ID}/taskInstances/TEST_TASK_ID/2005-04-02T21:37:42Z/xcomValues/"
        )
        assert response.status_code == 200


class TestPatchXcomValues:
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self, http_client):
        response = http_client.patch(
            "/dags/TEST_DAG_ID}/taskInstances/TEST_TASK_ID/2005-04-02T21:37:42Z/xcomValues"
        )
        assert response.status_code == 200


class TestPostXcomValue:
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self, http_client):
        response = http_client.post(
            "/dags/TEST_DAG_ID}/taskInstances/TEST_TASK_ID/2005-04-02T21:37:42Z/xcomValues/XCOM_KEY"
        )
        assert response.status_code == 200
