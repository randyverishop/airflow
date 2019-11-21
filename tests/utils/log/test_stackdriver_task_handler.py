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

import logging
import unittest
from datetime import datetime
from unittest import mock

from google.cloud.logging.resource import Resource

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.log.stackdriver_task_handler import StackdriverLoggingHandler
from airflow.utils.state import State


class TestStackdriverLoggingHandlerStandalone(unittest.TestCase):

    @mock.patch('airflow.utils.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_pass_message_to_client(self, mock_client):
        transport_type = mock.MagicMock()
        stackdriver_task_handler = StackdriverLoggingHandler(
            gcp_conn_id=None,
            transport=transport_type,
            labels={"key": 'value'}
        )
        logger = logging.getLogger("logger")
        logger.addHandler(stackdriver_task_handler)

        logger.info("test-message")
        stackdriver_task_handler.flush()

        transport_type.assert_called_once_with(mock_client.return_value, 'airflow')
        transport_type.return_value.send.assert_called_once_with(
            mock.ANY, 'test-message', labels={"key": 'value'}, resource=Resource(type='global', labels={})
        )
        mock_client.assert_called_once()


class TestStackdriverLoggingHandlerTask(unittest.TestCase):
    def setUp(self) -> None:
        self.transport_mock = mock.MagicMock()
        self.stackdriver_task_handler = StackdriverLoggingHandler(
            transport=self.transport_mock
        )
        self.logger = logging.getLogger("logger")

        date = datetime(2016, 1, 1)
        self.dag = DAG('dag_for_testing_file_task_handler', start_date=date)
        task = DummyOperator(task_id='task_for_testing_file_log_handler', dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=date)
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        self.addCleanup(self.dag.clear)

    @mock.patch('airflow.utils.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_set_labels(self, mock_client):
        self.stackdriver_task_handler.set_context(self.ti)
        self.logger.addHandler(self.stackdriver_task_handler)

        self.logger.info("test-message")
        self.stackdriver_task_handler.flush()

        labels = {
            'task_id': 'task_for_testing_file_log_handler',
            'dag_id': 'dag_for_testing_file_task_handler',
            'execution_date': '2016-01-01T00:00:00+00:00',
            'try_number': '1'
        }
        resource = Resource(type='global', labels={})
        self.transport_mock.return_value.send.assert_called_once_with(
            mock.ANY, 'test-message', labels=labels, resource=resource
        )

    @mock.patch('airflow.utils.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_append_labels(self, mock_client):
        self.stackdriver_task_handler = StackdriverLoggingHandler(
            transport=self.transport_mock,
            labels={"product.googleapis.com/task_id": "test-value"}
        )
        self.stackdriver_task_handler.set_context(self.ti)
        self.logger.addHandler(self.stackdriver_task_handler)

        self.logger.info("test-message")
        self.stackdriver_task_handler.flush()

        labels = {
            'task_id': 'task_for_testing_file_log_handler',
            'dag_id': 'dag_for_testing_file_task_handler',
            'execution_date': '2016-01-01T00:00:00+00:00',
            'try_number': '1',
            'product.googleapis.com/task_id': 'test-value'
        }
        resource = Resource(type='global', labels={})
        self.transport_mock.return_value.send.assert_called_once_with(
            mock.ANY, 'test-message', labels=labels, resource=resource
        )

    @mock.patch('airflow.utils.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_read_logs_for_all_try(self, mock_client):
        entry = mock.MagicMock(payload={"message": "TEXT"})
        page = [entry, entry]
        mock_client.return_value.list_entries.return_value.pages = (n for n in [page])
        mock_client.return_value.list_entries.return_value.next_page_token = None

        logs, metadata = self.stackdriver_task_handler.read(self.ti)
        mock_client.return_value.list_entries.assert_called_once_with(
            filter_='resource.type="global"\n'
                    'labels.task_id="task_for_testing_file_log_handler"\n'
                    'labels.dag_id="dag_for_testing_file_task_handler"\n'
                    'labels.execution_date="2016-01-01T00:00:00+00:00"',
            page_token=None
        )
        self.assertEqual(['TEXT\nTEXT'], logs)
        self.assertEqual([{'end_of_log': True, 'next_page_token': None}], metadata)

    @mock.patch('airflow.utils.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_read_logs_for_single_try(self, mock_client):
        entry = mock.MagicMock(payload={"message": "TEXT"})
        page = [entry, entry]
        mock_client.return_value.list_entries.return_value.pages = (n for n in [page])
        mock_client.return_value.list_entries.return_value.next_page_token = None

        logs, metadata = self.stackdriver_task_handler.read(self.ti, 3)
        mock_client.return_value.list_entries.assert_called_once_with(
            filter_='resource.type="global"\n'
                    'labels.task_id="task_for_testing_file_log_handler"\n'
                    'labels.dag_id="dag_for_testing_file_task_handler"\n'
                    'labels.execution_date="2016-01-01T00:00:00+00:00"\n'
                    'labels.try_number="3"',
            page_token=None
        )
        self.assertEqual(['TEXT\nTEXT'], logs)
        self.assertEqual([{'end_of_log': True, 'next_page_token': None}], metadata)

    @mock.patch('airflow.utils.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_read_logs_with_pagination(self, mock_client):
        entry = mock.MagicMock(payload={"message": "TEXT"})
        page = [entry, entry]
        mock_client.return_value.list_entries.return_value.pages = (n for n in [page])
        mock_client.return_value.list_entries.return_value.next_page_token = "TOKEN1"

        _, metadata1 = self.stackdriver_task_handler.read(self.ti, 3)
        mock_client.return_value.list_entries.assert_called_once_with(
            filter_=mock.ANY, page_token=None
        )
        self.assertEqual([{'end_of_log': False, 'next_page_token': 'TOKEN1'}], metadata1)

        mock_client.return_value.list_entries.return_value.pages = (n for n in [page])
        mock_client.return_value.list_entries.return_value.next_page_token = None
        _, metadata2 = self.stackdriver_task_handler.read(self.ti, 3, metadata1[0])
        mock_client.return_value.list_entries.assert_called_with(
            filter_=mock.ANY, page_token="TOKEN1"
        )
        self.assertEqual([{'end_of_log': True, 'next_page_token': None}], metadata2)

    @mock.patch('airflow.utils.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_read_logs_with_custom_resources(self, mock_client):
        resource = Resource(
            type="cloud_composer_environment",
            labels={
                "environment.name": 'test-instancce',
                "location": 'europpe-west-3',
                "project_id": "asf-project",
            },
        )
        self.stackdriver_task_handler = StackdriverLoggingHandler(
            transport=self.transport_mock,
            resource=resource
        )

        entry = mock.MagicMock(payload={"message": "TEXT"})
        page = [entry, entry]
        mock_client.return_value.list_entries.return_value.pages = (n for n in [page])
        mock_client.return_value.list_entries.return_value.next_page_token = None

        logs, metadata = self.stackdriver_task_handler.read(self.ti)
        mock_client.return_value.list_entries.assert_called_once_with(
            filter_='resource.type="cloud_composer_environment"\n'
                    'resource.labels."environment.name"="test-instancce"\n'
                    'resource.labels.location="europpe-west-3"\n'
                    'resource.labels.project_id="asf-project"\n'
                    'labels.task_id="task_for_testing_file_log_handler"\n'
                    'labels.dag_id="dag_for_testing_file_task_handler"\n'
                    'labels.execution_date="2016-01-01T00:00:00+00:00"',
            page_token=None
        )
        self.assertEqual(['TEXT\nTEXT'], logs)
        self.assertEqual([{'end_of_log': True, 'next_page_token': None}], metadata)


class TestStackdriverTaskHandlerAuthorization(unittest.TestCase):

    @mock.patch('airflow.utils.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_fallback_to_adc(self, mock_client):
        stackdriver_task_handler = StackdriverLoggingHandler(
            gcp_conn_id=None
        )

        client = stackdriver_task_handler.client

        mock_client.assert_called_once_with(credentials=None)
        self.assertEqual(mock_client.return_value, client)

    @mock.patch("airflow.gcp.hooks.base.GoogleCloudBaseHook")
    @mock.patch('airflow.utils.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_support_gcp_conn_id(self, mock_client, mock_hook):
        stackdriver_task_handler = StackdriverLoggingHandler(
            gcp_conn_id="test-gcp-conn"
        )

        client = stackdriver_task_handler.client

        mock_client.assert_called_once_with(credentials=mock_hook.return_value._get_credentials.return_value)
        self.assertEqual(mock_client.return_value, client)
