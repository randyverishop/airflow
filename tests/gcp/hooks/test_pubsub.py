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

from base64 import b64encode as b64e
import unittest

from google.api_core.exceptions import AlreadyExists
from google.cloud.exceptions import NotFound
from googleapiclient.errors import HttpError

from airflow.gcp.hooks.pubsub import PubSubException, PubSubHook
from tests.compat import mock

BASE_STRING = 'airflow.contrib.hooks.gcp_api_base_hook.{}'
PUBSUB_STRING = 'airflow.gcp.hooks.pubsub.{}'

EMPTY_CONTENT = b''
TEST_PROJECT = 'test-project'
TEST_TOPIC = 'test-topic'
TEST_SUBSCRIPTION = 'test-subscription'
TEST_UUID = 'abc123-xzy789'
TEST_MESSAGES = [
    {
        'data': b64e(b'Hello, World!'),
        'attributes': {'type': 'greeting'}
    },
    {'data': b64e(b'Knock, knock')},
    {'attributes': {'foo': ''}}]

EXPANDED_TOPIC = 'projects/{}/topics/{}'.format(TEST_PROJECT, TEST_TOPIC)
EXPANDED_SUBSCRIPTION = 'projects/{}/subscriptions/{}'.format(
    TEST_PROJECT, TEST_SUBSCRIPTION)


def mock_init(self, gcp_conn_id, delegate_to=None):  # pylint: disable=unused-argument
    pass


class TestPubSubHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__'),
                        new=mock_init):
            self.pubsub_hook = PubSubHook(gcp_conn_id='test')

    @mock.patch("airflow.gcp.hooks.pubsub.PubSubHook._get_credentials")
    @mock.patch("airflow.gcp.hooks.pubsub.PublisherClient")
    def test_publisher_client_creation(self, mock_client, mock_get_creds):
        result = self.pubsub_hook.get_conn()
        mock_client.assert_called_once_with(credentials=mock_get_creds.return_value)
        self.assertEqual(mock_client.return_value, result)
        self.assertEqual(self.pubsub_hook._client, result)

    @mock.patch("airflow.gcp.hooks.pubsub.PubSubHook._get_credentials")
    @mock.patch("airflow.gcp.hooks.pubsub.SubscriberClient")
    def test_subscriber_client_creation(self, mock_client, mock_get_creds):
        result = self.pubsub_hook.subscriber_client
        mock_client.assert_called_once_with(credentials=mock_get_creds.return_value)
        self.assertEqual(mock_client.return_value, result)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.get_conn'))
    def test_create_nonexistent_topic(self, mock_service):
        create_method = mock_service.return_value.create_topic
        self.pubsub_hook.create_topic(TEST_PROJECT, TEST_TOPIC)
        create_method.assert_called_once_with(
            name=EXPANDED_TOPIC,
            labels=None,
            message_storage_policy=None,
            kms_key_name=None,
            retry=None,
            timeout=None,
            metadata=None
        )

    @mock.patch(PUBSUB_STRING.format('PubSubHook.get_conn'))
    def test_delete_topic(self, mock_service):
        delete_method = mock_service.return_value.delete_topic
        self.pubsub_hook.delete_topic(TEST_PROJECT, TEST_TOPIC)
        delete_method.assert_called_once_with(
            topic=EXPANDED_TOPIC,
            retry=None,
            timeout=None,
            metadata=None
        )

    @mock.patch(PUBSUB_STRING.format('PubSubHook.get_conn'))
    def test_delete_nonexisting_topic_failifnotexists(self, mock_service):
        mock_service.return_value.delete_topic.side_effect = NotFound(
            'Topic does not exists: %s' % EXPANDED_TOPIC
        )
        with self.assertRaises(PubSubException) as e:
            self.pubsub_hook.delete_topic(TEST_PROJECT, TEST_TOPIC, True)

        self.assertEqual(str(e.exception), 'Topic does not exist: %s' % EXPANDED_TOPIC)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.get_conn'))
    def test_create_preexisting_topic_failifexists(self, mock_service):
        mock_service.return_value.create_topic.side_effect = AlreadyExists(
            'Topic already exists: %s' % TEST_TOPIC
        )
        with self.assertRaises(PubSubException) as e:
            self.pubsub_hook.create_topic(TEST_PROJECT, TEST_TOPIC, True)
        self.assertEqual(str(e.exception), 'Topic already exists: %s' % TEST_TOPIC)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.get_conn'))
    def test_create_preexisting_topic_nofailifexists(self, mock_service):
        mock_service.return_value.create_topic.side_effect = AlreadyExists(
            'Topic already exists: %s' % EXPANDED_TOPIC
        )
        self.pubsub_hook.create_topic(TEST_PROJECT, TEST_TOPIC)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    def test_create_nonexistent_subscription(self, mock_service):
        create_method = mock_service.create_subscription

        response = self.pubsub_hook.create_subscription(TEST_PROJECT, TEST_TOPIC, TEST_SUBSCRIPTION)
        create_method.assert_called_once_with(
            name=EXPANDED_SUBSCRIPTION,
            topic=EXPANDED_TOPIC,
            push_config=None,
            ack_deadline_seconds=10,
            retain_acked_messages=None,
            message_retention_duration=None,
            labels=None,
            retry=None,
            timeout=None,
            metadata=None,
        )
        self.assertEqual(TEST_SUBSCRIPTION, response)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    def test_create_subscription_different_project_topic(self, mock_service):
        create_method = mock_service.create_subscription
        response = self.pubsub_hook.create_subscription(
            TEST_PROJECT, TEST_TOPIC, TEST_SUBSCRIPTION, 'a-different-project'
        )
        expected_subscription = 'projects/{}/subscriptions/{}'.format(
            'a-different-project', TEST_SUBSCRIPTION
        )
        create_method.assert_called_once_with(
            name=expected_subscription,
            topic=EXPANDED_TOPIC,
            push_config=None,
            ack_deadline_seconds=10,
            retain_acked_messages=None,
            message_retention_duration=None,
            labels=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

        self.assertEqual(TEST_SUBSCRIPTION, response)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    def test_delete_subscription(self, mock_service):
        self.pubsub_hook.delete_subscription(TEST_PROJECT, TEST_SUBSCRIPTION)
        delete_method = mock_service.delete_subscription
        delete_method.assert_called_once_with(
            subscription=EXPANDED_SUBSCRIPTION,
            retry=None,
            timeout=None,
            metadata=None
        )

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    def test_delete_nonexisting_subscription_failifnotexists(self, mock_service):
        mock_service.delete_subscription.side_effect = NotFound(
            'Subscription does not exists: %s' % EXPANDED_SUBSCRIPTION
        )
        with self.assertRaises(PubSubException) as e:
            self.pubsub_hook.delete_subscription(TEST_PROJECT, TEST_SUBSCRIPTION, fail_if_not_exists=True)
        self.assertEqual(str(e.exception), 'Subscription does not exist: %s' % EXPANDED_SUBSCRIPTION)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    @mock.patch(PUBSUB_STRING.format('uuid4'), new_callable=mock.Mock(return_value=lambda: TEST_UUID))
    def test_create_subscription_without_subscription_name(self, mock_uuid, mock_service):  # noqa  # pylint: disable=unused-argument,line-too-long
        create_method = mock_service.create_subscription
        expected_name = EXPANDED_SUBSCRIPTION.replace(TEST_SUBSCRIPTION, 'sub-%s' % TEST_UUID)

        response = self.pubsub_hook.create_subscription(TEST_PROJECT, TEST_TOPIC)
        create_method.assert_called_once_with(
            name=expected_name,
            topic=EXPANDED_TOPIC,
            push_config=None,
            ack_deadline_seconds=10,
            retain_acked_messages=None,
            message_retention_duration=None,
            labels=None,
            retry=None,
            timeout=None,
            metadata=None,
        )
        self.assertEqual('sub-%s' % TEST_UUID, response)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    def test_create_subscription_with_ack_deadline(self, mock_service):
        create_method = mock_service.create_subscription

        response = self.pubsub_hook.create_subscription(
            TEST_PROJECT, TEST_TOPIC, TEST_SUBSCRIPTION, ack_deadline_secs=30
        )
        create_method.assert_called_once_with(
            name=EXPANDED_SUBSCRIPTION,
            topic=EXPANDED_TOPIC,
            push_config=None,
            ack_deadline_seconds=30,
            retain_acked_messages=None,
            message_retention_duration=None,
            labels=None,
            retry=None,
            timeout=None,
            metadata=None,
        )
        self.assertEqual(TEST_SUBSCRIPTION, response)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    def test_create_subscription_failifexists(self, mock_service):
        mock_service.create_subscription.side_effect = AlreadyExists(
            'Subscription already exists: %s' % EXPANDED_SUBSCRIPTION
        )
        with self.assertRaises(PubSubException) as e:
            self.pubsub_hook.create_subscription(
                TEST_PROJECT, TEST_TOPIC, TEST_SUBSCRIPTION, fail_if_exists=True
            )
        self.assertEqual(str(e.exception), 'Subscription already exists: %s' % EXPANDED_SUBSCRIPTION)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    def test_create_subscription_nofailifexists(self, mock_service):
        mock_service.create_subscription.side_effect = AlreadyExists(
            'Subscription already exists: %s' % EXPANDED_SUBSCRIPTION
        )
        response = self.pubsub_hook.create_subscription(TEST_PROJECT, TEST_TOPIC, TEST_SUBSCRIPTION)
        self.assertEqual(TEST_SUBSCRIPTION, response)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.get_conn'))
    def test_publish(self, mock_service):
        publish_method = mock_service.return_value.publish

        self.pubsub_hook.publish(TEST_PROJECT, TEST_TOPIC, TEST_MESSAGES)
        calls = [
            mock.call(topic=EXPANDED_TOPIC, data=message.get("data", b''), **message.get('attributes', {}))
            for message in TEST_MESSAGES
        ]
        publish_method.has_calls(calls)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    def test_pull(self, mock_service):
        pull_method = mock_service.pull
        pulled_messages = []
        for i, msg in enumerate(TEST_MESSAGES):
            pulled_messages.append({'ackId': i, 'message': msg})
        pull_method.return_value.received_messages = pulled_messages

        response = self.pubsub_hook.pull(TEST_PROJECT, TEST_SUBSCRIPTION, 10)
        pull_method.assert_called_once_with(
            subscription=EXPANDED_SUBSCRIPTION,
            max_messages=10,
            return_immediately=False,
            retry=None,
            timeout=None,
            metadata=None,
        )
        self.assertEqual(pulled_messages, response)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    def test_pull_no_messages(self, mock_service):
        pull_method = mock_service.pull
        pull_method.return_value.received_messages = []

        response = self.pubsub_hook.pull(TEST_PROJECT, TEST_SUBSCRIPTION, 10)
        pull_method.assert_called_once_with(
            subscription=EXPANDED_SUBSCRIPTION,
            max_messages=10,
            return_immediately=False,
            retry=None,
            timeout=None,
            metadata=None,
        )
        self.assertListEqual([], response)

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    def test_pull_fails_on_exception(self, mock_service):
        pull_method = mock_service.pull
        pull_method.side_effect = HttpError(
            resp={'status': '404'}, content=EMPTY_CONTENT)

        with self.assertRaises(Exception):
            self.pubsub_hook.pull(TEST_PROJECT, TEST_SUBSCRIPTION, 10)
            pull_method.assert_called_once_with(
                subscription=EXPANDED_SUBSCRIPTION,
                max_messages=10,
                return_immediately=False,
                retry=None,
                timeout=None,
                metadata=None,
            )

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    def test_acknowledge(self, mock_service):
        ack_method = mock_service.acknowledge

        self.pubsub_hook.acknowledge(TEST_PROJECT, TEST_SUBSCRIPTION, ['1', '2', '3'])
        ack_method.assert_called_once_with(
            subscription=EXPANDED_SUBSCRIPTION,
            ack_ids=['1', '2', '3'],
            retry=None,
            timeout=None,
            metadata=None
        )

    @mock.patch(PUBSUB_STRING.format('PubSubHook.subscriber_client'))
    def test_acknowledge_fails_on_exception(self, mock_service):
        ack_method = mock_service.acknowledge
        ack_method.side_effect = HttpError(
            resp={'status': '404'}, content=EMPTY_CONTENT)

        with self.assertRaises(Exception):
            self.pubsub_hook.acknowledge(
                TEST_PROJECT, TEST_SUBSCRIPTION, ['1', '2', '3'])
            ack_method.assert_called_once_with(
                subscription=EXPANDED_SUBSCRIPTION,
                ack_ids=['1', '2', '3'],
                retry=None,
                timeout=None,
                metadata=None
            )
