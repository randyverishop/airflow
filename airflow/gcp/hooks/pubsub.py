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
"""
This module contains a Google Pub/Sub Hook.
"""
from typing import List, Dict, Optional, Sequence, Tuple, Union
from uuid import uuid4

from cached_property import cached_property
from google.api_core.retry import Retry
from google.api_core.exceptions import AlreadyExists, GoogleAPICallError
from google.cloud.exceptions import NotFound
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient, types
from googleapiclient.errors import HttpError

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


def _format_subscription(project, subscription):
    return 'projects/{}/subscriptions/{}'.format(project, subscription)


def _format_topic(project, topic):
    return 'projects/{}/topics/{}'.format(project, topic)


class PubSubException(Exception):
    """
    Alias for Exception.
    """


class PubSubHook(GoogleCloudBaseHook):
    """
    Hook for accessing Google Pub/Sub.

    The GCP project against which actions are applied is determined by
    the project embedded in the Connection referenced by gcp_conn_id.
    """

    def __init__(self, gcp_conn_id: str = 'google_cloud_default', delegate_to: str = None) -> None:
        super().__init__(gcp_conn_id, delegate_to=delegate_to)
        self._client = None

    def get_conn(self) -> PublisherClient:
        """
        Retrieves connection to Google Cloud Pub/Sub.

        :return: Google Cloud Pub/Sub client object.
        :rtype: google.cloud.pubsub_v1.PublisherClient
        """
        if not self._client:
            self._client = PublisherClient(
                credentials=self._get_credentials(),
            )
        return self._client

    @cached_property
    def subscriber_client(self) -> SubscriberClient:
        """
        Creates SubscriberClient.

        :return: Google Cloud Pub/Sub client object.
        :rtype: google.cloud.pubsub_v1.SubscriberClient
        """
        return SubscriberClient(
            credentials=self._get_credentials(),
        )

    def publish(
        self,
        project: str,
        topic: str,
        messages: List[Dict],
    ) -> None:
        """
        Publishes messages to a Pub/Sub topic.

        :param project: the GCP project ID in which to publish
        :type project: str
        :param topic: the Pub/Sub topic to which to publish; do not
            include the ``projects/{project}/topics/`` prefix.
        :type topic: str
        :param messages: messages to publish; if the data field in a
            message is set, it should already be base64 encoded.
        :type messages: list of PubSub messages; see
            http://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
        """
        publisher = self.get_conn()
        topic_path = PublisherClient.topic_path(project, topic)  # pylint: disable=no-member

        # TODO validation of messages
        try:
            for message in messages:
                publisher.publish(
                    topic=topic_path,
                    data=message.get("data", b''),
                    **message.get('attributes', {})
                )
        except GoogleAPICallError as e:
            raise PubSubException(
                'Error publishing to topic {}'.format(topic_path), e)

    def create_topic(
        self,
        project: str,
        topic: str,
        fail_if_exists: bool = False,
        labels: Optional[Dict[str, str]] = None,
        message_storage_policy: Union[Dict, types.MessageStoragePolicy] = None,  # pylint: disable=no-member
        kms_key_name: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Creates a Pub/Sub topic, if it does not already exist.

        :param project: the GCP project ID in which to create the topic
        :type project: str
        :param topic: the Pub/Sub topic name to create; do not
            include the ``projects/{project}/topics/`` prefix.
        :type topic: str
        :param fail_if_exists: if set, raise an exception if the topic
            already exists
        :type fail_if_exists: bool
        :param labels: Client-assigned labels; see
            https://cloud.google.com/pubsub/docs/labels
        :type labels: Dict[str, str]
        :param message_storage_policy: Policy constraining the set
            of Google Cloud Platform regions where messages published to
            the topic may be stored. If not present, then no constraints
            are in effect.
        :type message_storage_policy:
            Union[Dict, google.cloud.pubsub_v1.types.MessageStoragePolicy]
        :param kms_key_name: The resource name of the Cloud KMS CryptoKey
            to be used to protect access to messages published on this topic.
            The expected format is
            ``projects/*/locations/*/keyRings/*/cryptoKeys/*``.
        :type kms_key_name: str
        :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]]
        """
        assert project is not None
        publisher = self.get_conn()
        topic_path = PublisherClient.topic_path(project, topic)  # pylint: disable=no-member
        try:
            # pylint: disable=no-member
            publisher.create_topic(
                name=topic_path,
                labels=labels,
                message_storage_policy=message_storage_policy,
                kms_key_name=kms_key_name,
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            )
        except AlreadyExists:
            message = 'Topic already exists: {}'.format(topic)
            self.log.warning(message)
            if fail_if_exists:
                raise PubSubException(message)
        except GoogleAPICallError as e:
            raise PubSubException('Error creating topic {}'.format(topic), e)

    def delete_topic(
        self,
        project: str,
        topic: str,
        fail_if_not_exists: bool = False,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Deletes a Pub/Sub topic if it exists.

        :param project: the GCP project ID in which to delete the topic
        :type project: str
        :param topic: the Pub/Sub topic name to delete; do not
            include the ``projects/{project}/topics/`` prefix.
        :type topic: str
        :param fail_if_not_exists: if set, raise an exception if the topic
            does not exist
        :type fail_if_not_exists: bool
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]]
        """
        assert project is not None
        publisher = self.get_conn()
        topic_path = PublisherClient.topic_path(project, topic)  # pylint: disable=no-member
        try:
            # pylint: disable=no-member
            publisher.delete_topic(
                topic=topic_path,
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            )
        except NotFound:
            message = 'Topic does not exist: {}'.format(topic_path)
            self.log.warning(message)
            if fail_if_not_exists:
                raise PubSubException(message)
        except GoogleAPICallError as e:
            raise PubSubException('Error deleting topic {}'.format(topic), e)

    # pylint: disable=too-many-arguments
    def create_subscription(
        self,
        topic_project: str,
        topic: str,
        subscription: Optional[str] = None,
        subscription_project: Optional[str] = None,
        ack_deadline_secs: int = 10,
        fail_if_exists: bool = False,
        push_config: Optional[Union[Dict, types.PushConfig]] = None,  # pylint: disable=no-member
        retain_acked_messages: Optional[bool] = None,
        message_retention_duration: Optional[Union[Dict, types.Duration]] = None,  # pylint: disable=no-member
        labels: Optional[Dict[str, str]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,

    ) -> str:
        """
        Creates a Pub/Sub subscription, if it does not already exist.

        :param topic_project: the GCP project ID of the topic that the
            subscription will be bound to.
        :type topic_project: str
        :param topic: the Pub/Sub topic name that the subscription will be bound
            to create; do not include the ``projects/{project}/subscriptions/``
            prefix.
        :type topic: str
        :param subscription: the Pub/Sub subscription name. If empty, a random
            name will be generated using the uuid module
        :type subscription: str
        :param subscription_project: the GCP project ID where the subscription
            will be created. If unspecified, ``topic_project`` will be used.
        :type subscription_project: str
        :param ack_deadline_secs: Number of seconds that a subscriber has to
            acknowledge each message pulled from the subscription
        :type ack_deadline_secs: int
        :param fail_if_exists: if set, raise an exception if the topic
            already exists
        :type fail_if_exists: bool
        :param push_config: If push delivery is used with this subscription,
            this field is used to configure it. An empty ``pushConfig`` signifies
            that the subscriber will pull and ack messages using API methods.
        :type push_config: Union[Dict, google.cloud.pubsub_v1.types.PushConfig]
        :param retain_acked_messages: Indicates whether to retain acknowledged
            messages. If true, then messages are not expunged from the subscription's
             backlog, even if they are acknowledged, until they fall out of the
             ``message_retention_duration`` window. This must be true if you would
            like to Seek to a timestamp.
        :type retain_acked_messages: bool
        :param message_retention_duration: How long to retain unacknowledged messages
            in the subscription's backlog, from the moment a message is published. If
            ``retain_acked_messages`` is true, then this also configures the
            retention of acknowledged messages, and thus configures how far back in
            time a ``Seek`` can be done. Defaults to 7 days. Cannot be more than 7
            days or less than 10 minutes.
        :type message_retention_duration: Union[Dict, google.cloud.pubsub_v1.types.Duration]
        :param labels: Client-assigned labels; see
            https://cloud.google.com/pubsub/docs/labels
        :type labels: Dict[str, str]
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]]
        :return: subscription name which will be the system-generated value if
            the ``subscription`` parameter is not supplied
        :rtype: str
        """
        subscriber = self.subscriber_client

        if not subscription:
            subscription = 'sub-{}'.format(uuid4())
        if not subscription_project:
            subscription_project = topic_project

        # pylint: disable=no-member
        subscription_path = SubscriberClient.subscription_path(subscription_project, subscription)
        topic_path = SubscriberClient.topic_path(topic_project, topic)

        try:
            subscriber.create_subscription(
                name=subscription_path,
                topic=topic_path,
                push_config=push_config,
                ack_deadline_seconds=ack_deadline_secs,
                retain_acked_messages=retain_acked_messages,
                message_retention_duration=message_retention_duration,
                labels=labels,
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            )
        except AlreadyExists:
            message = 'Subscription already exists: {}'.format(subscription_path)
            self.log.warning(message)
            if fail_if_exists:
                raise PubSubException(message)
        except GoogleAPICallError as e:
            raise PubSubException('Error creating subscription {}'.format(subscription_path), e)

        return subscription

    def delete_subscription(
        self,
        project: str,
        subscription: str,
        fail_if_not_exists: bool = False,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Deletes a Pub/Sub subscription, if it exists.

        :param project: the GCP project ID where the subscription exists
        :type project: str
        :param subscription: the Pub/Sub subscription name to delete; do not
            include the ``projects/{project}/subscriptions/`` prefix.
        :type subscription: str
        :param fail_if_not_exists: if set, raise an exception if the topic
            does not exist
        :type fail_if_not_exists: bool
        :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]]
        """
        subscriber = self.subscriber_client
        subscription_path = SubscriberClient.subscription_path(project, subscription)  # noqa E501 # pylint: disable=no-member,line-too-long

        try:
            # pylint: disable=no-member
            subscriber.delete_subscription(
                subscription=subscription_path,
                retry=retry,
                timeout=timeout,
                metadata=metadata
            )

        except NotFound:
            message = 'Subscription does not exist: {}'.format(subscription_path)
            self.log.warning(message)
            if fail_if_not_exists:
                raise PubSubException(message)
        except GoogleAPICallError as e:
            raise PubSubException('Error deleting subscription {}'.format(subscription_path), e)

    def pull(
        self,
        project: str,
        subscription: str,
        max_messages: int,
        return_immediately: bool = False,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[Dict]:
        """
        Pulls up to ``max_messages`` messages from Pub/Sub subscription.

        :param project: the GCP project ID where the subscription exists
        :type project: str
        :param subscription: the Pub/Sub subscription name to pull from; do not
            include the 'projects/{project}/topics/' prefix.
        :type subscription: str
        :param max_messages: The maximum number of messages to return from
            the Pub/Sub API.
        :type max_messages: int
        :param return_immediately: If set, the Pub/Sub API will immediately
            return if no messages are available. Otherwise, the request will
            block for an undisclosed, but bounded period of time
        :type return_immediately: bool
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]]
        :return: A list of Pub/Sub ReceivedMessage objects each containing
            an ``ackId`` property and a ``message`` property, which includes
            the base64-encoded message content. See
            https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/pull#ReceivedMessage
        """
        subscriber = self.subscriber_client
        subscription_path = SubscriberClient.subscription_path(project, subscription)  # noqa E501 # pylint: disable=no-member,line-too-long

        try:
            # pylint: disable=no-member
            response = subscriber.pull(
                subscription=subscription_path,
                max_messages=max_messages,
                return_immediately=return_immediately,
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            )
            return getattr(response, 'received_messages', [])
        except HttpError as e:
            raise PubSubException(
                'Error pulling messages from subscription {}'.format(
                    subscription_path), e)
        except GoogleAPICallError as e:
            raise PubSubException(
                'Error pulling messages from subscription {}'.format(subscription_path), e)

    def acknowledge(
        self,
        project: str,
        subscription: str,
        ack_ids: List[str],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Pulls up to ``max_messages`` messages from Pub/Sub subscription.

        :param project: the GCP project name or ID in which to create
            the topic
        :type project: str
        :param subscription: the Pub/Sub subscription name to delete; do not
            include the 'projects/{project}/topics/' prefix.
        :type subscription: str
        :param ack_ids: List of ReceivedMessage ackIds from a previous pull
            response
        :type ack_ids: list
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]]
        """
        subscriber = self.subscriber_client
        subscription_path = SubscriberClient.subscription_path(project, subscription)  # noqa E501 # pylint: disable=no-member,line-too-long

        try:
            # pylint: disable=no-member
            subscriber.acknowledge(
                subscription=subscription_path,
                ack_ids=ack_ids,
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            )
        except HttpError as e:
            raise PubSubException(
                'Error acknowledging {} messages pulled from subscription {}'
                .format(len(ack_ids), subscription_path), e)
        except GoogleAPICallError as e:
            raise PubSubException(
                'Error acknowledging {} messages pulled from subscription {}'.format(
                    len(ack_ids), subscription_path
                ), e
            )
