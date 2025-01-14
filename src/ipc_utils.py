# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import concurrent.futures
import json
import time

import awsiot.greengrasscoreipc
from awsiot.greengrasscoreipc.model import (QOS, GetConfigurationRequest,
                                            JsonMessage, PublishMessage,
                                            PublishToIoTCoreRequest,
                                            PublishToTopicRequest,
                                            SubscribeToIoTCoreRequest,
                                            SubscribeToTopicRequest,
                                            UnauthorizedError, SubscriptionResponseMessage)

from src import utils

logger = utils.logger

TIMEOUT = 10


class IPCUtils:

    def get_configuration(self):
        try:
            request = GetConfigurationRequest()
            operation = ipc_client.new_get_configuration()
            operation.activate(request).result(TIMEOUT)
            result = operation.get_response().result(TIMEOUT)
            return result.value
        except Exception:
            logger.exception(
                "Exception occured during fetching the configuration: "
            )

    def publish_message(self, topic, message, pubsub_to_iot_core):
        self.publish_to_pubsub_topic(topic, message)
        if pubsub_to_iot_core:
            self.publish_to_iot_topic(topic, message)

    def publish_to_pubsub_topic(self, topic, message):
        request = PublishToTopicRequest()
        request.topic = topic
        publish_message = PublishMessage()
        publish_message.json_message = JsonMessage()
        publish_message.json_message.message = message
        request.publish_message = publish_message
        operation = ipc_client.new_publish_to_topic()
        operation.activate(request)
        futureResponse = operation.get_response()
        try:
            futureResponse.result(TIMEOUT)
            logger.debug('Successfully published to pubsub topic: %s', topic)
        except concurrent.futures.TimeoutError:
            logger.exception(
                'Timeout occurred while publishing to pubsub topic: %s', topic)
        except UnauthorizedError:
            logger.exception(
                'Unauthorized error while publishing to pubsub topic: %s', topic)
        except Exception:
            logger.exception(
                'Exception while publishing to pubsub topic: %s', topic)

    def publish_to_iot_topic(self, topic, message):
        qos = QOS.AT_LEAST_ONCE
        request = PublishToIoTCoreRequest()
        request.topic_name = topic
        request.payload = json.dumps(message).encode('utf-8')
        request.qos = qos
        operation = ipc_client.new_publish_to_iot_core()
        operation.activate(request)
        futureResponse = operation.get_response()
        try:
            futureResponse.result(TIMEOUT)
            logger.debug('Successfully published to IoT core topic: %s', topic)
        except concurrent.futures.TimeoutError:
            logger.exception(
                'Timeout occurred while publishing to IoT core topic: %s', topic)
        except UnauthorizedError:
            logger.exception(
                'Unauthorized error while publishing to IoT core topic: %s', topic)
        except Exception:
            logger.exception(
                'Exception while publishing to IoT core topic: %s', topic)

    def subscribe_to_pubsub_topic(self, topic, handler):
        request = SubscribeToTopicRequest()
        request.topic = topic
        operation = ipc_client.new_subscribe_to_topic(handler)
        future = operation.activate(request)

        try:
            future.result(TIMEOUT)
            logger.debug('Successfully subscribed to pubsub topic: %s', topic)
        except concurrent.futures.TimeoutError:
            logger.exception(
                'Timeout occurred while subscribing to pubsub topic: %s', topic)
        except UnauthorizedError:
            logger.exception(
                'Unauthorized error while subscribing to pubsub topic: %s', topic)
        except Exception:
            logger.exception(
                'Exception while subscribing to pubsub topic: %s', topic)

        # Keep the thread alive, or the process will exit.
        try:
            while True:
                time.sleep(10)
        except InterruptedError:
            logger.info('Subscribe interrupted.')

    def subscribe_to_iot_topic(self, topic, handler):
        request = SubscribeToIoTCoreRequest()
        request.topic_name = topic
        request.qos = QOS.AT_MOST_ONCE
        operation = ipc_client.new_subscribe_to_iot_core(handler)
        future = operation.activate(request)

        try:
            future.result(TIMEOUT)
            logger.debug('Successfully subscribed to IoT core topic: %s', topic)
        except concurrent.futures.TimeoutError:
            logger.exception(
                'Timeout occurred while subscribing to IoT core topic: %s', topic)
        except UnauthorizedError:
            logger.exception(
                'Unauthorized error while subscribing to IoT core topic: %s', topic)
        except Exception:
            logger.exception(
                'Exception while subscribing to IoT core topic: %s', topic)

    def parse_subscription_response_message(event: SubscriptionResponseMessage) -> (str, dict):
        if event.binary_message is None:
            received_payload = event.json_message.message
            topic = event.json_message.context.topic
        else:
            received_payload = json.loads(event.binary_message.message)
            topic = event.binary_message.context.topic
        return topic, received_payload


# Get the ipc client
try:
    ipc_client = awsiot.greengrasscoreipc.connect()
    logger.info("Created IPC client...")
except Exception:
    logger.exception(
        "Exception occured during the creation of an IPC client: "
    )
    exit(1)
