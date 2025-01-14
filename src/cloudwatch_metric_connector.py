# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import re
from threading import Thread

import awsiot.greengrasscoreipc.client as client
from awsiot.greengrasscoreipc.model import (IoTCoreMessage,
                                            SubscriptionResponseMessage)

from src import ipc_utils, utils
from src.ipc_utils import IPCUtils
from src.metric.manager import MetricsManager
from src.request import PutMetricRequest

ipc = ipc_utils.IPCUtils()
config = ipc.get_configuration()

logger = utils.logger

# Setup Configuration
if utils.PUBLISH_REGION_KEY in config and config[utils.PUBLISH_REGION_KEY] != "":
    PUBLISH_REGION = config[utils.PUBLISH_REGION_KEY]
else:
    PUBLISH_REGION = utils.DEFAULT_PUBLISH_REGION

if utils.PUBLISH_INTERVAL_SEC_KEY in config:
    try:
        publish_interval = int(config[utils.PUBLISH_INTERVAL_SEC_KEY])
    except (ValueError, TypeError):
        logger.warning("Invalid PublishInterval type. Using the default PublishInterval value: %s"
                       , utils.DEFAULT_PUBLISH_INTERVAL_SEC)
        publish_interval = utils.DEFAULT_PUBLISH_INTERVAL_SEC

    if publish_interval > utils.MAX_PUBLISH_INTERVAL_SEC:
        logger.warning("PublishInterval can not be more than %s seconds, setting it to max value"
                       , utils.MAX_PUBLISH_INTERVAL_SEC)
        publish_interval = utils.MAX_PUBLISH_INTERVAL_SEC
    if publish_interval < 0:
        logger.warning("Invalid PublishInterval value. Using the default PublishInterval value: %s"
                       , utils.DEFAULT_PUBLISH_INTERVAL_SEC)
        publish_interval = utils.DEFAULT_PUBLISH_INTERVAL_SEC
    PUBLISH_INTERVAL_SEC = publish_interval
else:
    PUBLISH_INTERVAL_SEC = utils.DEFAULT_PUBLISH_INTERVAL_SEC

if utils.MAX_METRICS_KEY in config:
    try:
        max_metrics = int(config[utils.MAX_METRICS_KEY])
    except (ValueError, TypeError):
        logger.warning("Invalid MaxMetricsToRetain type. Using the default MaxMetricsToRetain value: %s"
                       , utils.DEFAULT_MAX_METRICS)
        max_metrics = utils.DEFAULT_MAX_METRICS

    if max_metrics < utils.MIN_MAX_METRICS:
        logger.warning("MaxMetricsToRetain can not be less than %s metrics, setting it to least value"
                       , utils.MIN_MAX_METRICS)
        max_metrics = utils.MIN_MAX_METRICS
    MAX_METRICS = max_metrics
else:
    MAX_METRICS = utils.DEFAULT_MAX_METRICS

if utils.INPUT_TOPIC_KEY in config and config[utils.INPUT_TOPIC_KEY] != "":
    INPUT_TOPIC = config[utils.INPUT_TOPIC_KEY]
else:
    INPUT_TOPIC = utils.DEFAULT_INPUT_TOPIC

if utils.OUTPUT_TOPIC_KEY in config and config[utils.OUTPUT_TOPIC_KEY] != "":
    OUTPUT_TOPIC = config[utils.OUTPUT_TOPIC_KEY]
else:
    OUTPUT_TOPIC = utils.DEFAULT_OUTPUT_TOPIC

if utils.PUBSUB_TO_IOT_CORE_KEY in config and config[utils.PUBSUB_TO_IOT_CORE_KEY] != "":
    PUBSUB_TO_IOT_CORE = config[utils.PUBSUB_TO_IOT_CORE_KEY]
else:
    PUBSUB_TO_IOT_CORE = utils.DEFAULT_PUBSUB_TO_IOT_CORE

pubsub_to_iot_core = False
if re.match(r'true', str(PUBSUB_TO_IOT_CORE), flags=re.IGNORECASE):
    pubsub_to_iot_core = True

logger.info("Using Configuration:")
logger.info("%s: %s", utils.PUBLISH_REGION_KEY, PUBLISH_REGION)
logger.info("%s: %s", utils.PUBLISH_INTERVAL_SEC_KEY, PUBLISH_INTERVAL_SEC)
logger.info("%s: %s", utils.MAX_METRICS_KEY, MAX_METRICS)
logger.info("%s: %s", utils.INPUT_TOPIC_KEY, INPUT_TOPIC)
logger.info("%s: %s", utils.OUTPUT_TOPIC_KEY, OUTPUT_TOPIC)
logger.info("%s: %s", utils.PUBSUB_TO_IOT_CORE_KEY, PUBSUB_TO_IOT_CORE)

metrics_manager = MetricsManager(
    PUBLISH_REGION, PUBLISH_INTERVAL_SEC, MAX_METRICS)


def main():
    # Subscribe to IoT Core topic
    if pubsub_to_iot_core:
        ipc.subscribe_to_iot_topic(INPUT_TOPIC, IoTCoreStreamHandler())

    # Subscribe to local Pub Sub topic
    ipc.subscribe_to_pubsub_topic(INPUT_TOPIC, PubSubStreamHandler())


def put_metrics(metric_request):
    for single_metric_datum in metric_request.metric_datum:
        single_metric_datum['Dimensions'].append({'Name': 'coreName', 'Value': utils.GG_CORE_NAME})
        metrics_manager.add_metric(metric_request.namespace, single_metric_datum)


class PubSubStreamHandler(client.SubscribeToTopicStreamHandler):
    def __init__(self):
        super().__init__()

    def on_stream_event(self, event: SubscriptionResponseMessage) -> None:
        try:
            topic, message = IPCUtils.parse_subscription_response_message(event)
            logger.debug("Received new message: %s", message)
            # Check if the message is an array of metrics
            if isinstance(message, list):
                for single_metric in message:
                    metric_request = PutMetricRequest(single_metric)
                    put_metrics(metric_request)
            else:
                # Existing code for single metric
                metric_request = PutMetricRequest(message)
                put_metrics(metric_request)
        except Exception as e:
            logger.exception("Error putting metrics to Cloudwatch: ")
            response = utils.generate_error_response(
                str(e.__class__), str(e), "")
            Thread(
                target=ipc.publish_message,
                args=(OUTPUT_TOPIC, response, pubsub_to_iot_core),
            ).start()
            logger.debug(json.dumps(response))

    def on_stream_error(self, error: Exception) -> bool:
        logger.exception("Received a stream error: ")
        return False  # Return True to close stream, False to keep stream open.


class IoTCoreStreamHandler(client.SubscribeToIoTCoreStreamHandler):
    def __init__(self):
        super().__init__()

    def on_stream_event(self, event: IoTCoreMessage) -> None:
        try:
            message = event.message.payload.decode('utf-8')
            dict_message = json.loads(message)
            logger.debug("Received new message: %s", message)
            # Check if the message is an array of metrics
            if isinstance(dict_message, list):
                for single_metric in dict_message:
                    metric_request = PutMetricRequest(single_metric)
                    put_metrics(metric_request)
            else:
                # Existing code for single metric
                metric_request = PutMetricRequest(dict_message)
                put_metrics(metric_request)
        except Exception as e:
            logger.exception("Error putting metrics to Cloudwatch: ")
            response = utils.generate_error_response(
                str(e.__class__), str(e), "")
            Thread(
                target=ipc.publish_message,
                args=(OUTPUT_TOPIC, response, pubsub_to_iot_core),
            ).start()
            logger.debug(json.dumps(response))

    def on_stream_error(self, error: Exception) -> bool:
        logger.exception("Received a stream error: ")
        return False  # Return True to close stream, False to keep stream open.
