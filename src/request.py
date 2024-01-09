# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import numbers
import time

from src.utils import *


class PutMetricRequest:

    def __init__(self, event):
        if not event:
            raise ValueError('input is empty')

        self.metric_datum = []
        self.parse_event(event)

    def add_dimension(self, dimension_name, dimension_value):
        # Since metric_datum is now a list, we need to modify this method.
        # This method will add the dimension to each metric datum in the list.
        for datum in self.metric_datum:
            datum['Dimensions'].append({'Name': dimension_name, 'Value': dimension_value})

    def parse_event(self, event):
        if type(event) is not dict:
            raise ValueError(
                'mandatory field ({}) is not a dict in the input'.format(FIELD_REQUEST))

        if event.get(FIELD_REQUEST) is None:
            raise ValueError(
                'mandatory field ({}) is absent in the input'.format(FIELD_REQUEST))

        self.parse_metric(event[FIELD_REQUEST])

    def parse_metric(self, metric):
        self.validate_metric(metric)

        self.namespace = metric.get(FIELD_NAMESPACE)
        self.parse_metric_datum(metric.get(FIELD_METRIC_DATA))

    def validate_metric(self, metric):
        if metric.get(FIELD_NAMESPACE) is None:
            raise ValueError(
                'mandatory field ({}) is absent in the input'.format(FIELD_NAMESPACE))

        if metric.get(FIELD_METRIC_DATA) is None:
            raise ValueError(
                'mandatory field ({}) is absent in the input'.format(FIELD_METRIC_DATA))

    def parse_metric_datum(self, metric_datum):
        if isinstance(metric_datum, list):
            for single_metric_datum in metric_datum:
                self.process_single_metric_datum(single_metric_datum)
        elif isinstance(metric_datum, dict):
            self.process_single_metric_datum(metric_datum)
        else:
            raise ValueError(
                'Incorrect payload format, field ({}) must be a dict or a list of dicts'.format(FIELD_METRIC_DATA))

    def process_single_metric_datum(self, single_metric_datum):
        self.validate_single_metric_datum(single_metric_datum)

        metric_name = single_metric_datum.get(FIELD_METRIC_NAME)
        metric_value = single_metric_datum.get(FIELD_METRIC_VALUE)
        unit = single_metric_datum.get(FIELD_METRIC_UNIT, 'Count')
        timestamp = single_metric_datum.get(FIELD_METRIC_TIMESTAMP, time.time())
        dimension = self.parse_dimensions(
            single_metric_datum.get(FIELD_DIMENSIONS))
        # Create a metric datum dictionary and append it to metric_datum
        single_datum = {
            'MetricName': metric_name,
            'Value': metric_value,
            'Dimensions': dimension,
            'Unit': unit,
            'Timestamp': timestamp
        }
        self.metric_datum.append(single_datum)

    def validate_metric_datum(self, metric_data):
        if isinstance(metric_data, list):
            for metric_datum in metric_data:
                self.validate_single_metric_datum(metric_datum)
        elif isinstance(metric_data, dict):
            self.validate_single_metric_datum(metric_data)
        else:
            raise ValueError(
                'Incorrect payload format, field ({}) must be a dict or a list of dicts'.format(FIELD_METRIC_DATA))

    def validate_single_metric_datum(self, metric_datum):
        if metric_datum.get(FIELD_METRIC_NAME) is None:
            raise ValueError(
                'mandatory field ({}) is absent in the input'.format(FIELD_METRIC_NAME))
        if metric_datum.get(FIELD_METRIC_VALUE) is None:
            raise ValueError(
                'mandatory field ({}) is absent in the input'.format(FIELD_METRIC_VALUE))

        if not isinstance(metric_datum.get(FIELD_METRIC_VALUE), numbers.Number):
            raise ValueError(
                'mandatory field ({}) is not a number'.format(FIELD_METRIC_VALUE))

        if metric_datum.get(FIELD_METRIC_UNIT) and metric_datum.get(FIELD_METRIC_UNIT) not in VALID_UNIT_VALUES:
            raise ValueError(
                'field ({}) is not a valid value, must be in ({})'.format(FIELD_METRIC_UNIT, VALID_UNIT_VALUES))

        if metric_datum.get(FIELD_METRIC_TIMESTAMP) is not None and not isinstance(
                metric_datum.get(FIELD_METRIC_TIMESTAMP), numbers.Number):
            raise ValueError('field ({}) is not a number, must be in (milliseconds)'.format(
                FIELD_METRIC_TIMESTAMP))

    def parse_dimensions(self, dimensions):
        if dimensions is None:
            return []

        self.validate_dimensions(dimensions)
        return [{
            'Name': dimension.get(FIELD_DIMENSION_NAME),
            'Value': dimension.get(FIELD_DIMENSION_VALUE)
        } for dimension in dimensions]

    def validate_dimensions(self, dimensions):
        if type(dimensions) is list:
            if len(dimensions) > MAX_DIMENSIONS_PER_METRIC:
                raise ValueError(
                    'More than ({}) entries present in field ({})'.format(MAX_DIMENSIONS_PER_METRIC, FIELD_DIMENSIONS))
            for dimension in dimensions:
                if dimension.get(FIELD_DIMENSION_NAME) is None:
                    raise ValueError(
                        'mandatory field ({}) is absent in the dimension'.format(FIELD_DIMENSION_NAME))

                if dimension.get(FIELD_DIMENSION_VALUE) is None:
                    raise ValueError('mandatory field ({}) is absent in the dimension'.format(
                        FIELD_DIMENSION_VALUE))
        else:
            raise ValueError(
                'field ({}) is not of type list in the input'.format(FIELD_DIMENSIONS))
