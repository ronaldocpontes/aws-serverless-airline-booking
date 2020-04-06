import datetime
import json
import logging
import os
from typing import Dict, List

import jsonschema

from lambda_python_powertools.helper.models import MetricUnit

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

CLOUDWATCH_EMF_SCHEMA = {
    "type": "object",
    "title": "Root Node",
    "required": ["_aws"],
    "properties": {
        "_aws": {
            "$id": "#/properties/_aws",
            "type": "object",
            "title": "Metadata",
            "required": ["Timestamp", "CloudWatchMetrics"],
            "properties": {
                "Timestamp": {
                    "$id": "#/properties/_aws/properties/Timestamp",
                    "type": "integer",
                    "title": "The Timestamp Schema",
                    "examples": [1565375354953],
                },
                "CloudWatchMetrics": {
                    "$id": "#/properties/_aws/properties/CloudWatchMetrics",
                    "type": "array",
                    "title": "MetricDirectives",
                    "items": {
                        "$id": "#/properties/_aws/properties/CloudWatchMetrics/items",
                        "type": "object",
                        "title": "MetricDirective",
                        "required": ["Namespace", "Dimensions", "Metrics"],
                        "properties": {
                            "Namespace": {
                                "$id": "#/properties/_aws/properties/CloudWatchMetrics/items/properties/Namespace",
                                "type": "string",
                                "title": "CloudWatch Metrics Namespace",
                                "examples": ["MyApp"],
                                "pattern": "^(.*)$",
                                "minLength": 1,
                            },
                            "Dimensions": {
                                "$id": "#/properties/_aws/properties/CloudWatchMetrics/items/properties/Dimensions",
                                "type": "array",
                                "title": "The Dimensions Schema",
                                "minItems": 1,
                                "items": {
                                    "$id": "#/properties/_aws/properties/CloudWatchMetrics/items/properties/Dimensions/items",
                                    "type": "array",
                                    "title": "DimensionSet",
                                    "minItems": 1,
                                    "maxItems": 9,
                                    "items": {
                                        "$id": "#/properties/_aws/properties/CloudWatchMetrics/items/properties/Dimensions/items/items",
                                        "type": "string",
                                        "title": "DimensionReference",
                                        "examples": ["Operation"],
                                        "pattern": "^(.*)$",
                                        "minItems": 1,
                                    },
                                },
                            },
                            "Metrics": {
                                "$id": "#/properties/_aws/properties/CloudWatchMetrics/items/properties/Metrics",
                                "type": "array",
                                "title": "MetricDefinitions",
                                "items": {
                                    "$id": "#/properties/_aws/properties/CloudWatchMetrics/items/properties/Metrics/items",
                                    "type": "object",
                                    "title": "MetricDefinition",
                                    "required": ["Name"],
                                    "properties": {
                                        "Name": {
                                            "$id": "#/properties/_aws/properties/CloudWatchMetrics/items/properties/Metrics/items/properties/Name",
                                            "type": "string",
                                            "title": "MetricName",
                                            "examples": ["ProcessingLatency"],
                                            "pattern": "^(.*)$",
                                        },
                                        "Unit": {
                                            "$id": "#/properties/_aws/properties/CloudWatchMetrics/items/properties/Metrics/items/properties/Unit",
                                            "type": "string",
                                            "title": "MetricUnit",
                                            "examples": ["Milliseconds"],
                                            "pattern": "^(Seconds|Microseconds|Milliseconds|Bytes|Kilobytes|Megabytes|Gigabytes|Terabytes|Bits|Kilobits|Megabits|Gigabits|Terabits|Percent|Count|Bytes\\/Second|Kilobytes\\/Second|Megabytes\\/Second|Gigabytes\\/Second|Terabytes\\/Second|Bits\\/Second|Kilobits\\/Second|Megabits\\/Second|Gigabits\\/Second|Terabits\\/Second|Count\\/Second|None)$",
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        }
    },
}


class MetricManager:
    def __init__(
        self, metric_set: Dict[str, str] = None, dimension_set: Dict = None, namespace: str = None
    ):
        self.metric_set = metric_set or {}
        self.dimension_set = dimension_set or {}
        self.namespace = os.getenv("POWERTOOLS_METRICS_NAMESPACE") or namespace

    def add_namespace(self, name: str):
        if self.namespace is not None:
            logger.warning(
                f"Namespace already set. Replacing '{self.namespace}' with '{self.namespace}'"
            )
        logger.debug(f"Adding metrics namespace: {name}")
        self.namespace = name

    def add_metric(self, name: str, unit: MetricUnit, value: float):
        if len(self.metric_set) == 100:
            logger.debug("Exceeded maximum of 100 metrics - Publishing existing metric set")
            metrics = self.serialize_metric_set()
            print(json.dumps(metrics, indent=4))

        if not isinstance(unit, MetricUnit):
            try:
                unit = MetricUnit[unit]
            except KeyError:
                unit_options = list(MetricUnit.__members__)
                raise ValueError(
                    f"Invalid metric unit '{unit}', expected either option: {unit_options}"
                )

        metric = {"Unit": unit.value, "Value": value}
        logger.debug(f"Adding metric: {name} with {metric}")
        self.metric_set[name] = metric

    def serialize_metric_set(self, metrics: Dict = None, dimensions: Dict = None) -> Dict:
        if metrics is None:
            metrics = self.metric_set

        if dimensions is None:
            dimensions = self.dimension_set

        logger.debug("Serializing...", {"metrics": metrics, "dimensions": dimensions})

        dimension_keys: List[str] = list(dimensions.keys())
        metric_names_unit: List[Dict[str, str]] = []
        metric_set: Dict[str, str] = {}

        for metric_name in metrics:
            metric: str = metrics[metric_name]
            metric_value: int = metric.get("Value", 0)
            metric_unit: str = metric.get("Unit")

            if metric_value > 0 and metric_unit is not None:
                metric_names_unit.append({"Name": metric_name, "Unit": metric["Unit"]})
                metric_set.update({metric_name: metric["Value"]})

        metrics_definition = {
            "CloudWatchMetrics": [
                {
                    "Namespace": "ServerlessAirline",
                    "Dimensions": [dimension_keys],
                    "Metrics": metric_names_unit,
                }
            ]
        }
        metrics_timestamp = {"Timestamp": int(datetime.datetime.now().timestamp() * 1000)}
        metric_set["_aws"] = {**metrics_timestamp, **metrics_definition}

        try:
            logger.debug("Validating serialized metrics against CloudWatch EMF schema", metric_set)
            jsonschema.validate(metric_set, schema=CLOUDWATCH_EMF_SCHEMA)
        except jsonschema.exceptions.ValidationError as e:
            message = f"Invalid format. Error: {e.message} ({e.validator}), Invalid item: {e.absolute_schema_path}"  # noqa: B306
            raise ValueError(message)
        return metric_set

    def add_dimension(self, name: str, value: float = 0):
        logger.debug(f"Adding dimension: {name}:{value}")
        self.dimension_set[name] = value
