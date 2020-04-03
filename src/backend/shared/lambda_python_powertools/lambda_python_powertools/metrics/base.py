from __future__ import absolute_import

import collections
import datetime
import json
import logging
import os
from typing import Dict, List

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class MetricManager:
    def __init__(self, metric_set: Dict[str, str] = None, dimension_set: Dict = None):
        self.metric_set = metric_set or collections.defaultdict()
        self.dimension_set = dimension_set or collections.defaultdict()

    def add_metric(self, name: str, unit: str, value: float):
        if len(self.metric_set) > 100:
            logger.debug("Exceeded maximum of 100 metrics - Publishing existing metric set")
            metrics = self.serialize_metric_set()
            print(json.dumps(metrics, indent=4))

        metric = {"Unit": unit, "Value": value}
        self.metric_set[name] = metric

    def serialize_metric_set(self, metrics: Dict = None, dimensions: Dict = None) -> Dict:
        if metrics is None:
            metrics = self.metric_set

        if dimensions is None:
            dimensions = self.dimension_set

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

        metrics["_aws"] = {**metrics_timestamp, **metrics_definition}

        return metrics

    def add_dimension(self, name: str, value: float = 0):
        if len(self.dimension_set) > 9:
            raise ValueError("Exceeded maximum of 9 dimensions that can be associated with metrics")

        if not value:
            raise ValueError("Dimension value cannot be 0")

        self.dimension_set[name] = value
