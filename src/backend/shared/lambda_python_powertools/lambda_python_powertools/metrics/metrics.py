import datetime
import os
import collections
import json
import functools
from typing import Any, Callable, Dict, List


class Metrics:
    _metrics = collections.defaultdict()
    _dimensions = collections.defaultdict()

    def __init__(self):
        super().__init__()

    def add_metric(self, name: str, unit: str, value: float):
        metric = {"Unit": unit, "Value": value}
        self._metrics[name] = metric

    def serialize_metrics(self) -> Dict:
        dimension_keys: List[str] = list(self._dimensions.keys())
        metric_names_unit: List[Dict[str, str]] = []
        metrics: Dict[str, str] = {}

        for metric_name in self._metrics:
            metric = self._metrics[metric_name]
            metric_value = metric.get("Value")
            metric_unit = metric.get("Unit")

            if metric_value > 0 and metric_unit is not None:
                metric_names_unit.append({"Name": metric_name, "Unit": metric["Unit"]})
                metrics.update({metric_name: metric["Value"]})

        metrics_definition = {
            "CloudWatchMetrics": [
                {
                    "Namespace": "ServerlessAirline",
                    "Dimensions": [dimension_keys],
                    "Metrics": metric_names_unit,
                }
            ]
        }
        metrics_timestamp = {
            "Timestamp": int(datetime.datetime.now().timestamp() * 1000)
        }

        metrics["_aws"] = {**metrics_timestamp, **metrics_definition}

        return metrics

    def add_dimension(self, name: str, value: float = 0):
        if len(self._dimensions) > 9:
            raise ValueError(
                "Exceeded maximum of 9 dimensions that can be associated with metrics"
            )

        if not value:
            raise ValueError("Dimension value cannot be 0")

        self._dimensions[name] = value

    def log_metrics(self, fn: Callable[[Any, Any], Any] = None):
        @functools.wraps(fn)
        def decorate(*args, **kwargs):
            try:
                metrics = self.serialize_metrics()
                print(json.dumps(metrics, indent=4))
            except Exception as err:
                raise err

        return decorate


# blah = Metrics()
# blah.add_metric(name="ColdStart", unit="Count", value=1)
# blah.add_metric(name="BookingConfirmation", unit="Count", value=1)
# blah.add_dimension(name="service", value="booking")
# blah.add_dimension(name="function_version", value="$LATEST")


# @blah.log_metrics()
# @tracer.handler()
# def testing():
#     return True
