import functools
import json
import logging
import os
from typing import Any, Callable

from lambda_python_powertools.metrics.base import MetricManager

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class Metrics(MetricManager):
    _metrics = {}
    _dimensions = {}

    def __init__(self, metric_set=None, dimension_set=None, namespace=None):
        super().__init__(
            metric_set=self._metrics, dimension_set=self._dimensions, namespace=namespace
        )

    def log_metrics(self, fn: Callable[[Any, Any], Any] = None):
        @functools.wraps(fn)
        def decorate(*args, **kwargs):
            try:
                metrics = self.serialize_metric_set()
                logger.debug("Publishing metrics", {"metrics": metrics})
                print(json.dumps(metrics, indent=4))
            except Exception as e:
                logger.error(e)
                raise e

        return decorate
