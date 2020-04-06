import json
import logging
import os
from contextlib import contextmanager
from typing import Dict

from lambda_python_powertools.helper.models import MetricUnit
from lambda_python_powertools.metrics.base import MetricManager

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class SingleMetric(MetricManager):
    def add_metric(self, name: str, unit: MetricUnit, value: str):
        if len(self.metric_set) > 0:
            logger.debug(f"Metric {name} already set, skipping...")
            return
        return super().add_metric(name, unit, value)


@contextmanager
def single_metric(namespace: str) -> SingleMetric:
    metric_set = None
    try:
        metric: SingleMetric = SingleMetric(namespace=namespace)
        yield metric
        logger.debug("Serializing single metric")
        metric_set: Dict = metric.serialize_metric_set()
    except Exception as e:
        logger.error(e)
        raise e
    finally:
        logger.debug("Publishing single metric", {"metric": metric})
        print(json.dumps(metric_set, indent=4))
