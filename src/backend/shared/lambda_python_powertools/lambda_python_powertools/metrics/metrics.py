import collections
import functools
import json
import logging
import os
from contextlib import contextmanager
from typing import Any, Callable

from lambda_python_powertools.metrics.base import MetricManager

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class Metrics(MetricManager):
    _metrics = collections.defaultdict()
    _dimensions = collections.defaultdict()

    def __init__(self, metric_set=None, dimension_set=None):
        self.metric_set = self._metrics
        self.dimension_set = self._dimensions

    def log_metrics(self, fn: Callable[[Any, Any], Any] = None):
        @functools.wraps(fn)
        def decorate(*args, **kwargs):
            try:
                metrics = self.serialize_metric_set()
                print(json.dumps(metrics, indent=4))
            except Exception as err:
                raise err

        return decorate


@contextmanager
def single_metric(namespace: str = None):
    try:
        metric = MetricManager()
        yield metric
    except Exception as err:
        raise err
    finally:
        metric_set = metric.serialize_metric_set()
        print(json.dumps(metric_set, indent=4))


# TODO - Clean up
# with single_metric() as m:
#     m.add_metric(name="ColdStart", unit="Count", value=1)
#     m.add_dimension(name="function_version", value="$LATEST")

# blah = Metrics()
# blah.add_metric(name="ColdStart", unit="Count", value=1)
# blah.add_metric(name="BookingConfirmation", unit="Count", value=1)
# blah.add_dimension(name="service", value="booking")
# blah.add_dimension(name="function_version", value="$LATEST")


# @blah.log_metrics()
# @tracer.lambda_handler()
# def testing():
#     return True
