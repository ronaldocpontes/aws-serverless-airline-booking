"""CloudWatch Embedded Metric Format utility
"""
from .metric import single_metric
from .metrics import Metrics

__all__ = ["Metrics", "single_metric"]
