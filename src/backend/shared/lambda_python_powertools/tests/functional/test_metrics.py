import io
import json
from typing import Dict, List

import pytest

from lambda_python_powertools.metrics import (
    MetricUnit,
    single_metric,
    Metrics,
    MetricUnitError,
    SchemaValidationError,
)
from lambda_python_powertools.metrics.base import MetricManager


@pytest.fixture
def stdout():
    return io.StringIO()


@pytest.fixture
def metric():
    return {"name": "single_metric", "unit": MetricUnit.Count, "value": 1}


@pytest.fixture
def metrics():
    return [
        {"name": "metric_one", "unit": MetricUnit.Count, "value": 1},
        {"name": "metric_two", "unit": MetricUnit.Count, "value": 1},
    ]


@pytest.fixture
def dimension():
    return {"name": "test_dimension", "value": "test"}


@pytest.fixture
def dimensions():
    return [
        {"name": "test_dimension", "value": "test"},
        {"name": "test_dimension_2", "value": "test"},
    ]


@pytest.fixture
def namespace():
    return {"name": "test_namespace"}


def serialize_metrics(metrics: List[Dict], dimensions: List[Dict], namespace: str) -> Dict:
    """ Helper function to build EMF object from a list of metrics, dimensions """
    m = MetricManager()
    for metric in metrics:
        m.add_metric(**metric)

    for dimension in dimensions:
        m.add_dimension(**dimension)

    m.add_namespace(name=namespace)
    return m.serialize_metric_set()


def serialize_single_metric(metric: Dict, dimension: Dict, namespace: str) -> Dict:
    """ Helper function to build EMF object from a given metric, dimension and namespace """
    m = MetricManager()
    m.add_metric(**metric)
    m.add_dimension(**dimension)
    m.add_namespace(name=namespace)
    return m.serialize_metric_set()


def test_single_metric(capsys, metric, dimension, namespace):
    with single_metric(**metric) as m:
        m.add_dimension(**dimension)
        m.add_namespace(**namespace)

    output = json.loads(capsys.readouterr().out.strip())
    expected = serialize_single_metric(metric=metric, dimension=dimension, namespace=namespace)

    # Timestamp will always be different
    del expected["_aws"]["Timestamp"]
    del output["_aws"]["Timestamp"]
    assert expected["_aws"] == output["_aws"]


def test_single_metric_one_metric_only(capsys, metric, dimension, namespace):
    with single_metric(**metric) as m:
        m.add_metric(name="second_metric", unit="Count", value=1)
        m.add_metric(name="third_metric", unit="Seconds", value=1)
        m.add_dimension(**dimension)
        m.add_namespace(**namespace)

    output = json.loads(capsys.readouterr().out.strip())
    expected = serialize_single_metric(metric=metric, dimension=dimension, namespace=namespace)

    # Timestamp will always be different
    del expected["_aws"]["Timestamp"]
    del output["_aws"]["Timestamp"]
    assert expected["_aws"] == output["_aws"]


def test_multiple_metrics(capsys, metrics, dimensions, namespace):
    m = Metrics()
    for metric in metrics:
        m.add_metric(**metric)

    for dimension in dimensions:
        m.add_dimension(**dimension)

    m.add_namespace(namespace)
    output = m.serialize_metric_set()
    expected = serialize_metrics(metrics=metrics, dimensions=dimensions, namespace=namespace)

    # Timestamp will always be different
    del expected["_aws"]["Timestamp"]
    del output["_aws"]["Timestamp"]
    assert expected["_aws"] == output["_aws"]


def test_multiple_namespaces(capsys, metric, dimension, namespace):
    with single_metric(**metric) as m:
        m.add_dimension(**dimension)
        m.add_namespace(**namespace)
        m.add_namespace(name="OtherNamespace")
        m.add_namespace(name="AnotherNamespace")

    output = json.loads(capsys.readouterr().out.strip())
    expected = serialize_single_metric(metric=metric, dimension=dimension, namespace=namespace)

    # Timestamp will always be different
    del expected["_aws"]["Timestamp"]
    del output["_aws"]["Timestamp"]
    assert expected["_aws"] == output["_aws"]


def test_incorrect_metric_unit(capsys, metric, dimension, namespace):
    metric["unit"] = "incorrect_unit"

    with pytest.raises(MetricUnitError):
        with single_metric(**metric) as m:
            m.add_dimension(**dimension)
            m.add_namespace(**namespace)


def test_schema_no_namespace(capsys, metric, dimension):
    with pytest.raises(SchemaValidationError):
        with single_metric(**metric) as m:
            m.add_dimension(**dimension)


def test_schema_incorrect_value(capsys, metric, dimension, namespace):
    metric["value"] = True
    with single_metric(**metric) as m:
        m.add_dimension(**dimension)
        m.add_namespace(**namespace)


def test_schema_no_metrics(capsys, dimensions, namespace):
    m = Metrics()
    m.add_namespace(**namespace)
    for dimension in dimensions:
        m.add_dimension(**dimension)
    with pytest.raises(SchemaValidationError):
        m.serialize_metric_set()
