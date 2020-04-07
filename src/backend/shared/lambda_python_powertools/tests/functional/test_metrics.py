import io
import json
from typing import Dict, List

import pytest

from lambda_python_powertools.metrics import (
    Metrics,
    MetricUnit,
    MetricUnitError,
    MetricValueError,
    SchemaValidationError,
    UniqueNamespaceError,
    single_metric,
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


def serialize_metrics(metrics: List[Dict], dimensions: List[Dict], namespace: Dict) -> Dict:
    """ Helper function to build EMF object from a list of metrics, dimensions """
    my_metrics = MetricManager()
    for metric in metrics:
        my_metrics.add_metric(**metric)

    for dimension in dimensions:
        my_metrics.add_dimension(**dimension)

    my_metrics.add_namespace(**namespace)
    return my_metrics.serialize_metric_set()


def serialize_single_metric(metric: Dict, dimension: Dict, namespace: Dict) -> Dict:
    """ Helper function to build EMF object from a given metric, dimension and namespace """
    my_metrics = MetricManager()
    my_metrics.add_metric(**metric)
    my_metrics.add_dimension(**dimension)
    my_metrics.add_namespace(**namespace)
    return my_metrics.serialize_metric_set()


def test_single_metric(capsys, metric, dimension, namespace):
    with single_metric(**metric) as my_metrics:
        my_metrics.add_dimension(**dimension)
        my_metrics.add_namespace(**namespace)

    output = json.loads(capsys.readouterr().out.strip())
    expected = serialize_single_metric(metric=metric, dimension=dimension, namespace=namespace)

    # Timestamp will always be different
    del expected["_aws"]["Timestamp"]
    del output["_aws"]["Timestamp"]
    assert expected["_aws"] == output["_aws"]


def test_single_metric_one_metric_only(capsys, metric, dimension, namespace):
    with single_metric(**metric) as my_metrics:
        my_metrics.add_metric(name="second_metric", unit="Count", value=1)
        my_metrics.add_metric(name="third_metric", unit="Seconds", value=1)
        my_metrics.add_dimension(**dimension)
        my_metrics.add_namespace(**namespace)

    output = json.loads(capsys.readouterr().out.strip())
    expected = serialize_single_metric(metric=metric, dimension=dimension, namespace=namespace)

    # Timestamp will always be different
    del expected["_aws"]["Timestamp"]
    del output["_aws"]["Timestamp"]
    assert expected["_aws"] == output["_aws"]


def test_multiple_metrics(capsys, metrics, dimensions, namespace):
    my_metrics = Metrics()
    for metric in metrics:
        my_metrics.add_metric(**metric)

    for dimension in dimensions:
        my_metrics.add_dimension(**dimension)

    my_metrics.add_namespace(**namespace)
    output = my_metrics.serialize_metric_set()
    expected = serialize_metrics(metrics=metrics, dimensions=dimensions, namespace=namespace)

    # Timestamp will always be different
    del expected["_aws"]["Timestamp"]
    del output["_aws"]["Timestamp"]
    assert expected["_aws"] == output["_aws"]


def test_multiple_namespaces(capsys, metric, dimension, namespace):
    namespace_a = {"name": "OtherNamespace"}
    namespace_b = {"name": "AnotherNamespace"}

    with pytest.raises(UniqueNamespaceError):
        with single_metric(**metric) as m:
            m.add_dimension(**dimension)
            m.add_namespace(**namespace)
            m.add_namespace(**namespace_a)
            m.add_namespace(**namespace_b)


def test_log_metrics_no_function_call(capsys, metrics, dimensions, namespace):
    my_metrics = Metrics()
    my_metrics.add_namespace(**namespace)
    for metric in metrics:
        my_metrics.add_metric(**metric)
    for dimension in dimensions:
        my_metrics.add_dimension(**dimension)

    @my_metrics.log_metrics
    def lambda_handler(evt, handler):
        return True

    lambda_handler({}, {})
    output = json.loads(capsys.readouterr().out.strip())
    expected = serialize_metrics(metrics=metrics, dimensions=dimensions, namespace=namespace)

    # Timestamp will always be different
    del expected["_aws"]["Timestamp"]
    del output["_aws"]["Timestamp"]
    assert expected["_aws"] == output["_aws"]


def test_log_metrics_call_function(capsys, metrics, dimensions, namespace):
    my_metrics = Metrics()

    @my_metrics.log_metrics(call_function=True)
    def lambda_handler(evt, handler):
        my_metrics.add_namespace(**namespace)
        for metric in metrics:
            my_metrics.add_metric(**metric)
        for dimension in dimensions:
            my_metrics.add_dimension(**dimension)
        return True

    lambda_handler({}, {})

    output = json.loads(capsys.readouterr().out.strip())
    expected = serialize_metrics(metrics=metrics, dimensions=dimensions, namespace=namespace)

    # Timestamp will always be different
    del expected["_aws"]["Timestamp"]
    del output["_aws"]["Timestamp"]
    assert expected["_aws"] == output["_aws"]


def test_namespace_env_var(monkeypatch, capsys, metric, dimension, namespace):
    monkeypatch.setenv("POWERTOOLS_METRICS_NAMESPACE", namespace["name"])

    with single_metric(**metric) as my_metrics:
        my_metrics.add_dimension(**dimension)
        monkeypatch.delenv("POWERTOOLS_METRICS_NAMESPACE")

    output = json.loads(capsys.readouterr().out.strip())
    expected = serialize_single_metric(metric=metric, dimension=dimension, namespace=namespace)

    # Timestamp will always be different
    del expected["_aws"]["Timestamp"]
    del output["_aws"]["Timestamp"]
    assert expected["_aws"] == output["_aws"]


def test_log_metrics_schema_error(capsys, metrics, dimensions, namespace):
    # It should error out because by default log_metrics doesn't invoke a function
    # so when decorator runs it'll raise an error while trying to serialize metrics
    my_metrics = Metrics()

    @my_metrics.log_metrics
    def lambda_handler(evt, handler):
        my_metrics.add_namespace(namespace)
        for metric in metrics:
            my_metrics.add_metric(**metric)
        for dimension in dimensions:
            my_metrics.add_dimension(**dimension)
            return True

    with pytest.raises(SchemaValidationError):
        lambda_handler({}, {})


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
    metric["value"] = "some_value"
    with pytest.raises(MetricValueError):
        with single_metric(**metric) as m:
            m.add_dimension(**dimension)
            m.add_namespace(**namespace)


def test_schema_no_metrics(capsys, dimensions, namespace):
    my_metrics = Metrics()
    my_metrics.add_namespace(**namespace)
    for dimension in dimensions:
        my_metrics.add_dimension(**dimension)
    with pytest.raises(SchemaValidationError):
        my_metrics.serialize_metric_set()
