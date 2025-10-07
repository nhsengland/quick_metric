"""
Tests for pipeline integration functionality.

Tests the GenerateMetricsStage and create_metrics_stage function to ensure
they work correctly with the oops-its-a-pipeline framework.
"""

import pandas as pd
import pytest

from oops_its_a_pipeline import PipelineConfig, PipelineContext
from oops_its_a_pipeline.exceptions import PipelineStageValidationError

import quick_metric.pipeline as pipeline_module
from quick_metric._exceptions import RegistryLockError
from quick_metric._method_definitions import metric_method
from quick_metric.pipeline import (
    GenerateMetricsStage,
    create_metrics_stage,
)


@pytest.fixture
def test_data():
    """Common test data fixture."""
    return pd.DataFrame(
        {
            "category": ["A", "A", "B", "B"],
            "value": [1, 2, 3, 4],
        }
    )


@pytest.fixture
def pipeline_config():
    """Common pipeline config fixture."""

    class TestConfig(PipelineConfig):
        pass

    return TestConfig()


@pytest.fixture
def basic_metrics_config():
    """Basic metrics configuration fixture."""

    @metric_method
    def count_records(data: pd.DataFrame) -> int:
        return len(data)

    return {
        "test_metric": {"method": ["count_records"], "filter": {}},
        "category_a_count": {"method": ["count_records"], "filter": {"category": "A"}},
    }


class TestGenerateMetricsStage:
    """Test GenerateMetricsStage class."""

    @pytest.mark.parametrize(
        ("attribute", "expected_value"),
        [
            ("name", "generate_metrics"),
            ("input_keys", ("data", "config")),
            ("output_keys", ("metrics",)),
        ],
    )
    def test_stage_creation(self, attribute, expected_value):
        """Test stage creation with default parameters."""
        stage = GenerateMetricsStage()
        assert getattr(stage, attribute) == expected_value

    def test_stage_execution_with_dict_config(
        self, test_data, pipeline_config, basic_metrics_config
    ):
        """Test stage execution with dictionary config."""
        stage = GenerateMetricsStage()
        context = PipelineContext(pipeline_config, "test_run")
        context["data"] = test_data
        context["config"] = basic_metrics_config

        result_context = stage.run(context)
        assert "metrics" in result_context

    def test_stage_execution_invalid_data_type(self, pipeline_config):
        """Test stage with invalid data type."""
        stage = GenerateMetricsStage()
        context = PipelineContext(pipeline_config, "test_run")
        context["data"] = "not_a_dataframe"  # Invalid data type
        context["config"] = {}

        with pytest.raises(PipelineStageValidationError, match="Expected pandas DataFrame"):
            stage.run(context)

    def test_stage_error_handling_with_mock(
        self, test_data, pipeline_config, basic_metrics_config, mocker
    ):
        """Test error handling using pytest-mock to simulate failures."""
        stage = GenerateMetricsStage()
        context = PipelineContext(pipeline_config, "test_run")
        context["data"] = test_data
        context["config"] = basic_metrics_config

        # Mock generate_metrics to raise an exception using pytest-mock
        mock_generate = mocker.patch("quick_metric.pipeline.generate_metrics")
        mock_generate.side_effect = RuntimeError("Simulated failure")

        with pytest.raises(PipelineStageValidationError, match="failed during metrics generation"):
            stage.run(context)

    def test_stage_registry_lock_error_with_mock(
        self, test_data, pipeline_config, basic_metrics_config, mocker
    ):
        """Test registry lock error handling using pytest-mock."""
        stage = GenerateMetricsStage()
        context = PipelineContext(pipeline_config, "test_run")
        context["data"] = test_data
        context["config"] = basic_metrics_config

        # Mock the generate_metrics to raise a RegistryLockError using pytest-mock
        mock_generate = mocker.patch("quick_metric.pipeline.generate_metrics")
        mock_generate.side_effect = RegistryLockError("get_methods", "Lock failed")

        with pytest.raises(PipelineStageValidationError, match="failed during metrics generation"):
            stage.run(context)

    def test_stage_spy_on_generate_metrics(
        self, test_data, pipeline_config, basic_metrics_config, mocker
    ):
        """Test using mocker.spy to verify function calls without changing behavior."""
        stage = GenerateMetricsStage()
        context = PipelineContext(pipeline_config, "test_run")
        context["data"] = test_data
        context["config"] = basic_metrics_config

        # Spy on generate_metrics to verify it gets called with correct arguments
        spy_generate = mocker.spy(pipeline_module, "generate_metrics")

        result_context = stage.run(context)

        # Verify the function was called
        spy_generate.assert_called_once()

        # Verify the call arguments
        call_args = spy_generate.call_args
        assert call_args.kwargs["data"] is test_data
        assert call_args.kwargs["config"] == basic_metrics_config
        assert "metrics" in result_context


class TestCreateMetricsStage:
    """Test create_metrics_stage convenience function."""

    def test_create_metrics_stage_default_parameters(self):
        """Test creating stage with default parameters."""
        stage = create_metrics_stage()
        assert isinstance(stage, GenerateMetricsStage)

    @pytest.mark.parametrize(
        ("attribute", "expected_value"),
        [
            ("name", "generate_metrics"),
            ("input_keys", ("data", "config")),
            ("output_keys", ("metrics",)),
        ],
    )
    def test_create_metrics_stage_default_attributes(self, attribute, expected_value):
        """Test default stage attributes."""
        stage = create_metrics_stage()
        assert getattr(stage, attribute) == expected_value


class TestPipelineIntegration:
    """Test integration with oops-its-a-pipeline Pipeline."""

    def test_stage_with_config_object_with_nested_config_attr(self, test_data, pipeline_config):
        """Test config object with nested config attribute."""

        @metric_method
        def nested_test(data: pd.DataFrame) -> int:
            return data.shape[0]

        class ConfigWithDictConfig:
            def __init__(self):
                # This should hit the condition with isinstance(config.config, dict)
                self.config = {"nested_metric": {"method": ["nested_test"], "filter": {}}}

        stage = GenerateMetricsStage()
        context = PipelineContext(pipeline_config, "test_run")
        context["data"] = test_data
        context["config"] = ConfigWithDictConfig()

        result_context = stage.run(context)
        assert "metrics" in result_context
        assert "nested_metric" in result_context["metrics"]
        assert result_context["metrics"]["nested_metric"]["nested_test"] == 4

    def test_stage_with_config_object_with_non_dict_config_attr(self, test_data, pipeline_config):
        """Test config object with non-dict config attribute."""

        class ConfigWithNonDictConfig:
            def __init__(self):
                # This hits line 284: config = config.config (when not isinstance dict)
                self.config = "not_a_dict"

        stage = GenerateMetricsStage()
        context = PipelineContext(pipeline_config, "test_run")
        context["data"] = test_data
        context["config"] = ConfigWithNonDictConfig()

        # This should fail because config.config is not a dict
        with pytest.raises(PipelineStageValidationError, match="failed during metrics generation"):
            stage.run(context)
