"""
Tests for pipeline integration functionality.

Tests the GenerateMetricsStage and create_metrics_stage function to ensure
they work correctly with the oops-its-a-pipeline framework.
"""

import pandas as pd
import pytest

from quick_metric.method_definitions import metric_method

# Skip all tests if oops-its-a-pipeline is not available
pytest_plugins = []

try:
    from oops_its_a_pipeline import Pipeline, PipelineConfig, PipelineContext
    from quick_metric.pipeline import (
        GenerateMetricsStage,
        create_metrics_stage,
    )

    PIPELINE_AVAILABLE = True
except ImportError:
    PIPELINE_AVAILABLE = False


@pytest.mark.skipif(not PIPELINE_AVAILABLE, reason="oops-its-a-pipeline not available")
class TestGenerateMetricsStage:
    """Test GenerateMetricsStage functionality."""

    @pytest.mark.parametrize(
        ("attribute", "expected_value"),
        [
            ("name", "generate_metrics"),
            ("input_keys", ("data", "config")),
            ("output_keys", ("metrics",)),
        ],
    )
    def test_stage_creation(self, attribute, expected_value):
        """Test creating a GenerateMetricsStage."""
        stage = GenerateMetricsStage()
        assert getattr(stage, attribute) == expected_value

    @pytest.mark.parametrize(
        ("attribute", "expected_value"),
        [
            ("name", "custom_metrics"),
            ("input_keys", ("df", "metrics_config")),
            ("output_keys", ("results",)),
        ],
    )
    def test_stage_with_custom_parameters(self, attribute, expected_value):
        """Test creating stage with custom input/output parameters."""
        stage = GenerateMetricsStage(
            data_input="df",
            config_input="metrics_config",
            metrics_output="results",
            name="custom_metrics",
        )
        assert getattr(stage, attribute) == expected_value

    def test_stage_with_metrics_methods_input(self):
        """Test creating stage with custom metrics methods input."""
        stage = GenerateMetricsStage(metrics_methods_input="custom_methods")

        assert stage.input_keys == ("data", "config", "custom_methods")
        assert stage.output_keys == ("metrics",)

    def test_stage_execution_with_dict_config(self):
        """Test executing stage with dictionary configuration."""

        @metric_method
        def test_count(data):
            return len(data)

        # Create test data
        test_data = pd.DataFrame({"category": ["A", "B", "A", "C"], "value": [1, 2, 3, 4]})

        config = {
            "category_a_count": {
                "method": ["test_count"],
                "filter": {"category": "A"},
            },
            "total_count": {"method": ["test_count"], "filter": {}},
        }

        # Create and execute stage
        stage = GenerateMetricsStage()

        class TestConfig(PipelineConfig):
            pass

        context = PipelineContext(TestConfig(), "test_run")
        context["data"] = test_data
        context["config"] = config

        result_context = stage.run(context)

        # Check that metrics key exists
        assert "metrics" in result_context

    @pytest.mark.parametrize("metric_name", ["category_a_count", "total_count"])
    def test_stage_execution_contains_expected_metrics(self, metric_name):
        """Test that execution results contain expected metric keys."""

        @metric_method
        def test_count(data):
            return len(data)

        test_data = pd.DataFrame({"category": ["A", "B", "A", "C"], "value": [1, 2, 3, 4]})

        config = {
            "category_a_count": {
                "method": ["test_count"],
                "filter": {"category": "A"},
            },
            "total_count": {"method": ["test_count"], "filter": {}},
        }

        stage = GenerateMetricsStage()

        class TestConfig(PipelineConfig):
            pass

        context = PipelineContext(TestConfig(), "test_run")
        context["data"] = test_data
        context["config"] = config

        result_context = stage.run(context)
        metrics = result_context["metrics"]
        assert metric_name in metrics

    @pytest.mark.parametrize(
        ("metric_name", "expected_count"),
        [
            ("category_a_count", 2),  # Two 'A' records
            ("total_count", 4),  # All records
        ],
    )
    def test_stage_execution_metric_values(self, metric_name, expected_count):
        """Test that execution results have correct metric values."""

        @metric_method
        def test_count(data):
            return len(data)

        test_data = pd.DataFrame({"category": ["A", "B", "A", "C"], "value": [1, 2, 3, 4]})

        config = {
            "category_a_count": {
                "method": ["test_count"],
                "filter": {"category": "A"},
            },
            "total_count": {"method": ["test_count"], "filter": {}},
        }

        stage = GenerateMetricsStage()

        class TestConfig(PipelineConfig):
            pass

        context = PipelineContext(TestConfig(), "test_run")
        context["data"] = test_data
        context["config"] = config

        result_context = stage.run(context)
        metrics = result_context["metrics"]
        assert metrics[metric_name]["test_count"] == expected_count

    def test_stage_execution_invalid_data_type(self):
        """Test stage fails with invalid data type."""
        stage = GenerateMetricsStage()

        class TestConfig(PipelineConfig):
            pass

        context = PipelineContext(TestConfig(), "test_run")
        context["data"] = "not_a_dataframe"  # Invalid data type
        context["config"] = {}

        # Should raise validation error
        with pytest.raises(Exception) as exc_info:
            stage.run(context)

        assert "Expected pandas DataFrame" in str(exc_info.value)

    def test_stage_execution_with_metrics_generation_error(self):
        """Test stage handles metrics generation errors properly."""
        stage = GenerateMetricsStage()

        class TestConfig(PipelineConfig):
            pass

        context = PipelineContext(TestConfig(), "test_run")
        context["data"] = pd.DataFrame({"test": [1, 2, 3]})
        context["config"] = {
            "invalid_metric": {
                "method": ["nonexistent_method"],  # This should cause an error
                "filter": {},
            }
        }

        # Should raise validation error
        with pytest.raises(Exception) as exc_info:
            stage.run(context)

        assert "failed during metrics generation" in str(exc_info.value)


@pytest.mark.skipif(not PIPELINE_AVAILABLE, reason="oops-its-a-pipeline not available")
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

    def test_create_metrics_stage_custom_parameters(self):
        """Test creating stage with custom parameters."""
        stage = create_metrics_stage(
            data_input="input_df",
            config_input="metric_config",
            metrics_output="calculated_metrics",
            name="custom_stage",
        )
        assert isinstance(stage, GenerateMetricsStage)

    @pytest.mark.parametrize(
        ("attribute", "expected_value"),
        [
            ("name", "custom_stage"),
            ("input_keys", ("input_df", "metric_config")),
            ("output_keys", ("calculated_metrics",)),
        ],
    )
    def test_create_metrics_stage_custom_attributes(self, attribute, expected_value):
        """Test custom stage attributes."""
        stage = create_metrics_stage(
            data_input="input_df",
            config_input="metric_config",
            metrics_output="calculated_metrics",
            name="custom_stage",
        )
        assert getattr(stage, attribute) == expected_value


@pytest.mark.skipif(not PIPELINE_AVAILABLE, reason="oops-its-a-pipeline not available")
class TestPipelineIntegration:
    """Test integration with oops-its-a-pipeline Pipeline."""

    def test_full_pipeline_execution(self):
        """Test complete pipeline execution with metrics generation."""

        # Register a test method
        @metric_method
        def pipeline_test_count(data):
            return len(data)

        # Create test data and config
        test_data = pd.DataFrame({"category": ["X", "Y", "X"], "value": [10, 20, 30]})

        test_config = {
            "x_category_count": {
                "method": ["pipeline_test_count"],
                "filter": {"category": "X"},
            }
        }

        # Create pipeline config
        class MetricsConfig(PipelineConfig):
            model_config = {"arbitrary_types_allowed": True}

            data: pd.DataFrame = test_data
            config: dict = test_config

        # Create and run pipeline
        pipeline = Pipeline(MetricsConfig())
        pipeline.add_stage(create_metrics_stage())

        result = pipeline.run("test_pipeline_run")

        # Verify results
        assert "metrics" in result
        metrics = result["metrics"]
        assert "x_category_count" in metrics
        assert metrics["x_category_count"]["pipeline_test_count"] == 2

    def test_pipeline_method_chaining(self):
        """Test pipeline with method chaining and multiple stages."""

        # Register a test method
        @metric_method
        def chain_test_sum(data):
            return data["value"].sum()

        def create_test_data():
            return pd.DataFrame({"type": ["A", "B", "A"], "value": [100, 200, 300]})

        def create_test_config():
            return {"type_a_sum": {"method": ["chain_test_sum"], "filter": {"type": "A"}}}

        def validate_results(metrics):
            # 100 + 300 = 400 (sum of 'A' type values)
            assert metrics["type_a_sum"]["chain_test_sum"] == 400
            return "validation_passed"

        # Create pipeline config (no initial data needed)
        class EmptyConfig(PipelineConfig):
            pass

        # Create pipeline and add stages
        # Note: add_stage returns None, so don't chain
        pipeline = Pipeline(EmptyConfig())
        pipeline.add_function_stage(create_test_data, outputs="data")
        pipeline.add_function_stage(create_test_config, outputs="config")
        pipeline.add_stage(create_metrics_stage())  # This returns None
        pipeline.add_function_stage(validate_results, inputs="metrics", outputs="validation")

        result = pipeline.run("chain_test_run")

        # Verify pipeline completed successfully
        assert result["validation"] == "validation_passed"
        assert "metrics" in result
        assert "data" in result
        assert "config" in result
