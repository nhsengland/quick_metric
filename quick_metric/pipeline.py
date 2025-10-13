"""
Pipeline integration for oops-its-a-pipeline framework.

Pipeline stages that wrap quick_metric functionality for use within
oops-its-a-pipeline workflows with flexible input/output mapping
and comprehensive error handling.

Classes
-------
GenerateMetricsStage : Pipeline stage class for metrics generation

Functions
---------
create_metrics_stage : Convenience factory function for creating stages

Examples
--------
Basic pipeline stage creation:

```python
from quick_metric.pipeline import create_metrics_stage
from oops_its_a_pipeline import Pipeline

stage = create_metrics_stage()
pipeline = Pipeline(config).add_stage(stage)
results = pipeline.run("pipeline_run")
```

Custom input/output mapping:

```python
stage = create_metrics_stage(
    data_input="processed_data",
    config_input="metric_definitions",
    metrics_output="business_metrics"
)
```
"""

from pathlib import Path
from typing import Optional

from loguru import logger
from oops_its_a_pipeline import PipelineStage
from oops_its_a_pipeline.exceptions import PipelineStageValidationError
import pandas as pd

from quick_metric._core import generate_metrics


class GenerateMetricsStage(PipelineStage):
    """
    Pipeline stage for generating metrics using quick_metric framework.

    This stage wraps the `generate_metrics` function to be used within
    oops-its-a-pipeline workflows. It takes a DataFrame and configuration
    as inputs and produces calculated metrics as output.

    The stage handles multiple configuration formats, validates inputs,
    and provides comprehensive error reporting. It's designed to be
    thread-safe and integrates seamlessly with the pipeline logging system.

    Parameters
    ----------
    data_input : str, default "data"
        Name of the context variable containing the pandas DataFrame.
        The DataFrame should contain the data to be analyzed.
    config_input : str, default "config"
        Name of the context variable containing the metrics configuration.
        Can be a Path to YAML file, dict with config, or PipelineConfig object.
    metrics_methods_input : str, optional
        Name of the context variable containing custom metrics methods.
        If provided, these methods will be available in addition to
        globally registered methods. If None, uses only registered methods.
    metrics_output : str, default "metrics"
        Name to assign to the generated metrics results in the context.
        Results are stored as a nested dictionary structure.
    name : str, optional
        Custom name for this stage for logging and identification.
        If None, uses "generate_metrics".

    Attributes
    ----------
    data_input : str
        The name of the input data variable
    config_input : str
        The name of the configuration variable
    metrics_methods_input : str or None
        The name of the custom methods variable
    metrics_output : str
        The name of the output metrics variable

    Raises
    ------
    PipelineStageValidationError
        If input data is not a pandas DataFrame, if configuration is invalid,
        or if metrics generation fails for any reason.

    Notes
    -----
    The stage automatically handles different configuration object types:
    - If config has a 'config' attribute, extracts it
    - If config is a Path, passes it through for YAML loading
    - If config is a dict, uses it directly
    - Otherwise raises a validation error

    Thread Safety
    -------------
    This stage is thread-safe and can be used in concurrent pipeline
    execution environments. The underlying metrics generation is also
    thread-safe through the MetricRegistry locking mechanism.

    Examples
    --------
    Creating a stage with default parameters:

    ```python
    stage = GenerateMetricsStage()
    pipeline.add_stage(stage)
    ```

    Creating a stage with custom input/output mapping:

    ```python
    stage = GenerateMetricsStage(
        data_input="raw_data",
        config_input="analysis_config",
        metrics_output="business_metrics",
        name="business_analysis"
    )
    ```

    Using with custom methods:

    ```python
    stage = GenerateMetricsStage(
        metrics_methods_input="domain_specific_methods"
    )
    ```

    Complete pipeline example:

    ```python
    from oops_its_a_pipeline import Pipeline, PipelineConfig
    import pandas as pd

    class MetricsConfig(PipelineConfig):
        data: pd.DataFrame = pd.DataFrame({
            'category': ['A', 'B'], 'value': [1, 2]
        })
        config: dict = {
            'test_metric': {'method': ['count'], 'filter': {}}
        }

    stage = GenerateMetricsStage()
    pipeline = Pipeline(MetricsConfig())
    pipeline.add_stage(stage)
    results = pipeline.run("metrics_run")
    print(results['metrics'])
    ```

    See Also
    --------
    create_metrics_stage : Convenience factory function
    quick_metric._core.generate_metrics : Underlying metrics function
    oops_its_a_pipeline.PipelineStage : Base class
    """

    def __init__(
        self,
        data_input: str = "data",
        config_input: str = "config",
        metrics_methods_input: Optional[str] = None,
        metrics_output: str = "metrics",
        name: Optional[str] = None,
    ):
        """
        Initialize the GenerateMetricsStage.

        Parameters
        ----------
        data_input : str, default "data"
            Name of the context variable containing the pandas DataFrame.
        config_input : str, default "config"
            Name of the context variable containing the metrics configuration
            (either a Path to YAML file or a dictionary).
        metrics_methods_input : str, optional
            Name of the context variable containing custom metrics methods.
            If None, uses the default registered methods.
        metrics_output : str, default "metrics"
            Name to assign to the generated metrics results in the context.
        name : str, optional
            Custom name for this stage. If None, uses "generate_metrics".
        """
        # Build inputs tuple
        inputs = [data_input, config_input]
        if metrics_methods_input:
            inputs.append(metrics_methods_input)

        super().__init__(
            inputs=tuple(inputs),
            outputs=metrics_output,
            name=name or "generate_metrics",
        )

        self.data_input = data_input
        self.config_input = config_input
        self.metrics_methods_input = metrics_methods_input
        self.metrics_output = metrics_output

    def run(self, context) -> object:
        """
        Execute the metrics generation stage.

        Parameters
        ----------
        context : PipelineContext
            Runtime context containing input data and configuration.

        Returns
        -------
        PipelineContext
            Updated context with metrics results.

        Raises
        ------
        PipelineStageValidationError
            If input data is not a pandas DataFrame or if metrics
            generation fails.
        """
        logger.debug(f"Executing {self.name} stage")

        try:
            # Extract inputs from context
            data = context[self.data_input]
            config = context[self.config_input]

            # Handle different config types - extract dict if it's a config object
            if hasattr(config, "config") and isinstance(config.config, dict):
                config = config.config
            elif hasattr(config, "__dict__") and not isinstance(config, (dict, Path)):
                # If it's a config object, try to extract the config attribute
                if hasattr(config, "config"):
                    config = config.config
                else:
                    error_msg = f"Config object must have 'config' attribute, got {type(config)}"
                    raise ValueError(error_msg)

            # Validate data input
            if not isinstance(data, pd.DataFrame):
                raise PipelineStageValidationError(
                    f"Stage '{self.name}': Expected pandas DataFrame for "
                    f"'{self.data_input}', got {type(data)}"
                )

            # Get optional metrics methods
            metrics_methods = None
            if self.metrics_methods_input:
                metrics_methods = context.get(self.metrics_methods_input)

            logger.info(f"Generating metrics for DataFrame with {len(data)} rows")

            # Generate metrics using the core function
            results = generate_metrics(data=data, config=config, metrics_methods=metrics_methods)

            # Store results in context
            context[self.metrics_output] = results

            logger.success(f"Generated {len(results)} metrics successfully")

        except PipelineStageValidationError:
            # Re-raise pipeline-specific validation errors
            raise
        except Exception as error:
            logger.error(f"Metrics generation failed: {str(error)}")
            raise PipelineStageValidationError(
                f"Stage '{self.name}' failed during metrics generation: {str(error)}"
            ) from error

        return context


def create_metrics_stage(
    data_input: str = "data",
    config_input: str = "config",
    metrics_methods_input: Optional[str] = None,
    metrics_output: str = "metrics",
    name: Optional[str] = None,
) -> GenerateMetricsStage:
    """
    Convenience function to create a GenerateMetricsStage.

    This function provides a more concise way to create a metrics generation
    stage for use in pipeline method chaining. It's the recommended way to
    create metrics stages as it provides a clean, functional interface.

    The function acts as a factory, creating and configuring a
    GenerateMetricsStage instance with the specified parameters. This is
    particularly useful in method-chaining pipeline construction patterns.

    Parameters
    ----------
    data_input : str, default "data"
        Name of the context variable containing the pandas DataFrame.
        This should reference a variable in the pipeline context that
        contains the data to be analyzed.
    config_input : str, default "config"
        Name of the context variable containing the metrics configuration.
        Can reference a Path to YAML file, a dictionary with metric
        definitions, or a PipelineConfig object with a 'config' attribute.
    metrics_methods_input : str, optional
        Name of the context variable containing custom metrics methods.
        If provided, these methods will be merged with globally registered
        methods. The variable should contain a dict mapping method names
        to callable functions. If None, only uses registered methods.
    metrics_output : str, default "metrics"
        Name to assign to the generated metrics results in the context.
        The results will be stored as a nested dictionary structure
        where keys are metric names and values are method results.
    name : str, optional
        Custom name for this stage for logging and pipeline visualization.
        If None, the stage will use "generate_metrics" as its name.

    Returns
    -------
    GenerateMetricsStage
        Configured metrics generation stage ready to be added to a pipeline.
        The stage is fully initialized and can be used immediately.

    Notes
    -----
    This function is the preferred way to create metrics stages as it:
    - Provides a clean, functional interface
    - Works well with method chaining
    - Reduces boilerplate code
    - Maintains consistency across projects

    The returned stage can be used in both declarative and method-chaining
    pipeline construction patterns.

    Examples
    --------
    Basic usage with default parameters:

    ```python
    from quick_metric.pipeline import create_metrics_stage
    stage = create_metrics_stage()
    pipeline.add_stage(stage)
    ```

    Custom input/output mapping:

    ```python
    stage = create_metrics_stage(
        data_input="processed_data",
        config_input="metric_definitions",
        metrics_output="business_metrics"
    )
    ```

    Method chaining pipeline construction:

    ```python
    from oops_its_a_pipeline import Pipeline
    pipeline = (Pipeline(config)
        .add_function_stage(load_data, outputs="data")
        .add_function_stage(load_config, outputs="metrics_config")
        .add_stage(create_metrics_stage(
            config_input="metrics_config",
            metrics_output="calculated_metrics"
        ))
        .add_function_stage(save_results, inputs="calculated_metrics"))
    ```

    With custom methods and naming:

    ```python
    stage = create_metrics_stage(
        metrics_methods_input="domain_methods",
        name="domain_analysis",
        metrics_output="domain_metrics"
    )
    ```

    Multiple metrics stages in one pipeline:

    ```python
    pipeline = (Pipeline(config)
        .add_stage(create_metrics_stage(
            config_input="basic_config",
            metrics_output="basic_metrics",
            name="basic_analysis"
        ))
        .add_stage(create_metrics_stage(
            config_input="advanced_config",
            metrics_output="advanced_metrics",
            name="advanced_analysis"
        )))
    ```

    See Also
    --------
    GenerateMetricsStage : The underlying stage class
    quick_metric._core.generate_metrics : Core metrics generation function
    oops_its_a_pipeline.Pipeline : Pipeline construction
    quick_metric._method_definitions.metric_method : Method registration
    """
    return GenerateMetricsStage(
        data_input=data_input,
        config_input=config_input,
        metrics_methods_input=metrics_methods_input,
        metrics_output=metrics_output,
        name=name,
    )
