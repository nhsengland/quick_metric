# Pipeline Integration

The pipeline module provides seamless integration with the oops-its-a-pipeline framework, enabling Quick Metric to be used within complex data processing workflows.

## Classes

### GenerateMetricsStage

::: quick_metric.pipeline.GenerateMetricsStage
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

A pipeline stage that wraps Quick Metric functionality for use in oops-its-a-pipeline workflows. Handles configuration, validation, and metrics generation within pipeline contexts.

**Key Features:**
- Thread-safe metrics generation within pipeline contexts
- Flexible input/output naming for pipeline variable mapping
- Comprehensive error handling with pipeline-specific exceptions
- Support for multiple configuration formats
- Optional custom metrics methods injection

**Examples:**

Basic usage:
```python
from quick_metric.pipeline import GenerateMetricsStage

stage = GenerateMetricsStage()
pipeline.add_stage(stage)
```

Custom configuration:
```python
stage = GenerateMetricsStage(
    data_input="processed_data",
    config_input="analysis_config",
    metrics_output="business_metrics",
    name="kpi_analysis"
)
```

## Functions

### create_metrics_stage()

::: quick_metric.pipeline.create_metrics_stage
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

Convenience factory function for creating GenerateMetricsStage instances. This is the recommended approach for creating metrics stages.

**Examples:**

Method chaining pipeline:
```python
from oops_its_a_pipeline import Pipeline
from quick_metric.pipeline import create_metrics_stage

pipeline = (Pipeline(config)
    .add_function_stage(load_data, outputs="data")
    .add_function_stage(prepare_config, outputs="config")
    .add_stage(create_metrics_stage())
    .add_function_stage(save_results, inputs="metrics"))
```

Custom parameters:
```python
stage = create_metrics_stage(
    data_input="clean_data",
    config_input="metrics_definitions",
    metrics_output="calculated_kpis"
)
```

## Pipeline Usage Patterns

### Basic Pipeline

```python
from oops_its_a_pipeline import Pipeline, PipelineConfig
from quick_metric.pipeline import create_metrics_stage
import pandas as pd

class Config(PipelineConfig):
    model_config = {'arbitrary_types_allowed': True}
    data: pd.DataFrame = your_dataframe
    config: dict = your_metrics_config

pipeline = Pipeline(Config()).add_stage(create_metrics_stage())
results = pipeline.run("analysis")
```

### Multi-Stage Pipeline

```python
def load_data():
    return pd.read_csv("data.csv")

def prepare_metrics_config():
    return {
        'performance_metrics': {
            'method': ['count_records', 'mean_value'],
            'filter': {'status': 'active'}
        }
    }

def save_results(metrics):
    # Save to database or file
    pass

pipeline = (Pipeline()
    .add_function_stage(load_data, outputs="data")
    .add_function_stage(prepare_metrics_config, outputs="config")
    .add_stage(create_metrics_stage())
    .add_function_stage(save_results, inputs="metrics"))
```

### Pipeline with Custom Methods

```python
from quick_metric import metric_method

@metric_method
def efficiency_score(data):
    return data['completed'].sum() / len(data) * 100

# Custom methods are automatically available in pipelines
stage = create_metrics_stage(
    metrics_methods_input="custom_methods"  # Optional: reference additional methods
)
```

## Configuration Handling

The pipeline stage accepts configurations in multiple formats:

### Dictionary Configuration
```python
config = {
    'metric_name': {
        'method': ['count_records'],
        'filter': {'column': 'value'}
    }
}
```

### YAML File Path
```python
from pathlib import Path
config = Path('metrics.yaml')
```

### PipelineConfig Object
```python
class MetricsConfig(PipelineConfig):
    config: dict = your_config_dict
```

## Error Handling

Pipeline stages wrap all errors in `PipelineStageValidationError` with detailed context:

```python
from oops_its_a_pipeline.exceptions import PipelineStageValidationError

try:
    results = pipeline.run("analysis")
except PipelineStageValidationError as e:
    print(f"Pipeline failed at stage '{e.stage_name}': {e}")
    # Error includes stage name and operation context
```

## Thread Safety

The pipeline integration is fully thread-safe and can be used in concurrent pipeline execution environments. The underlying Quick Metric components handle concurrent access appropriately.

## Best Practices

1. **Use create_metrics_stage()**: Prefer the factory function over direct class instantiation
2. **Meaningful stage names**: Use descriptive names for logging and debugging
3. **Clear variable mapping**: Use descriptive input/output variable names
4. **Configuration validation**: Validate configurations before pipeline execution
5. **Error handling**: Implement appropriate error handling for production pipelines