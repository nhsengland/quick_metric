# Pipeline Integration Guide

Learn how to integrate Quick Metric into data processing workflows using the `oops-its-a-pipeline` framework.

## Overview

Quick Metric provides seamless integration with `oops-its-a-pipeline` for building complex data processing workflows. This allows you to:

- Chain metrics generation with data preprocessing steps
- Use metrics results in downstream pipeline stages
- Handle configuration and data dependencies automatically
- Scale metrics processing within larger workflows

## Basic Pipeline Usage

### Simple Integration

```python
from oops_its_a_pipeline import Pipeline, PipelineConfig
from quick_metric.pipeline import create_metrics_stage

class Config(PipelineConfig):
    model_config = {'arbitrary_types_allowed': True}
    data = your_dataframe
    config = your_metrics_config

pipeline = Pipeline(Config()).add_stage(create_metrics_stage())
results = pipeline.run("analysis")

# Access metrics
metrics = results.context["metrics"]
```

### Advanced Pipeline Configuration

```python
# Multi-stage pipeline with data processing
pipeline = (Pipeline(config)
    .add_function_stage(load_data, outputs="raw_data")
    .add_function_stage(clean_data, inputs="raw_data", outputs="clean_data")
    .add_stage(create_metrics_stage(
        data_input="clean_data",
        config_input="metrics_config",
        metrics_output="business_metrics",
        output_format="flat_dataframe"
    ))
    .add_function_stage(save_results, inputs="business_metrics"))

results = pipeline.run("full_analysis")
```

## Configuration Options

### Stage Parameters

```python
create_metrics_stage(
    data_input="processed_data",      # Input DataFrame variable name
    config_input="metrics_config",    # Configuration variable name  
    metrics_output="calculated_metrics", # Output variable name
    name="business_metrics",          # Stage name for logging
    output_format="dataframe"         # Output format
)
```

### Using YAML Configuration Files

```python
stage = create_metrics_stage(
    data_input="clean_data",
    config_file_path="config/pipeline_metrics.yaml",
    metrics_output="pipeline_metrics"
)
```

## Error Handling

Pipeline stages include comprehensive error handling:

```python
try:
    results = pipeline.run("analysis")
except Exception as e:
    print(f"Pipeline failed: {e}")
    # Detailed error information available
```

## Best Practices

### Stage Naming

Use descriptive names for pipeline stages to improve debugging and monitoring:

```python
create_metrics_stage(
    name="customer_acquisition_metrics",
    data_input="customer_data",
    config_input="acquisition_config"
)
```

### Data Dependencies

Clearly define input/output dependencies between stages:

```python
# Stage 1: Data preprocessing
pipeline.add_function_stage(preprocess_data, 
                           inputs="raw_data", 
                           outputs="clean_data")

# Stage 2: Metrics calculation  
pipeline.add_stage(create_metrics_stage(
    data_input="clean_data",
    metrics_output="metrics_results"
))

# Stage 3: Results processing
pipeline.add_function_stage(analyze_metrics,
                           inputs="metrics_results",
                           outputs="final_analysis")
```

For complete pipeline documentation, see the [oops-its-a-pipeline documentation](https://github.com/datasciencecampus/oops-its-a-pipeline).
