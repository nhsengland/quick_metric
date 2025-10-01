# API Reference

This section contains comprehensive documentation for the Quick Metric framework. The framework consists of five main modules that work together to provide a complete metrics generation solution.

## Modules Overview

### [Core Functions](core.md)
Main entry points for the framework including `generate_metrics()` and `interpret_metric_instructions()`. These functions orchestrate the complete workflow from configuration to results.

### [Method Definitions](method_definitions.md)
The `@metric_method` decorator and method registry system. This module handles registration and management of custom metric functions with thread-safe operations.

### [Filter](filter.md)
Sophisticated data filtering logic supporting complex YAML configurations with logical operators, comparisons, and nested conditions.

### [Apply Methods](apply_methods.md)  
Method execution engine with robust error handling and logging. Safely executes registered methods on filtered data with comprehensive error reporting.

### [Pipeline Integration](pipeline.md)
Seamless integration with oops-its-a-pipeline framework, enabling Quick Metric to be used within larger data processing workflows.

## Framework Architecture

```text
Configuration → Core → Filter → Apply Methods → Results
     ↓           ↓       ↓          ↓            ↓
  [YAML/Dict] → Parse → Subset → Execute → [Nested Dict]
```

### Pipeline Integration

```text
Pipeline Context → GenerateMetricsStage → Core Processing → Context Update
      ↓                    ↓                    ↓               ↓
[data, config] → [generate_metrics] → [filter + methods] → [context['metrics']]
```

## Quick API Overview

### Core Entry Points

```python
from quick_metric import generate_metrics, interpret_metric_instructions

# Main entry point (recommended)
results = generate_metrics(data, config)

# Legacy entry point  
results = interpret_metric_instructions(data, instructions)
```

### Method Registration

```python
from quick_metric import metric_method

@metric_method
def custom_metric(data, **kwargs):
    return len(data)
```

### Pipeline Usage

```python
from quick_metric.pipeline import create_metrics_stage

stage = create_metrics_stage()
pipeline.add_stage(stage)
```

## Getting Started with the API

Each module page contains:

- **Module Overview** - Description of the module's purpose and functionality
- **Functions** - Detailed documentation for all public functions
- **Classes** - Complete class documentation with methods and attributes  
- **Examples** - Practical usage examples for common scenarios
- **Parameters** - Comprehensive parameter documentation with types and descriptions
- **Returns** - Return value documentation with expected types and formats
- **Exceptions** - Custom exceptions and error handling information

Navigate to any module page above to explore the detailed API documentation.
