# Getting Started

This guide will help you get started with **Quick Metric** and begin using the framework for your analytical workflows.

## Installation

### Using pip

    pip install quick-metric

### Using uv (recommended)

    uv add quick-metric

## Basic Usage

### 1. Import the Framework

    from quick_metric import method_definitions, apply_methods, filter, interpret_instructions

### 2. Define a Simple Method

    def calculate_mean(data, **kwargs):
        """Calculate the mean of a dataset."""
        return data.mean()
    
    # Create method definition
    mean_method = method_definitions.create_method(
        name="mean_calculation",
        function=calculate_mean,
        description="Calculate mean values from dataset"
    )

### 3. Apply the Method

    import pandas as pd
    
    # Sample data
    data = pd.DataFrame({'values': [1, 2, 3, 4, 5]})
    
    # Apply method
    result = apply_methods.execute_method(mean_method, data)
    print(f"Mean value: {result}")

## Key Concepts

### Method Definitions
Methods are the core analytical functions that you want to apply to your data. They are defined with standardized metadata including name, description, and parameters.

### Filtering Logic
The framework can intelligently determine when methods should be applied based on data characteristics, method requirements, and user-defined criteria.

### Method Application
Execute methods on datasets with built-in error handling, logging, and result validation.

### Instruction Interpretation
Parse and validate method instructions, parameters, and configurations to ensure methods are applied correctly.

## Next Steps

- Explore the [API Reference](api_reference/index.md) for detailed documentation
- Review specific modules for your use case:
    - [Method Definitions](api_reference/method_definitions.md) - Core method creation and management
    - [Apply Methods](api_reference/apply_methods.md) - Method execution and error handling
    - [Filter](api_reference/filter.md) - Filtering logic and conditions
    - [Interpret Instructions](api_reference/interpret_instructions.md) - Parameter parsing and validation
