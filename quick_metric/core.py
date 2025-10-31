"""
Main orchestration and workflow functionality for Quick Metric.

Provides entry points for the quick_metric framework, handling the complete
workflow from YAML configuration reading to metric result generation.

Functions
---------
read_metric_instructions : Load metric configurations from YAML files
interpret_metric_instructions : Execute complete metric workflow on data
generate_metrics : Main public API for generating metrics from data

Examples
--------
Load configuration from YAML file:

```python
from pathlib import Path
from quick_metric.core import read_metric_instructions

config_path = Path('metrics.yaml')
instructions = read_metric_instructions(config_path)
```

Execute complete workflow:

```python
import pandas as pd
from quick_metric.core import interpret_metric_instructions
from quick_metric.registry import metric_method

@metric_method
def count_records(data):
    return len(data)

data = pd.DataFrame({'category': ['A', 'B', 'A'], 'value': [1, 2, 3]})
config = {
    'category_metrics': {
        'method': ['count_records'],
        'filter': {'category': 'A'}
    }
}
store = interpret_metric_instructions(data, config)
print(store.value('category_metrics', 'count_records'))  # 2
```

YAML Configuration Format
--------------------------
```yaml
metric_instructions:
  split_by: 'region'  # Optional: split data by column(s)
  metric_name:
    method: ['method1', 'method2']
    filter:
      column_name: value
      and:
        condition1: value1
        condition2: value2
    split_by: ['region', 'site']  # Optional: override global split
```

See Also
--------
filter : Data filtering functionality used by this module
apply_methods : Method execution functionality used by this module
method_definitions : Method registration system used by this module
"""

from pathlib import Path
from typing import Optional, Union

from loguru import logger
import pandas as pd

from quick_metric._apply_methods import apply_methods
from quick_metric._config import normalize_method_specs, read_metric_instructions
from quick_metric._filter import apply_filter
from quick_metric._split import normalize_split_by, process_with_split
from quick_metric.exceptions import MetricSpecificationError, MetricSpecificationWarning
from quick_metric.registry import METRICS_METHODS
from quick_metric.store import MetricsStore


def interpret_metric_instructions(
    data: pd.DataFrame,
    metric_instructions: dict,
    metrics_methods: Optional[dict] = None,
) -> MetricsStore:
    """
    Apply filters and methods from metric instructions to a DataFrame.

    Creates MetricResult objects directly and returns them in a MetricsStore
    with proper dimension indexing.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame to be processed.
    metric_instructions : dict
        Dictionary containing the metrics and their filter/method conditions.
        May include a top-level 'split_by' key to apply globally.
    metrics_methods : dict, optional
        Dictionary of available methods. Defaults to METRICS_METHODS.

    Returns
    -------
    MetricsStore
        Store containing typed MetricResult objects with dimension indexing.

    Raises
    ------
    MetricSpecificationError
        If metric instructions are invalid or missing required keys.

    Examples
    --------
    Simple scalar metrics:
    ```python
    config = {
        'total_count': {
            'method': ['count_records'],
            'filter': {}
        }
    }
    store = interpret_metric_instructions(data, config)
    # ScalarResult: 1000
    ```

    With global split_by (scalar → series):
    ```python
    config = {
        'split_by': 'region',
        'total_count': {
            'method': ['count_records'],
            'filter': {}
        }
    }
    store = interpret_metric_instructions(data, config)
    # SeriesResult: region=[R1: 400, R2: 600]
    ```

    With multiple splits (scalar → dataframe):
    ```python
    config = {
        'split_by': ['region', 'site'],
        'total_count': {
            'method': ['count_records'],
            'filter': {}
        }
    }
    store = interpret_metric_instructions(data, config)
    # DataFrameResult with MultiIndex: (region, site)
    ```

    Metric-level override:
    ```python
    config = {
        'split_by': 'region',
        'total_count': {
            'method': ['count_records'],
            'filter': {},
            'split_by': ['region', 'site']  # Override with more granular split
        }
    }
    ```
    """
    if metrics_methods is None:
        metrics_methods = METRICS_METHODS

    logger.info(f"Processing {len(metric_instructions)} metrics on DataFrame with {len(data)} rows")

    # Basic validation
    if not isinstance(metric_instructions, dict):
        raise MetricSpecificationError(
            "metric_instructions must be a dictionary", method_spec=metric_instructions
        )

    if data.empty:
        logger.warning("Input DataFrame is empty")

    # Extract and normalize global split_by
    global_split_by = normalize_split_by(metric_instructions.get("split_by"))
    if global_split_by:
        logger.info(f"Global split_by configured: {global_split_by}")

    store = MetricsStore()

    for metric_name, metric_instruction in metric_instructions.items():
        # Skip the split_by key itself
        if metric_name == "split_by":
            continue

        with logger.contextualize(metric=metric_name):
            logger.trace("Processing metric")

            # Validate metric instruction structure
            if not isinstance(metric_instruction, dict):
                raise MetricSpecificationError(
                    f"Metric '{metric_name}' instruction must be a dictionary",
                    method_spec=metric_instruction,
                )

            if "method" not in metric_instruction:
                raise MetricSpecificationError(
                    f"Metric '{metric_name}' missing required 'method' key",
                    method_spec=metric_instruction,
                )

            # Determine split_by for this metric (metric-level overrides global)
            split_by = normalize_split_by(metric_instruction.get("split_by", global_split_by))

            if split_by:
                logger.debug(f"Splitting by: {split_by}")

            # Apply filter
            if "filter" not in metric_instruction:
                MetricSpecificationWarning(
                    metric_name,
                    "No 'filter' key specified",
                    "All data will be used. Consider adding 'filter: {}' to be explicit",
                )
                filter_spec = {}
            else:
                filter_spec = metric_instruction["filter"]

            filtered_data = apply_filter(data_df=data, filters=filter_spec)
            logger.trace(f"Filtered to {len(filtered_data)} rows")

            # Normalize method specifications
            normalized_methods = normalize_method_specs(metric_instruction["method"])

            # Process with or without splitting
            if split_by:
                process_with_split(
                    data=filtered_data,
                    split_by=split_by,
                    method_specs=normalized_methods,
                    metrics_methods=metrics_methods,
                    store=store,
                    metric_name=metric_name,
                )
            else:
                with logger.contextualize(methods=normalized_methods):
                    apply_methods(
                        data=filtered_data,
                        method_specs=normalized_methods,
                        metrics_methods=metrics_methods,
                        store=store,
                        metric_name=metric_name,
                    )

            logger.success("Metric completed successfully")

    logger.success(f"Successfully processed all {len(metric_instructions)} metrics")
    return store


def generate_metrics(
    data: pd.DataFrame,
    config: Union[Path, dict],
    metrics_methods: Optional[dict] = None,
) -> MetricsStore:
    """
    Generate metrics from data and return a MetricsStore.

    Dimensions are intrinsic to the data returned by methods - they are NOT
    passed as global parameters.

    This is the primary entry point for the quick_metric framework. It executes
    metric methods on filtered data and returns a MetricsStore containing typed
    results (Scalar, Series, DataFrame).

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame to process
    config : Path or dict
        Path to a YAML configuration file or a dictionary of metric instructions.

        Expected dict structure:
        ```python
        {
            'metric_name': {
                'method': ['method1', 'method2'],  # or single string
                'filter': {...}  # filter specification
            }
        }
        ```
    metrics_methods : dict, optional
        Custom metrics methods registry. If None, uses the global registry.

    Returns
    -------
    MetricsStore
        Store containing all metric results as typed MetricResult objects.
        Results contain dimensions intrinsic to the data structure:
        - ScalarResult: no dimensions
        - SeriesResult: one dimension (the index)
        - DataFrameResult: N dimensions (all columns except value_column)

    Examples
    --------
    Using a dictionary configuration:

    ```python
    import pandas as pd
    from quick_metric import generate_metrics, metric_method

    @metric_method
    def count_records(data):
        return len(data)

    data = pd.DataFrame({'category': ['A', 'B', 'A'], 'value': [1, 2, 3]})
    config = {
        'category_a_count': {
            'method': ['count_records'],
            'filter': {'category': 'A'}
        }
    }

    store = generate_metrics(data, config)

    # Access the value
    count = store.value('category_a_count', 'count_records')  # 2

    # Or get the result object
    result = store['category_a_count', 'count_records']
    print(result.data)  # 2
    ```

    Working with dimensional data:

    ```python
    @metric_method
    def count_by_category(data):
        # Returns Series with category as dimension
        return data.groupby('category').size()

    store = generate_metrics(data, {'counts': {'method': ['count_by_category']}})

    result = store['counts', 'count_by_category']
    print(result.dimensions())  # ['category']
    print(result.data)  # Series with category index
    ```

    Export to different formats:

    ```python
    # As nested dict
    nested = store.to_nested_dict()
    # {'category_a_count': {'count_records': 2}}

    # As flat DataFrame
    df = store.to_dataframe()
    # metric            method          value  date     site
    # category_a_count  count_records   2      2025-01  R0A
    ```

    Using a YAML file:

    ```python
    from pathlib import Path
    config_path = Path('my_metrics.yaml')
    store = generate_metrics(data, config_path)
    ```

    Raises
    ------
    FileNotFoundError
        If the config path does not exist.
    MetricSpecificationError
        If a YAML file doesn't contain 'metric_instructions' key or is invalid.
        If config parameter is not a valid type.
    """
    logger.info("Starting metric generation")

    # Handle different config input types
    if isinstance(config, Path):
        logger.debug(f"Loading configuration from file: {config}")
        metric_instructions = read_metric_instructions(config)
    elif isinstance(config, dict):
        logger.debug("Using provided dictionary configuration")
        metric_instructions = config
    else:
        raise MetricSpecificationError(
            f"Config must be a pathlib.Path object or dict, got {type(config)}", method_spec=config
        )

    # Generate metrics and return MetricsStore directly
    store = interpret_metric_instructions(
        data=data,
        metric_instructions=metric_instructions,
        metrics_methods=metrics_methods,
    )

    logger.success(f"Generated {len(store)} metric results")
    return store
