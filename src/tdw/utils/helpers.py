"""
Utility functions module.

This module provides common utility functions used across the application.
"""


def filter_sources(sources, config):
    """
    Filters sources and their datasets based on the enabled flags in the configuration.

    :param sources: List of source objects.
    :param config: Configuration dictionary containing 'sources' with their enabled states.
    :return: A list of filtered source objects with only the enabled datasets.
    """
    enabled_sources = []
    for source in sources:
        source_config = config.get("sources", {}).get(source.name, {})
        if source_config.get("enabled", False):
            datasets_config = source_config.get("datasets", {})
            source.datasets = [d for d in source.datasets if datasets_config.get(d.name, {}).get("enabled", False)]
            enabled_sources.append(source)
    return enabled_sources


def filter_datasets(sources, dataset_name):
    """
    Filters a list of sources when dataset_name is in "source.dataset" format.
    If dataset_name doesn't contain a dot, returns sources unchanged.

    Args:
        sources (list): A list of source objects.
        dataset_name (str): A string possibly in "source.dataset" format.

    Returns:
        list: A filtered list of sources.
    """
    if "." in dataset_name:
        source_filter, dataset_filter = dataset_name.split(".", 1)
        filtered_sources = []
        for source in sources:
            if source.name == source_filter:
                filtered_datasets = [ds for ds in source.datasets if ds.name == dataset_filter]
                if filtered_datasets:
                    source.datasets = filtered_datasets
                    filtered_sources.append(source)
        return filtered_sources
    return sources


def replace_api_key(obj, api_key_value):
    """
    Recursively replaces all occurrences of the placeholder "{api_key}" in a given object
    with the provided api_key_value. The function traverses nested structures, handling
    dictionaries and lists by processing their elements recursively.
    For string values, it replaces the placeholder with api_key_value. All other data types are returned unchanged.

    Args:
        obj: The input object which can be a dictionary, list, or any other type. If it's a dictionary or list,
             the function will recursively process its elements.
        api_key_value (str): The string that will replace any occurrence of "{api_key}" found in string values.

    Returns:
        The processed object with the "{api_key}" placeholder replaced by api_key_value in all string values.
    """
    if isinstance(obj, dict):
        return {k: replace_api_key(v, api_key_value) for k, v in obj.items()}
    if isinstance(obj, list):
        return [replace_api_key(item, api_key_value) for item in obj]
    if isinstance(obj, str):
        return obj.replace("{api_key}", api_key_value)
    return obj
