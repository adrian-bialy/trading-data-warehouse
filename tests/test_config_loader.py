"""
Module: test_config_loader

This module contains unit tests for the ConfigLoader class, ensuring that both input and source configuration
files are properly loaded and processed. The tests cover scenarios where the configuration files are missing
(as indicated by a FileNotFoundError) as well as successfully loading and parsing valid configuration files.
"""

from unittest.mock import patch, mock_open
import pytest
from src.tdw.config.configure import ConfigLoader


@pytest.fixture
def mock_config_file():
    """
    Return a multi-line string that simulates a configuration file in YAML format.

    The string includes two key-value pairs:

    Returns:
        str: A multi-line string representing the mock configuration file.
    """
    return """
    key1: value1
    key2: value2
    """


@pytest.fixture
def mock_source_config_file():
    """
    Returns a mock source configuration as a multiline string.

    This function simulates the content of a configuration file used for testing.
    The returned string is formatted in a YAML-like style and contains key-value pairs.

    :return: A mock configuration string representing source configurations.
    :rtype: str
    """
    return """
    source_key1: source_value1
    source_key2: source_value2
    """


def test_load_input_config_file_not_found():
    """
    Test that ConfigLoader.load_input_config() raises a FileNotFoundError when the configuration file is not found.

    This test verifies that attempting to load a configuration from a non-existent file path
    correctly triggers a FileNotFoundError, ensuring that the error message contains the
    specific file name.
    """
    loader = ConfigLoader(input_config_path="non_existent_config.yaml")
    with pytest.raises(FileNotFoundError, match="Configuration file not found: non_existent_config.yaml"):
        loader.load_input_config()


def test_load_input_config_success(mock_config_file):  # pylint: disable=redefined-outer-name
    """
    Test that ConfigLoader.load_input_config successfully reads a valid configuration file.

    This test uses a patched version of the built-in open function with a predefined YAML string
    and ensures that os.path.exists confirms the file's existence. It then verifies that the parsed
    configuration dictionary matches the expected output.
    """
    loader = ConfigLoader(input_config_path="config.yaml")
    with patch("builtins.open", mock_open(read_data=mock_config_file)), patch("os.path.exists", return_value=True):
        config = loader.load_input_config()
        assert config == {"key1": "value1", "key2": "value2"}


def test_load_source_config_file_not_found():
    """
    Test that load_source_config raises a FileNotFoundError when the source configuration file is missing.

    This test mocks os.path.exists to always return False, simulating the absence of the specified configuration file.
    It asserts that calling load_source_config with a non-existent source correctly triggers a FileNotFoundError,
    and that the error message includes "Source configuration file not found:".
    """
    loader = ConfigLoader()
    with patch("os.path.exists", return_value=False):
        with pytest.raises(FileNotFoundError, match="Source configuration file not found:"):
            loader.load_source_config("non_existent_source")


def test_load_source_config_success(mock_source_config_file):  # pylint: disable=redefined-outer-name
    """
    Test load_source_config successfully retrieves and parses the configuration.

    This test patches the built-in open function to simulate reading a configuration file containing
    the source-specific settings, and it patches os.path.exists to indicate that the config file exists.
    It verifies that ConfigLoader.load_source_config returns the expected dictionary of configuration
    values for a given source name.
    """
    loader = ConfigLoader()
    source_name = "test_source"
    with patch("builtins.open", mock_open(read_data=mock_source_config_file)):
        with patch("os.path.exists", return_value=True):
            config = loader.load_source_config(source_name)
        assert config == {"source_key1": "source_value1", "source_key2": "source_value2"}
