"""
Module: configure

This module provides functionality to load YAML configuration files for the
application. It contains a single class, ConfigLoader, that is responsible for
reading both the primary configuration file ("config.yaml" by default) and
source-specific configuration files located in designated directories.
"""

import os
import yaml


class ConfigLoader:
    """
    A configuration loader that provides methods for loading YAML config files.

    Attributes:
        input_config_path (str):
            The file path for the primary configuration file
            (defaults to "config.yaml").
        input_config (dict or None):
            Stores the configuration data loaded from the primary configuration file.

    Methods:
        load_input_config() -> dict:
            Loads and returns the primary configuration from the YAML file
            specified by input_config_path. Raises a FileNotFoundError if
            the file does not exist.
        load_source_config(source_name: str) -> dict:
            Constructs the file path for a source-specific configuration file
            located in the "tdw/ingest/datasets/{source_name}" directory.
            Loads and returns the configuration from the YAML file at that
            location. Raises a FileNotFoundError if the source configuration file
            does not exist.
    """

    def __init__(self, input_config_path: str = "config.yaml"):
        self.input_config_path = input_config_path
        self.input_config = None

    def load_input_config(self) -> dict:
        """
        Load and return the input configuration from a YAML file.

        This method reads the configuration file located at self.input_config_path. If the file does not exist,
        a FileNotFoundError is raised. Otherwise, the configuration is loaded using yaml.safe_load and stored in
        self.input_config.

        Returns:
            dict: The configuration data loaded from the YAML file.

        Raises:
            FileNotFoundError: If the configuration file is not found at self.input_config_path.
        """
        if not os.path.exists(self.input_config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.input_config_path}")

        with open(self.input_config_path, "r", encoding="utf-8") as f:
            self.input_config = yaml.safe_load(f)
        return self.input_config

    def load_source_config(self, source_name: str) -> dict:
        """
        Loads configuration for a specified source by reading its YAML config file.

        Constructs the file path by navigating three directories up from the current file,
        then appending "tdw/ingest/datasets/{source_name}/config.yaml". It then opens and parses
        the YAML configuration file, returning the result as a dictionary.

        Parameters:
            source_name (str): Name of the source configuration to load.

        Returns:
            dict: The configuration settings loaded from the YAML file.

        Raises:
            FileNotFoundError: If the configuration file does not exist at the specified path.
        """
        source_config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "tdw",
            "ingest",
            "datasets",
            source_name,
            "config.yaml",
        )
        if not os.path.exists(source_config_path):
            raise FileNotFoundError(f"Source configuration file not found: {source_config_path}")

        with open(source_config_path, "r", encoding="utf-8") as f:
            source_config = yaml.safe_load(f)
        return source_config
