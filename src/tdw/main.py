"""
Trading Data Warehouse CLI Module

This module provides a command line interface (CLI) to manage and execute different data processing
tasks for a Trading Data Warehouse. It supports two primary actions:
    - Ingest: Loads and processes data from various sources using Spark, after filtering based on
        configurations and available datasets.
    - Transform: Acts as a placeholder for future data transformation logic.

Key Components:
        - ingest(dataset_name: str):
                Loads global configurations and filters available data sources and their datasets based on
                the provided dataset name. Replaces placeholder API keys from the environment variables,
                initializes a Spark session, and processes source-specific data by chaining read, process,
                and write operations. A PostgreSQL target connection is maintained for writing processed data
                and is closed upon completion.

        - transform():
                Serves as a placeholder function intended for future implementation of data transformation
                procedures on the ingested trading data.

        - main():
                Acts as the entry point for the CLI. It parses command line arguments to determine the action
                (ingest or transform) and the target dataset, loads environment variables, and calls the
                appropriate processing function based on the user's input.

Usage:
        Execute the module from the command line with the following options:
            --action  : Specifies the action to perform, either "ingest" or "transform".
            --dataset : Indicates the dataset to process (e.g., "yahoo.symbols", or "yahoo" for all datasets).

Example:
        python main.py --action ingest --dataset yahoo.symbols

"""

import logging
import argparse
import os
import sys

from dotenv import load_dotenv, find_dotenv
from pyspark.sql import SparkSession

from tdw.utils.helpers import filter_sources, filter_datasets, replace_api_key
from tdw.config.spark import get_session
from tdw.config.auth import PostgresAuth
from tdw.config.configure import ConfigLoader
from tdw.ingest.sources import load_sources

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def ingest(dataset_name: str):
    """
    Ingest data for the specified dataset from configured sources into the bronze layer.

    This function performs the following steps:
    1. Loads the global input configuration.
    2. Retrieves all available sources and filters them based on the input config and
        the provided dataset_name.
    3. Loads and applies per-source authentication configuration.
    4. Exits gracefully if no sources match the dataset.
    5. Initializes a SparkSession and a Postgres target connection.
    6. For each source and its datasets:
        a. Retrieves source- and dataset-specific settings.
        b. Substitutes any `{api_key}` placeholders in the source configuration with
            environment variable values (e.g. `MY_SOURCE_KEY`).
        c. Executes the ETL pipeline: read(), process(), write() into the "bronze" layer.
    7. Closes the Postgres connection upon completion.

    Args:
         dataset_name (str): The name of the dataset to ingest.

    Raises:
         ValueError: If the expected environment variable for a source API key
                         (e.g. `<SOURCE_NAME>_KEY`) is not set.
         SystemExit: If no matching sources are found for the specified dataset,
                         exits with status code 0.
    """
    cl = ConfigLoader()
    input_config = cl.load_input_config()

    sources = load_sources()
    sources = filter_sources(sources, input_config)
    sources = filter_datasets(sources, dataset_name)

    # Update each source to use its specific configuration with BaseAPIAuth
    for source in sources:
        source_config = cl.load_source_config(source.name)
        source.config = source.config(source_config=source_config)

    if not sources:
        logger.warning("No sources found for the specified dataset.")
        sys.exit(0)

    spark: SparkSession = get_session()
    target = PostgresAuth(source_config)

    for source in sources:
        input_source_config = input_config.get("sources", {}).get(source.name, {})
        source_config = source.config.get_config()

        # Replace all occurrences of '{api_key}' with the corresponding environment variable value
        api_key_value = os.getenv(f"{source.name.upper()}_KEY")
        if api_key_value:
            source_config = replace_api_key(source_config, api_key_value)
        else:
            raise ValueError(
                f"Variable {source.name.upper()}_KEY not found in environment variables."
            )

        for dataset in source.datasets:
            dataset_config = input_source_config.get("datasets", {}).get(
                dataset.name, {}
            )
            logger.info("Ingesting %s.%s", source.name, dataset.name)
            source.process(
                spark,
                source_config,
                dataset_config,
                dataset.endpoint,
                dataset.dependency,
                target,
                "bronze",
                dataset.query_params,
            ).read().process().write()

    target.close_connection()


def transform():
    """
    Perform data transformation on trading data.

    This function currently serves as a placeholder for future data transformation logic.
    """
    pass


def main():
    """
    Main entry point for the Trading Data Warehouse CLI.

    This function parses command line arguments to determine which action to perform on a specific dataset.
    It supports the following command line options:
        --action: (str, required) The action to perform. Must be one of "ingest" or "transform".
        --dataset: (str, required) Specifies the dataset to process, e.g., "yahoo.symbols", or "yahoo" for all datasets.

    After parsing the arguments and loading environment variables, the function executes:
        - ingest(dataset) if the action is "ingest"
        - transform() if the action is "transform"
    """
    parser = argparse.ArgumentParser(description="Trading Data Warehouse CLI")
    parser.add_argument(
        "--action",
        type=str,
        required=True,
        choices=["ingest", "transform"],
        help="The action to perform (ingest or transform)",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        help="The dataset to ingest/transform (e.g., 'yahoo.symbols', 'yahoo.quotes' or 'yahoo' for all datasets)",
    )
    args = parser.parse_args()
    load_dotenv(find_dotenv(), override=True)

    if args.action == "ingest":
        ingest(args.dataset)
    elif args.action == "transform":
        transform()


if __name__ == "__main__":
    main()
