"""
Module: ingestor.py

This module provides the BaseIngestor class, a framework for ingesting data from external APIs,
processing it with Apache Spark, and writing the processed data to a target database via JDBC.

Key functionalities:
    - Dynamically constructs a Spark DataFrame schema by loading YAML configurations that define
      the expected data structure, including support for nested (dictionary/list) types.
    - Reads data from an API endpoint using HTTP GET requests and processes pagination when enabled.
    - Parses and validates JSON responses, extracting data based on a configurable response path.
    - Processes the DataFrame by appending a SHA-256 hash for row integrity and a timestamp for tracking.
    - Writes the processed DataFrame to a JDBC target using a dynamically generated table name.
    - Ensures target schemas are created if they do not already exist.

Example usage:
    ingestor = BaseIngestor(
        spark,
        query_params
    ingestor.read().process().write()

Note:
    - The YAML schema file must include 'responsePath' configurations.
    - Proper API headers, base URL, and timeout settings must be provided in the source configuration.
    - The module supports both paginated requests (via the _paginate method) and single API calls.
"""

import requests
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    BooleanType,
    TimestampType,
    ArrayType,
)


class BaseIngestor:
    """
    BaseIngestor provides a framework for ingesting data from external sources,
    converting it into a Spark DataFrame based on a schema defined in a YAML file,
    processing the data and writing it to a target database via JDBC.
    """

    def __init__(
        self,
        spark: SparkSession,
        source_config,
        dataset_config,
        endpoint,
        target,
        layer,
        query_params=None,
    ) -> None:
        self.spark = spark
        self.source_config = source_config
        self.dataset_config = dataset_config
        self.endpoint = endpoint
        self.target = target
        self.layer = layer
        self.query_params = query_params
        self.df = None

        self.schema_path = (
            f"src/tdw/ingest/datasets/{self.source_config['name']}/schema/{self.dataset_config['name']}.yaml"
        )
        self.schema = self.__get_schema()
        self.result_path = self.__get_result_path()
        self.pagination = self.__get_pagination_config()

        # Create target schema
        self.target.execute(f"CREATE SCHEMA IF NOT EXISTS {self.layer}")

    def __get_schema(self) -> StructType:
        """
        Private method to generate a Spark StructType schema from a YAML file.

        This method reads a YAML file specified by `self.schema_path` and constructs
        a Spark StructType schema based on the structure and data types defined in the YAML.
        It supports nested schemas for dictionaries and lists.

        Returns:
            StructType: A Spark StructType object representing the schema.

        Raises:
            FileNotFoundError: If the schema file specified by `self.schema_path` does not exist.
            yaml.YAMLError: If there is an error parsing the YAML file.
        """
        with open(self.schema_path, "r", encoding="utf-8") as f:
            schema_yaml = yaml.safe_load(f)

        type_mapping = {
            "float": FloatType(),
            "integer": IntegerType(),
            "string": StringType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType(),
        }

        def build_schema(schema_dict: dict) -> StructType:
            fields = []
            for column in schema_dict.get("columns", []):
                col_name = column.get("name")
                col_type = column.get("type")
                if col_type in ("dict", "dictionary", "list") and "columns" in column:
                    nested_schema = build_schema(column)
                    field_type = ArrayType(nested_schema) if col_type == "list" else nested_schema
                else:
                    field_type = type_mapping.get(col_type, StringType())
                fields.append(StructField(col_name, field_type, True))
            return StructType(fields)

        return build_schema(schema_yaml)

    def __get_result_path(self) -> str:
        """
        Private method to retrieve the response path from the schema YAML file.

        Reads the YAML file specified by `self.schema_path` and extracts the value
        associated with the "responsePath" key. If the key is not found, a ValueError
        is raised.

        Returns:
            str: The response path extracted from the schema YAML.

        Raises:
            ValueError: If the "responsePath" key is missing in the schema YAML.
        """
        with open(self.schema_path, "r", encoding="utf-8") as f:
            schema_yaml = yaml.safe_load(f)
        response_path = schema_yaml.get("responsePath")
        if response_path is None:
            raise ValueError("Missing 'responsePath' in schema YAML.")
        return response_path

    def __get_pagination_config(self) -> dict:
        with open(self.schema_path, "r", encoding="utf-8") as f:
            schema_yaml = yaml.safe_load(f)
        response_path = schema_yaml.get("pagination")
        if response_path is None:
            raise ValueError("Missing 'pagination' in schema YAML.")
        return response_path

    def _paginate(
        self, full_url: str, headers: dict, query_params: dict, result_path: str, max_pages: int = None
    ) -> list:
        """
        Paginates through results by incrementing the 'page' query parameter until no results are returned.

        Args:
            full_url (str): The full API URL.
            headers (dict): Request headers.
            query_params (dict): Base query parameters.
            result_path (str): The key used to retrieve results from the JSON response.

        Returns:
            list: Combined list of all results from each page.
        """
        # TODO: Improve pagination handling - when to stop, how many pages to fetch, etc.
        page = 1
        all_results = []
        while True if max_pages is None else page <= max_pages:
            query_params["page"] = str(page)
            response = requests.get(
                full_url,
                headers=headers,
                params=query_params,
                timeout=self.source_config.get("variables", {}).get("timeout", 30),
            )
            if not response.ok:
                raise ValueError(
                    f"Request failed on page {page} with status code {response.status_code}: {response.text}"
                )
            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError as exc:
                raise ValueError(f"Failed to decode JSON on page {page}. Response text: {response.text}") from exc

            page_result = data.get(result_path)
            if not page_result:
                # Exit when no results are returned
                break
            all_results.extend(page_result)
            page += 1

        return all_results

    def read(self):
        """
        Read data from the configured API endpoint and load it into a Spark DataFrame.

        Constructs the full URL from `base_url` and `endpoint`, applies RapidAPI headers and query parameters,
        and executes either paginated requests (if enabled) or a single GET request. Parses the JSON response,
        extracts the data at `result_path`, and creates a Spark DataFrame using the provided `schema`.

        Raises:
            ValueError: If the required RapidAPI headers are missing from the source configuration.

        Returns:
            self: The ingestor instance with `self.df` set to the resulting Spark DataFrame.
        """
        pagination = self.pagination
        spark = self.spark
        base_url = self.source_config.get("variables", {}).get("base_url")
        full_url = base_url.rstrip("/") + "/" + self.endpoint.lstrip("/")
        schema = self.schema
        result_path = self.result_path

        headers = self.source_config.get("headers", {}).get("rapidapi", {})
        if not headers:
            raise ValueError("Missing headers in source configuration.")

        query_params = self.query_params
        # TODO: Process requests on worker nodes
        if pagination["enabled"]:
            result = self._paginate(full_url, headers, query_params, result_path, pagination.get("maxPages", None))
        else:
            response = requests.get(
                full_url,
                headers=headers,
                params=query_params,
                timeout=self.source_config.get("variables", {}).get("timeout", 30),
            )
            data = response.json()
            result = data[result_path]

        df = spark.createDataFrame(result, schema)

        self.df = df
        return self

    def process(self):
        """
        Processes the ingestor's DataFrame by appending a SHA-256 hash of each row
        and a processing timestamp.

        This method performs the following operations on self.df:
        1. Computes a SHA-256 hash across all existing column values in each row,
            separated by "||", and stores the result in a new column "_rowHash".
        2. Adds a new column "_processedTimestamp" containing the current processing time.

        Returns:
             self: The ingestor instance with the updated DataFrame (self.df).
        """
        df = self.df

        df = df.withColumn("_rowHash", sha2(concat_ws("||", *[col(c) for c in df.columns]), 256))
        df = df.withColumn("_processedTimestamp", current_timestamp())
        self.df = df

        return self

    def write(self):
        """
        Write the current DataFrame to a JDBC table.

        Constructs the target table name from `self.layer`, `self.source_config['name']`
        and `self.dataset_config['name']`, then writes `self.df` to the configured
        JDBC URL using overwrite mode and the provided connection properties.

        Returns:
            Ingestor: The same instance (self) for method chaining.
        """
        table_name = f"{self.layer}.{self.source_config['name']}_{self.dataset_config['name']}"

        jdbc_url = self.target.jdbc_url
        connection_properties = self.target.connection_properties

        self.df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)
        return self
