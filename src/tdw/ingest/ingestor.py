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
    LongType,
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
        dependency,
        target,
        layer,
        query_params=None,
    ) -> None:
        self.spark = spark
        self.source_config = source_config
        self.dataset_config = dataset_config
        self.endpoint = endpoint
        self.dependency = dependency
        self.target = target
        self.layer = layer
        self.query_params = query_params
        self.query_params_list = None
        self.df = None

        self.schema_path = (
            f"src/tdw/ingest/datasets/{self.source_config['name']}/schema/{self.dataset_config['name']}.yaml"
        )
        self.schema = self.__get_schema()
        self.result_path = self.__get_result_path()
        self.pagination = self.__get_pagination_config()
        self.dependency_df = self.__load_dependency()

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
            "long": LongType(),
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

    def __load_dependency(self):
        """
        Load the dependency dataset from the target JDBC source and assign it to `self.dependency_df`.

        This private method checks if `self.dependency` is defined. If so, it constructs the table
        name using the format "<layer>.<source_name>_<dependency>", reads the corresponding table
        from the JDBC source configured in `self.target`, and stores the result in
        `self.dependency_df`. If no dependency is specified, `self.dependency_df` remains None.

        Returns
        -------
        pyspark.sql.DataFrame or None
            The loaded dependency DataFrame when `self.dependency` is set; otherwise, None.
        """
        self.dependency_df = None

        if self.dependency is not None:
            table_name = f"{self.layer}.{self.source_config['name']}_{self.dependency}"
            jdbc_url = self.target.jdbc_url
            connection_properties = self.target.connection_properties

            self.dependency_df = self.spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

        return self.dependency_df

    def _extract_results(self, raw):
        if isinstance(raw,list):
            return raw
        # if the API gave us a dict, flatten nested dict
        else:
            results = []
            if isinstance(raw, dict):
                flat = self._flatten_nested_dict(raw)
                results.append(flat)
        return results
        

    def _flatten_nested_dict(self, raw, prefix="",data=None):
        if data is None:
            data = {}

        if isinstance(raw, dict):
            for key, value in raw.items():
                self._flatten_nested_dict(value,f"{prefix}_{key}" if prefix else key, data)
        else:
            data[prefix] = raw
                
        return data

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

            page_result = self._extract_results(data.get(result_path, []))
            if not page_result:
                # Exit when no results are returned
                break
            all_results.extend(page_result)
            page += 1

        return all_results

    def _get_query_params_list(self):
        """
        Generate a list of query-parameter dictionaries, substituting any placeholder values
        with actual data from a dependency DataFrame.

        This method examines `self.query_params` for any string values wrapped in braces
        (e.g. "{symbol}"), treating them as placeholders. If `self.dependency` is not None:
          1. It separates static parameters from placeholders.
          2. It reads distinct values for each placeholder column from `self.dependency_df`.
          3. It produces one parameter dict per distinct row, filling in placeholder keys
             with the corresponding DataFrame values and including all static parameters.

        If there are no placeholders, or if `self.dependency` is None, the method simply
        returns a single-element list containing the original `self.query_params`.

        The resulting list is stored in `self.query_params_list` before being returned.

        Returns:
            List[Dict[str, Any]]: A list of parameter dictionaries ready for query execution.
        """
        if self.dependency is not None:
            # identify which query‐params are placeholders like "{symbol}"
            placeholders = {
                key: val.strip("{}")
                for key, val in self.query_params.items()
                if isinstance(val, str) and val.startswith("{") and val.endswith("}")
            }
            # keep the rest as static
            static_params = {key: val for key, val in self.query_params.items() if key not in placeholders}

            query_params_list = []
            if placeholders:
                # select distinct values from dependency_df for those placeholder columns
                cols = list(placeholders.values())
                for row in self.dependency_df.select(*cols).distinct().collect():
                    params = static_params.copy()
                    for qp_key, col_name in placeholders.items():
                        params[qp_key] = getattr(row, col_name)
                    query_params_list.append(params)
            else:
                query_params_list = [self.query_params]
        else:
            query_params_list = [self.query_params]

        self.query_params_list = query_params_list
        return self.query_params_list

    def read(self):
        """
        Read data from a REST API into a Spark DataFrame.

        This method constructs the full request URL from the configured base URL and endpoint, retrieves
        the necessary headers from the source configuration, and builds a list of query parameter sets
        (including any placeholder substitutions). For each set of parameters, it either:
          - invokes a pagination helper to retrieve multiple pages of results (if pagination is enabled), or
          - makes a single HTTP GET request and checks for errors.

        The raw JSON payload is extracted using the configured `result_path`, transformed into Python
        records, and enriched with any placeholder parameter values. All records are then aggregated,
        converted into a Spark DataFrame using the provided schema, stored in `self.df`, and the instance
        itself is returned for further chaining.

        Raises:
            ValueError: If required headers are missing or if an HTTP request returns a non-OK status.

        Returns:
            self: The current object with `self.df` set to the loaded DataFrame.
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

        # TODO: Process requests on worker nodes
        # TODO: Verify if API response is successful and handle errors
        query_params_list = self._get_query_params_list()
        # figure out which query‐params were placeholders (e.g. "{symbol}")
        placeholder_keys = [
            k for k, v in self.query_params.items() if isinstance(v, str) and v.startswith("{") and v.endswith("}")
        ]
        query_params_list = query_params_list[:2]
        all_results = []

        for params in query_params_list:
            if pagination["enabled"]:
                batch = self._paginate(
                    full_url,
                    headers,
                    params,
                    result_path,
                    pagination.get("maxPages", None),
                )
            else:
                response = requests.get(
                    full_url,
                    headers=headers,
                    params=params,
                    timeout=self.source_config.get("variables", {}).get("timeout", 30),
                )
                if not response.ok:
                    raise ValueError(f"Request failed with status {response.status_code}: {response.text}")
                data = response.json()
                raw = data.get(result_path, [])
                batch = self._extract_results(raw)

            # inject each placeholder value into every record so DF has those columns
            for rec in batch:
                for key in placeholder_keys:
                    rec[key] = params.get(key)
            all_results.extend(batch)

        df = spark.createDataFrame(all_results, schema)
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
