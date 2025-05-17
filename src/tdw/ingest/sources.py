"""
This module defines classes and functions that configure data ingestion sources
for an API-based trading data ingestion system.
"""

from dataclasses import dataclass
from typing import Optional, Dict
from tdw.ingest.ingestor import BaseIngestor
from tdw.config.auth import (
    BaseAPIAuth,
)


@dataclass
class Source:
    """
    A representation of a data source used by the ingestion system.

    Attributes:
        process (BaseIngestor): The process responsible for ingesting data.
        name (str): The unique identifier for the data source.
        config (BaseAPIAuth): The configuration and authentication details for API access.
        datasets (List[Dataset]): A list of associated datasets for processing.
        dependency (Optional[str]): An optional dependency that may be required by the source, defaults to None.
    """

    process: BaseIngestor
    name: str
    config: BaseAPIAuth
    datasets: list["Dataset"]
    dependency: Optional[str] = None


@dataclass
class Dataset:
    """
    Represents a dataset configuration for an ingestion source.

    Attributes:
        name (str): The name of the dataset.
        endpoint (str): The URL endpoint from which data will be ingested.
        dependency (Optional[str]): Optional dependency information for the dataset; defaults to None.
        query_params (Optional[Dict[str, str]]): Optional query parameters for API calls; defaults to None.
        pagination (Optional[bool]): Flag indicating whether the endpoint supports pagination; defaults to False.
    """

    name: str
    endpoint: str
    dependency: Optional[str] = None
    query_params: Optional[Dict[str, str]] = None
    pagination: Optional[bool] = False


def load_sources():
    """
    Load and return a list of API ingestion sources with their configurations.

    This function compiles and returns a list of Source objects. Each Source is configured
    with the appropriate processing class, name, authentication configuration class, and any necessary
    dependency definitions. Additionally, associated datasets for each source are defined, including
    endpoint and query parameters that specify the data to retrieve.

    Returns:
        list[Source]: A list containing the initialized Source objects with their respective configurations.
    """

    return [
        Source(
            process=BaseIngestor,
            name="rapid_yahoo",
            config=BaseAPIAuth,
            dependency=None,
            datasets=[
                Dataset(
                    name="tickers",
                    dependency=None,
                    endpoint="v2/markets/tickers",
                    query_params={"type": "STOCKS"},
                ),
                Dataset(
                    name="history_1m",
                    dependency="tickers",
                    endpoint="v1/markets/stock/history",
                    query_params={"symbol": "{symbol}", "interval": "1m"},
                ),
                Dataset(
                    name="history_5m",
                    dependency="tickers",
                    endpoint="v1/markets/stock/history",
                    query_params={"symbol": "{symbol}", "interval": "5m"},
                ),
                Dataset(
                    name="history_15m",
                    dependency="tickers",
                    endpoint="v1/markets/stock/history",
                    query_params={"symbol": "{symbol}", "interval": "15m"},
                ),
                Dataset(
                    name="history_30m",
                    dependency="tickers",
                    endpoint="v1/markets/stock/history",
                    query_params={"symbol": "{symbol}", "interval": "30m"},
                ),
                Dataset(
                    name="history_1d",
                    dependency="tickers",
                    endpoint="v1/markets/stock/history",
                    query_params={"symbol": "{symbol}", "interval": "1d"},
                ),
                Dataset(
                    name="history_1wk",
                    dependency="tickers",
                    endpoint="v1/markets/stock/history",
                    query_params={"symbol": "{symbol}", "interval": "1wk"},
                ),
                Dataset(
                    name="history_1mo",
                    dependency="tickers",
                    endpoint="v1/markets/stock/history",
                    query_params={"symbol": "{symbol}", "interval": "1mo"},
                ),
                Dataset(
                    name="history_3mo",
                    dependency="tickers",
                    endpoint="v1/markets/stock/history",
                    query_params={"symbol": "{symbol}", "interval": "3mo"},
                ),
            ],
        ),
    ]
