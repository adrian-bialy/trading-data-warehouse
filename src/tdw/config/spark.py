"""
Module for creating and managing a PySpark SparkSession for the trading-dw project.

This module sets up the environment variables for PySpark to use the current Python
executable for both driver and worker nodes. It provides utility functions to create
a new Spark session with specific configurations and to retrieve the active Spark session,
ensuring that only one session is active at any time.
"""

import os
import sys
from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def create_spark_session(app_name="LocalSparkSession", master="local[*]", **configs):
    """
    Creates and returns a PySpark session with specified configurations.
    """
    builder = SparkSession.builder.appName(app_name).master(master)

    # Example config: adding a package for PostgreSQL and reducing shuffle partitions
    builder = builder.config("spark.jars.packages", "org.postgresql:postgresql:42.5.4")
    builder = builder.config("spark.sql.shuffle.partitions", "10")

    for key, value in configs.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def get_session(app_name="LocalSparkSession", master="local[*]", **configs):
    """
    Returns the active Spark session, creating it if needed.
    Caches the session on the function to avoid globals.
    """
    if not hasattr(get_session, "session"):
        get_session.session = create_spark_session(app_name, master, **configs)
    return get_session.session
