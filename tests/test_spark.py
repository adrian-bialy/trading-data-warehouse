"""
Module for testing the Spark session creation utility.

This module contains tests for the create_spark_session function, which is used to instantiate
and configure a SparkSession with both default and custom settings.
"""

from pyspark.sql import SparkSession

from src.tdw.config.spark import create_spark_session, get_session


def test_create_spark_session_default():
    """
    Test that create_spark_session returns a properly configured SparkSession.

    This function verifies the following:
    - The created session is an instance of SparkSession.
    - The Spark configuration has the default settings:
        - spark.app.name is set to "LocalSparkSession".
        - spark.master is set to "local[*]".
        - spark.jars.packages is set to "org.postgresql:postgresql:42.5.4".
        - spark.sql.shuffle.partitions is set to "10".
    - The SparkSession is properly stopped after the test runs.
    """
    session = create_spark_session()
    try:
        # Verify the returned object is a SparkSession
        assert isinstance(session, SparkSession)

        # Access the Spark configuration
        conf = session.sparkContext.getConf()

        # Check the default app name and master settings
        assert conf.get("spark.app.name") == "LocalSparkSession"
        assert conf.get("spark.master") == "local[*]"

        # Check the default configurations set in create_spark_session
        assert conf.get("spark.jars.packages") == "org.postgresql:postgresql:42.5.4"
        assert conf.get("spark.sql.shuffle.partitions") == "10"
    finally:
        session.stop()


def test_create_spark_session_with_custom_configs():
    """
    Test the creation of a Spark session with custom configurations.

    This test function does the following:
    - Creates a Spark session with a specific application name ("TestSession"),
        master ("local[4]"), and additional custom configuration key-value pairs.
    - Verifies that the session's configuration:
        * correctly sets the custom app name and master settings.
        * retains default configurations such as 'spark.jars.packages' and
            'spark.sql.shuffle.partitions'.
        * applies the provided custom configurations, like 'spark.executor.memory'
            and 'spark.some.config'.
    - Ensures that the Spark session is properly stopped after the test,
        preserving resources and avoiding side effects.
    """
    custom_configs = {"spark.executor.memory": "2g", "spark.some.config": "some_value"}
    session = create_spark_session(app_name="TestSession", master="local[4]", **custom_configs)
    try:
        conf = session.sparkContext.getConf()

        # Verify custom app name and master settings
        assert conf.get("spark.app.name") == "TestSession"
        assert conf.get("spark.master") == "local[4]"

        # Check that default configs are still applied
        assert conf.get("spark.jars.packages") == "org.postgresql:postgresql:42.5.4"
        assert conf.get("spark.sql.shuffle.partitions") == "10"

        # Verify that custom configurations are set
        assert conf.get("spark.executor.memory") == "2g"
        assert conf.get("spark.some.config") == "some_value"
    finally:
        session.stop()


def test_get_session():
    """
    Test the get_session function to ensure it returns the same Spark session instance.

    This test function does the following:
    - Calls get_session to create a new Spark session.
    - Calls get_session again to retrieve the existing session.
    - Asserts that both calls return the same session instance.
    - Ensures that the Spark session is properly stopped after the test,
        preserving resources and avoiding side effects.
    """
    session1 = get_session()
    try:
        session2 = get_session()
        assert session1 is session2
    finally:
        session1.stop()
