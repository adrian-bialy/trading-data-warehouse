"""
Module: tdw.config.auth
------------------------
This module provides authentication mechanisms for different interfaces, including API-based authentication
and PostgreSQL database connectivity.
"""

import os
import psycopg2


class BaseAPIAuth:
    """
    BaseAPIAuth provides a foundational interface for API-based authentication.

    Attributes:
        config (str): The source or settings used for configuring the API authentication.
    """

    def __init__(self, source_config: str):
        self.config = source_config

    def get_auth(self):
        """
        Retrieve authentication details.

        This method is intended to be overridden by subclasses to provide
        specific authentication mechanisms. By default, it raises a
        NotImplementedError, indicating that the current authentication
        is based solely on an API key.

        Raises:
            NotImplementedError: If the method is not overridden in a subclass.
        """
        raise NotImplementedError("Current auth is based on API Key only.")

    def get_config(self):
        """
        Retrieves the current configuration settings.

        Returns:
            The configuration data stored in the instance.
        """
        return self.config


class PostgresAuth:
    """
    The PostgresAuth class provides functionality to configure, establish, and manage a connection
    to a PostgreSQL database. It builds connection details from environmental variables, constructs
    a JDBC URL, and encapsulates methods to execute SQL commands.

    Attributes:
        config (dict): The initial source configuration provided at instantiation.
        _conn (psycopg2.extensions.connection): The active PostgreSQL connection instance.
        jdbc_url (str): The constructed JDBC URL for the PostgreSQL database.
        connection_config (dict): A dictionary containing connection parameters such as host, dbname,
                                  user, password, and driver.
        port (str): The port number for the PostgreSQL server (default is "5432").
        user (str): The username for authentication.
        host (str): The hostname of the PostgreSQL server.
        dbname (str): The database name.
        password (str): The password for the PostgreSQL user.
        connection_properties (dict): A dictionary of properties (user, password, driver) used for JDBC connections.
    """

    def __init__(self, source_config):
        self.config = source_config
        self._conn = None
        self.jdbc_url = self.get_jdbc_url()
        self.connection_config = self.get_connection_config()

        self.port = "5432"
        self.user = self.connection_config.get("user")
        self.host = self.connection_config.get("host")
        self.dbname = self.connection_config.get("dbname")
        self.password = self.connection_config.get("password")

        self._conn = self.get_connection()

        self.connection_properties = {"user": self.user, "password": self.password, "driver": "org.postgresql.Driver"}

    def get_connection(self):
        """
        Attempt to establish a connection to the PostgreSQL database using the configured parameters.

        This method uses psycopg2 to attempt a connection to the database. If successful, returns a connection object.
        If the connection fails, a ConnectionError is raised with the underlying exception message.

        Returns:
            psycopg2.extensions.connection: A connection object for interacting with the PostgreSQL database.

        Raises:
            ConnectionError: If the connection attempt fails.
        """
        try:
            return psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.dbname,
                user=self.user,
                password=self.password,
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}") from e

    def close_connection(self):
        """
        Closes the active connection if it exists and sets the connection attribute to None.

        Returns:
            self: The current instance, enabling method chaining.
        """
        if self._conn:
            self._conn.close()
            self._conn = None
        return self

    def get_auth(self):
        """
        Return a dictionary containing PostgreSQL connection credentials.

        This dictionary is constructed from environment variables:
            - "host": The PostgreSQL server address obtained from the POSTGRES_HOST variable.
            - "user": The username for the PostgreSQL connection from the POSTGRES_USER variable.
            - "password": The password associated with the given user from the POSTGRES_PASSWORD variable.
            - "dbname": The name of the PostgreSQL database from the POSTGRES_DB variable.

        Returns:
            dict: A dictionary with keys "host", "user", "password", and "dbname".
        """
        return {
            "host": os.getenv("POSTGRES_HOST"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "dbname": os.getenv("POSTGRES_DB"),
        }

    def get_connection_config(self):
        """
        Retrieve the database connection configuration.

        This method obtains the authentication details by calling the `get_auth()` method
        and constructs a dictionary containing the required connection parameters:
            - host: The hostname for the database connection.
            - dbname: The name of the database.
            - user: The username for authentication.
            - password: The password for authentication.
            - driver: The JDBC driver identifier for PostgreSQL.

        Returns:
            dict: A dictionary with the database connection configuration.
        """
        auth_details = self.get_auth()
        self.connection_config = {
            "host": auth_details.get("host"),
            "dbname": auth_details.get("dbname"),
            "user": auth_details.get("user"),
            "password": auth_details.get("password"),
            "driver": "org.postgresql.Driver",
        }
        return self.connection_config

    def get_jdbc_url(self):
        """
        Generate a JDBC URL for connecting to a PostgreSQL database.

        This method retrieves the authentication details using get_auth(),
        including the host and database name. The PostgreSQL port is obtained
        from the environment variable "POSTGRES_PORT", defaulting to "5432" if not set.
        It then constructs the JDBC URL in the format:
            jdbc:postgresql://<host>:<port>/<dbname>

        Returns:
            str: The constructed JDBC URL.
        """
        auth_details = self.get_auth()
        host = auth_details.get("host")
        dbname = auth_details.get("dbname")
        port = os.getenv("POSTGRES_PORT", "5432")

        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
        return self.jdbc_url

    def execute(self, sql: str, *params):
        """
        Execute a SQL query using an active PostgreSQL connection.

        This method validates that a connection exists before creating a cursor to execute the
        provided SQL string. It accepts additional parameters to bind securely into the query,
        executes the query, and commits the transaction.

        Args:
            sql (str): The SQL query string to execute.
            *params: Optional parameters to bind into the SQL query.

        Raises:
            ConnectionError: If no active PostgreSQL connection is available.
        """
        if not self._conn:
            raise ConnectionError("No valid PostgreSQL connection.")
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            self._conn.commit()
