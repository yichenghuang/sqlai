import os
import logging
import MySQLdb
from sqlai.core.datasource.datasource import DataSource


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class MySQLDataSource(DataSource):
    """
    Concrete implementation for MySQL using MySQLdb.
    Args:
        connection_params (dict): Dictionary containing connection parameters.
        - host (stt, optional): The database host.
        - port (int, optional): The database port number (default: 3306).
        - user (str, optional): The database username. Defaults to
                  `os.getenv('MYSQL_USER')` or empty string if unset.
        - password (str, optional): The database password. Defaults to
                  `os.getenv('MYSQL_PASSWORD')` or empty string if unset.
        - database (str, optional): The database name (optional for MySQLdb).
    """

    def __init__(cls, conn_params: dict):
        cls._conn_params = conn_params.copy()
        if not cls._conn_params.get('host'):
            cls._conn_params['host'] = "127.0.0.1"
        if not cls._conn_params.get('user'):
            cls._conn_params['user'] = os.getenv('MYSQL_USER') or ""
        if not cls._conn_params.get('password'):
            cls._conn_params['password'] = os.getenv('MYSQL_PASSWORD') or ""
        if not cls._conn_params.get('port'):
            cls._conn_params['port'] = 3306
        if not cls._conn_params.get('database'):
            cls._conn_params['database'] = ""    
        super().__init__(cls._conn_params)
        cls._conn = None

    def connect(cls):
        if not cls._conn:
            try:
                cls._conn = MySQLdb.connect(
                    host = cls._conn_params['host'],
                    port = cls._conn_params['port'],
                    user = cls._conn_params['user'],
                    passwd = cls._conn_params['password'],
                    database = cls._conn_params['database'],
                )
            except MySQLdb.Error as err:
                raise ConnectionError(f"Failed to connect to MySQL: {err}")
            
    def disconnect(cls):
        if cls._conn:
            cls._conn.close()

    def execute(cls, query: str):
        """
        Execute a query (SQL or equivalent) and return the result table.

        Args:
            query: the SQL query

        Returns:
            list[dict[str, any]]: A list of dictionaries, each representing a table row with column names as keys.            []
            Example: [{"col1": value1, "col2": value2}, ...]
        """
        
        logger.info(f"executing query '{query}'")
        cursor = cls._conn.cursor()
        cursor.execute(query)
        # Get column names from cursor.description
        columns = [desc[0] for desc in cursor.description]
        rows = [
            {
                col: 'NULL' if val is None else str(val)
                for col, val in zip(columns, row)
            }
            for row in cursor.fetchall()
        ]

        cursor.close()
        return rows

