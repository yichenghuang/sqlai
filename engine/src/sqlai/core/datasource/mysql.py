import os
import logging
import MySQLdb
from typing import List, Dict, Any
from sqlai.core.datasource.datasource import DataSource
from sqlai.utils.str_utils import extract_port, make_collectioname


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class MySQLDataSource(DataSource):
    """
    Concrete implementation for MySQL using MySQLdb.
    Args:
        connection_params (dict): Dictionary containing connection parameters.
        - host[:port] (str, optional): The database host and port number.
        - username (str, optional): The database username. Defaults to
                  `os.getenv('MYSQL_USER')` or empty string if unset.
        - password (str, optional): The database password. Defaults to
                  `os.getenv('MYSQL_PASSWORD')` or empty string if unset.
        - database (str, optional): The database name (optional for MySQLdb).
    """

    def __init__(cls, conn_params: dict):
        super().__init__(conn_params)

        logger.info(conn_params)
        if not cls._conn_params.get('host'):
            cls._conn_params['host'] = "127.0.0.1"
            cls._conn_params['port'] = 3306
        else:
            cls._conn_params['port'] = extract_port(cls._conn_params['host'], 
                                                    3306)
        if not cls._conn_params.get('username'):
            cls._conn_params['username'] = os.getenv('MYSQL_USER') or ""
        if not cls._conn_params.get('password'):
            cls._conn_params['password'] = os.getenv('MYSQL_PASSWORD') or ""
        if not cls._conn_params.get('database'):
            cls._conn_params['database'] = ""

    def connect(cls):
        if not cls._conn:
            try:
                cls._conn = MySQLdb.connect(
                    host = cls._conn_params['host'],
                    port = cls._conn_params['port'],
                    user = cls._conn_params['username'],
                    passwd = cls._conn_params['password'],
                    database = cls._conn_params['database'],
                )
                cursor = cls._conn.cursor()
                cursor.execute('SELECT @@server_uuid;')
                row = cursor.fetchone()
                cls._sys_id = make_collectioname(row[0])
                cursor.close()

            except MySQLdb.Error as err:
                raise ConnectionError(f"Failed to connect to MySQL: {err}")
            
    def disconnect(cls):
        if cls._conn:
            cls._conn.close()

    def sys_id(cls):
        return cls._sys_id    

    def get_cursor(cls):
        """ Return a cursor """
        return cls._conn.cursor()

    def close_cursor(cls, cursor):
        """ Close a cursor """
        cursor.close()

    def get_databases(cls, cursor):
        """ Return databases """
        system_dbs = {'information_schema', 'mysql', 'performance_schema', 'sys'}
        cursor.execute("SHOW DATABASES")
        dbs = cursor.fetchall() # Fetches all rows 
        return [row[0] for row in dbs if row[0] not in system_dbs]
    
    def get_tables(cls, cursor, db: str):
        """ Return tables in a database """
        
        cursor.execute(f"USE {db}")
        cursor.execute("SHOW TABLES")
        tbls = cursor.fetchall() # Fetches all rows 
        return [row[0] for row in tbls]
    
    def inspect_table(cls, cursor, db: str, tbl: str, rows = 5):
        """ 
        Inspect a table and return its data, schema and comment. 
        
        Returns:
            tuple: A tuple containing:
                - list[list]: A list of lists, where the first list contains column headers and each subsequent list represents a row of data.
                - list[tuple]: A list of tuples, where each tuple contains (column_name, data_type) for the table schema.ct: A dictionary describing the table schema, with column names as keys and data types as values.
                - str: The comment or description associated with the table.

        """
        cursor.execute(f"SELECT * FROM {tbl} LIMIT 5")
        # Get column headers
        headers = [desc[0] for desc in cursor.description]
        # Get rows and combine with headers
        rows = cursor.fetchall()
        # Convert to strings to so len() can work on them.
        table = [headers] + [
            [ 
                'NULL' if value is None else str(value)
                for value in row
            ]
            for row in rows
        ]

        # Get table schema
        cursor.execute(f"""SELECT COLUMN_NAME, DATA_TYPE
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE TABLE_SCHEMA = '{db}' 
                       AND TABLE_NAME = '{tbl}'""");
        schema = cursor.fetchall()

        if (len(headers) != len(schema)):
            schema = None

        # Get table comment
        cursor.execute(f"""SELECT TABLE_COMMENT
                       FROM INFORMATION_SCHEMA.TABLES
                       WHERE TABLE_SCHEMA = '{db}'
                       AND TABLE_NAME = '{tbl}'""")
        row = cursor.fetchone()
        comment = row[0] if row else ''
        
        return table, schema, comment

    def execute(cls, cursor, query: str) -> List[Dict[str, Any]]:
        """
        Execute a query (SQL or equivalent) and return the result table.

        Args:
            query: the SQL query

        Returns:
            list[dict[str, any]]: A list of dictionaries, each representing a table row with column names as keys.            []
            Example: [{"col1": value1, "col2": value2}, ...]
        """
        
        logger.info(f"executing query '{query}'")
        # cursor = cls._conn.cursor()
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

        # cursor.close()
        return rows

