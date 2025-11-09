from abc import ABC, abstractmethod
from readerwriterlock import rwlock
from typing import List, Dict, Any

class DataSource(ABC):
    """
    Abstract base class for a data source. Subclasses should implement 
    connection logic and query execution for specific databases or services. 
    This allows easy extension to other databases or even non-SQL services 
    (e.g., APIs) by overriding the methods accordingly.
    """
    
    def __init__(cls, conn_params: dict):
        """
        Initialize with connection parameters.
        :param connection_params: Dict of params like {'host': '', 'user': '', 'password': '', 'database': ''} for SQL DBs.
        """
        cls._conn_params = conn_params.copy()
        cls._conn = None
        cls._sys_id = None  # data source unique id
        cls._rwlock = rwlock.RWLockRead()   # Reader priority
        cls._rlock = cls._rwlock.gen_rlock()
        cls._wlock = cls._rwlock.gen_wlock()

    @abstractmethod
    def connect(cls):
        """Establish a connection to the data source."""
        pass

    @abstractmethod
    def disconnect(cls):
        """Close the connection to the data source."""
        pass

    @abstractmethod      
    def sys_id(cls, cursor):
        """ Return data source system/unique id."""
        pass

    @abstractmethod
    def get_cursor(cls):
        """ Return a cursor """
        pass

    @abstractmethod
    def close_cursor(cls, cursor):
        """ Close a cursor """
        pass

    @abstractmethod
    def get_databases(cls, cursor):
        """ Return databases """
        pass

    @abstractmethod
    def get_tables(cls, cursor, db: str):
        """ Return tables in a database """
        pass

    @abstractmethod
    def inspect_table(cls, cursor, db: str, tbl: str, rows = 5):
        """ 
        Inspect a table and return its data, schema and comment. 
        
        Returns:
            tuple: A tuple containing:
                - list[tuple]: A list of tuples, where the first list contains column headers and each subsequent list represents a row of data.
                - list[tuple]: A list of tuples, where each tuple contains (column_name, data_type) for the table schema.
                - str: The comment or description associated with the table.

        """
        pass

    @abstractmethod
    def execute(cls, cursor, query: str) -> List[Dict[str, Any]]:
        """
        Execute a query (SQL or equivalent) and return the result table.

        Args:
            query: the SQL query

        Returns:
            list[dict[str, any]]: A list of dictionaries, each representing a table row with column names as keys.            []
            Example: [{"col1": value1, "col2": value2}, ...]
        """
        pass

    def r_lock(cls):
        """
        Read lock.
        """
        cls._rlock.acquire()

    def r_unlock(cls):
        """
        Read unlock.
        """
        cls._rlock.release()

    def w_lock(cls):
        """
        Write lock.
        """
        cls._wlock.acquire()

    def w_unlock(cls):
        """
        Write unlock.
        """
        cls._wlock.release()
