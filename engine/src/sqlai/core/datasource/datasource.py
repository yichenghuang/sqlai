from abc import ABC, abstractmethod

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
        cls.conn_params = conn_params
        cls.conn = None

    @abstractmethod
    def connect(cls):
        """Establish a connection to the data source."""
        pass

    @abstractmethod
    def disconnect(cls):
        """Close the connection to the data source."""
        pass

    @abstractmethod
    def execute(cls, query: str):
        """
        Execute a query (SQL or equivalent) and return the result table.

        Args:
            query: the SQL query

        Returns:
            list[dict[str, any]]: A list of dictionaries, each representing a table row with column names as keys.            []
            Example: [{"col1": value1, "col2": value2}, ...]
        """
        pass

