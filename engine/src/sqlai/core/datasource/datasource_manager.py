import time
import random
import logging
from typing import List, Dict, Any
from threading import Lock
from sqlai.core.datasource.datasource import DataSource
from sqlai.core.datasource.mysql import MySQLDataSource
from sqlai.tbl_milvus import TableMilvus


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
tbl_vs = TableMilvus()

def get_datasource_type(src_type: str, conn_params: dict) -> DataSource:
    match src_type:
        case 'mysql':  return MySQLDataSource(conn_params)
    
    return None


_salt = random.randint(0, 2**32 - 1)  # Random salt for added entropy

class DataSourceManager:
    """
    Manager class to handle multiple data sources by identifiers.
    Allows registering sources with unique IDs and executing queries using the ID.
    """
    
    def __init__(cls):
        cls._lock = Lock()
        cls._sources = {}  # Dict of {id: DataSource}

    def get_unique_id(cls):
        while True:
            seed = f"{time.time_ns()}{_salt}"
            id = abs(hash(seed)) % 2**32  # 32-bit unsigned integer
            if id not in cls._sources and id != 0:
                return id

    def register(cls, src_type: str, conn_params: dict) -> dict:
        """
        Register a data source with a unique identifier.
        
        Args:
            src_type (str): data source type.
            conn_params (dict): parameters used to connect to the data source.
        
        Returns:
            dict: A dictionary containing scan information with the following keys:
                - data_src_id (str): The unique identifier for the data source.
                - scan_time (str): The timestamp of the latest scan
                - error (str): The error message if connection fails, otherwise empty string.
        """
        with cls._lock:
            data_src = get_datasource_type(src_type, conn_params)
            if not data_src:
                return 0
            data_src.connect()
            sys_id = data_src.sys_id()
            data_src_id = cls.get_unique_id()
            cls._sources[data_src_id] = data_src
            # load vector database collection via datasource system id
            tbl_vs.load_collection(sys_id)
            logger.info(f"Register data source '{src_type}' id: {data_src_id}")
            return {'data_src_id' : str(data_src_id), 
                    'scan_time': time.strftime('%Y-%m-%d %H:%M:%S')}

    def get_datasource(cls, data_src_id: str) -> DataSource:
        """
        Retrieve the DataSource object by its identifier for direct use.

        Args: 
            data_src_id: The identifier of the data source.

        Returns: 
            DataSource: the DataSource instance.
        """
        with cls._lock:
            data_src = cls._sources.get(int(data_src_id))
            if not data_src:
                raise ValueError(f"Source ID '{data_src_id}' not found.")
            return data_src
    
    def execute(cls, data_src_id: str, qry: str) -> List[Dict[str, Any]]:
        """
        Execute a query on the specified data source using its id.
        
        Args:
            data_src_id (int): The identifier of the data source.
            qry (str): The SQL/query to execute.
        
        Returns:
            The result from the data source's execute method.
        """
        logger.info(f"data source: {data_src_id}")
        data_src = cls.get_datasource(data_src_id)  # Reuse get_source for DRY
        return data_src.execute(qry)
    

