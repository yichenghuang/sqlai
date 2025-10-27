import datetime
import sys
import logging
from typing import List, Dict, Any
from typing import List, Generator
from fastmcp import FastMCP
from sqlai.utils import json_formatter
from sqlai.core.datasource.datasource_manager import DataSourceManager
from sqlai.text_to_sql import text_to_sql
from sqlai.core.job_tracker import JobTracker
from sqlai.scan_datasource import start_scan_datasource


logger = logging.getLogger()
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(json_formatter.JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def is_running_in_docker():
    try:
        with open('/proc/self/cgroup', 'r') as f:
            return 'docker' in f.read() or 'containerd' in f.read()
    except FileNotFoundError:
        return False

if is_running_in_docker():
    host = '0.0.0.0'
else:
    host = 'localhost'

host = '0.0.0.0'

mcp = FastMCP(name="SQLAIServer", host=host)
ds_manager = DataSourceManager()
tracker = JobTracker()

# def query(data_src_id:str, qry: str) -> List[Dict[str, Any]]:
@mcp.tool()
def query(data_src_id:str, qry: str) -> dict:
    """
    Execute a query at a data source

    Args:
        'data_src_id' (str): data source id.
        'qry' (str): query to be executed.

    Returns:
        list[dict[str, any]]: A list of dictionaries, each representing a table row with column names as keys.            []
        Example: [{"col1": value1, "col2": value2}, ...]
    """
    ds = ds_manager.get_datasource(data_src_id)
    sql = text_to_sql(ds.sys_id(), qry)
    logger.info(f'sql: {sql}')
    res = ds.execute(sql)

    return {'sql': sql, 'data': res}


@mcp.tool()
def scan_datasource(data_src_id: str) -> dict:
    ds = ds_manager.get_datasource(data_src_id)
    start_scan_datasource(ds, datetime.datetime.now())
    return {'job_id': data_src_id }


@mcp.tool()
def scan_progress(job_id: str) -> dict:
    ds = ds_manager.get_datasource(job_id)
    progress, timestamp = tracker.get_progress(ds.sys_id())
    return {
        'progress' : int(progress), 
        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S')
        }

   
@mcp.tool(output_schema={
    "type": "object", 
    "properties": {
        "data_src_id": {"type": "string"},
        "scan_time": {"type": "string"}
    }
})
def connect_datasource(type: str, conn_params: dict) -> dict:
    """
    Connect to a datasource.

    Args:
        'type' (str): specify data source type, e.g., mysql. Required.
        'conn_params' (dict): connection parameters, such as:
            - host[:port] (str, optional): The datasource host.
            - user (str, optional): The daasource username.
            - password (str, optional): The daasource password.
            - database (str, optional): The database name.

    Returns:
        dict: A dictionary containing:
            - data_src_id (str): A unique identifier for the data source.
            - scan_time (str): The latest scan time (e.g., '2025-10-01 17:32:00').
    """
    return ds_manager.register(type, conn_params)

def main():
    mcp.run()

if __name__ == "__main__":
    main()
