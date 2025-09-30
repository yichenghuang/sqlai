import sys
import logging
from typing import List, Generator
from fastmcp import FastMCP
from sqlai.utils import json_formatter
from sqlai.core.datasource.datasource_manager import DataSourceManager
from sqlai.text_to_sql import text_to_sql


logger = logging.getLogger()
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(json_formatter.JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

mcp = FastMCP(name="TableNLPServer")
# tbl_vdb = TableMilvus()
ds_manager = DataSourceManager()


@mcp.tool()
def query(ds_id:int, qry: str):
    """
    Execute a query at a data source

    Args:
        'ds_id' (int): data source id.
        'qry' (str): query to be executed.

    Returns:
        Table result in the following format:
        - Streamed SSE Events:
            - First event: {"type": "header", "data": ["col1", "col2", ...]}
            - Subsequent events: {"type": "row", "data": ["val1", "val2", ...]}
            - Final event: {"type": "end"}
        - Non-streaming:
            - {"header": ["col1", "col2", ...], "rows": [["val1", "val2", ...], ...]}
    """

    is_streaming = getattr(mcp, 'is_streaming', lambda: True)()
    logger.info(f"is_streaming: {is_streaming}")

    # if is_streaming:
    #     # Streaming mode: directly return f's generator
    #     return f(query)

    sql = text_to_sql(qry)
    res = ds_manager.execute(ds_id, sql)
    logger.info(res)
    return res


@mcp.tool()
def test(tbl: str) -> dict:
    return { "test": tbl , "res": "mcp test" }


@mcp.tool()
def connect_datasource(type: str, conn_params: dict) -> int:
    """
    Connect to a datasource.

    Args:
        'type' (str): specify data source type, e.g., mysql. Required.
        'conn_params' (dict): connection parameters, such as:
            - host (str, optional): The datasource host.
            - port (int, optional): The daasource port number/
            - user (str, optional): The daasource username.
            - password (str, optional): The daasource password.
            - database (str, optional): The database name.

    Returns:    
        datasource_id: a datasource identifier
    """
    ds_id = ds_manager.register(type, conn_params)
    return ds_id

def main():
    mcp.run()

if __name__ == "__main__":
    mcp.run()
