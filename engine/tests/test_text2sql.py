import sys
import logging
from sqlai.text_to_sql import text_to_sql
from sqlai.utils import json_formatter
from sqlai.core.datasource.mysql import MySQLDataSource


logger = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(json_formatter.JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)


if __name__ == '__main__':
    logger.info("Test text2sql")
    
    mysql_params = {}
    mysql_source = MySQLDataSource(mysql_params)  # Env vars resolved here
    mysql_source.connect()
    mysql_sys_id = mysql_source.sys_id()
    cursor = mysql_source.get_cursor()

    # sql = text_to_sql(mysql_sys_id, "Show me the total revenue by Product categories")
    # sql = text_to_sql(mysql_sys_id, "不同性別的銷售總額")
    sql = text_to_sql(mysql_sys_id, "男女生的銷售額各是多少")
    #res = text_to_sql("Show the total nubmer of female and male customers")
    #res = text_to_sql("What is the average GDP growth rate of each state in Malaysia in 2019?")
    #res = text_to_sql("Find the top 5 customers who spent the most in 2025.")
    print(sql)

 
    # manager.register_source('mysql_db', mysql_source)
    sql_res = mysql_source.execute(cursor, sql)
    mysql_source.close_cursor(cursor)
    print(sql_res)