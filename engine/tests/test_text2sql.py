import json
import sys
import os
import logging
from sqlai.text_to_sql import text_to_sql
from sqlai.utils import json_formatter
from sqlai.core.datasource.mysql import MySQLDataSource

mysql_host = "127.0.0.1"
username = os.getenv('DB_USERNAME') or ""
password = os.getenv('DB_PASSWORD') or ""  

logger = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(json_formatter.JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)


if __name__ == '__main__':
    logger.info("Test text2sql")
    conn_params = {'host': mysql_host, 'username': username, 'password': password}

    mysql_source = MySQLDataSource(conn_params)  # Env vars resolved here
    mysql_source.connect()
    mysql_sys_id = mysql_source.sys_id()
    cursor = mysql_source.get_cursor()

    # sql = text_to_sql(mysql_sys_id, "Show me the total revenue by Product categories")
    # sql = text_to_sql(mysql_sys_id, "不同性別的銷售總額")
    # sql = text_to_sql(mysql_sys_id, "男女生的銷售額各是多少")
    sql_json = text_to_sql(mysql_sys_id,"How many accounts who have region in Prague are eligible for loans?")
    # sql_json = text_to_sql(mysql_sys_id,"The average unemployment ratio of 1995 and 1996, which one has higher percentage?")
    # sql_json = text_to_sql(mysql_sys_id,"List out the no. of districts that have female average salary is more than 6000 but less than 10000?")
    
    #sql = text_to_sql(mysql_sys_id, "What is the average GDP growth rate of each state in Malaysia in 2019?")
    # sql = text_to_sql(mysql_sys_id,"The average unemployment ratio of 1995 and 1996, which one has higher percentage?")
    #sql = text_to_sql(mysql_sys_id,"Find the top 5 customers who spent the most in 2025.")

 
    # manager.register_source('mysql_db', mysql_source)
    # sql_res = mysql_source.execute(cursor, sql)
    # mysql_source.close_cursor(cursor)
    print(sql_json)