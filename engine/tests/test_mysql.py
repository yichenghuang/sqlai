import os
import sys
import json
import logging
import MySQLdb
from sqlai import tbl_annotor
from sqlai.utils import json_formatter


mysql_host = "127.0.0.1"
username = os.getenv('DB_USERNAME') or ""
password = os.getenv('DB_PASSWORD') or ""  


logger = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(json_formatter.JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def write_jsonl(filename, text):
    try:
        if isinstance(text, str):
            data = json.loads(text) # convert string to json object
        else:
            data = text  # Use as-is if already a Python object
        with open(filename, 'a') as file:
            json.dump(data, file, ensure_ascii=False)
            file.write('\n')  # Append newline for JSONL format
        print(f"Successfully appended text to {filename}")
    except Exception as e:
        print(f"Error appending to file: {e}")

def annotate_db_table(conn, db):
    with conn.cursor() as cursor:
        try:
            cursor.execute(f"USE {db}")
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall() # Fetches all rows 

            for tbl in tables:
                logger.info(tbl)
                cursor.execute(f"SELECT * FROM {tbl[0]} LIMIT 5")
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
                               AND TABLE_NAME = '{tbl[0]}'""");
                schema = cursor.fetchall()
                if (len(headers) != len(schema)):
                    schema = None

                # Get table comment
                cursor.execute(f"""SELECT TABLE_COMMENT
                                 FROM INFORMATION_SCHEMA.TABLES
                                 WHERE TABLE_SCHEMA = '{db}'
                                 AND TABLE_NAME = '{tbl[0]}'""")
                row = cursor.fetchone()
                comment = row[0] if row else ''

                tbl_annot, col_annot = tbl_annotor.annotate_table(table, schema, 
                                                                 comment)
                table_annot_json = json.loads(tbl_annot)
 
                tbl_meta = {"db": db, "table": tbl[0], "description": comment, 
                            "schema": col_annot}
                table_annot_json["metadata"] = tbl_meta
                logger.info(table_annot_json)
                write_jsonl(f'mysql_tbl_annot.jsonl', table_annot_json)

        except MySQLdb.Error as e:
            logger.info(f"Error: {e}")
            return 0

    return 0


def test_mysql():
    conn = MySQLdb.connect(
        host = mysql_host,
        user = username,
        passwd = password,
    )

    cursor = conn.cursor()

    try:
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall() # Fetches all rows
        for db in databases:
            logger.info(f"Processing database: {db[0]}")
            annotate_db_table(conn, db[0])

    except MySQLdb.Error as e:
        logger.info(f"Error: {e}")
        return 0


if __name__ == '__main__':
    logger.info("Test annotation on mysql database")
    test_mysql()