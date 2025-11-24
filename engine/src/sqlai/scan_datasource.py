import json
import logging
import datetime
import threading
from sqlai import tbl_annotor
from sqlai.core.datasource.datasource import DataSource
from sqlai.tbl_milvus import TableMilvus
from sqlai.core.job_tracker import JobTracker


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _serialize_value(value) -> str:
    """Recursively converts a value (string, list, or dict) into a flat string."""
    if isinstance(value, list):
        # Join list items with commas
        return ", ".join([_serialize_value(item) for item in value])
    elif isinstance(value, dict):
        # Recursively process dict keys/values
        parts = []
        for k, v in value.items():
            # Format as "key: value"
            parts.append(f"{k}: {_serialize_value(v)}")
        return "; ".join(parts)
    else:
        # Treat as a basic string
        return str(value)


def create_table_embedding_input(table_annot_json, col_annot_json):
    """
    Combines the table tag and column annotations into a custom string
    by treating all keys as general content labels.
    """
    
    # 1. Generate the TABLE: section (Holistic Context)
    table_parts = []
    for key, value in table_annot_json.items():
        serialized_value = _serialize_value(value)
        # Format as "Key: serialized_value"
        table_parts.append(f"{key}: {serialized_value}")
        
    table_context = "TABLE: " + "; ".join(table_parts) + "."
    
    # 2. Generate the COLUMNS: section (Specific Details)
    
    column_strings = []
    # Iterate through each column, treating the column name as the primary key
    for col_name, col_data in col_annot_json.items():
        # Serialize the column data (category, property, tags, etc.)
        col_content = _serialize_value(col_data)
        
        # Format as "column_name (content)"
        col_string = f"{col_name} ({col_content})"
        column_strings.append(col_string)
        
    columns_context = "COLUMNS: " + "; ".join(column_strings) + "."
    
    # 3. Combine both sections
    return f"{table_context} {columns_context}"


def scan_table(data_src: DataSource, cursor, db: str, tbl: str):
    """Scans a table and returns its annotated metadata in JSON format.

    Args:
        data_src: DataSource object to interact with the database.
        cursor: Database cursor for executing queries.
        db: Name of the database.
        tbl: Name of the table.

    Returns:
        dict: JSON object containing table annotations and metadata.
    """
    tbl_data, schema, comment = data_src.inspect_table(cursor, db, tbl)

    tbl_annot_json, col_annot_json = tbl_annotor.annotate_table(tbl_data, schema, comment)
    # table_annot_json = json.loads(tbl_annot)
 
    tbl_annot_json.update({"db": db, "table": tbl, "comment": comment,
                "schema": col_annot_json})

    print(tbl_annot_json)

    return tbl_annot_json


def scan_datasource(data_src: DataSource, complete_time: datetime.datetime):
    """Scans all databases and tables in a data source, updating progress in 
       JobTracker.

    Args:
        data_src: DataSource object to interact with the database.
        complete_time: Previous completion time for the job.

    Returns:
        int: Number of tables scanned.
    """
    
    tbl_vdb = TableMilvus()
    tracker = JobTracker()

    sys_id = data_src.sys_id()
    cursor = data_src.get_cursor()
    dbs = data_src.get_databases(cursor)

    # For simplicity, drop the collection in the vector database
    tbl_vdb.drop_collection(sys_id)
    tbl_vdb.load_collection(sys_id)

    num_tbls = 0

    tracker.add_job(sys_id, complete_time)

    if not dbs:
        tracker.mark_complete(sys_id)
        return num_tbls
    
    num_dbs = len(dbs)
    db_share = 100.0 / num_dbs
    current_progress = 0.0

    logger.info(f"db_share: {db_share}")
    for db in dbs:
        tables = data_src.get_tables(cursor, db)
        total_tables = len(tables)

        if total_tables == 0:
            current_progress += db_share
            tracker.update_progress(sys_id, current_progress)
            continue

        processed_tables = 0
        for tbl in tables:
            logger.info(tbl)
            tbl_scan = scan_table(data_src, cursor, db, tbl)
            # table_annotation = create_table_embedding_input(tbl_scan['table_annotation'],
            #     tbl_scan['metadata']['schema'])
            print(tbl_scan)
            table_annotation_str = _serialize_value(tbl_scan)

            res = tbl_vdb.insert_tables(sys_id,
                                        table_annotation_str, 
                                        tbl_scan['table'],
                                        tbl_scan)
            processed_tables += 1
            logger.info(f"db: {db} tble: {tbl} scanned")

            db_progress_fraction = processed_tables / total_tables
            incremental_progress = db_progress_fraction * db_share
            new_total_progress = current_progress + incremental_progress
            tracker.update_progress(sys_id, new_total_progress)

        current_progress += db_share

    tracker.mark_complete(sys_id)
    print(sys_id)

    return num_tbls


def start_scan_datasource(data_src: DataSource, 
                          complete_time: datetime.datetime):
    """Starts the database scan in a background thread.

    Args:
        data_src: DataSource object for the scan.
        complete_time: Required completion time for the job.

    Returns:
        str: The unique job ID aka data_src_id for querying progress.
    """
    def run_scan():
        scan_datasource(data_src, complete_time)
    # Start the thread and return immediately
    thread = threading.Thread(target=run_scan)
    thread.start()
