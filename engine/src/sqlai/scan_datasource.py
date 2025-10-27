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

    tbl_annot, col_annot = tbl_annotor.annotate_table(tbl_data, schema, comment)
    table_annot_json = json.loads(tbl_annot)
 
    tbl_meta = {"db": db, "table": tbl, "description": comment,
                "schema": col_annot}
    table_annot_json["metadata"] = tbl_meta

    return table_annot_json


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
            res = tbl_vdb.insert_tables(sys_id,
                                        tbl_scan['table_annotation'], 
                                        tbl_scan['metadata']['table'],  
                                        tbl_scan['metadata'])
            processed_tables += 1
            logger.info(f"db: {db} tble: {tbl} scanned")

            db_progress_fraction = processed_tables / total_tables
            incremental_progress = db_progress_fraction * db_share
            new_total_progress = current_progress + incremental_progress
            tracker.update_progress(sys_id, new_total_progress)

        current_progress += db_share

    tracker.mark_complete(sys_id)

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
