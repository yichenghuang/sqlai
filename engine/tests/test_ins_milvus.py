import json
import logging
import sys
from pymilvus import CollectionSchema, FieldSchema, DataType, Collection
from sentence_transformers import SentenceTransformer
from sqlai.utils import json_formatter
from sqlai.tbl_milvus import TableMilvus

logger = logging.getLogger()
# handler = logging.FileHandler(sys.stdout)
# Create a StreamHandler for stdout
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(json_formatter.JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def read_jsonl(filename):
    objects = []
    try:
        with open(filename, 'r') as file:
            for line in file:
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        obj = json.loads(line)
                        objects.append(obj)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON line: {e}")
        return objects
    except FileNotFoundError:
        print(f"Error: File {filename} not found")
        return []
    except Exception as e:
        print(f"Error reading file: {e}")
        return []

def test_ins_milvus():
    tbl_annot = read_jsonl('mysql_tbl_annot.jsonl')

    tbl_vdb = TableMilvus()

    for annot in tbl_annot:
        res = tbl_vdb.insert_tables(annot['table_annotation'], annot['metadata']['table'], annot['metadata'])
# matches = tbl_vdb.search_tables(query='weather station')
# for item in matches:
#     metadata = item['metadata']
#     score = item['score']
#     print(f"Metadata: {metadata}, Score: {score}")

if __name__ == '__main__':
    logger.info("Test inserting mysql table annotatons into milvus")
    test_ins_milvus()