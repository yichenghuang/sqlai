import logging
from sqlai.qry_analyzer import analyze_query
from sqlai.tbl_milvus import TableMilvus
from sqlai.utils.str_utils import parse_json, remove_code_block
from sqlai.llm_service import llm_chat


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# milvus singleton for tables 
tbl_vdb = TableMilvus()

text2sql_sys_prompt = """
You are an expert SQL query generator. 
Your task: Convert natural language questions into SQL queries using the provided table descriptions.

Rules:
- Only use the provided tables and columns.
- Use joins only when needed.
- Prefer descriptive fields over IDs where relevant.
- Ignore irrelevant tables and columns.
- Return only the SQL query, nothing else.

Example:
User question:
"Find the top 5 customers who spent the most in 2024."

Available tables:
[
  {
    "db": "salesdb",
    "table": "customers",
    "schema": {
      "id": {"schemaOrgProperty": "schema:identifier", "schemaOrgType": "schema:Integer", 
             "description": "Unique identifier for the person.", "type": "int"},
      "name": {"schemaOrgProperty": "schema:name", "schemaOrgType": "schema:Text", 
               "description": "The name of the person.", "type": "varchar"}
      "gender": {"schemaOrgProperty": "schema:gender", "schemaOrgType": "schema:Text",
                 "description": "The gender of the person.", "type": "varchar"}
    }
  },
  {
    "db": "salesdb",
    "table": "orders",
    "schema": {
      "order_id": {"schemaOrgProperty": "schema:orderNumber", "schemaOrgType": "schema:Text", 
                   "description": "The identifier of the order.", "type": "int"},
      "customer_id": {"schemaOrgProperty": "schema:customer", "schemaOrgType": "schema:Integer", 
                   "description": "The identifier of the customer who placed the order.", "type": "int"}
      "Product_id": {"schemaOrgProperty": "schema:productID", "schemaOrgType": "schema:Integer", 
                     "description": "The identifier of the product included in the order.", "type": "int"}
      "date": {"schemaOrgProperty": "schema:orderDate", "schemaOrgType": "schema:DateTime", 
               "description": "The date and time when the order was placed.", "type": "datetime"}
      "amount": {"schemaOrgProperty": "schema:totalPrice", "schemaOrgType": "schema:Number", 
                 "description": "The total price of the order.", "type": "double"}}

    }
  }
]

Expected SQL:
SELECT c.name, SUM(o.amount) AS total_spent
FROM salesdb.customers c
JOIN salesdb.orders o ON c.id = o.customer_id
WHERE YEAR(o.date) = 2024
GROUP BY c.name
ORDER BY total_spent DESC
LIMIT 5;

"""

text2sql_user_prompt = """
### Input
User question:
{user_query}

Available tables (JSON list of dicts):
{tables_json}

### Output
A SQL query.

"""


def text_to_sql(sys_id, user_qry):
    ana_qry = analyze_query(user_qry)
    print("analyzed query: ", ana_qry)
    qry_json = parse_json(ana_qry)
    search_text = qry_json["search_text"]

    matched_tbls = tbl_vdb.search_tables(sys_id, search_text)

    # Filter dictionaries with score > 0.8
    filtered_tbls = [item for item in matched_tbls if item["score"] > 0.8]

    if not filtered_tbls:
        sorted_tbls = sorted(matched_tbls, key=lambda x: x["score"], reverse=True)
        filtered_tbls = sorted_tbls[:1]

    logger.info("text2sql", extra={"queried tables": 
        [{"table": item["metadata"]["table"], "score": item["score"]} for item in filtered_tbls]})

    qry = text2sql_user_prompt.format(user_query = user_qry, 
          tables_json=[tbl["metadata"] for tbl in filtered_tbls])
    
    response = llm_chat(qry, text2sql_sys_prompt)
    sql_str = remove_code_block(response, 'sql')
    return sql_str