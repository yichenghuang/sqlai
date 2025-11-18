import json
import logging
from sqlai.qry_analyzer import analyze_query
from sqlai.tbl_milvus import TableMilvus
from sqlai.utils.str_utils import parse_json, serialize_value
from sqlai.llm_service import llm_chat


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# milvus singleton for tables 
tbl_vdb = TableMilvus()

# - Generate SQL compatible with {database_name} database.

text2sql_sys_prompt = """
You are an expert SQL query generator. 
Your task is to generate **correct, efficient SQL** based on the 
**user question**, the **structured analytical intent**, and the provided 
**tables descriptions**.

Rules:
- Only use the provided tables and columns.
- Follow the intent JSON exactly:
   - `metrics`: Apply correct aggregation (e.g., COUNT → `COUNT(*)`, average → `AVG(col)`)
   - `attributes`: Use in `GROUP BY`, `SELECT`, or joins
   - `filters`: Translate into `WHERE` or `HAVING` clauses **with exact logic**
   - `time_constraints`: Apply to date/time columns
- Use joins only when needed.
- Prefer INNER JOINs over subqueries for linking tables.
- Use subqueries only if they’re logically required (e.g., aggregation before filtering)
- Prefer descriptive fields (e.g., `customer_name` over `customer_id`).
- Ignore irrelevant tables and columns.
- Always include the database name as a prefix when tables come from different databases.
- Prioritize each column’s comment for semantic cues (e.g., year, data source, context).
- When columns are similar, choose based on the comment/description that best matches the user’s intent.
- If some information is ambiguous or missing, make the best reasonable assumption and lower your confidence accordingly.
- Return the result strictly as a valid JSON object with the following fields:
  {
    "sql": "<the generated SQL query as a string>",
    "used_tables": [{"db": "<database_name>", "table": "<table_name>"}, ...]
    "confidence": <float between 0 and 1>
  }

Output rules:
- The "sql" value must contain the full SQL query string.
- The "used_tables" array must list all tables referenced in the SQL.

Guidelines for "confidence":
- 0.9-1.0: Very clear, unambiguous, matches schema perfectly.
- 0.7–0.9: Mostly confident, but some minor ambiguity or missing detail.
- 0.4–0.7: Some uncertainty (e.g., column name guess, unclear join path or aggregation logic).
- <0.4: High ambiguity, schema mismatch, or risky operation.

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

Expected output:
{
  "sql": "SELECT c.name, SUM(o.amount) AS total_spent FROM customers c JOIN orders o ON c.id = o.customer_id WHERE YEAR(o.date) = 2024 GROUP BY c.name ORDER BY total_spent DESC LIMIT 5;",
  "used_tables": [
    {"db": "salesdb", "table": "customers"},
    {"db": "salesdb", "table": "orders"}
  ]
  "confidence": 0.95
}

"""

text2sql_user_prompt = """
### Input
User question:
{user_query}

Analytical Intent (MUST FOLLOW):
{intent_json}

Available tables:
{tables_json}
"""

# refine prompt
text2sql_refine_sys_prompt = """
You are an expert SQL query auditor and refiner.

Your task:
Given a natural language question, the database schema, and a previously generated SQL query with low confidence, you must:
1. Analyze whether the SQL correctly answers the question using the provided schema.
2. Identify issues (e.g., incorrect columns, missing filters, wrong joins).
3. Revise the SQL to fix those issues and increase correctness.
4. Recalculate a new confidence score (0–1).

Rules:
- Use only provided tables and columns.
- Keep query logic simple and accurate.
- Avoid unnecessary subqueries if the same result can be expressed through joins.
- Prefix tables with their database name if they come from different databases.
- Output must be a **valid JSON** in this format:

{
  "analysis": "<brief explanation of what was wrong and what was changed>",
  "sql": "<corrected SQL query>",
  "used_tables": [{"db": "<database_name>", "table": "<table_name>"},...],
  "confidence": <float between 0 and 1>
}
"""

text2sql_refine_user_prompt = """
### Input
User question:
{user_query}

Analytical Intent (MUST FOLLOW):
{intent_json}

Available tables:
{tables_json}

Previously generated SQL (low confidence = {confidence}):
{prev_sql}

Review and improve this query to better match the question and schema.
"""


def find_matched_tables(sys_id, search_text, threshold):
    # qry_json = parse_json(intent_json)
    # search_text = qry_json["search_text"]

    matched_tbls = tbl_vdb.search_tables(sys_id, search_text)
 
    # Filter dictionaries with score > 0.77, an empiric value!
    filtered_tbls = [item for item in matched_tbls if item["score"] > threshold]

    logger.info("matched_tbls", extra={"filtered_tbls": filtered_tbls})

    if not filtered_tbls:
        sorted_tbls = sorted(matched_tbls, key=lambda x: x["score"], reverse=True)
        filtered_tbls = sorted_tbls[:1]

    if not filtered_tbls:
        return None

    logger.info("text2sql", extra={"queried tables": 
      [{"table": item["metadata"]["table"], "score": item["score"]} for item in filtered_tbls]})

    tables_json = [tbl["metadata"] for tbl in filtered_tbls]
    return tables_json


def text_to_sql(sys_id, user_qry, max_retries=5):
    sql = ""
    confidence = 0.0
    intent_json = None
    tables_json = None
    threshold = 0.7
    for attempt in range(1, max_retries + 1):
        if (attempt == 1 or confidence < 0.2):
            # Low confidence is most likely due to missing table matches.
            # Re-run table matching if necessary.
            if (attempt > 1):
                threshold -= 0.05
            qry_intent = analyze_query(user_qry)
            intent_json = json.loads(qry_intent)
            print("analyzed query: ", intent_json)
            search_text = serialize_value(intent_json["semantic"])
            tables_json = find_matched_tables(sys_id, search_text, threshold)
            
            if tables_json is None:
                continue
            
            qry = text2sql_user_prompt.format(user_query = user_qry, 
                intent_json = intent_json, tables_json=tables_json)
            response = llm_chat(qry, text2sql_sys_prompt)
        else:
            qry = text2sql_refine_user_prompt.format(user_query = user_qry, 
                intent_json = intent_json, tables_json=tables_json, 
                confidence=confidence, prev_sql=sql)
            response = llm_chat(qry, text2sql_refine_sys_prompt)

        sql_json = json.loads(response)
        logger.info(sql_json)
        confidence = sql_json["confidence"]
        # sql = sql_json["sql"]
        if confidence > 0.7:
            return sql_json
        
    return None, None