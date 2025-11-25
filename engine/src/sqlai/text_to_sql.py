import json
import logging
from sqlai.core.datasource import datasource
from sqlai.qry_analyzer import analyze_query
from sqlai.tbl_milvus import TableMilvus
from sqlai.utils.str_utils import parse_json, serialize_value
from sqlai.llm_service import llm_chat


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# milvus singleton for tables 
tbl_vdb = TableMilvus()

# - Generate SQL compatible with {database_name} database.

  #  "db": "salesdb",
  #   "table": "customers",
  #   "schema": {
  #     "id": {"schemaOrgProperty": "schema:identifier", "schemaOrgType": "schema:Integer", 
  #            "description": "Unique identifier for the person.", "type": "int"},
  #     "name": {"schemaOrgProperty": "schema:name", "schemaOrgType": "schema:Text", 
  #              "description": "The name of the person.", "type": "varchar"}
  #     "gender": {"schemaOrgProperty": "schema:gender", "schemaOrgType": "schema:Text",
  #                "description": "The gender of the person.", "type": "varchar"}
  #   }
  # },
  # {
  #   "db": "salesdb",
  #   "table": "orders",
  #   "schema": {
  #     "order_id": {"schemaOrgProperty": "schema:orderNumber", "schemaOrgType": "schema:Text", 
  #                  "description": "The identifier of the order.", "type": "int"},
  #     "customer_id": {"schemaOrgProperty": "schema:customer", "schemaOrgType": "schema:Integer", 
  #                  "description": "The identifier of the customer who placed the order.", "type": "int"}
  #     "Product_id": {"schemaOrgProperty": "schema:productID", "schemaOrgType": "schema:Integer", 
  #                    "description": "The identifier of the product included in the order.", "type": "int"}
  #     "date": {"schemaOrgProperty": "schema:orderDate", "schemaOrgType": "schema:DateTime", 
  #              "description": "The date and time when the order was placed.", "type": "datetime"}
  #     "amount": {"schemaOrgProperty": "schema:totalPrice", "schemaOrgType": "schema:Number", 
  #                "description": "The total price of the order.", "type": "double"}}


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
- Prioritize each column’s 'col_comment' for semantic cues (e.g., year, data source, context).
- When columns are similar, identify the explicit noun in the user's query that 
  defines the filter. 
  * Choose the column whose `col_comment` (or column name if no comment) contains 
    the exact noun/phrase or its closest direct synonym.
  * Never select a column just because it contains a related but broader/looser word.
  * Only if no exact intent match exists, choose column based on the 
    col_comment, description or name that best matches the user’s intent.
- Ignore irrelevant tables and columns.
- Always include the database name as a prefix when tables come from different databases.
- When columns are similar, choose based on the comment/description that best matches the user’s intent.
- If some information is ambiguous or missing, make the best reasonable assumption and lower your confidence accordingly.
- Your response must be valid JSON only as below. No other text.

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

Analytical intent:
{
  "intent": "Ordering and Filtering",
  "metrics": [
    "Total amount spent by customer"
  ],
  "attributes": [
    "Customer"
  ],
  "filters": [],
  "time_constraints": [
    "Year 2024"
  ],
  "search_text": "Customer orders with order date and amount spent."
}

Available tables:
[
  {
    "db": "salesdb",
    "table": "customers",
    "schema": {
      "id": {"description": "Unique identifier for the person.", "type": "int", "col_comment"=""},
      "name": {"description": "The name of the person.", "type": "varchar", "col_comment"=""}
      "gender": {"description": "The gender of the person.", "type": "varchar", "col_comment"=""}
    }
  },
  {
    "db": "salesdb",
    "table": "orders",
    "schema": {
      "order_id": {"description": "The identifier of the order.", "type": "int", "col_comment=""},
      "customer_id": {"description": "The identifier of the customer who placed the order.", "type": "int", "col_comment=""}
      "Product_id": {"description": "The identifier of the product included in the order.", "type": "int", "col_comment=""}
      "date": {"description": "The date and time when the order was placed.", "type": "datetime", "col_comment=""}
      "amount": {"description": "The total price of the order.", "type": "double", "col_comment=""}}

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

Analytical intent:
{intent_json}

Available tables:
{tables_json}
"""

# refine prompt
text2sql_refine_sys_prompt = """
You are an expert SQL query auditor and refiner.

**REGENERATION INSTRUCTION:**
  You are fixing a previously generated SQL query with low confidence given the 
  natural language question, the database schema, and a previously generated SQL
  query.
1. **CRITICAL REVIEW:** Compare the previous SQL with the original user query. 
   Determine which concepts/filters in the query were missed or handled incorrectly.
2. **SEMANTIC SEARCH:** If the missing filter or required data is not available 
   in the tables currently joined, perform a semantic search across the entire 
   schema to locate the correct table and column (using the description and 
   col_comment).
3. **JOIN CORRECTION:** If a new table is required, you **must** add the correct 
   `JOIN` clause.
4. **OUTPUT:** Generate the complete, corrected SQL and recalcuate a new 
   confidence socre(0-1). Correctness and completeness are paramount.

Rules:
- Use only provided tables and columns.
- Keep query logic simple and accurate.
- Avoid unnecessary subqueries if the same result can be expressed through joins.
- Prioritize each column’s 'col_comment' for semantic cues (e.g., year, data source, context).
- When columns are similar, identify the explicit noun in the user's query that 
  defines the filter. 
  * Choose the column whose col_comment (or column name if no comment) contains 
    the exact noun/phrase or its closest direct synonym.
  * Never select a column just because it contains a related but broader/looser word.
  * Only if no exact intent match exists, choose column based on the 
    col_comment, description or name that best matches the user’s intent.
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


# refine prompt
text2sql_revise_sys_prompt = """
You are an expert SQL analyst and debugger. 
Your task is to analyze an incorrect SQL query generated by another LLM, 
identify all errors (especially in the WHERE clause, JOIN conditions, 
column selections, aggregations, and filtering logic), and produce a correct, 
efficient SQL query that exactly answers the user's natural-language question.

You will be given:
1. The user's original question
2. 3. The faulty SQL query
3. The relevant table schemas (with column names, data types, descriptions, and 
  col_comment)

  


"""

def find_matched_tables(matched_tbls, threshold):
    # qry_json = parse_json(intent_json)
    # search_text = qry_json["search_text"]

    # matched_tbls = tbl_vdb.search_tables(sys_id, search_text)
 
    # Filter dictionaries with score > 0.77, an empiric value!
    filtered_tbls = [item for item in matched_tbls if item["score"] > threshold]

    # logger.info("matched_tbls", extra={"filtered_tbls": filtered_tbls})

    if not filtered_tbls:
        sorted_tbls = sorted(matched_tbls, key=lambda x: x["score"], reverse=True)
        filtered_tbls = sorted_tbls[:1]

    if not filtered_tbls:
        return None

    logger.info("text2sql", extra={"queried tables": 
      [{"table": item["table"], "score": item["score"]} for item in filtered_tbls]})

    # tables_json = [tbl["metadata"] for tbl in filtered_tbls]
    filtered_tbl_list = [
      {
        'db': d['db'],
        'tbl': d['table'],
        'comment': d.get('comment', ''),
        'schema': d['schema']
      }
      for d in filtered_tbls
    ]
    return filtered_tbl_list


def text_to_sql(sys_id, user_qry, sql, sql_error, max_retries=5):
    confidence = 0.0
    intent_json = None
    tables_json = None
    threshold = 0.70
    threshold_delta = 0.1
    matched_tbls = None
    for attempt in range(1, max_retries + 1):
        if (attempt == 1 or confidence < 0.2):
            # Low confidence is most likely due to missing table matches.
            # Re-run table matching if necessary.
            if (attempt > 1 or sql is not None):
                threshold -= threshold_delta
                threshold_delta *= 0.7
            qry_intent = analyze_query(user_qry)
            print("analyzed query: ", qry_intent)
            intent_json = json.loads(qry_intent)

            # search_text = serialize_value(intent_json)
            search_text = intent_json["search_text"]
            matched_tbls = tbl_vdb.search_tables(sys_id, search_text)

            tables_json = find_matched_tables(matched_tbls, threshold)
            
            if tables_json is None:
                continue

        if sql is None:    
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
        sql = sql_json["sql"]
        if confidence >= 0.9:
            return sql_json
        
    return None


def is_valid_result(result: list) -> bool:
    # No rows at all
    if not result:
        return True
    # More than 1 row → probably real data
    if len(result) > 1:
        return True

    row = result[0]
    # If row is empty dict
    if not row:
        return True
    # Check every value in the row
    for value in row.values():
        # Normalize to string for safe comparison
        print(value)
        val_str = str(value).strip().upper()
        if val_str not in {'0', 'NULL', 'NONE', ''} and value is not None:
            return True  # At least one real value → good result

    return False


def robust_text_to_sql(ds, qry):
    cursor = ds.get_cursor()
    sql = None
    sql_error = None
    res = None
    for attempt in range(1, 4):  # 1st and 2nd attempt only
        sql_json = text_to_sql(ds.sys_id(), qry, sql, sql_error)
        if sql_json is None:
            continue
        
        logger.info(f'sql: {sql_json}')

        db = sql_json["used_tables"][0]["db"]
        sql = sql_json["sql"]
        try:
            ds.execute(cursor, f"USE `{db}`")       # ignore return

            res = ds.execute(cursor, sql)
            print(len(res))
            if is_valid_result(res):
              print(f"Number of tries: {attempt}")
              break  # Success → exit loop early
        except Exception as e:
            sql_error = str(e)
            continue

    ds.close_cursor(cursor)
    
    return res, sql