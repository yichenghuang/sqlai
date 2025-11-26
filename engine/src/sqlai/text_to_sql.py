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
- Any double quote (") inside a string value (especially inside SQL queries) MUST be escaped as \".
- Backslashes inside string values must be escaped as \\.
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
  query and possibly the analysis of why the SQL is wrong.
1. **CRITICAL REVIEW:** Compare the previous SQL with the original user query.
   Consider the given analysis and determine which concepts/filters in the query 
   were missed or handled incorrectly.
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
- Use exact column name in SQL.
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

Analysis of previous SQL:
{analysis}

Review and improve this query to better match the question and schema.
"""


text2sql_review_sys_prompt = """
You are an expert SQL correctness reviewer. Your only job is to critically examine a 
generated SQL query and determine whether it is 100% correct given:

1. The exact database schema (column names, types, descriptions, and column comments)
2. The original natural-language user question
3. The LLM-generated query intent
4. The analysis of SQL correctness (if provided)

Pay extreme attention to filtering conditions (WHERE clause, HAVING, JOIN ON conditions 
that act like filters). The most common fatal mistake is using the wrong column because 
two columns have similar names.

Verification checklist you MUST follow and explicitly reason about:

1. Column semantic match
   - For every filtering condition (col = 'value', col LIKE '...', etc.), identify the primary
     term used in the query or query intent
     (e.g., if the query is "accounts who have region in Prague," the term is 'region').
   - If there is other column whose **col_comment** contains the noun, return false.

2. Join correctness
   - If the filter is applied after a JOIN, is the column coming from the correct table?

3. Edge cases
   - Case sensitivity, trailing spaces, partial vs exact match requirements
   - NULL handling
   - Whether an IN () list is complete

4. Overall query logic
   - Does the SELECT, GROUP BY, aggregation match the question?
   - Are all necessary tables joined?

Output format (strictly follow):

{
  "is_correct": true|false,
  "analysis": "clear explanation of what was wrong"
 }
"""

text2sql_review_user_prompt ="""
### Input
User question:
{user_query}

User Intent:
{intent_json}

Tables schema:
{tables_json}

Previously generated SQL:
{prev_sql}
"""

#  "critical_errors": [
#     "Bullet list of fatal errors (if any). If none, put []"
#   ],
#   "warnings": [
#     "Non-fatal issues or improvements (can be empty)"
#   ],
#   "recommended_fix": "If is_correct = false, provide the complete corrected SQL. If true, put null",
#   "explanation": "Clear step-by-step reasoning (mandatory, 3–10 sentences) that proves why each filtering condition is right or wrong, quoting schema descriptions/comments and the user question"


def get_used_tables(table_list, used_list):
    used = {(d['db'], d['table']) for d in used_list}
    return [t for t in table_list if (t['db'], t['table']) in used]


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
        'table': d['table'],
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
    sql_analysis = "None"
    for attempt in range(1, max_retries + 1):
        if (attempt == 1 or confidence < 0.2 or intent_json is None):
            # Low confidence is most likely due to missing table matches.
            # Re-run table matching if necessary.
            if (attempt > 1 or sql is not None):
                threshold -= threshold_delta
                threshold_delta *= 0.7
            qry_intent = analyze_query(user_qry)
            try:
                intent_json = json.loads(qry_intent)
            except json.JSONDecodeError as e:
                continue
            

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
                confidence=confidence, prev_sql=sql, analysis = sql_analysis)
            response = llm_chat(qry, text2sql_refine_sys_prompt)
        try:
            sql_json = json.loads(response)
        except json.JSONDecodeError as e:
            continue

        logger.info(sql_json)
        confidence = sql_json["confidence"]
        if sql is not None:
          sql_analysis = sql_json["analysis"]

        sql = sql_json["sql"]
        if confidence >= 0.9:
            used_tables = sql_json["used_tables"]
            matched_used_tables = get_used_tables(matched_tbls, used_tables)
            qry = text2sql_review_user_prompt.format(user_query = user_qry, 
                intent_json = intent_json, tables_json=matched_used_tables, 
                confidence=confidence, prev_sql=sql)
            response = llm_chat(qry, text2sql_review_sys_prompt)
            print(response)   
            try:
                review_sql_json = json.loads(response)
            except json.JSONDecodeError as e:
                continue

            if review_sql_json["is_correct"] is True:
                return sql_json
            else:
                sql_analysis = review_sql_json["analysis"]

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
        val_str = str(value).strip().upper()
        if val_str not in {'0', 'NULL', 'NONE', ''} and value is not None:
            print(val_str)
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
            print(res)
            if is_valid_result(res):
              print(f"Number of tries: {attempt}")
              break  # Success → exit loop early
        except Exception as e:
            sql_error = str(e)
            continue

    ds.close_cursor(cursor)
    
    return res, sql