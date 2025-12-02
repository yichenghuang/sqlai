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


domain_rules = """
{
  "column_mappings": {
    "region": "A3",
  },
  "keyword_mapping_rules": [
    { "if_user_mentions": ["loans"], 
      "join": "financial.loan"},
  ],
}
"""

# Rules:
# - Only use the provided tables and columns.
# - Follow the intent JSON exactly:
#    - `metrics`: Apply correct aggregation (e.g., COUNT → `COUNT(*)`, average → `AVG(col)`)
#    - `attributes`: Use in `GROUP BY`, `SELECT`, or joins
#    - `filters`: Translate into `WHERE` or `HAVING` clauses **with exact logic**
#    - `time_constraints`: Apply to date/time columns
# - Use joins only when needed.
# - Prefer INNER JOINs over subqueries for linking tables.
# - Use subqueries only if they’re logically required (e.g., aggregation before filtering)
# - Prefer descriptive fields (e.g., `customer_name` over `customer_id`).
# - Prioritize each column’s 'col_comment' for semantic cues (e.g., year, data source, context).
# - When columns are similar, identify the explicit noun in the user's query that 
#   defines the filter. 
#   * Choose the column whose `col_comment` (or column name if no comment) contains 
#     the exact noun/phrase or its closest direct synonym.
#   * Never select a column just because it contains a related but broader/looser word.
#   * Only if no exact intent match exists, choose column based on the 
#     col_comment, description or name that best matches the user’s intent.
# - Ignore irrelevant tables and columns.
# - Always include the database name as a prefix when tables come from different databases.
# - When columns are similar, choose based on the comment/description that best matches the user’s intent.
# - DOMAIN-SPECIFIC RULES override ambiguous user wording or schema ambiguity.
#   If there is any conflict between general interpretation and domain-specific rules,
#   ALWAYS apply the domain-specific rule.
#   When interpreting user text, resolve column in this order:
#   (1) Domain-specific rules
#   (2) col_ocmment
#   (3) colum name
#   (4) column description

# - If some information is ambiguous or missing, make the best reasonable assumption and lower your confidence accordingly.

text2sql_sys_prompt = """
You are an expert SQL query generator. 
Your task is to generate **correct SQL only** based on: the 
**user question**, the **extracted analytical intent**, and the 
**tables schema**, and the DOMAIN-SPECIFIC RULES (if provided).

==========================
  General Rules
==========================
- Only use the provided tables and columns.
- Use JOINs only when necessary (prefer INNER JOINs).
- Use subqueries only when logically required (e.g., aggregation before filtering).
- Always prefix tables with database names when multiple databases are involved.
- Prefer descriptive fields (e.g., customer_name over customer_id).
- Ignore irrelevant tables and columns.
- Time constraints must apply to valid date/time columns.
- Metrics must apply correct aggregation: COUNT → COUNT(*), AVG → AVG(col), etc.
- Attributes should appear in SELECT, GROUP BY, or JOINs..
- Filters must translate into WHERE or HAVING with exact logic.

==========================
  COLUMN RESOLUTION RULES
==========================
When determining which column matches a user phrase:

(1) DOMAIN-SPECIFIC RULES  
    - These OVERRIDE all natural-language interpretation and schema ambiguity.  
    - If domain rules match, ALWAYS use those columns even if user wording seems ambiguous.

(2) Column 'col_comment'  
    - Prefer columns whose comment explicitly contains the noun/phrase used in the query.

(3) Column name  
    - Use exact or closest direct synonym match in the name.

(4) Column description  
    - Use description only if no better match exists.

Additional selection constraints:
- Never choose a column just because it contains a loosely related or broader word.
- Only if no exact match exists, choose based on best semantic fit across comment/name/description.

==========================
  NO BUSINESS-LOGIC INFERENCE RULE
==========================
This means:
- Do NOT assume thresholds, classifications, statuses, or business rules.
- Do NOT infer meanings like "active", "valid", "current", "successful", 
  "eligible", "completed", "latest", etc., unless the intent JSON provides 
  explicit filters.
- Do NOT transform or reinterpret user intent using business knowledge 
  (e.g., “Prague region” → assume multiple districts unless provided).
- Only map user phrasing to columns; never inject your own domain assumptions.

If the input does not specify a condition, DO NOT invent one.

==========================
  INTENT JSON COMPLIANCE
==========================
Follow the intent JSON STRICTLY:
- metrics: generate correct aggregations.
- attributes: place in SELECT/GROUP BY or JOIN.
- filters: convert directly into WHERE/HAVING with exact comparison logic.
- time_constraints: apply strictly to date/time fields.

==========================
  SQL OUTPUT RULES
==========================
Produce a valid JSON only as below. No other text.
  {
    "sql": "<the generated SQL query as a string>",
    "used_tables": [{"db": "<database_name>", "table": "<table_name>"}, ...]
    "confidence": <float between 0 and 1>
  }
  
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

Domain-specific rules:
{domain_rules}
"""

# refine prompt
text2sql_refine_sys_prompt = """
You are an expert SQL query auditor and refiner.

You are fixing a previously generated SQL query with low confidence, given:
- the natural language question,
- the database schema,
- a previously generated SQL query,
- optional analysis of why the previous SQL is wrong,
- and optional DOMAIN-SPECIFIC RULES provided in the user prompt.

### REGENERATION INSTRUCTION ### 

1. CRITICAL REVIEW:
   Compare the previous SQL with the original user query.
   Identify missing filters, misinterpreted columns, missing JOINs, or incorrect 
   use of tables/aggregations. Incorporate the given analysis if provided.

2. APPLY DOMAIN-SPECIFIC RULES (if provided):
   - These rules override ambiguous user wording.
   - They define mandatory column mappings, keyword→column links, 
     or preset filter conditions.
   - Always apply these rules before any semantic guessing or 
     schema-based inference.
   - If domain rules conflict with the previous SQL, correct the SQL.

3. SEMANTIC SEARCH:
   If required filters/fields are not in the currently joined tables,
   perform a semantic search across the entire schema using:
   - column name,
   - description,
   - col_comment.
   Select the most semantically correct column.

4. JOIN CORRECTION: 
   If required data lives in another table, you MUST add the correct JOIN.

4. OUTPUT: 
   Generate the complete, corrected SQL and recalcuate a new 
   confidence socre(0-1). Correctness and completeness are paramount.


### Rules ### 

- Use only provided tables and columns.
- Keep logic minimal and correct.
- Avoid unnecessary subqueries if a join is sufficient.
- Use exact column name from the schema.
- Prioritize each column’s "col_comment" as the strongest semantic signal.
- When columns are similar, 
  * Match the explicit noun/phrase from the user’s query.
  * Only use broader synonyms if no direct match exists.
- If domain-specific rules specify a column or filter, ALWAYS use them.


### OUTPUT FORMAT (MUST BE VALID JSON) ###

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

Domain-specific rules:
{domain_rules}

Review and improve this query to better match the question and schema.
"""


text2sql_review_sys_prompt = """
You are an expert SQL correctness reviewer. Your only job is to critically examine a 
generated SQL query and determine whether it is 100% correct given:

1. The exact database schema (column names, types, descriptions, and column comments)
2. The original natural-language user question
3. The LLM-generated query intent
4. The analysis of SQL correctness (if provided)
5. The DOMAIN-SPECIFIC RULES (if provided)

Domain-specific rules define mandatory column mappings, keyword→column associations,
or required filter conditions. These rules override ambiguous natural-language wording.
If a domain rule contradicts the SQL query, the SQL must be marked incorrect.

-------------------------------------
MANDATORY VERIFICATION CHECKLIST
You MUST follow these and explicitly reason about them:
-------------------------------------

1. Column semantic match
   - For every filtering condition (col = 'value', col LIKE '...', etc.), 
     * identify the primary noun/phrase term from the user query or query intent.
     * Check whether the SQL uses the correct column by:
       a) Matching with domain-specific rules (highest priority)
       b) Matching the column's col_comment (strongest semantic indicator)
       c) Matching description or column name if needed
   - If another column has a col_comment that better matches the explicit intent noun,
     the SQL is incorrect.

2. Join correctness
   - When a filtered column comes from a JOINed table, confirm the SQL joins the correct
     table and uses the correct ON condition.
   - Ensure the filter is not mistakenly applied using a semantically wrong table alias.

3. Overall query logic
   - SELECT clause matches the aggregation or measure requested
   - GROUP BY is present when required
   - All necessary tables are joined and no unnecessary tables are included
   - No missing filters implied by domain rules or by the user’s intent

4. Filter Source Verification (MANDATORY — step-by-step reasoning)
   For every filter in the SQL (WHERE, HAVING, JOIN ON filters), you MUST:
     1. Explicitly list each filter found in the SQL.
     2. For each filter, examine its possible origin in this exact order:
        (a) Does the user question explicitly or implicitly request it?
        (b) Does the intent JSON require it?
        (c) Is it required by domain-specific rules?
        (d) Is it required for the JOIN logic to function?
     3. If a filter does NOT come from (a), (b), (c), or (d),
        it is an unnecessary or unjustified filter,
        and the SQL must be marked incorrect.
     4. A filter that restricts results beyond what the user intended
      makes the SQL incorrect, even if it appears “reasonable.”


### Output format (strictly follow) ###

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

Domain-specific rules:
{domain_rules}
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
                intent_json = intent_json, tables_json=tables_json,
                domain_rules = domain_rules)
            response = llm_chat(qry, text2sql_sys_prompt)
        else:
            qry = text2sql_refine_user_prompt.format(user_query = user_qry, 
                intent_json = intent_json, tables_json=tables_json, 
                confidence=confidence, prev_sql=sql, analysis = sql_analysis,
                domain_rules = domain_rules)
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
                confidence=confidence, prev_sql=sql, domain_rules = domain_rules)
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