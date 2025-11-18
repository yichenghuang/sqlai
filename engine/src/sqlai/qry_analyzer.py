import json
import logging
import sys
from sqlai.llm_service import llm_chat
from sqlai.utils.str_utils import serialize_value

analyze_table_qry = """You are a data analyst assistant. Your task is to analyze 
a natural language question and extract its structured intention.

### User Question:
{USER_QUERY}

### Instructions:

Return JSON with these fields in English:

- **intent**: Main analytical goal (e.g., aggregation, filtering, join, grouping, ordering).
- **metrics**: Quantifiable measures requested (e.g., revenue, count of custoge smers, average salary). Use full descriptive phrases.
- **attributes**: Dimensions or categorical fields involved in grouping, filtering, or analysis — **include inferred attributes** even if only values are mentioned (e.g., "female" → "gender").
- **filters** : Specific conditions applied (e.g., "average salary > 6000", "after 2021", "only female customers").
- **time constraints**: Any time-based filters (e.g., "in 2023", "last quarter").
- **search text**: A concise, table-purpose-focused description for vector search. Must include:
   - **Table purpose** (e.g., "orders", "demographics"). 
   - **Broad column descriptions** using **attribute names**, not values (e.g., 'gender', 'location', 'date').
   - **Base metric fields** (e.g., 'salary', 'employee count') — avoid aggregates like 'total', 'average'.   
   - **Inferred attributes**: Map common values to attributes:
        - "female", "male" → include **gender**
        - "adult", "child" → include **age group**
        - "urban", "rural" → include **area type**
        - percentages/ratios → may imply **population**, **household**
   - **Never include filter logic** like "only", "more than", "group by", or specific values in search_text.

### Output Format (JSON):
{{
  "intent": "...",
  "metrics": [...],
  "attributes": [...],
  "filters": [...],
  "time_constraints": [...],
  "search_text": "..."
}}
"""

query_analyzer_sys_prompt = """
You are a Text-to-SQL intent extraction assistant. Your job is to convert 
user question into a two-layer structured representation:

1. **Semantic**: Optimized for vector-based table and column matching.
2. **Logic**: Preserves all analytical and filtering conditions for SQL generation.

### OUTPUT FORMAT 

{
  "semantic": {
    "intent": "",
    "metrics": [],
    "attributes": [],
    "concepts": [],
    "search_text": ""
  },
  "logic": {
    "group_by": [],
    "filters": [],
    "time_constraints": [],
    "order_by": [],
    "limit": null
  }
}

### FIELD DEFINITIONS

#### 1. RETRIEVAL LAYER (For Vector Matching)
This layer must **not** contain specific values or filter logic.

- **intent**: High-level purpose. Choose one: "aggregation", "lookup", "filtering", "comparison", "topk", "ranking", "join".
- **metrics**: Quantifiable measures (e.g., "revenue", "customer count"). Use canonical names.
- **attributes**: Categorical fields/dimensions (e.g., "gender", "product category"). 
  Include inferred attributes.
- **concepts**: Core business entities or domains (e.g., "Order", "Product", "Customer").
- **search_text**: A concise natural-language summary for vector retrieval. 
  MUST combine concepts, metrics, and attributes. 
  MUST NOT copy the user query or contain filter/aggregate words.

#### 2. LOGIC LAYER (For SQL Generation)
This layer preserves all analytical and conditional details.

* **group_by**: List of attributes required for SQL GROUP BY.
* **filters**: Full, explicit logical filter expressions including values, **excluding temporal conditions**. E.g., "price > 1000", "region = 'Europe'".
* **time_constraints**: Full, interpreted temporal filters. E.g., "order_date between '2024-01-01' and '2024-12-31'".
* **order_by**: Sorting instructions (e.g., "sales DESC").
* **limit**: Numeric limit if explicitly requested (else null).

### IMPORTANT RULES

* Always fill all fields (use an empty array `[]` or `null` if not applicable).
* Do not hallucinate table or column names.
* Normalize synonyms to canonical forms.
* Retrieval Layer MUST NOT contain literal values.
* Logic Layer MUST include all values and full expressions.
* Output must ALWAYS be valid JSON in english.

"""


query_analyzer_user_prompt = """
### User Question:
{USER_QUERY}
"""

logger = logging.getLogger()


def analyze_query(user_qry):
    """
    Analyze a user query and extract its intention and details
    """
    qry = query_analyzer_user_prompt.format(USER_QUERY=user_qry)
    response = llm_chat(qry, query_analyzer_sys_prompt)
    return response

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python qry_analyzer.py \"your query here\"")
        sys.exit(1)
    
    query = " ".join(sys.argv[1:])  # supports multi-word queries
    
    response = analyze_query(query)
    # response = analyze_query("Show me the total revenue and count of orders by country for 2023 spring, but only for customers in Europe.")
    # response = analyze_query("Show me the total revenue by Product categories")
    # response = analyze_query("不同性別的銷售總額")
    print(response)
    qry_json = json.loads(response)
    s = serialize_value(qry_json["semantic"])
    print(s)
