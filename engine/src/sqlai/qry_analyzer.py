import json
import logging
from sqlai.llm_service import llm_chat

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

logger = logging.getLogger()


def analyze_query(user_qry):
    """
    Analyze a user query and extract its intention and details
    """
    qry = analyze_table_qry.format(USER_QUERY=user_qry)
    response = llm_chat(qry)
    return response

if __name__ == '__main__':
    logger.info("Test table analyzer")
    # response = analyze_query("Show me the total revenue and count of orders by country for 2023 spring, but only for customers in Europe.")
    response = analyze_query("Show me the total revenue by Product categories")
    print(response)
