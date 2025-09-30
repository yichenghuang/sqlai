import json
import logging
from sqlai.llm_service import llm_chat

analyze_table_qry = """You are a data analyst assistant. 
Your task is to analyze a natural language question and extract its core intent 
and important details for SQL generation.

### User Question:
{USER_QUERY}

### Instructions:
1. Identify the **main intent** of the query (e.g., aggregation, filtering, join, grouping, ordering).
2. Extract **measures/metrics** the user wants (e.g., revenue, count of customers, average order value).
3. Extract **dimensions/attributes** that the user wants the data grouped or filtered by (e.g., country, year, customer name).
4. Extract **filters/conditions** and **time constraints** (e.g., "in 2023", "customers in Europe").
5. Generate a **descriptive search text** from the user query that looks like a database table description. 
   including:
   - *table purpose** (e.g., "orders", "customers", "sales").
   - Broad **column description** like 'location', 'date' to describe attributes relevant to the query 
   - Use base metric names (e.g., 'revenue', 'sales amount') when describing metrics, avoiding aggregated terms like 'total', 'sum', or 'average'.
   - Avoiding filter details and specific phrases like 'filter by', 'only', or 'group by'.

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
