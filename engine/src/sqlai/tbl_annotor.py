import json
import logging
from wcwidth import wcswidth
from sqlai.llm_service import llm_chat


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

ann_col_qry = """I will give you a sample table.

For each column, return:
1. The best-matching `schema.org` property name (not just the type — use the 
   most semantically appropriate property, such as `schema:orderDate`, 
   `schema:birthDate`, `schema:dateCreated`, etc.).
2. Its corresponding `schema.org` type (e.g., `schema:Date`, `schema:Text`, 
   `schema:Number`, etc.).
3. Its brief description.

Return your answer as a JSON mapping of column names to objects with 
`schemaOrgProperty `, `schemaOrgType`, and `description`.

Please infer based on the column name and example values.

Here is the sample table:

"""

table_schema_annot_qry = """You are given the structured schema of a table in 
JSON format. Each column has a name, its mapped schema.org property, 
its mapped schema.org type, a brief description, and its database data type.

Your task:

Write a concise, semantically rich table annotation (150–250 words) to enable 
semantic search for table matching that summarizes:

1. What kind of data the table contains.
2. The overall purpose or domain of the table (e.g., geographic, infrastructure, 
    social data).
3. Key column roles and data types.
4. Potential use cases (optional)

Instructions:

* Return your answer as a JSON mapping of `table_annotation`.
* Do not list all column names or types explicitly.
* Summarize the structure and semantics naturally.
* Do not mention “JSON” or the schema format.

Table Schema JSON:

"""

table_desc_qry = """You are given the structured schema of a table in JSON 
format. Each column has a name, its database data type, 
its mapped schema.org property, its mapped schema.org type, 
and a brief description.

Your task:

Write a concise, semantically rich table annotation (150–250 words) to enable 
semantic search for table matching that summarizes:

1. What kind of data the table contains.
2. The overall purpose or domain of the table (e.g., geographic, infrastructure, 
    social data).
3. Key column roles and data types.
4. Potential use cases (optional)

Instructions:

* Return your answer as a JSON mapping of `table_annotation`.
* Do not list all column names or types explicitly.
* Summarize the structure and semantics naturally.
* Do not mention “JSON” or the schema format.

Table Schema JSON:

"""

# 1. **Category**: The primary high-level semantic entity or data type of the 
#    column. You **must select one** from the following constrained list:
#     * `Person`, `Organization`, `Location`, `Product`, `Event`, `Date/Time`, 
#     `Quantity/Measurement`, `Text/Description`, `Identifier/Code`, 
#     `Boolean/Status`.
# 2.  **schemaOrgProperty**: The most semantically appropriate `schema.org` 
#     property name (e.g., `schema:orderDate`, `schema:birthDate`, 
#     `schema:streetAddress`).
# `category`, `schemaOrgProperty`,
# 
# For each column, return:
# -  **tags**: A list of 1–3 precise, fine-grained semantic terms derived from 
#     the column's name, comment, and sample values.
#     * Tags must be **short phrases** (not sentences).
#     * They should include synonyms and specific context to enhance human searchability.
#     * Avoid duplicates or overly general terms. 

table_col_annot_sys_prompt = """ 
You are an expert in relational database schema annotation. 
Your task is to analyze each column in a table and return its
brief description. 
Please infer based on the column name, example values and column comment.

Return your final output as a JSON object, mapping column names to an object 
containing the keys and `description`.
"""

table_col_annot_user_prompt = """
### Input table

#### Column Definitions
{col_def}

#### Sample Data:
{sample_data}
"""


table_annot_sys_prompt_v0 = """
You are an expert in data cataloging, specifically tasked with synthesizing 
high-level metadata from detailed schema annotations.

Your task is to analyze the provided column annotations and return a concise, 
structured table-level tag in JSON format.

Output JSON MUST contain ONLY the following four keys, with the values being 
lists of strings (for main_entities, semantic_roles, and summary_keywords) 
or a single string (for domain):
1. 'main_entities': The 2-3 primary entities described in the table 
   (e.g., 'Customer', 'Order', 'Product').
2. 'domain': The specific industry or context of the data (e.g., 
   'Retail Logistics', 'Financial Services', 'Clinical Trials').
3. 'semantic_roles': The 4-5 most important schema.org properties present in 
   the columns (e.g., 'schema:orderDate', 'schema:unitPrice').
4. 'summary_keywords': 5-7 general keywords describing the table's overall 
   purpose and content (e.g., 'historical data', 'account status', 
   'location').

Do not include any descriptive text, explanations, or extraneous information 
outside of the required JSON object.
"""

table_annot_sys_prompt = """ 
You are given the structured schema of a table in JSON format. Each column has a name, its database data type, its mapped schema.org property, its mapped schema.org type, and a brief description.
Your task:

Write a concise, semantically rich table annotation (50–100 words) to enable semantic search for table matching that summarizes:

- What kind of data the table contains.
- The overall purpose or domain of the table (e.g., geographic, infrastructure, social data).
- Key column roles, data types and its name.
- Potential use cases.

Instructions:

* Return your answer as a JSON mapping of table_annotation.
* Do not list all column names or types explicitly.
* Summarize the structure and semantics naturally.
* Do not mention “JSON” or the schema format.

"""


table_annot_user_prompt = """
### Input
1. COLUMN ANNOTATIONS:
{col_annot}

2. SAMPLE DATA:
{sample_data}

3. TABLE COMMENT:
{tbl_comment}
"""


def pad_to_width(text, width):
    """Pad text to the specified display width."""
    current_width = wcswidth(text)
    if current_width >= width:
        return text
    return text + " " * (width - current_width)


def dict_table_to_markdown(data):
    """
    Convert a list-of-dicts table to a Markdown table.
    
    Args:
        data: List of dicts, e.g., [{'id':1, 'name':'Apple', 'price':10}, ...]
    
    Returns:
        String containing the Markdown table, or None if data is empty
    """
    if not data:
        return None
    
    headers = sorted(data[0].keys())
    # Calculate maximum width for each column
    max_lengths = {header: wcswidth(header) for header in headers}
    for item in data:
        for header in headers:
            value = str(item.get(header, ""))
            max_lengths[header] = max(max_lengths[header], wcswidth(value))
    # Build the Markdown table
    table = "| " + " | ".join(pad_to_width(header, max_lengths[header])
                              for header in headers) + " |\n"
    table += "| " + " | ".join("-" * max_lengths[header] 
                               for header in headers) + " |\n"
    for item in data:
        row = "| " + " | ".join(pad_to_width(str(item.get(header, "")), 
            max_lengths[header]) for header in headers) + " |\n"
        table += row
    
    return table


def list_table_to_markdown(data):
    """
    Convert a list-of-lists table (first sublist is headers, rest are rows) to 
    a Markdown table.
    
    Args:
        data: List of lists, e.g., [['id','name','price'],[1,'Apple',10], ...]
    
    Returns:
        String containing the Markdown table, or None if data is empty
    """
    if not data or len(data) < 1:
        return None
    
    headers = data[0]
    rows = data[1:] if len(data) > 1 else []
    # Calculate maximum width for each column
    max_lengths = {header: wcswidth(str(header)) for header in headers}
    for row in rows:
        for i, value in enumerate(row):
            header = headers[i]
            max_lengths[header] = max(max_lengths[header], wcswidth(value))
    # Build the Markdown table
    table = "| " + " | ".join(pad_to_width(str(header), max_lengths[header])
                              for header in headers) + " |\n"
    table += "| " + " | ".join("-" * max_lengths[header]
                               for header in headers) + " |\n"   
    for row in rows:
        table += "| " + " | ".join(pad_to_width(str(value), 
                                   max_lengths[headers[i]])
                                   for i, value in enumerate(row)) + " |\n"
    
    return table


# def annotate_columns(data):
#     if isinstance(data[0], dict):
#         prompt = ann_col_qry + dict_table_to_markdown(data)
#     else:
#         prompt = ann_col_qry + list_table_to_markdown(data)
        
#     response = llm_chat(prompt)
#     return response


# def annotate_table_schema_by_columns(col_annot, tbl_comment = None):
#     """
#     Annotation a table by columns with database schema
#     """
#     qry = table_schema_annot_qry + col_annot
#     response = llm_chat(qry)
#     return response


# def annotate_table_by_columns(col_json):
#     """
#     Annotation a table by columns
#     """
#     qry = table_desc_qry + col_json
#     response = llm_chat(qry)
#     return response


# def annotate_table(data, schema = None, tbl_comment = None):
#     """
#     Annotation a table by a sample table data,
#     schema is ((col1, type), ...) if any,
#     Return table annotation and columns schema.org
#     """

#     col_annot = annotate_columns(data)

#     if (schema):
#         schema_lookup = {col_name: (col_type, col_comment)
#                          for col_name, col_type, col_comment in schema}
#         col_json = json.loads(col_annot)
#         for col_name, annot in col_json.items():          # col_annot == col_json
#             if col_name in schema_lookup:                  # safety net
#                 col_type, col_comment = schema_lookup[col_name]
#                 annot['type']        = col_type
#                 annot['col_comment'] = col_comment

#         col_annot = json.dumps(col_json)
#         tbl_annot = annotate_table_schema_by_columns(col_annot, tbl_comment)
#     else:
#         return annotate_table_by_columns(col_annot), col_annot

#     return tbl_annot, col_annot


def annotate_columns(tbl_data, schema):
    prompt = table_col_annot_user_prompt.format(col_def = schema, sample_data = tbl_data)

    response = llm_chat(prompt, table_col_annot_sys_prompt)
    print(response)
    return response


def annotate_table(data, schema = None, tbl_comment = None):
    """
    Annotation a table by a sample table data,
    schema is ((col1, type, comment), ...) if any,
    Return table annotation and columns schema.org
    """
    tbl_data = None
    if isinstance(data[0], dict):
        tbl_data = dict_table_to_markdown(data)
    else:
        tbl_data = list_table_to_markdown(data)


    col_annot = annotate_columns(tbl_data, schema)

    schema_lookup = {col_name: (col_type, col_comment)
                  for col_name, col_type, col_comment in schema}
    col_annot_json = json.loads(col_annot)
    for col_name, annot in col_annot_json.items():          # col_annot == col_json
        if col_name in schema_lookup:                  # safety net
            col_type, col_comment = schema_lookup[col_name]
            annot['type']        = col_type
            annot['col_comment'] = col_comment

    print(col_annot_json)

    col_annot = json.dumps(col_annot_json)

    prompt = table_annot_user_prompt.format(col_annot = col_annot, 
        sample_data = tbl_data, tbl_comment = tbl_comment)
    tbl_annot = llm_chat(prompt, table_annot_sys_prompt)

    tbl_annot_json = json.loads(tbl_annot)
    tbl_annot_json = {"table_annotation": tbl_annot_json}

    print(tbl_annot_json)

    return tbl_annot_json, col_annot_json