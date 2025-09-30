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
3. Its breif description.

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

def write_jsonl(filename, text):
    try:
        if isinstance(text, str):
            data = json.loads(text) # convert string to json object
        else:
            data = text  # Use as-is if already a Python object
        with open(filename, 'a') as file:
            json.dump(data, file, ensure_ascii=False)
            file.write('\n')  # Append newline for JSONL format
        print(f"Successfully appended text to {filename}")
    except Exception as e:
        print(f"Error appending to file: {e}")


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


def annotate_columns(data):
    if isinstance(data[0], dict):
        prompt = ann_col_qry + dict_table_to_markdown(data)
    else:
        prompt = ann_col_qry + list_table_to_markdown(data)
        
    response = llm_chat(prompt)
    return response


def annotate_table_schema_by_columns(col_annot, tbl_comment = None):
    """
    Annotation a table by columns with database schema
    """
    qry = table_schema_annot_qry + col_annot
    response = llm_chat(qry)
    return response


def annotate_table_by_columns(col_json):
    """
    Annotation a table by columns
    """
    qry = table_desc_qry + col_json
    response = llm_chat(qry)
    return response


def annotate_table(data, schema = None, tbl_comment = None):
    """
    Annotation a table by a sample table data,
    schema is ((col1, type), ...) if any,
    Return table annotation and columns schema.org
    """

    col_annot = annotate_columns(data)

    if (schema):
        col_json = json.loads(col_annot)
        for (_, type), (_, annot) in zip(schema, col_json.items()):
            annot["type"] = type
        col_annot = json.dumps(col_json)
        tbl_annot = annotate_table_schema_by_columns(col_annot, tbl_comment)
    else:
        return annotate_table_by_columns(col_annot), col_annot

    return tbl_annot, col_annot