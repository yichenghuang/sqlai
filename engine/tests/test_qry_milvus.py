from sqlai.tbl_milvus import TableMilvus

tbl_vdb = TableMilvus()


def getQueryTables(qry):
    """
    Annotation a table by columns
    """
    qry = table_desc_qry + col_json
    response = llm_chat(qry)
    return response


matches = tbl_vdb.search_tables(query='Get sales for different months')
print(len(matches))
for item in matches:
    metadata = item['metadata']
    score = item['score']
    print(f"Metadata: {metadata}, Score: {score}")
