from sqlai.tbl_milvus import TableMilvus

tbl_vdb = TableMilvus()


def getQueryTables(qry):
    """
    Annotation a table by columns
    """
    qry = table_desc_qry + col_json
    response = llm_chat(qry)
    return response


matches = tbl_vdb.search_tables('_ef992a97be0311f0a4fa2eb586cb076e', query='Accounts table containing region and loan eligibility columns?')
print(len(matches))
for item in matches:
    metadata = item['metadata']
    score = item['score']
    print(f"Metadata: {metadata}, Score: {score}")
