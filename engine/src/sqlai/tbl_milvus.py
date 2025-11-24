import logging
from pymilvus import MilvusClient, DataType
from sentence_transformers import SentenceTransformer, util as sen_trans_util
from sqlai.core import SingletonMeta


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())



class TableMilvus(metaclass=SingletonMeta):
    def __init__(cls, uri: str = None, 
                 embedding_model: str = 'BAAI/bge-small-en-v1.5', dim: int = 384):
        """
        Initialize the TableMilvus with Milvus connection and embedding model.
        
        Args:
            uri (str): Milvus server host (default: None).
            embedding_model (str): Sentence model for embeddings (default: 'BAAI/bge-small-en-v1.5').
            dim (int): Embedding dimension (default: 384 for MiniLM).
        """
        # cls.collection_name = collection_name
        cls.model = SentenceTransformer(embedding_model)
        cls.dim = dim

        if uri is not None:
            cls.client = MilvusClient(uri=uri)
            logger.info("Using remote Milvus")
        else:
            cls.client = client = MilvusClient("milvus.db")
            logger.info("Using local Milvus")

    def load_collection(cls, collection_name: str):
        if not cls.client.has_collection(collection_name):
            cls._create_collection(collection_name)
        cls.client.load_collection(collection_name = collection_name)

    def _create_collection(cls, collection_name: str):        
        schema = MilvusClient.create_schema(
            auto_id=True, 
            enable_dynamic_field=True,)

        # Add fields to schema
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=cls.dim)
        schema.add_field(field_name="name_embedding", datatype=DataType.FLOAT_VECTOR, dim=cls.dim)
        # schema.add_field(field_name="data_src_id", datatype=DataType.VARCHAR, max_length=128)
        schema.add_field(field_name="metadata", datatype=DataType.JSON)

        index_params = cls.client.prepare_index_params()
        index_params.add_index(
            field_name = "embedding",
            index_name = "embedding_index",
            index_type = "AUTOINDEX",
            metric_type = "IP"
        )
        # index_params.add_index(
        #     field_name = "data_src_id",
        #     index_name = "data_src_id_index",
        #     index_type = "",
        # )

        cls.client.create_collection(
            collection_name = collection_name,
            schema = schema,
            index_params = index_params
        )
        logger.info(f"Collection {collection_name} created")    

    def drop_collection(cls, collection_name: str):
        return cls.client.drop_collection(collection_name = collection_name)

    def insert_tables(cls, collection_name: str, tbl_annot: str, tbl_name: str, 
                      metadata) -> None:
        """
        Generate embeddings for table annotations and insert them into Milvus with metadata.
        
        Args:
            collection_name (str): collection name (usually datasource's sys_id)
            tbl_annot (str): table description
            tbl_name (str): table name
            metadata: metadata
        """

        # Generate embeddings
        embeddings = cls.model.encode([tbl_annot], show_progress_bar=False)
        name_embeddings = cls.model.encode([tbl_name], show_progress_bar=False)
        # Prepare data for insertion
        data = [
            {"embedding": embeddings[0].tolist(),
             "name_embedding": name_embeddings[0].tolist(),
             "metadata": metadata}
        ]
        # Insert into collection
        res = cls.client.insert(collection_name=collection_name, data=data)
        return res

    def get_model(cls):
        return cls.model
    
    def delete_tables(cls, collection_name: str):
        """
        Delete tables using data source id
        
        Args:
            data_src_id (str): data source id
        """
        deleted_tbls = cls.client.delete(
            collection_name = collection_name,
            filter = "id >= 0" 
        )
        deleted_length = len(deleted_tbls)
        logger.info(f"{deleted_length} tables are deleted")
        return deleted_length


    def search_tables(cls, collection_name: str, query: str, limit: int = 10):
        """
        Search for tables matching a natural language query using semantic similarity.
        
        Args:
            query (str): The natural language query to search for matching tables.
            limit (int, optional): The number of top results to return. Defaults to 10.
        
        Returns:
            List[Dict]: A list of dictionaries containing matching tables with metadata, and similarity score.
                Each dictionary has the following structure:
                {
                    "metadata": { 
                        'db': <str>
                        'table': <str>
                        'description': <str> 
                        'schema': <str> 
                    },
                    "score": <float>
                }

                The 'schema' field in 'metadata' is a JSON string that can be parsed into a dictionary with the following structure:
                {
                   "<column_name_1>": {
                        "schemaOrgProperty": <schema.org property>
                        "schemaOrgType": <schema.org type>
                        "description": <str>
                        "type": <data type>
                    },
                    "<column_name_2>": {
                        "schemaOrgProperty": <schema.org property>
                        "schemaOrgType": <schema.org type>
                        "description": <str>
                        "type": <data type>
                    }, ...
                } 
        """
        query_embedding = cls.model.encode([query], 
                                            show_progress_bar=False)[0].tolist()
        results = cls.client.search(
            collection_name=collection_name,
            data=[query_embedding],
            anns_field="embedding",
            limit=limit,
            search_params={"metric_type": "IP"},
            output_fields=["name_embedding", "metadata"],
        )
    
        matches=[]
        # score=[]
        # for hit in results[0]:
        #     cosine_score = sen_trans_util.cos_sim(hit["entity"]["name_embedding"], query_embedding)[0][0]
        #     similarity_score = float((cosine_score + 1) / 2)
        #     score.append(max(hit["distance"], similarity_score))
        #     # score.append(hit["distance"])

        # for hit, score in zip(results[0], score): 
        #     matched_tbl = hit["entity"]["metadata"]
        #     matched_tbl["score"] = score
        #     matches.append(matched_tbl)

        for hit in results[0]: 
            matched_tbl = hit["entity"]["metadata"]
            matched_tbl["score"] = hit["distance"]
            matches.append(matched_tbl)


        # matches = [
        #     # {hit["entity"]["metadata"], "score": score}
        #     {"metadata": hit["entity"]["metadata"], "score": score}
        #     for hit,score in zip(results[0], score)
        # ]
        # for hit in matches:
        #     print(hit['table'], hit['score'])

        return matches