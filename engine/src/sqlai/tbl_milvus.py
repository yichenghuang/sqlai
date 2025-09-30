from pymilvus import MilvusClient, DataType
from sentence_transformers import SentenceTransformer, util as sen_trans_util
from sqlai.core import SingletonMeta

class TableMilvus(metaclass=SingletonMeta):
    def __init__(self, uri: str = None, 
                 collection_name: str = 'tables', 
                 embedding_model: str = 'BAAI/bge-small-en-v1.5', dim: int = 384):
        """
        Initialize the TableMilvus with Milvus connection and embedding model.
        
        Args:
            uri (str): Milvus server host (default: None).
            collection_name (str): Name of the Milvus collection (default: 'tables').
            embedding_model (str): Sentence model for embeddings (default: 'BAAI/bge-small-en-v1.5').
            dim (int): Embedding dimension (default: 384 for MiniLM).
        """
        self.collection_name = collection_name
        self.model = SentenceTransformer(embedding_model)
        self.dim = dim

        if uri is not None:
            self.client = MilvusClient(uri=uri)
            print("Using remote Milvus database")
        else:
            self.client = client = MilvusClient("milvus.db")
            print("Using local Milvus database")

        if not self.client.has_collection(collection_name):
            self._create_collection()
        
        self.client.load_collection(collection_name = self.collection_name)

    def _create_collection(self):
        schema = MilvusClient.create_schema(
            auto_id=True, 
            enable_dynamic_field=True,)

        # Add fields to schema
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=self.dim)
        schema.add_field(field_name="name_embedding", datatype=DataType.FLOAT_VECTOR, dim=self.dim)
        schema.add_field(field_name="metadata", datatype=DataType.JSON)

        index_params = self.client.prepare_index_params()
        index_params.add_index(
            field_name = "embedding",
            index_name = "embedding_index",
            index_type = "AUTOINDEX",
            metric_type = "IP"
        )

        self.client.create_collection(
            collection_name = self.collection_name,
            schema = schema,
            index_params = index_params
        )
        print(f"Collection {self.collection_name} created")    

    def insert_tables(self, tbl_annot: str, tbl_name: str, metadata) -> None:
        """
        Generate embeddings for table annotations and insert them into Milvus with metadata.
        
        Args:
            tbl_annot (str): table description
            tbl_name (str): table name
            metadata: metadata
        """
        # Generate embeddings
        embeddings = self.model.encode([tbl_annot], show_progress_bar=False)
        name_embeddings = self.model.encode([tbl_name], show_progress_bar=False)
        # Prepare data for insertion
        data = [
            {"embedding": embeddings[0].tolist(),
             "name_embedding": name_embeddings[0].tolist(),
             "metadata": metadata}
        ]
        # Insert into collection
        res = self.client.insert(collection_name=self.collection_name, data=data)
        return res

    def get_model(self):
        return self.model

    def search_tables(self, query: str, limit: int = 10):
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
        query_embedding = self.model.encode([query], 
                                            show_progress_bar=False)[0].tolist()
        results = self.client.search(
            collection_name=self.collection_name,
            data=[query_embedding],
            anns_field="embedding",
            limit=limit,
            search_params={"metric_type": "IP"},
            output_fields=["name_embedding", "metadata"],
        )
    
        score=[]
        for hit in results[0]:
            cosine_score = sen_trans_util.cos_sim(hit["entity"]["name_embedding"], query_embedding)[0][0]
            similarity_score = float((cosine_score + 1) / 2)
            score.append(max(hit["distance"], similarity_score))

        matches = [
            {"metadata": hit["entity"]["metadata"], "score": score}
            for hit,score in zip(results[0], score)
        ]
        
        return matches