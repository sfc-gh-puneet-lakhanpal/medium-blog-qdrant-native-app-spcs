import os
QDRANT_URL = os.environ.get("QDRANT_URL", "http://instances.qdrantprimaryservice.qdrant-app-core-schema:6333/")
COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "snowflake-arctic-demo")
EMBEDDINGS_MODEL = os.environ.get("EMBEDDINGS_MODEL", "snowflake/snowflake-arctic-embed-s")
TEXT_FIELD_NAME = "document"