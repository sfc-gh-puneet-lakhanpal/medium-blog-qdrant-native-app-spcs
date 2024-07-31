import logging
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from typing import List
from qdrant_client import QdrantClient
from qdrant_client.http.models.models import Filter
from config import QDRANT_URL, EMBEDDINGS_MODEL, TEXT_FIELD_NAME, COLLECTION_NAME
from qdrant_client.models import Filter, FieldCondition, MatchText
import numpy as np
import time
import re
from fastapi import FastAPI, Request
import os
from snowflake.snowpark import Session
SNOW_TOKEN_PATH = '/snowflake/session/token'
SNOW_HOST = os.getenv('SNOWFLAKE_HOST')
SNOW_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOW_WAREHOUSE = "QDRANT_ON_SPCS_APP_QDRANT_QUERY_WAREHOUSE"
SNOW_DATABASE="qdrant_db"
SNOW_SCHEMA="quickstart_sch"
def _get_login_token():
    with open(SNOW_TOKEN_PATH, 'r') as f:
        return f.read()
def get_snow_connection_params():
    return {
        "host" : SNOW_HOST,
        "account": SNOW_ACCOUNT,
        "token": _get_login_token(),
        "warehouse": SNOW_WAREHOUSE,
        "authenticator": 'oauth',
        "database" : SNOW_DATABASE,
        "schema" : SNOW_SCHEMA,
        "client_session_keep_alive": True
    }
snowflake_conn_params = get_snow_connection_params()
session = Session.builder.configs(snowflake_conn_params).create()
class SemanticSearcher:

    def __init__(self, collection_name: str):
        self.collection_name = collection_name
        self.qdrant_client = QdrantClient(url=QDRANT_URL)

    def search(self, text: str, filter_: dict = None) -> list:
        #start_time = time.time()
        hits = self.qdrant_client.search(
            collection_name="Quickstart",
            query_vector=self.collection_name,
            with_vectors=False,
            with_payload=True,
            limit=5
        )
        return hits

class KeywordSearcher:
    def __init__(self, collection_name: str):
        self.highlight_field = TEXT_FIELD_NAME
        self.collection_name = collection_name
        self.qdrant_client = QdrantClient(url=QDRANT_URL)

    def highlight(self, record, query) -> dict:
        text = record[self.highlight_field]

        for word in query.lower().split():
            if len(word) > 4:
                pattern = re.compile(fr"(\b{re.escape(word)}?.?\b)", flags=re.IGNORECASE)
            else:
                pattern = re.compile(fr"(\b{re.escape(word)}\b)", flags=re.IGNORECASE)
            text = re.sub(pattern, r"<b>\1</b>", text)

        record[self.highlight_field] = text
        return record

    def search(self, query, top=5):
        hits = self.qdrant_client.scroll(
            collection_name=self.collection_name,
            scroll_filter=Filter(
                must=[
                    FieldCondition(
                        key=TEXT_FIELD_NAME,
                        match=MatchText(text=query),
                    )
                ]),
            with_payload=True,
            with_vectors=False,
            limit=top
        )
        return [self.highlight(hit.payload, query) for hit in hits[0]]



searchers = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    searchers["semantic_searcher"] = SemanticSearcher(collection_name=COLLECTION_NAME)
    searchers["keyword_searcher"] = KeywordSearcher(collection_name=COLLECTION_NAME)
    yield

def read_item(q: str, semantic: bool = True):
    return {
        "result": searchers["semantic_searcher"].search(text=q) if semantic else searchers["keyword_searcher"].search(query=q)
    }

# Logging
def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter(
            '%(name)s [%(asctime)s] [%(levelname)s] %(message)s'))
    logger.addHandler(handler)
    return logger


logger = get_logger('snowpark-container-service')

app = FastAPI(lifespan=lifespan)

@app.post("/search", tags=["Endpoints"])
async def search(request: Request):
    start = time.time()
    request_body = await request.json()
    semantic = request_body['semantic']
    q = request_body['query']
    response = read_item(q=q, semantic=semantic)
    timediff = time.time() - start
    logger.info(f'TIMEDIFF:{timediff} secs')
    return response