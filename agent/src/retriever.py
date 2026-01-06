"""Vector store initialization for RAG context retrieval."""

import os

from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_postgres import PGVector

load_dotenv()


def get_vector_store():
    """
    Initialize and return PGVector store for schema metadata retrieval.

    Connects to the same PostgreSQL database used by the MCP server,
    accessing the schema_embeddings table created in Phase 2.

    Returns:
        PGVector: Configured vector store instance
    """
    # Connection string to the postgres-db container from Phase 1/2
    # Ensure pgvector extension is enabled on the target DB
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "pagila")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "root_password")

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    return PGVector(
        embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
        collection_name="schema_metadata",
        connection=connection_string,
        use_jsonb=True,
    )
