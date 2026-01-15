"""Vector store initialization for RAG context retrieval."""

from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_postgres import PGVector

from common.config.env import get_env_str

load_dotenv()


def get_vector_store():
    """
    Initialize and return PGVector store for schema metadata retrieval.

    Connects to the same PostgreSQL database used by the MCP server,
    accessing the schema_embeddings table created in Phase 2.

    Returns:
        PGVector: Configured vector store instance
    """
    db_host = get_env_str("DB_HOST", "localhost")
    db_port = get_env_str("DB_PORT", "5432")
    db_name = get_env_str("DB_NAME", "pagila")
    db_user = get_env_str("DB_USER", "text2sql_ro")
    db_password = get_env_str("DB_PASS", get_env_str("DB_PASSWORD", "secure_agent_pass"))

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    return PGVector(
        embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
        collection_name="schema_metadata",
        connection=connection_string,
        use_jsonb=True,
    )
