-- Enable the vector extension
-- This must run after the database is created but before any vector operations
CREATE EXTENSION IF NOT EXISTS vector;

-- Verify extension is installed
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'vector'
    ) THEN
        RAISE EXCEPTION 'pgvector extension failed to install';
    END IF;
END $$;

-- Create the table for storing schema embeddings
CREATE TABLE IF NOT EXISTS public.schema_embeddings (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL UNIQUE,
    schema_text TEXT NOT NULL,  -- The raw text description used for RAG context
    embedding vector(384),      -- 384 dimensions matching bge-small-en-v1.5
    metadata JSONB,              -- Flexible storage for extra context
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create the HNSW index for fast cosine similarity search
-- 'vector_cosine_ops' optimizes for cosine distance (<=>)
-- m=16, ef_construction=64 are default values suitable for small-medium datasets
CREATE INDEX IF NOT EXISTS schema_embeddings_embedding_idx
    ON public.schema_embeddings
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- Grants removed as we connect as superuser/rw user in control plane for now
