-- Dynamic Few-Shot Learning Schema
-- This script creates the table to store Golden SQL examples and their embeddings
-- for semantic retrieval during SQL generation

-- Create table to store Golden SQL examples for few-shot learning
CREATE TABLE IF NOT EXISTS public.sql_examples (
    id SERIAL PRIMARY KEY,
    question TEXT NOT NULL,           -- The user's natural language question
    sql_query TEXT NOT NULL,          -- The verified SQL answer
    embedding vector(384),            -- Embedding of the question (bge-small model)
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_sql_examples_question UNIQUE (question)
);

-- Create HNSW index for fast cosine similarity search
CREATE INDEX IF NOT EXISTS idx_sql_examples_embedding
ON public.sql_examples
USING hnsw (embedding vector_cosine_ops);

-- Create index on question for text search (optional, for debugging)
CREATE INDEX IF NOT EXISTS idx_sql_examples_question
ON public.sql_examples
USING gin (to_tsvector('english', question));

-- Grant access to the agent user (SELECT for retrieval, INSERT for embedding generation)
GRANT SELECT, INSERT, UPDATE ON public.sql_examples TO bi_agent_ro;
GRANT USAGE ON SEQUENCE public.sql_examples_id_seq TO bi_agent_ro;

-- Add comment for documentation
COMMENT ON TABLE public.sql_examples IS 'Golden SQL examples for dynamic few-shot learning. Embeddings are generated from questions.';
