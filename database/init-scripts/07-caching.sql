-- Semantic Caching Schema
-- This script creates the table to store cached SQL queries with tenant isolation
-- Uses vector similarity search to match semantically similar queries

-- Create semantic cache table for query result caching
CREATE TABLE IF NOT EXISTS public.semantic_cache (
    cache_id SERIAL PRIMARY KEY,
    tenant_id INT NOT NULL,           -- Security: Scope cache to tenant
    user_query TEXT NOT NULL,
    query_embedding vector(384),       -- Embedding of user_query (bge-small)
    generated_sql TEXT NOT NULL,       -- We cache the SQL logic (not results)
    schema_version VARCHAR(10) DEFAULT 'v1', -- Cache invalidation versioning
    cache_type VARCHAR(20) DEFAULT 'sql',
    similarity_score FLOAT,            -- Store similarity for analysis
    hit_count INT DEFAULT 0,           -- Track cache usage
    created_at TIMESTAMP DEFAULT NOW(),
    last_accessed_at TIMESTAMP DEFAULT NOW()
);

-- Composite index: Filter by tenant first, then do vector search
CREATE INDEX IF NOT EXISTS idx_cache_tenant ON public.semantic_cache(tenant_id);
CREATE INDEX IF NOT EXISTS idx_cache_vector
ON public.semantic_cache
USING hnsw (query_embedding vector_cosine_ops);

-- Composite index for tenant + vector search optimization
-- Note: PostgreSQL doesn't support composite index with vector directly
-- We rely on idx_cache_tenant for filtering and idx_cache_vector for similarity
CREATE INDEX IF NOT EXISTS idx_cache_created_at ON public.semantic_cache(created_at);

-- Grant access to agent user
GRANT SELECT, INSERT, UPDATE, DELETE ON public.semantic_cache TO text2sql_ro;
GRANT USAGE ON SEQUENCE public.semantic_cache_cache_id_seq TO text2sql_ro;

-- Add comment for documentation
COMMENT ON TABLE public.semantic_cache IS 'Semantic cache for SQL queries. Uses vector similarity to match user intent. Tenant-scoped for security.';
COMMENT ON COLUMN public.semantic_cache.similarity_score IS 'Cosine similarity score (0-1) between cached query and new query. Threshold: 0.95.';
