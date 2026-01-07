# Text 2 SQL

A natural language interface that enables users to query databases using plain English. This project demonstrates an intelligent system that bridges the gap between natural language and SQL.

The system automatically:
*   **Understands intent**: Deciphers the meaning behind user questions.
*   **Retrieves context**: Finds relevant database tables and schemas.
*   **Executes securely**: Generates and runs SQL queries with built-in safety checks.
*   **Formats results**: Returns data in a clear, user-friendly format.

Designed for security and scalability, it uses modern containerization and secure access patterns to ensure robust performance.

## System Flow

```mermaid
flowchart TB
    subgraph Agent["🤖 Agent System (LangGraph)"]
        UserQuery["User Query<br/>'Show payments'"]
        AgentState["LangGraph Agent State<br/>Maintains conversation history"]
        RetrieveNode["Retrieve Context Node<br/>agent_core/nodes/retrieve.py"]
        CheckCache["Check Cache<br/>Semantic similarity"]
        CacheHit{"Cache<br/>Hit?"}
        GenerateNode["Generate SQL Node<br/>agent_core/nodes/generate.py"]
        ExecuteNode["Execute SQL Node<br/>agent_core/nodes/execute.py"]
        CacheUpdate["Cache SQL<br/>On success"]
        CorrectNode["Correct SQL Node<br/>agent_core/nodes/correct.py"]
        SynthesizeNode["Synthesize Insight Node<br/>agent_core/nodes/synthesize.py"]
        Response["Natural Language Response"]

        UserQuery --> AgentState
        AgentState --> RetrieveNode
        RetrieveNode --> CheckCache
        CheckCache --> CacheHit
        CacheHit -->|"Similarity >= 0.95"| ExecuteNode
        CacheHit -->|"Miss"| GenerateNode
        GenerateNode --> ExecuteNode
        ExecuteNode -->|"Success"| CacheUpdate
        CacheUpdate --> SynthesizeNode
        ExecuteNode -->|"Error & retries < 3"| CorrectNode
        CorrectNode -->|"Loop back"| ExecuteNode
        ExecuteNode -->|"Error & retries >= 3"| Response
        SynthesizeNode --> Response
    end

    subgraph Observability["📡 Observability"]
        MLflow["MLflow Tracking Server<br/>Traces & Metrics"]
        MLflowDB[("MLflow DB")]
        MinIO[("MinIO Artifacts")]

        MLflow --> MLflowDB
        MLflow --> MinIO
    end

    subgraph VectorStore["📊 Vector Store (Agent)"]
        PGVectorAgent["PGVector Store<br/>agent_core/retriever.py"]
        OpenAIEmbed["OpenAI Embeddings<br/>text-embedding-3-small"]
    end

    subgraph MCPServer["🔧 MCP Server (FastMCP)"]
        MCPTools["MCP Tools<br/>mcp_server/tools.py"]
        RAGEngine["RAG Engine<br/>mcp_server/rag.py"]
        CacheModule["Cache Module<br/>mcp_server/cache.py"]
        FastEmbed["fastembed<br/>BGE-small"]
        SecurityCheck["SQL Security Checks<br/>Regex validation"]
    end

    subgraph Database["🗄️ PostgreSQL Database"]
        PGVectorDB["pgvector Extension<br/>Vector similarity search"]
        SchemaEmbeddings["schema_embeddings Table<br/>Stores schema vectors"]
        SemanticCache["semantic_cache Table<br/>Cached SQL queries"]
        PagilaDB["Pagila Database<br/>Sample data"]
    end

    %% Agent Observability
    RetrieveNode -.->|"Trace"| MLflow
    GenerateNode -.->|"Trace"| MLflow
    ExecuteNode -.->|"Trace"| MLflow
    CorrectNode -.->|"Trace"| MLflow
    SynthesizeNode -.->|"Trace"| MLflow

    %% Agent to Vector Store
    RetrieveNode -->|"Similarity search"| PGVectorAgent
    PGVectorAgent -->|"Query embeddings"| OpenAIEmbed
    PGVectorAgent -->|"Vector search"| PGVectorDB
    PGVectorDB --> SchemaEmbeddings

    %% Agent to MCP Server
    CheckCache -->|"HTTP/JSON"| MCPTools
    MCPTools -->|"Cache lookup"| CacheModule
    CacheModule -->|"Vector search"| PGVectorDB
    PGVectorDB --> SemanticCache

    ExecuteNode -->|"HTTP/JSON"| MCPTools
    MCPTools -->|"Semantic search"| RAGEngine
    RAGEngine -->|"Generate embeddings"| FastEmbed
    RAGEngine -->|"Vector search"| PGVectorDB
    MCPTools -->|"Validate SQL"| SecurityCheck
    SecurityCheck -->|"execute_sql_query_tool"| PagilaDB
    PagilaDB -->|"Results"| MCPTools
    MCPTools -->|"JSON response"| ExecuteNode

    CacheUpdate -->|"HTTP/JSON"| MCPTools
    MCPTools -->|"Cache update"| CacheModule
    CacheModule -->|"Store SQL"| SemanticCache

    %% Agent to LLM
    GenerateNode -->|"LLM call"| OpenAILLM["OpenAI GPT-5.2<br/>SQL generation"]
    CorrectNode -->|"LLM call"| OpenAILLM
    SynthesizeNode -->|"LLM call"| OpenAILLM2["OpenAI GPT-5.2<br/>Natural language"]

    style Agent fill:#5B9BD5
    style MCPServer fill:#FF9800
    style Database fill:#4CAF50
    style VectorStore fill:#9C27B0
    style Observability fill:#E1BEE7
```

## Core Features

*   **Multi-Provider LLM Support**: Switch between OpenAI, Anthropic (Claude), and Google (Gemini) via UI or environment variables.
*   **Intelligent Query Generation**: Uses a LangGraph-orchestrated reasoning loop (Retrieve → Generate → Execute → Correct → Synthesize) to ensure accuracy.
*   **Secure Access**: Built on the Model Context Protocol (MCP) server, enforcing read-only permissions and SQL safety checks.
*   **RAG & Semantic Search**: Uses `pgvector` and `fastembed` to dynamically find relevant tables and few-shot examples based on the user's question.
*   **Self-Correction**: Automatically detects SQL errors and retries generation with error context up to 3 times.
*   **Performance Caching**: Semantic caching stores successful query patterns to reduce latency and API costs.
*   **Full Observability**: Integrated MLflow tracing provides end-to-end visibility into the agent's reasoning steps and performance metrics.

## Project Structure

```text
text2sql/
├── agent/                      # LangGraph AI agent
│   ├── src/agent_core/         # Core logic (nodes, graph, state)
│   ├── tests/                  # Unit tests (mocked)
│   └── tests_integration/      # Live integration tests
├── mcp-server/                 # Database access tools (FastMCP)
│   ├── src/mcp_server/         # Server implementation
│   ├── tests/                  # Unit tests
│   └── tests_integration/      # RLS & database integration tests
├── streamlit/                  # Web interface
├── database/                   # Init scripts & schema
└── docker-compose.yml          # Service orchestration
```

## Quick Start

### Prerequisites
*   Docker & Docker Compose
*   Python 3.12+ (for local development)

### Setup & Run

1.  **Initialize Data**: Download the Pagila sample database.
    ```bash
    ./database/init-scripts/download_data.sh
    ```

2.  **Configure Environment** (optional): Set your preferred LLM provider in `.env`:
    ```bash
    LLM_PROVIDER=openai      # Options: openai, anthropic, google
    LLM_MODEL=gpt-5.2        # Or: claude-sonnet-4-20250514, gemini-2.5-flash-preview-05-20
    ANTHROPIC_API_KEY=...    # Required for Anthropic
    GOOGLE_API_KEY=...       # Required for Google
    ```

3.  **Start Services**: Build and launch the container cluster.
    ```bash
    docker compose up -d --build
    ```

### Access Points

| Service | URL | Description |
|---------|-----|-------------|
| **Web UI** | `http://localhost:8501` | Streamlit interface for end-users |
| **MCP Server** | `http://localhost:8000/sse` | Tool server for the agent |
| **MLflow UI** | `http://localhost:5001` | Traces and metrics dashboard |
| **MinIO** | `http://localhost:9001` | S3-compatible artifact store |

## Testing

Run the isolated unit test suite (no Docker required):
```bash
pytest agent/tests mcp-server/tests
```
