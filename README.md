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

        %% Nodes
        RouterNode["Router Node<br/>(LLM)<br/>agent_core/nodes/router.py"]
        ClarifyNode["Clarify Node<br/>(Human Input)<br/>agent_core/nodes/clarify.py"]
        RetrieveNode["Retrieve Context Node<br/>(Tool)<br/>agent_core/nodes/retrieve.py"]
        PlanNode["Plan SQL Node<br/>(LLM)<br/>agent_core/nodes/plan.py"]
        GenerateNode["Generate SQL Node<br/>(LLM)<br/>agent_core/nodes/generate.py"]
        ValidateNode["Validate SQL Node<br/>(Logic)<br/>agent_core/nodes/validate.py"]
        ExecuteNode["Execute SQL Node<br/>(Tool)<br/>agent_core/nodes/execute.py"]
        CorrectNode["Correct SQL Node<br/>(LLM)<br/>agent_core/nodes/correct.py"]
        SynthesizeNode["Synthesize Insight Node<br/>(LLM)<br/>agent_core/nodes/synthesize.py"]
        Response["Natural Language Response"]

        %% Flow
        UserQuery --> AgentState
        AgentState --> RouterNode

        RouterNode -->|"Ambiguous?"| ClarifyNode
        ClarifyNode -->|"User feedback"| RouterNode

        RouterNode -->|"Clear"| RetrieveNode
        RetrieveNode --> PlanNode
        PlanNode --> GenerateNode
        GenerateNode --> ValidateNode

        ValidateNode -->|"Invalid"| CorrectNode
        ValidateNode -->|"Valid"| ExecuteNode

        ExecuteNode -->|"Success"| SynthesizeNode
        ExecuteNode -->|"Error"| CorrectNode

        CorrectNode -->|"Retry (Loop)"| ValidateNode
        SynthesizeNode --> Response

        %% Fallback for max retries
        ExecuteNode -->|"Max Retries"| Response
    end

    subgraph Observability["📡 Observability"]
        MLflow["MLflow Tracking Server<br/>Traces & Metrics"]
        MLflowDB[("MLflow DB")]
        MinIO[("MinIO Artifacts")]

        MLflow --> MLflowDB
        MLflow --> MinIO
    end

    subgraph Ingestion["🛠️ Data Ingestion"]
        SeederService["Seeder Service (Manual)<br/>text2sql_seeder"]
        SeedData["Seed Data (JSON)<br/>Tables & Examples"]
    end

    subgraph MCPServer["🔧 MCP Server (FastMCP)"]
        MCPTools["MCP Tools<br/>mcp_server/tools/"]
        SemanticTool["get_semantic_subgraph<br/>mcp_server/tools/semantic.py"]
        CacheModule["Cache Module<br/>mcp_server/cache.py"]
        SecurityCheck["SQL Security Checks<br/>AST validation"]
    end

    subgraph GraphDB["🔷 Memgraph (Graph DB)"]
        VectorIndex["Vector Index (usearch)<br/>Semantic search"]
        SchemaGraph["Schema Graph<br/>Tables, Columns, FKs"]
    end

    subgraph Database["🗄️ PostgreSQL Database"]
        PGVectorDB["pgvector Extension<br/>Few-shot examples"]
        SemanticCache["semantic_cache Table<br/>Cached SQL queries"]
        PagilaDB["Pagila Database<br/>Sample data"]
    end

    %% Initialization Flow
    SeedData --> SeederService
    SeederService -->|"Hydrate Graph"| SchemaGraph
    SeederService -->|"Upsert Examples"| PGVectorDB

    %% Agent Observability
    RouterNode -.->|"Trace"| MLflow
    RetrieveNode -.->|"Trace"| MLflow
    PlanNode -.->|"Trace"| MLflow
    GenerateNode -.->|"Trace"| MLflow
    ValidateNode -.->|"Trace"| MLflow
    ExecuteNode -.->|"Trace"| MLflow
    CorrectNode -.->|"Trace"| MLflow
    SynthesizeNode -.->|"Trace"| MLflow

    %% Agent to MCP Server (Retrieval Phase - Now Graph-based)
    RetrieveNode -->|"Call Tool: get_semantic_subgraph"| MCPTools
    MCPTools --> SemanticTool
    SemanticTool -->|"Vector Search"| VectorIndex
    SemanticTool -->|"Graph Traversal"| SchemaGraph
    SchemaGraph -->|"Return Schema Graph"| RetrieveNode

    %% Agent to MCP Server (Generation Phase - Simplified)
    GenerateNode -->|"Call Tool: get_few_shot_examples"| MCPTools
    MCPTools -->|"Fetch Examples"| PGVectorDB

    %% Agent to MCP Server (Execution Phase)
    ExecuteNode -->|"Call Tool: execute_sql_query"| MCPTools
    MCPTools -->|"Validate SQL"| SecurityCheck
    SecurityCheck -->|"Execute"| PagilaDB
    PagilaDB -->|"Results"| MCPTools
    MCPTools -->|"JSON response"| ExecuteNode

    %% Agent to LLM
    RouterNode -->|"LLM call"| OpenAILLM["LLM Provider<br/>(OpenAI/Anthropic/Google)"]
    PlanNode -->|"LLM call"| OpenAILLM
    GenerateNode -->|"LLM call"| OpenAILLM
    CorrectNode -->|"LLM call"| OpenAILLM
    SynthesizeNode -->|"LLM call"| OpenAILLM

    %% Seeder & Enrichment
    SeederService -->|"Generate Descriptions"| OpenAILLM
    SeederService -->|"Generate Embeddings"| Embeddings["Embedding Model<br/>(text-embedding-3-small)"]

    %% Semantic Search Embeddings
    SemanticTool -->|"Embed Query"| Embeddings

    style Agent fill:#5B9BD5
    style MCPServer fill:#FF9800
    style Database fill:#4CAF50
    style GraphDB fill:#9C27B0
    style Init fill:#FFC107
    style Observability fill:#E1BEE7
```


## Core Features

*   **Multi-Provider LLM Support**: Switch between OpenAI, Anthropic (Claude), and Google (Gemini) via UI or environment variables.
*   **Intelligent Query Generation**: Uses a LangGraph-orchestrated reasoning loop (Retrieve → Generate → Execute → Correct → Synthesize) to ensure accuracy.
*   **Secure Access**: Built on the Model Context Protocol (MCP) server, enforcing read-only permissions and SQL safety checks.
*   **Graph-Based Schema Retrieval**: Uses Memgraph for semantic vector search and graph traversal, returning relevant tables, columns, and relationships in a single call.
*   **Manual Seeding**: Dedicated seeder service hydrates the schema graph and initializes golden examples on demand, decoupling ingestion from startup.
*   **Self-Correction**: Automatically detects SQL errors and retries generation with error context up to 3 times.
*   **Performance Caching**: Semantic caching stores successful query patterns to reduce latency and API costs.
*   **Full Observability**: Integrated MLflow tracing provides end-to-end visibility into the agent's reasoning steps and performance metrics.

## Advanced Architecture Features

### AST Validation
SQL queries are parsed and validated using `sqlglot` before execution:
- **Security Checks**: Blocks access to restricted tables (`payroll`, `credentials`, `pg_*`)
- **Forbidden Commands**: Prevents `DROP`, `DELETE`, `INSERT`, `UPDATE` operations
- **Metadata Extraction**: Captures table lineage, column usage, and join complexity

### SQL-of-Thought Planner
Complex queries are decomposed into logical steps before SQL synthesis:
- **Schema Linking**: Identifies relevant tables and columns
- **Clause Mapping**: Breaks down query into FROM, JOIN, WHERE, GROUP BY components
- **Procedural Planning**: Generates numbered step-by-step execution plan

### Error Taxonomy
12 error categories with targeted correction strategies:
- `AGGREGATION_MISUSE`, `MISSING_JOIN`, `TYPE_MISMATCH`, `AMBIGUOUS_COLUMN`
- `SYNTAX_ERROR`, `NULL_HANDLING`, `SUBQUERY_ERROR`, `PERMISSION_DENIED`
- `FUNCTION_ERROR`, `CONSTRAINT_VIOLATION`, `LIMIT_EXCEEDED`, `DATE_TIME_ERROR`

### Human-in-the-Loop
Ambiguous queries trigger clarification via LangGraph interrupts:
- **Ambiguity Detection**: Identifies unclear schema references, missing temporal constraints
- **Interrupt Mechanism**: Pauses execution to collect user clarification
- **State Persistence**: Uses checkpointer for conversation continuity

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
