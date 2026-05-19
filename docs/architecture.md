# MCP Server Architecture

This document shows the current architecture of the IDICO MCP Server using Mermaid diagrams grounded in the repository implementation.

## 1. System Context

```mermaid
flowchart LR
    client[MCP Client / AI Assistant]
    server[IDICO MCP Server]
    entra[Azure Entra ID]
    redis[Azure Redis<br/>OAuth Client Storage]
    netsuite[NetSuite]
    pg[(PostgreSQL)]
    data[(Local data/ artifacts)]

    client -->|Invoke MCP tools| server
    server -->|Authenticate users| entra
    server -->|Persist OAuth state| redis
    server -->|Query business data| netsuite
    server -->|Query analytics / audit logs| pg
    server -->|Save generated datasets| data
```

## 2. Container Diagram

```mermaid
flowchart TB
    subgraph MCP["IDICO MCP Server"]
        app[FastMCP App<br/>main.py]
        middleware[Middleware Layer<br/>auth identity + timing + audit]
        auth[Auth Layer<br/>AzureProvider + identity resolver]
        
        subgraph features["Feature Modules"]
            sales[Sales]
            ops[Operations]
            perf[Performance]
            files[Files]
            notif[Notifications]
        end

        utils[Shared Utilities<br/>envelope + dates + transformations + dataset persistence]
        connectors[Data Connectors<br/>NetSuite client + PostgreSQL client]

        app --> middleware
        app --> auth
        app --> features
        features --> utils
        features --> connectors
        middleware --> connectors
    end

    entra[Azure Entra ID]
    redis[Azure Redis]
    netsuite[NetSuite]
    pg[(PostgreSQL)]
    data[(data/ artifacts)]

    auth --> entra
    auth --> redis
    connectors --> netsuite
    connectors --> pg
    utils --> data
```

## 3. Request / Sequence Flow

```mermaid
sequenceDiagram
    participant C as MCP Client
    participant A as FastMCP App
    participant M as Middleware
    participant T as Feature Tool
    participant U as Use Case
    participant Q as Query Builder
    participant D as Domain Logic
    participant N as NetSuite/PostgreSQL
    participant E as Envelope Builder
    participant L as Audit Logger

    C->>A: Invoke tool
    A->>M: Execute wrapped tool
    M->>M: Resolve authenticated user
    M->>M: Start timer
    M->>T: Call tool function
    T->>U: Execute use case
    U->>Q: Build sql + params
    U->>N: Execute query
    N-->>U: rows + columns
    U->>D: Compute KPIs / summaries
    D-->>U: business metrics
    U->>E: Build MCP response
    E-->>U: structured envelope
    U-->>T: final response
    T-->>M: final response
    M->>L: Persist tool call + duration + user
    M-->>A: final response
    A-->>C: MCP structured response
```

## 4. Feature Internal Pattern

```mermaid
flowchart LR
    tool[tools.py]
    usecase[use_cases/]
    query[queries/]
    domain[domain/]
    connector[connections/]
    envelope[utils/envelope.py]
    artifact[data/ dataset]

    tool --> usecase
    usecase --> query
    usecase --> domain
    query --> connector
    usecase --> envelope
    usecase --> artifact
```

## Notes

- Main entrypoint: `main.py`
- Cross-cutting execution wrapper: `middleware.py`
- Auth provider: `auth/provider.py`
- Identity resolution: `auth/identity.py`
- Response contract: `utils/envelope.py`
- Main domain structure: `features/<domain>/{tools.py,use_cases/,queries/,domain/}`

## Current design risks

- `main_final.py` appears to be a legacy or duplicate entrypoint.
- `connections/netsuite/client.py` contains hardcoded token material and should be moved to secure secret management.
- PostgreSQL connector handles both data access and audit logging, which increases coupling.
