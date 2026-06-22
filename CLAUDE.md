# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Run the MCP server (port 8000, streamable-http)
uv run main.py

# Run all tests
uv run python -m pytest tests/

# Run a single test file
uv run python -m pytest tests/test_response_envelope_contract.py

# Lint
uv run ruff check .
uv run ruff format .

# Docker
docker compose up --build
```

## Architecture

This is a **FastMCP server** that exposes read-only analytical tools over NetSuite (JDBC) and PostgreSQL. Tools are registered via a central middleware and served over `streamable-http` on port 8000.

### Request flow

1. `main.py` — composes the `FastMCP` app, calls `register_tools(app, <DOMAIN>_TOOLS)` for each domain.
2. `middleware.py:register_tool` — wraps every tool function: resolves the authenticated user, runs the sync function in `asyncio.to_thread`, measures duration, and logs to PostgreSQL via `log_tool_call`.
3. **Tool function** (`features/<domain>/tools.py`) — normalises parameters, sets date defaults using `utils/date.py`.
4. **Use case** (`features/<domain>/use_cases/`) — orchestrates: calls query builder, opens DB connection, fetches data, builds envelope.
5. **Query builder** (`features/<domain>/queries/`) — returns `(sql, params)` tuple; no I/O.
6. **Domain** (`features/<domain>/domain/`) — pure business logic, KPI calculations, aggregations.

### Feature domains

| Domain | Entry point | Data source |
|---|---|---|
| `sales` | `SALES_TOOLS` | NetSuite |
| `operations` | `OPS_TOOLS` | NetSuite + PostgreSQL |
| `performance` | `PERFORMANCE_TOOLS` | PostgreSQL |
| `financial` | `FINANCIAL_TOOLS` | NetSuite |
| `files` | `FILES_TOOLS` | local `data/` |
| `notifications` | `NOTIFICATION_TOOLS` | Power Automate |

### Connections

- `connections/netsuite/client.py` — JDBC via `jaydebeapi`; requires JDK. Uses `.env` loaded via `load_dotenv()`. Use `NetSuiteConnection(...).managed()` as a context manager.
- `connections/postgresql/client.py` — `psycopg` async; `execute_pg_query` targets `PGHOST`, `execute_pg_query_dev` targets `PGHOST_DEV`.

### Response envelope

Every tool must return this structure (enforced by tests):

```python
{
    "meta": {
        "tool_name": str,
        "generated_at": str,
        "source_systems": list[str],
        "filters": dict,          # normalised param values
        "row_count": int,
        "column_count": int,
        "columns": list,
        "time_range": dict,
        "dataset_reference": str | None,
    },
    "kpi_metrics": dict,          # primary answer surface; no internal keys like full_data_reference
    "artifacts": {"dataset": dict | None},
    "details": dict,              # optional supplemental tables
}
```

Helper: `utils/envelope.py`. Tests in `tests/test_response_envelope_contract.py` assert exact key sets for each tool.

### Adding a new tool

1. Create `features/<domain>/queries/<capability>.py` → returns `(sql, params)`.
2. Create `features/<domain>/domain/<capability>.py` → pure KPI logic.
3. Create `features/<domain>/use_cases/<capability>.py` → orchestrate and return envelope.
4. Add the tool function to `features/<domain>/tools.py` and append to the `<DOMAIN>_TOOLS` list.
5. The tool function must be **synchronous** — the middleware runs it in a thread.
6. Add `readOnlyHint=True` via `fn.MCP_ANNOTATIONS` if any deviation from the default is needed (default is already read-only in `middleware.py`).

### Key conventions

- Tool functions are **sync**; the async wrapper is added by `middleware.register_tool`.
- Date defaults (`initial_date` / `final_date`) are resolved in the tool function using `utils/date.get_month_start_and_today()`.
- String filter params (customer names, IS names) are uppercased before reaching the query layer; SQL uses `LIKE '%VALUE%'` patterns.
- Datasets are saved to `data/` via `utils/json_df.save_result_to_json`; the filename timestamp format is `YYYYMMDD_HHMMSS_<name>_data.json`.
- `kpi_metrics` must not contain internal fields like `full_data_reference` — strip them before building the envelope.

## Environment variables

See `.env.example` for the full list. Key groups:

- **Azure auth/Redis**: `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`, `AZURE_REDIS_URL`, `JWT_SIGNING_KEY`, `STORAGE_ENCRYPTION_KEY`, `BASE_URL`
- **NetSuite**: `DRIVER_NETSUITE`, `URL_NETSUITE`, `USER_NETSUITE`, `PWD_NETSUITE`
- **PostgreSQL**: `PGHOST`, `PGHOST_DEV`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`

Auth is currently **disabled** in `main.py` (`auth=auth_provider` is commented out). The `AzureProvider` + Azure Redis infrastructure is wired but not enforced at runtime.
