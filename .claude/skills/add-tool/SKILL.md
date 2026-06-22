---
name: add-tool
description: Scaffold a new MCP tool following the 4-layer pattern (queries → domain → use_cases → tools.py). Use when adding a new analytical capability to an existing feature domain.
user-invocable: true
allowed-tools:
  - Read
  - Write
  - Edit
  - Bash(find *)
  - Bash(grep *)
---

# /add-tool — Scaffold a New MCP Tool

Arguments: `$ARGUMENTS`

Expected format: `<domain> <capability> <source>`

- `domain`: existing feature domain — `sales`, `operations`, `performance`, `financial`
- `capability`: snake_case name for the new capability (e.g. `returns`, `credit_notes`)
- `source`: data source — `netsuite` or `postgresql`

---

## Step 1 — Parse and validate arguments

Extract `domain`, `capability`, and `source` from `$ARGUMENTS`.

If any argument is missing, ask the user before proceeding:
- Which domain? (`sales`, `operations`, `performance`, `financial`)
- What is the capability name in snake_case?
- Data source: `netsuite` or `postgresql`?

Also ask: **What parameters does this tool accept?** (besides the standard date range). List names, types, and whether optional.

---

## Step 2 — Confirm nothing already exists

Check that none of the 4 target files exist yet:

```
features/<domain>/queries/<capability>.py
features/<domain>/domain/<capability>.py
features/<domain>/use_cases/<capability>.py
```

And check that `features/<domain>/tools.py` already exists (the domain must exist). If the domain doesn't exist, stop and tell the user to create the domain directory structure first.

---

## Step 3 — Create `features/<domain>/queries/<capability>.py`

This file returns `(sql, params)`. No I/O, no imports from connections.

Template for **NetSuite** source:

```python
def get_<capability>_data(<params>) -> tuple[str, list]:
    where_clauses = [
        # always-on filters here
    ]
    params = [<required_params>]

    <for each optional param:>
    if <param>:
        where_clauses.append("<SQL condition with ?>")
        params.append(<param>)

    sql = f"""
    SELECT
        -- columns here
    FROM <table>
    WHERE {' AND '.join(where_clauses)}
    ORDER BY <column>;
    """
    return sql, params
```

Template for **PostgreSQL** source:

```python
def get_<capability>_data(<params>) -> tuple[str, list]:
    where_clauses = [
        # always-on filters here
    ]
    params = [<required_params>]

    <for each optional param:>
    if <param>:
        where_clauses.append("<col> ILIKE %s")
        params.append(f"%{<param>}%")

    sql = f"""
    SELECT
        -- columns here
    FROM <table>
    WHERE {' AND '.join(where_clauses)}
    ORDER BY <column>;
    """
    return sql, params
```

Note: NetSuite uses `?` placeholders; PostgreSQL uses `%s`.

---

## Step 4 — Create `features/<domain>/domain/<capability>.py`

Pure business logic — takes a `pd.DataFrame`, returns a `dict`. No I/O, no DB calls.

```python
import pandas as pd


def <capability>_summary(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            # return zero-value structure matching non-empty shape
        }

    # KPI calculations here
    # ...

    return {
        # structured result — all values must be JSON-serialisable
        # (convert np.integer → int, np.floating → float, Timestamps → isoformat)
    }
```

Do **not** include a `full_data_reference` key — the use case strips it and the envelope tests check for its absence.

---

## Step 5 — Create `features/<domain>/use_cases/<capability>.py`

Orchestrates query → connection → dataset save → domain logic → envelope.

**NetSuite source:**

```python
from typing import Any, Dict

from connections.netsuite.client import NetSuiteConnection
from features.<domain>.domain.<capability> import <capability>_summary
from features.<domain>.queries.<capability> import get_<capability>_data
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe


def execute(<params>) -> Dict[str, Any]:
    sql, params = get_<capability>_data(<params>)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql, params)

    dataset_reference = save_result_to_json(
        columns, rows,
        f"<Capability> dataset between {initial_date} and {final_date}",
        name="<capability>_data",
    )
    df = tuple_to_dataframe(columns, rows)
    summary = <capability>_summary(df)
    summary.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_<capability>",
        summary=summary,
        filters={
            "initial_date": initial_date,
            "final_date": final_date,
            # other params — use `or None` for optional string params
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )
```

**PostgreSQL source** (use `execute_pg_query` for prod schema, `execute_pg_query_dev` for dev schema):

```python
from typing import Any, Dict

from connections.postgresql.client import execute_pg_query  # or execute_pg_query_dev
from features.<domain>.domain.<capability> import <capability>_summary
from features.<domain>.queries.<capability> import get_<capability>_data
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe


def execute(<params>) -> Dict[str, Any]:
    sql, params = get_<capability>_data(<params>)
    columns, rows = execute_pg_query(sql, params)

    dataset_reference = save_result_to_json(
        columns, rows,
        "<Capability> dataset description",
        name="<capability>_data",
    )
    df = tuple_to_dataframe(columns, rows)
    summary = <capability>_summary(df)

    return build_tool_response(
        tool_name="get_<capability>",
        summary=summary,
        filters={
            "initial_date": initial_date,
            "final_date": final_date,
            # other params
        },
        source_systems=["postgresql"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )
```

---

## Step 6 — Add the tool function to `features/<domain>/tools.py`

Read the current `features/<domain>/tools.py`. Add:

1. The import for the new use case at the top:
   ```python
   from features.<domain>.use_cases import <capability>
   ```

2. The tool function, following exactly this shape (sync, Optional params, docstring with "Use this tool when..." and Args + Returns):
   ```python
   def get_<capability>(
       initial_date: Optional[str] = None,
       final_date: Optional[str] = None,
       <other_optional_params>: Optional[str] = "",
   ) -> Dict[str, Any]:
       """<One-line description>.

       Use this tool when the user asks for <...>.

       Args:
           initial_date: Start date in YYYY-MM-DD format; defaults to month start.
           final_date: End date in YYYY-MM-DD format; defaults to today.
           <param>: <description>; optional.

       Returns:
           Dict[str, Any]: <summary of what kpi_metrics contains>.
       """
       start_of_month, today_date = get_month_start_and_today()
       start_q_date = initial_date or start_of_month
       final_q_date = final_date or today_date
       normalized_<param> = <param>.upper() if <param> else ""
       return <capability>.execute(start_q_date, final_q_date, normalized_<param>)
   ```

3. Append `get_<capability>` to the domain's `TOOLS` list at the bottom of the file.

The tool function **must be synchronous** — `middleware.register_tool` wraps it with `asyncio.to_thread`.

---

## Step 7 — Report what was created

List the 4 files created/modified with their paths. Remind the user:

- Run `uv run python -m pytest tests/test_response_envelope_contract.py` to check the envelope contract isn't broken.
- Add an envelope contract test for the new tool in `tests/test_response_envelope_contract.py` following the existing pattern (mock connections and use_case internals, assert exact key sets on the response).
