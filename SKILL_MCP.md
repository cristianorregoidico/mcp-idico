## Overview

This skill enables the agent to use **IDRA IDICO**, an MCP Server for real-time access to sales, operations, and performance data sourced from **NetSuite** and **PostgreSQL** to deliver clear, actionable business insights related to the IDICO Operation.

---

## Available Tools

### Sales

| Tool | Purpose | Key Parameters |
|------|---------|---------------|
| `get_quotes` | Quotes summary with KPIs, margins, and status breakdown | `initial_date`, `final_date`, `inside_sales`, `customer_name` |
| `get_bookings` | Bookings totals, average ticket, margin metrics, subsidiary breakdown | `initial_date`, `final_date`, `customer_name`, `inside_sales` |
| `get_quoted_items` | Items or brands quoted — frequency, IS coverage, vendor ranking | `initial_date`, `final_date`, `customer_name`, `inside_sales`, `topic` (`"items"` or `"brand"`) |
| `get_sold_items` | Items or brands sold — volume, margin, product group distribution | `initial_date`, `final_date`, `customer_name`, `inside_sales`, `topic` (`"items"` or `"brand"`) |
| `get_opportunities` | Sales pipeline: opportunity count, status breakdown, top customers by IS | `initial_date`, `final_date`, `inside_sales`, `customer_name` |
| `get_vendors_to_quote` | Vendor recommendations for a customer-brand pair using historical data | `customer_name` (required), `brand` (required) |
| `get_events_summary` | Customer call/event summaries for relationship insights | `start_date`, `final_date`, `customer_name`, `organizer`, `subject` |

### Operations

| Tool | Purpose | Key Parameters |
|------|---------|---------------|
| `get_helga_guides` | Shipping guides filtered by PO, status, or service | `po`, `status`, `service` |
| `get_otd_indicators` | On-time delivery percentage, monthly breakdown, PO state distribution | `initial_date`, `final_date`, `so_number` |
| `get_customer_imports` | Customer import totals, brand/vendor breakdown, trends | `customer_name` (required) |

### Performance

| Tool | Purpose | Key Parameters |
|------|---------|---------------|
| `get_inside_sales_performance_report` | IS response times (opp→quote), hit rates, performance scores | `initial_date`, `final_date` |
| `get_scorecard_by_is` | Daily, monthly, and yearly scorecards per IS rep or all reps | `inside_sales` (optional) |

### Data & Notifications

| Tool | Purpose | Key Parameters |
|------|---------|---------------|
| `get_dataset` | Retrieve a full previously saved dataset by its reference filename | `data_set_reference` (required) |
| `send_email` | Send an email or Teams message via Power Automate | `subject`, `body`, `recipients` (all required); `is_teams_message`, `from_email` (optional) |

---

## Default Behaviors

- **Default date range:** When `initial_date` / `final_date` are omitted, all date-range tools default to **month start → today**.
- **All dates use format:** `YYYY-MM-DD`.
- **`topic` parameter:** Use `"items"` for item-level analysis or `"brand"` for vendor/brand-level analysis in `get_quoted_items` and `get_sold_items`.
- **`customer_name` and `inside_sales`:** Case-insensitive partial matches are accepted.
- **`get_vendors_to_quote`:** Both `customer_name` and `brand` are required and will be uppercased internally.
- **Datasets:** Large result sets are automatically saved to disk. The response includes a `dataset_reference` filename. Use `get_dataset` to retrieve the full data when needed.

---

## Response Structure

Every tool returns a standardized JSON envelope:

```
meta          → filters applied, source systems, row/column counts, dataset_reference
kpi_metrics   → computed KPIs (totals, averages, rankings, breakdowns)
artifacts     → dataset filename and 5-row preview
details       → tool-specific supplemental data
```

Always surface `kpi_metrics` as the primary answer. Use `artifacts.data_preview` for quick inline context. Suggest `get_dataset` when the user needs the full underlying data.

---

## How to Use This Server Effectively

### Choosing the Right Tool

- **"How are we doing this month?"** → Start with `get_bookings` + `get_quotes` for sales overview.
- **"Which products are trending?"** → Use `get_sold_items` with `topic="brand"` or `topic="items"`.
- **"Who should I quote for this customer?"** → Use `get_vendors_to_quote`.
- **"How is [rep name] performing?"** → Use `get_inside_sales_performance_report` + `get_scorecard_by_is`.
- **"Is our delivery on time?"** → Use `get_otd_indicators`.
- **"What is [customer] importing?"** → Use `get_customer_imports`.
- **"Any shipping issues?"** → Use `get_helga_guides` filtered by `status`.
- **"Notify the team"** → Use `send_email`.

### Combining Tools

When a question spans multiple dimensions, call tools in parallel where parameters are independent:

```
get_quotes + get_bookings       → conversion rate (quotes → bookings)
get_opportunities + get_bookings → pipeline vs closed analysis
get_sold_items + get_bookings   → margin by product mix
```

### Retrieving Full Datasets

1. Run any analytical tool → note the `dataset_reference` in the response.
2. Call `get_dataset(data_set_reference="<filename>")` to get all rows/columns.
3. Use the full dataset for custom filtering, exports, or further analysis.

---

## Constraints & Limits

- **Read-only by default.** All tools except `send_email` are read-only and safe to call without side effects.
- **`send_email` has real-world effects.** Confirm recipients and content before calling. It sends actual emails or Teams messages via Power Automate.
- **NetSuite queries can be slow.** Queries that span large date ranges (e.g., full year) may take several seconds.
- **No schema modification.** This server provides analytics access only — it does not insert, update, or delete records in any source system.
- **Dataset files are ephemeral.** Files in `data/` are not guaranteed to persist indefinitely. Retrieve datasets promptly after generation.


---

## Glossary

| Term | Meaning |
|------|---------|
| IS / Inside Sales | Internal sales representative |
| Booking | Confirmed sales order |
| Quote | Sales quotation (not yet confirmed) |
| OTD | On-Time Delivery |
| SO | Sales Order number |
| PO | Purchase Order number |
| Helga | Internal name for the shipping/guide tracking system |
| Dataset reference | Filename returned by a tool pointing to a saved JSON dataset in `data/` |
| Power Automate | Microsoft workflow automation used for email/Teams delivery |
