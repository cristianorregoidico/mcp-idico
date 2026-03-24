# IDICO IDRA MCP

Servidor MCP construido con `FastMCP` para exponer consultas analíticas de ventas, operaciones y desempeño comercial a partir de NetSuite y PostgreSQL. El servicio entrega respuestas resumidas listas para consumo por clientes MCP y, cuando aplica, guarda datasets completos en `data/` para consultas posteriores o exportación a Excel.

## Qué hace este proyecto

Este repositorio centraliza consultas predefinidas y transformaciones en `pandas` para responder preguntas de negocio como:

- cotizaciones por Inside Sales o cliente
- bookings y margen por periodo
- oportunidades y conversión comercial
- items cotizados o vendidos
- scorecards y performance de Inside Sales
- OTD, guías Helga e importaciones por cliente
- recuperación de datasets JSON y archivos Excel generados por tools previas

La aplicación se publica como un servidor MCP HTTP en:

- host: `0.0.0.0`
- puerto: `8000`
- transporte: `streamable-http`
- endpoint: `/mcp`

## Arquitectura

Flujo general:

1. `main.py` registra todas las tools MCP.
2. `tools/` encapsula los casos de uso expuestos al cliente MCP.
3. `connections/` construye y ejecuta consultas contra NetSuite y PostgreSQL.
4. `analitycs/` transforma los resultados tabulares en resúmenes JSON.
5. `utils/` guarda datasets, genera Excel y resuelve utilidades de fechas.

Componentes principales:

- `main.py`: arranque del servidor MCP y registro de tools.
- `tools/sales.py`: tools comerciales y de ventas.
- `tools/operations.py`: tools operativas.
- `tools/performance.py`: tools de performance y scorecards.
- `tools/files.py`: acceso a datasets JSON y archivos Excel generados.
- `connections/netsuite.py`: conexión JDBC a NetSuite usando `jaydebeapi` y `NQjc.jar`.
- `connections/postgresql.py`: ejecución de consultas en PostgreSQL.
- `data/`: datasets JSON y Excel generados en tiempo de ejecución.

## Requisitos

- Python `>= 3.10.12`
- Java/JDK disponible para el driver JDBC de NetSuite
- Acceso de red a NetSuite y PostgreSQL
- Variables de entorno configuradas para ambas conexiones

Dependencias principales definidas en `pyproject.toml`:

- `fastmcp`
- `mcp[cli]`
- `pandas`
- `openpyxl`
- `jaydebeapi`
- `psycopg[binary]`
- `python-dotenv`

## Variables de entorno

El proyecto carga variables desde `.env` para NetSuite mediante `load_dotenv()`. PostgreSQL se lee desde variables de entorno del proceso.

### NetSuite

Variables requeridas por [`connections/netsuite.py`](/home/cod/dev/labs/mcp/idico-mcp/connections/netsuite.py):

- `DRIVER_NETSUITE`
- `URL_NETSUITE`
- `USER_NETSUITE`
- `PWD_NETSUITE`

El driver JDBC se toma desde:

- [`connections/lib/NQjc.jar`](/home/cod/dev/labs/mcp/idico-mcp/connections/lib/NQjc.jar)

### PostgreSQL

Variables usadas por [`connections/postgresql.py`](/home/cod/dev/labs/mcp/idico-mcp/connections/postgresql.py):

- `PGHOST`
- `PGHOST_DEV`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

Notas:

- `execute_pg_query()` usa `PGHOST`.
- `execute_pg_query_dev()` usa `PGHOST_DEV`.
- varias tools operan actualmente contra `PGHOST_DEV`.

## Instalación y ejecución local

### Opción 1: usando `uv`

```bash
uv sync
uv run main.py
```

### Opción 2: usando `pip`

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Si todo está configurado correctamente, el servidor quedará escuchando en `http://localhost:8000/mcp`.

## Ejecución con Docker

### Build y arranque

```bash
docker compose up --build
```

La composición actual:

- expone `8000:8000`
- carga variables desde `.env`
- usa una imagen basada en `python:3.12-slim`
- instala `default-jdk-headless` para soportar el driver JDBC de NetSuite

Archivos relacionados:

- [`Dockerfile`](/home/cod/dev/labs/mcp/idico-mcp/Dockerfile)
- [`docker-compose.yml`](/home/cod/dev/labs/mcp/idico-mcp/docker-compose.yml)

## Catálogo de tools

Los nombres públicos de las tools corresponden a los nombres de función registrados en `main.py`.

### Sales

| Tool | Parámetros | Descripción |
| --- | --- | --- |
| `get_quotes` | `initial_date`, `final_date`, `inside_sales`, `customer_name` | Resume cotizaciones por periodo, Inside Sales y cliente. Genera dataset JSON y archivo Excel. |
| `get_bookings` | `initial_date`, `final_date`, `customer_name`, `inside_sales` | Resume bookings, margen, términos, top clientes y KPIs agregados. Genera dataset JSON. |
| `get_quoted_items` | `initial_date`, `final_date`, `customer_name`, `inside_sales` | Analiza items cotizados por cliente, marca, vendor e Inside Sales. Genera dataset JSON. |
| `get_sold_items` | `initial_date`, `final_date`, `customer_name`, `inside_sales` | Analiza items vendidos, marcas, vendors y distribución comercial. Genera dataset JSON. |
| `get_opportunities` | `initial_date`, `final_date`, `inside_sales` | Resume oportunidades por periodo e Inside Sales. Genera dataset JSON. |
| `get_vendors_to_quote` | `customer_name`, `brand` | Sugiere vendors a cotizar combinando histórico por cliente/marca y país/marca desde PostgreSQL. |

### Operations

| Tool | Parámetros | Descripción |
| --- | --- | --- |
| `get_helga_guides` | `po`, `status`, `service` | Recupera guías Helga filtradas por PO, estado o servicio. Genera dataset JSON. |
| `get_otd_indicators` | `initial_date`, `final_date`, `so_number` | Calcula indicadores OTD por mes y, si se envía `so_number`, devuelve detalle de la orden. Genera dataset JSON. |
| `get_customer_imports` | `customer_name` | Resume importaciones de un cliente: montos FOB/CIF, marcas, vendors, años e indicadores asociados. |

### Performance

| Tool | Parámetros | Descripción |
| --- | --- | --- |
| `get_inside_sales_performance_report` | `initial_date`, `final_date` | Calcula tiempos de respuesta, hitrates y score de performance de Inside Sales. Genera dataset JSON. |
| `get_scorecard_by_is` | `inside_sales` | Devuelve scorecards diario, mensual y anual desde PostgreSQL. |

### Files

| Tool | Parámetros | Descripción |
| --- | --- | --- |
| `get_dataset` | `data_set_reference` | Recupera un dataset JSON generado previamente. |
| `get_excel_file` | `file_name` | Devuelve un archivo `.xlsx` previamente generado. Incluye validación contra path traversal. |

## Comportamiento por defecto de fechas

Las tools no usan exactamente la misma ventana por defecto. Según implementación:

- `get_quotes`: hoy a hoy
- `get_opportunities`: hoy a hoy
- `get_inside_sales_performance_report`: hoy a hoy
- `get_bookings`: primer día del mes actual a hoy
- `get_quoted_items`: primer día del mes actual a hoy
- `get_sold_items`: primer día del mes actual a hoy
- `get_otd_indicators`: primer día del mes actual a hoy

El helper que define estas fechas está en [`utils/date.py`](/home/cod/dev/labs/mcp/idico-mcp/utils/date.py).

## Datasets y archivos generados

Varias tools persisten el resultado completo de las consultas para poder reutilizarlo después.

### JSON

Los datasets se guardan en `data/` con nombre timestamped, por ejemplo:

```text
20260129_152522_get_quotes.json
```

La estructura es:

```json
{
  "data_set_description": "Descripción del dataset",
  "columns": ["col1", "col2"],
  "rows": [
    ["valor1", "valor2"]
  ]
}
```

### Excel

Algunas tools también exportan el `DataFrame` completo a `.xlsx`, por ejemplo:

```text
20260129_152522_get_quotes.xlsx
```

La lógica de persistencia está en:

- [`utils/json_df.py`](/home/cod/dev/labs/mcp/idico-mcp/utils/json_df.py)

## Detalles operativos importantes

- El servidor registra las tools con `readOnlyHint=True` y `destructiveHint=False`.
- El proyecto no expone escritura sobre bases de datos; las tools actuales son de consulta y análisis.
- Las consultas SQL están predefinidas en `connections/netsuite_querys.py` y `connections/postgresql_querys.py`.
- Algunas tools guardan referencias al dataset completo bajo la clave `full_data_reference`.
- `get_quotes` también devuelve `excel_file` cuando genera una exportación.

## Desarrollo

Script auxiliar disponible:

- [`test.py`](/home/cod/dev/labs/mcp/idico-mcp/test.py): script manual de prueba y exploración local. No corresponde a una suite automatizada formal.

Estado actual del repositorio:

- no se detectó una carpeta de tests automatizados
- la documentación debe considerarse alineada con la implementación actual de `main.py` y `tools/`

## Sugerencia de `.env`

Ejemplo mínimo:

```env
DRIVER_NETSUITE=...
URL_NETSUITE=...
USER_NETSUITE=...
PWD_NETSUITE=...

PGHOST=...
PGHOST_DEV=...
PGPORT=5432
PGDATABASE=...
PGUSER=...
PGPASSWORD=...
```
