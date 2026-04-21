# IDRA / IDICO MCP Server

Servidor MCP construido con `FastMCP` para exponer consultas analíticas y operativas de IDICO a partir de NetSuite y PostgreSQL, con autenticación en Azure / Microsoft Entra ID y almacenamiento OAuth en Azure Redis.

El proyecto entrega respuestas estructuradas para clientes MCP, guarda datasets reutilizables en `data/` y registra auditoría básica por tool invocado.

---

## Qué hace este proyecto

Este servidor permite consultar información de negocio como:

- cotizaciones, bookings y oportunidades comerciales
- items cotizados o vendidos
- vendors sugeridos para cotizar por cliente y marca
- scorecards y performance de Inside Sales
- OTD, guías Helga e importaciones por cliente
- resúmenes de eventos / llamadas comerciales
- recuperación de datasets generados previamente

Además:

- autentica usuarios con `AzureProvider`
- persiste estado OAuth en Azure Redis
- identifica qué usuario ejecuta cada petición
- registra llamadas a tools en PostgreSQL

---

## Arquitectura actual

### Flujo general

1. `main.py` compone el servidor FastMCP.
2. `auth/` encapsula autenticación, Redis y resolución de identidad.
3. `server/tool_registry.py` registra tools con un wrapper común.
4. `tools/` expone los casos de uso MCP.
5. `connections/` ejecuta consultas contra NetSuite y PostgreSQL.
6. `analitycs/` transforma resultados tabulares en resúmenes de negocio.
7. `utils/` construye envelopes, persiste datasets y resuelve helpers comunes.

### Módulos principales

| Ruta | Responsabilidad |
|---|---|
| `main.py` | Arranque del servidor y composición general |
| `auth/redis_client.py` | Crear cliente Azure Redis |
| `auth/provider.py` | Crear `AzureProvider` |
| `auth/debug.py` | Resolver el usuario autenticado y emitir `AUTH-DEBUG` |
| `server/tool_registry.py` | Registrar tools con wrapper compartido |
| `tools/sales.py` | Tools comerciales |
| `tools/operations.py` | Tools operativas |
| `tools/performance.py` | Tools de desempeño |
| `tools/files.py` | Recuperación de datasets |
| `connections/netsuite.py` | Conexión JDBC a NetSuite |
| `connections/postgresql.py` | Consultas y auditoría en PostgreSQL |
| `data/` | Artefactos JSON/Excel generados |

---

## Autenticación y seguridad

### Azure / Microsoft Entra ID

El servidor usa `AzureProvider` de FastMCP con scope requerido:

- `mcp-access`

La autenticación ya no usa Auth0.

### Azure Redis

Azure Redis se usa como backend de `client_storage` para persistir estado OAuth entre reinicios e instancias.

Puntos importantes:

- conexión mediante `AZURE_REDIS_URL`
- `decode_responses=True` para compatibilidad con `RedisStore`
- cifrado de datos en reposo mediante `FernetEncryptionWrapper`

### Identificación del usuario por petición

Antes de ejecutar cada tool se resuelve el usuario autenticado:

1. `preferred_username`
2. si no existe, `name`
3. si algo falla, `"Not Identified"`

Además se imprime un log como:

```text
[AUTH-DEBUG] tool=get_bookings auth=usuario@idico.com
```

---

## Observabilidad y auditoría

Cada tool se ejecuta a través de un wrapper central que:

- captura el usuario autenticado
- ejecuta el tool en `asyncio.to_thread(...)`
- mide duración
- registra resultado o error en PostgreSQL

La función `log_tool_call(...)` persiste actualmente:

- `tool_name`
- `username`
- `params`
- `response`
- `duration_ms`

Esto permite trazabilidad básica sin tocar la lógica individual de cada tool.

---

## Catálogo de tools

> Los nombres públicos corresponden a las funciones registradas en el servidor.

### Sales

| Tool | Parámetros | Descripción |
|---|---|---|
| `get_quotes` | `initial_date`, `final_date`, `inside_sales`, `customer_name` | Resume cotizaciones por periodo, cliente e Inside Sales. Guarda dataset JSON. |
| `get_bookings` | `initial_date`, `final_date`, `customer_name`, `inside_sales` | Resume bookings, margen, KPIs agregados y composición comercial. Guarda dataset JSON. |
| `get_quoted_items` | `initial_date`, `final_date`, `customer_name`, `inside_sales`, `topic` | Analiza items o recurrencia de marcas cotizadas. Guarda dataset JSON. |
| `get_sold_items` | `initial_date`, `final_date`, `customer_name`, `inside_sales`, `topic` | Analiza items vendidos o recurrencia de marcas vendidas. Guarda dataset JSON. |
| `get_opportunities` | `initial_date`, `final_date`, `inside_sales`, `customer_name` | Resume oportunidades y pipeline comercial por periodo. Guarda dataset JSON. |
| `get_vendors_to_quote` | `customer_name`, `brand` | Sugiere vendors a cotizar combinando histórico por cliente/marca y país/marca. |
| `get_events_summary` | `start_date`, `final_date`, `customer_name`, `organizer`, `subject` | Recupera y resume eventos / llamadas comerciales para análisis de relaciones o insights. |

### Operations

| Tool | Parámetros | Descripción |
|---|---|---|
| `get_helga_guides` | `po`, `status`, `service` | Recupera guías Helga por PO, estado o servicio. Guarda dataset JSON. |
| `get_otd_indicators` | `initial_date`, `final_date`, `so_number` | Calcula indicadores OTD por periodo y puede devolver detalle de una SO específica. Guarda dataset JSON. |
| `get_customer_imports` | `customer_name` | Resume importaciones por cliente: montos, marcas, vendors y tendencias. |

### Performance

| Tool | Parámetros | Descripción |
|---|---|---|
| `get_inside_sales_performance_report` | `initial_date`, `final_date` | Calcula tiempos de respuesta, hitrate y score de desempeño de Inside Sales. Guarda dataset JSON. |
| `get_scorecard_by_is` | `inside_sales` | Devuelve scorecard diario, mensual y anual desde PostgreSQL. |

### Files

| Tool | Parámetros | Descripción |
|---|---|---|
| `get_dataset` | `data_set_reference` | Recupera un dataset JSON guardado previamente. |

> Nota: `get_excel_file` existe en código pero actualmente no está incluido en `FILES_TOOLS`, por lo que no queda expuesto como tool MCP.

---

## Formato de respuesta

Las tools usan `utils/envelope.py` para devolver una estructura homogénea:

```json
{
  "meta": {
    "tool_name": "get_bookings",
    "generated_at": "2026-04-21T00:00:00+00:00",
    "source_systems": ["netsuite"],
    "filters": {},
    "row_count": 100,
    "column_count": 12,
    "columns": [],
    "time_range": {
      "start_date": "2026-04-01",
      "end_date": "2026-04-21"
    },
    "dataset_reference": "20260421_101010_bookings_data.json"
  },
  "kpi_metrics": {},
  "artifacts": {
    "dataset": {}
  },
  "details": {}
}
```

Campos frecuentes:

- `meta`: trazabilidad y filtros aplicados
- `kpi_metrics`: resumen principal para el cliente MCP
- `artifacts.dataset`: referencia reutilizable al dataset completo
- `details`: tablas auxiliares o información complementaria

---

## Datasets y artefactos

Muchas tools guardan el resultado tabular completo en `data/`.

### JSON

Ejemplo:

```text
20260421_101010_bookings_data.json
```

Estructura:

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

El proyecto tiene helper para exportar DataFrames a Excel (`save_df_to_excel`), pero hoy la exportación no está activada en todas las tools ni el archivo Excel está expuesto como tool MCP.

---

## Comportamiento por defecto de fechas

La mayoría de las tools con rango temporal usan:

- fecha inicial: primer día del mes actual
- fecha final: hoy

Esto aplica, por ejemplo, a:

- `get_quotes`
- `get_bookings`
- `get_quoted_items`
- `get_sold_items`
- `get_opportunities`
- `get_otd_indicators`
- `get_inside_sales_performance_report`
- `get_events_summary`

El helper está en:

- `utils/date.py`

---

## Fuentes de datos

### NetSuite

Se usa conexión JDBC a través de `jaydebeapi` y el driver configurado localmente.

Consultas típicas:

- cotizaciones
- bookings
- oportunidades
- desempeño comercial
- items vendidos/cotizados

### PostgreSQL

Se usa para:

- scorecards
- OTD
- guías Helga
- imports
- vendors sugeridos
- eventos / llamadas
- auditoría de tools

Hay uso diferenciado de:

- `PGHOST`
- `PGHOST_DEV`

según la función utilizada.

---

## Variables de entorno

El proyecto depende de variables de entorno para autenticación, Redis y bases de datos.

Notas:

- `connections/netsuite.py` sí carga `.env` mediante `load_dotenv()`.
- para autenticación Azure, Redis y PostgreSQL es recomendable exportar variables en el entorno o usar `docker compose` con `env_file`.

### Azure Auth / Redis

- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_TENANT_ID`
- `AZURE_REDIS_URL`
- `JWT_SIGNING_KEY`
- `STORAGE_ENCRYPTION_KEY`
- `BASE_URL`

### NetSuite

- `DRIVER_NETSUITE`
- `URL_NETSUITE`
- `USER_NETSUITE`
- `PWD_NETSUITE`

### PostgreSQL

- `PGHOST`
- `PGHOST_DEV`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

### Variables heredadas / históricas

Todavía pueden existir variables viejas de Auth0 en `.env`, pero la implementación actual usa Azure.

---

## Requisitos

- Python `>= 3.12`
- Java / JDK para conexión JDBC a NetSuite
- Acceso de red a NetSuite, PostgreSQL, Azure Redis y endpoints de Azure auth

Dependencias principales:

- `fastmcp`
- `mcp[cli]`
- `pandas`
- `openpyxl`
- `jaydebeapi`
- `psycopg[binary]`
- `cryptography`
- `py-key-value-aio[redis]`

---

## Instalación y ejecución local

### Opción 1: `uv`

```bash
uv sync
uv run main.py
```

### Opción 2: `venv` + `pip`

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

El servidor levanta en:

- host: `0.0.0.0`
- puerto: `8000`
- transporte: `streamable-http`

> El path HTTP depende de la configuración de FastMCP; en `main.py` no se fuerza actualmente un `path` personalizado.

---

## Docker

### Build y arranque

```bash
docker compose up --build
```

Características actuales:

- expone `8000:8000`
- carga `.env`
- usa `python:3.12-slim`
- instala `default-jdk-headless`
- ejecuta `uv run main.py`

Archivos:

- `Dockerfile`
- `docker-compose.yml`

---

## Desarrollo

### Estado del proyecto

- arquitectura modularizada para auth y registro de tools
- sin suite automatizada formal visible en el repositorio
- existe `test.py` como script manual de exploración

### Convenciones importantes

- las tools son de solo lectura
- el wrapper central aplica `readOnlyHint=True`
- las consultas SQL están predefinidas en `connections/*_querys.py`
- la lógica de negocio vive principalmente en `analitycs/`

---

## Estructura resumida del repositorio

```text
.
├── auth/
├── analitycs/
├── connections/
├── data/
├── server/
├── tools/
├── utils/
├── main.py
├── Dockerfile
├── docker-compose.yml
└── pyproject.toml
```

---

## Notas

- La documentación de detalle sobre la migración a Azure y la captura del usuario autenticado quedó ampliada en `REPORTE_2026-04-20.md`.
- Si en el futuro se reactiva exportación Excel como artefacto público, conviene actualizar también este README y `FILES_TOOLS`.
