# IDICO Sales MCP

Aplicación FastMCP (`idico-sales`) que expone herramientas para consultar NetSuite y entregar resúmenes de ventas, cotizaciones y artículos cotizados. Usa consultas SQL a NetSuite y transformaciones en pandas para devolver métricas listas para consumo por clientes como Claude o ChatGPT.

## Requisitos
- Python 3.10+
- Dependencias definidas en `requirements.txt`/`pyproject.toml`
- Acceso a NetSuite con las credenciales configuradas en variables de entorno esperadas por `connections.netsuite`

## Ejecución
```bash
source .venv/activate
# Usando UV
uv sync 
uv run main.py
# Usando PiP
pip install -r requirements.txt
python main.py 

#Usando Docker
docker compose up --build
```
El servidor MCP se levanta por defecto en `0.0.0.0:8000` (transporte SSE).

## Estructura rápida
- `main.py`: definición de tools MCP y orquestación de consultas + transformaciones.
- `connections/`: conexión y consultas SQL a NetSuite.
- `analitycs/`: transformaciones y agregados de datos en pandas.
- `utils/`: utilidades (fechas, etc.).

## Tools y alcance

| Tool | Propósito | Entradas | Salida |
| --- | --- | --- | --- |
| `quotes_by_inside_sales` | Resumen de cotizaciones gestionadas por Inside Sales en un rango de fechas. | `initial_date`, `final_date` (ISO-8601, por defecto hoy–hoy); `inside_sales` (opcional, sin distinción de mayúsculas). | KPIs por Inside Sales (monto total, # de cotizaciones, ticket promedio, ranking), desglose por estatus con listas de cotizaciones, win-rate (por cantidad y monto), distribución de incoterms, más totales generales, resumen por periodo y timeline diario. |
| `sales_by_inside_sales` | Resumen de órdenes de venta (bookings) gestionadas por Inside Sales en un rango de fechas. | `initial_date`, `final_date` (ISO-8601, por defecto hoy–hoy); `inside_sales` (opcional, sin distinción de mayúsculas). | KPIs por Inside Sales (monto total, # de órdenes, ticket promedio, ranking), desglose por estatus con listas de órdenes, top clientes por Inside Sales, más totales generales, resumen por periodo y timeline diario. |
| `bookings_by_period` | Bookings y margen bruto por periodo, subsidiaria y entidad. | `initial_date`, `final_date` (ISO-8601, por defecto primer día del mes actual a hoy). | Totales generales, KPIs por periodo y subsidiaria, top clientes por monto bruto, buckets de margen por cliente. (El query limita a ciertas subsidiarias y excluye entidades/empleados específicos.) |
| `customer_bookings_by_period` | Bookings de un cliente específico, agrupados por periodo. | `initial_date`, `final_date` (ISO-8601, por defecto primer día del mes actual a hoy); `customer_name` (requerido, sin distinción de mayúsculas). | Totales generales (gross/net/margen, GM% ponderado), timeline por periodo, desglose por estatus con listas de órdenes, outliers (top órdenes y órdenes con margen negativo). |
| `items_quoted_by_customer` | Ítems cotizados a un cliente específico en un rango de fechas, con contexto de Inside Sales. | `initial_date`, `final_date` (ISO-8601, por defecto primer día del mes actual a hoy); `customer_name` (requerido, sin distinción de mayúsculas); `inside_sales` (filtro opcional). | Ranking de vendors, ranking de marcas, cobertura por Inside Sales (marcas/vendors/grupos de producto), desglose cliente–marca, top ítems cotizados (líneas, cantidad, valor). |
