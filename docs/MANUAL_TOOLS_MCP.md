# Manual de tools del MCP de IDICO

## Qué es este MCP Server

Este servidor MCP expone consultas de negocio listas para usar sobre NetSuite y otras herramientas de IDICO. Sirve para que un asistente o cliente MCP pueda pedir resúmenes comerciales, operativos y de performance sin conocer SQL ni la estructura interna de datos.

## Qué puede hacer en términos de negocio

Permite responder preguntas como:

- cómo vienen las cotizaciones, bookings y oportunidades
- qué items o marcas se cotizan y venden con más frecuencia
- qué vendors conviene cotizar para un cliente y una marca
- cómo está el desempeño de Inside Sales
- cómo vienen OTD, guías Helga e importaciones
- cómo revisar datasets guardados de consultas previas

## Cómo pensar el uso de las tools

- `Sales`: análisis comercial y comportamiento de clientes, marcas e Inside Sales.
- `Operations`: seguimiento operativo y logística.
- `Performance`: métricas de velocidad, conversión y scorecards.
- `Files`: recuperación de resultados ya generados.

Regla simple:

- si querés una foto ejecutiva, usá las tools de resumen
- si querés volver a ver un resultado anterior, usá `get_dataset`
- si un filtro no aplica, en varias tools se puede omitir o enviar vacío

## Catálogo de tools

### Sales

#### `get_quotes`
- Para qué sirve: resume cotizaciones por período, cliente e Inside Sales.
- Cuándo conviene usarla: cuando necesitás ver volumen, montos, margen y distribución de cotizaciones.
- Parámetros: `initial_date`, `final_date`, `inside_sales`, `customer_name`.
- Qué devuelve normalmente: KPIs de cotizaciones, mix de estados, márgenes, top clientes y distribución por vendedor/país/términos.
- Dataset reutilizable: sí. Guarda un JSON en `data/` y devuelve su referencia.

#### `get_bookings`
- Para qué sirve: resume bookings del período con métricas de negocio.
- Cuándo conviene usarla: cuando querés leer ventas cerradas o bookings por cliente o Inside Sales.
- Parámetros: `initial_date`, `final_date`, `customer_name`, `inside_sales`.
- Qué devuelve normalmente: total de bookings, cantidad de órdenes, ticket promedio, margen, top clientes, distribución por términos y por subsidiaria.
- Dataset reutilizable: sí.

#### `get_quoted_items`
- Para qué sirve: analiza los items cotizados en el período.
- Cuándo conviene usarla: cuando querés ver qué productos, marcas o vendors aparecen más en cotizaciones.
- Parámetros: `initial_date`, `final_date`, `customer_name`, `inside_sales`, `topic`.
- Qué devuelve normalmente: resumen de vendors, marcas, Inside Sales, cliente-marca y top items cotizados.
- Dataset reutilizable: sí.
- Nota: `topic` puede usarse como `items` o `brand` para cambiar el foco del resumen.

#### `get_sold_items`
- Para qué sirve: analiza los items vendidos en el período.
- Cuándo conviene usarla: cuando querés entender qué se vendió, con qué margen y qué marcas pesan más.
- Parámetros: `initial_date`, `final_date`, `customer_name`, `inside_sales`, `topic`.
- Qué devuelve normalmente: resumen general, top items por volumen, monto y margen, items con margen bajo, vendors y distribución por marca y grupo de producto.
- Dataset reutilizable: sí.
- Nota: `topic` puede ponerse en `brand` para enfocar el análisis en marcas.

#### `get_opportunities`
- Para qué sirve: resume oportunidades comerciales del período.
- Cuándo conviene usarla: cuando querés mirar pipeline, clientes con más oportunidades o actividad por Inside Sales.
- Parámetros: `initial_date`, `final_date`, `inside_sales`, `customer_name`.
- Qué devuelve normalmente: total de oportunidades, clientes, distribución por vendedor y estado, casos atrasados y oportunidades por período.
- Dataset reutilizable: sí.

#### `get_vendors_to_quote`
- Para qué sirve: sugiere vendors para cotizar según cliente y marca.
- Cuándo conviene usarla: cuando querés una recomendación rápida de a quién pedir precio para una combinación cliente-marca.
- Parámetros: `customer_name`, `brand`.
- Qué devuelve normalmente: listas de vendors sugeridos con dos vistas, una por cliente-marca y otra por país-marca.
- Dataset reutilizable: no guarda un dataset separado.
- Nota: ambos parámetros son obligatorios.

#### `get_events_summary`
- Para qué sirve: resume eventos, llamadas o actividades comerciales de un cliente.
- Cuándo conviene usarla: cuando querés entender conversaciones, temas repetidos u objeciones comerciales.
- Parámetros: `start_date`, `final_date`, `customer_name`, `organizer`, `subject`.
- Qué devuelve normalmente: cantidad total de llamadas y un bloque con los textos recuperados para análisis posterior.
- Dataset reutilizable: no guarda dataset.
- Nota: los filtros se pueden dejar vacíos si no aplican, pero la tool pide los 5 parámetros.

### Operations

#### `get_helga_guides`
- Para qué sirve: recupera guías Helga por PO, estado o servicio.
- Cuándo conviene usarla: cuando necesitás seguimiento operativo de guías pendientes o por estado.
- Parámetros: `po`, `status`, `service`.
- Qué devuelve normalmente: listado de guías con campos como país, ciudad, fecha, tracking y estado.
- Dataset reutilizable: sí.
- Nota: si no enviás `status`, la consulta por defecto excluye las guías ya entregadas.

#### `get_otd_indicators`
- Para qué sirve: calcula indicadores de entrega a tiempo.
- Cuándo conviene usarla: cuando querés medir cumplimiento logístico por período o revisar una orden puntual.
- Parámetros: `initial_date`, `final_date`, `so_number`.
- Qué devuelve normalmente: porcentaje de entrega a tiempo por mes, resumen general y distribución por estado de PO.
- Dataset reutilizable: sí.
- Nota: si enviás `so_number`, la respuesta agrega el detalle de esa orden.

#### `get_customer_imports`
- Para qué sirve: resume importaciones de un cliente.
- Cuándo conviene usarla: cuando querés ver montos, marcas, vendors y tendencia de importaciones por cliente.
- Parámetros: `customer_name`.
- Qué devuelve normalmente: resumen de importaciones con totales y agrupaciones útiles para lectura ejecutiva.
- Dataset reutilizable: no guarda dataset.

### Performance

#### `get_inside_sales_performance_report`
- Para qué sirve: analiza performance de Inside Sales.
- Cuándo conviene usarla: cuando querés ver tiempos de respuesta, conversión y score general del equipo.
- Parámetros: `initial_date`, `final_date`.
- Qué devuelve normalmente: métricas globales, tiempos de respuesta oportunidad→quote, hit rates y ranking por Inside Sales.
- Dataset reutilizable: sí.

#### `get_scorecard_by_is`
- Para qué sirve: trae el scorecard de un Inside Sales en vista diaria, mensual y anual.
- Cuándo conviene usarla: cuando querés seguimiento recurrente de una persona o comparar su evolución.
- Parámetros: `inside_sales`.
- Qué devuelve normalmente: tres bloques de scorecard, uno por cada granularidad temporal.
- Dataset reutilizable: no guarda dataset.

### Files

#### `get_dataset`
- Para qué sirve: recupera un dataset guardado previamente por otra tool.
- Cuándo conviene usarla: cuando necesitás volver a revisar el detalle completo que quedó persistido en `data/`.
- Parámetros: `data_set_reference`.
- Qué devuelve normalmente: el JSON completo del dataset guardado.
- Dataset reutilizable: no aplica; justamente recupera uno existente.

## Capacidades transversales del servidor

- Autenticación: usa Azure / Microsoft Entra ID con scope `mcp-access`.
- Identificación de usuario: cada llamada intenta registrar quién ejecutó la tool.
- Trazabilidad: se registra nombre de tool, usuario, parámetros, respuesta y duración en PostgreSQL.
- Ejecución segura para el cliente MCP: las tools se publican como de solo lectura.
- Formato de respuesta: la mayoría devuelve un envelope con `meta`, `kpi_metrics`, `artifacts.dataset` y, a veces, `details`.
- Reutilización: varias tools guardan un JSON en `data/` y devuelven su referencia para consultarlo después con `get_dataset`.

## Limitaciones y notas útiles

- Hay 13 tools MCP expuestas actualmente.
- Existe 1 tool implementada pero no publicada: `get_excel_file`.
- `get_excel_file` está en `tools/files.py`, pero no se registra en el servidor.
- Algunas tools devuelven un nombre técnico interno distinto al nombre público del MCP; para uso diario, mandan el nombre registrado.
- La mayoría de las fechas, si no se informan, arrancan desde el primer día del mes actual hasta hoy.
- El documento prioriza el código real del servidor; si el README dice algo distinto, conviene tomar este manual como referencia más cercana al comportamiento actual.

## Resumen del catálogo

- Expuestas: `get_quotes`, `get_bookings`, `get_quoted_items`, `get_sold_items`, `get_opportunities`, `get_vendors_to_quote`, `get_events_summary`, `get_helga_guides`, `get_otd_indicators`, `get_customer_imports`, `get_inside_sales_performance_report`, `get_scorecard_by_is`, `get_dataset`.
- No expuesta: `get_excel_file`.
