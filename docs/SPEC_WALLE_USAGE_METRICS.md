# Spec: Walle Usage Metrics Tool

Esta feature agrega una nueva tool MCP en `features/operations` para consultar estadísticas de uso de Walle desde PostgreSQL. La tool debe ejecutar los cuatro bloques de información relacionados con Walle —emails resumidos, emails analizados, acciones sugeridas y uso de endpoints—, guardar esos resultados como datasets de referencia y construir las métricas con Pandas en la capa `domain`.

## Decisión principal

La implementación será una sola tool pública de operaciones:

1. **Una sola tool MCP** para métricas de Walle.
2. **Cuatro queries PostgreSQL parametrizadas**.
3. **Construcción dinámica del `WHERE`** según filtros.
4. **Placeholders `%s` de `psycopg`**, nunca valores quemados.
5. **Queries originales como datasets de detalle**, guardadas como `dataset_reference`.
6. **Capa domain con Pandas** para calcular KPIs a partir de los datasets consultados.
7. **Detalle limitado solo como muestra de respuesta**; los KPIs no deben depender de un `LIMIT 100`.

## Ubicación esperada

| Responsabilidad | Archivo / módulo |
|---|---|
| Tool MCP pública | `features/operations/tools.py` |
| Orquestación del caso de uso | `features/operations/use_cases/walle_usage.py` |
| Queries SQL parametrizadas | `features/operations/queries/walle_usage.py` |
| Cálculo de métricas | `features/operations/domain/walle_usage.py` |
| Tests | `tests/` |

> Las queries de esta feature deben quedar bajo `features/operations/queries`. No deben vivir en `connections/postgresql/queries.py`.

## Firma propuesta de la tool

```python
def get_walle_usage(
    initial_date: Optional[str] = None,
    final_date: Optional[str] = None,
    po_name: Optional[str] = None,
    limit: int = 100,
) -> Dict[str, Any]:
```

## Parámetros

| Parámetro | Requerido | Default | Descripción |
|---|---:|---|---|
| `initial_date` | No | Inicio del mes actual | Fecha inicial en formato `YYYY-MM-DD`. Obligatoria para búsqueda por rango cuando no llega `po_name`. |
| `final_date` | No | Fecha actual | Fecha final en formato `YYYY-MM-DD`. Obligatoria para búsqueda por rango cuando no llega `po_name`. |
| `po_name` | No | `None` | Filtro exacto por PO. Si llega, las queries asociadas a PO no deben incluir rango de fechas. |
| `limit` | No | `100` | Cantidad máxima de filas de muestra en `details`. No debe limitar el dataset usado para calcular métricas. Debe tener máximo controlado, por ejemplo `500`. |

## Regla crítica de filtros dinámicos

Las queries relacionadas con PO deben construir el `WHERE` con esta prioridad:

| Caso | Filtro que debe aplicarse |
|---|---|
| `po_name` viene con valor | Usar solo `po.po_name = %s`. **No incluir rango de fechas**. |
| `po_name` no viene | Usar rango de fechas con `initial_date` y `final_date`. |

Esta regla aplica a:

- Emails resumidos por AI.
- Emails analizados por AI.
- Acciones sugeridas por AI.

La query de **Uso de Walle** no recibe `po_name`; siempre debe usar rango de fechas.

### Ejemplo de construcción esperada

```python
where_clauses = []
params = []

if po_name:
    where_clauses.append("po.po_name = %s")
    params.append(po_name)
else:
    where_clauses.append('poe.email_date::date BETWEEN %s AND %s')
    params.extend([initial_date, final_date])
```

> El f-string solo se permite para unir fragmentos SQL controlados por el código, por ejemplo `WHERE {' AND '.join(where_clauses)}`. No se permite interpolar valores enviados por el usuario.

## Resolución de fechas

Si `po_name` no viene, la tool debe asegurar que existan fechas:

```python
if not po_name and (not initial_date or not final_date):
    initial_date, final_date = get_month_start_and_today()
```

Si `po_name` viene, las fechas pueden llegar pero **no deben agregarse** al `WHERE` de las queries por PO.

La query de uso de Walle debe resolver fechas siempre:

```python
if not initial_date or not final_date:
    initial_date, final_date = get_month_start_and_today()
```

## Queries corregidas

Las siguientes queries muestran la forma esperada con placeholders `%s`. La implementación debe retornarlas como `(sql, params)` desde funciones dedicadas en `features/operations/queries/walle_usage.py`.

> Importante: estas queries son los **datasets base** para construir métricas en Pandas. Por eso no deben usar `LIMIT` en la versión que alimenta el cálculo de KPIs. El parámetro `limit` solo debe aplicarse después, sobre los DataFrames, para construir muestras en `details`.

### 1. Emails resumidos por AI

#### Reglas de filtro

| Caso | Condición |
|---|---|
| Con `po_name` | `po.po_name = %s` |
| Sin `po_name` | `poe.email_date::date BETWEEN %s AND %s` |

#### Query base corregida

```sql
SELECT
    po.po_name,
    po.po_status,
    po.vendor_name,
    po.last_expediting_date,
    po.receive_by,
    po.new_receive_by,
    poe.email_id,
    poe.email_date,
    poe.subject,
    poe.email_type
FROM walle.purchaseorder po
INNER JOIN walle.purchaseorder_email poe
    ON po.po_id = poe.po_id
INNER JOIN walle.model_email_logistics_summary emls
    ON poe.email_id = emls.email_id
WHERE {dynamic_where}
ORDER BY poe.email_date NULLS LAST;
```

#### Construcción dinámica esperada

```python
if po_name:
    dynamic_where = "po.po_name = %s"
    params = [po_name]
else:
    dynamic_where = "poe.email_date::date BETWEEN %s AND %s"
    params = [initial_date, final_date]
```

### 2. Emails analizados por AI

#### Reglas de filtro

| Caso | Condición |
|---|---|
| Con `po_name` | `po.po_name = %s` |
| Sin `po_name` | `mea."createdAt"::date BETWEEN %s AND %s` |

#### Query base corregida

```sql
SELECT
    po.po_name,
    mea.id,
    mea.email_id,
    mea.version,
    mea.scenario,
    mea.confidence,
    mea.initial_info,
    mea.db_updates,
    mea.extracted,
    mea.notes,
    mea."createdAt",
    mea.llm_meta,
    mea.actions,
    mea.flags,
    mea.evidence
FROM walle.purchaseorder po
INNER JOIN walle.purchaseorder_email poe
    ON po.po_id = poe.po_id
INNER JOIN walle.model_expediting_analysis mea
    ON poe.email_id = mea.email_id
WHERE {dynamic_where}
ORDER BY mea."createdAt" NULLS LAST;
```

#### Construcción dinámica esperada

```python
if po_name:
    dynamic_where = "po.po_name = %s"
    params = [po_name]
else:
    dynamic_where = 'mea."createdAt"::date BETWEEN %s AND %s'
    params = [initial_date, final_date]
```

### 3. Acciones sugeridas por AI

#### Reglas de filtro

| Caso | Condición |
|---|---|
| Con `po_name` | `po.po_name = %s` |
| Sin `po_name` | `eas."createdAt"::date BETWEEN %s AND %s` |

#### Query base corregida

```sql
SELECT
    eas.po_id,
    po.po_name,
    eas.actions,
    eas."createdAt",
    eas."updatedAt"
FROM walle.expediting_action_suggestions eas
LEFT JOIN walle.purchaseorder po
    ON po.po_id = eas.po_id
WHERE {dynamic_where}
ORDER BY eas."createdAt" NULLS LAST;
```

#### Construcción dinámica esperada

```python
if po_name:
    dynamic_where = "po.po_name = %s"
    params = [po_name]
else:
    dynamic_where = 'eas."createdAt"::date BETWEEN %s AND %s'
    params = [initial_date, final_date]
```

### 4. Uso de Walle

Esta query no recibe `po_name`. Siempre debe filtrar por rango de fechas.

#### Query base corregida

```sql
SELECT
    el.method,
    el.endpoint,
    el.status_code,
    el.duration_ms,
    el."createdAt"
FROM walle.event_log el
WHERE el.method = 'POST'
  AND el."createdAt"::date BETWEEN %s AND %s
ORDER BY el."createdAt" DESC;
```

#### Parámetros esperados

```python
params = [initial_date, final_date]
```

## Construcción de métricas con Pandas

Los KPIs deben calcularse en `features/operations/domain/walle_usage.py`, no con queries agregadas SQL en la implementación inicial.

### Flujo esperado

1. El use case ejecuta las 4 queries base.
2. Convierte cada resultado a un DataFrame.
3. Guarda cada dataset como referencia para detalle.
4. Envía los DataFrames al domain.
5. El domain retorna el objeto `summary` con las métricas.
6. El use case genera muestras limitadas con `df.head(limit)`.

### Firma sugerida del domain

```python
def build_walle_usage_metrics(
    summarized_emails_df: pd.DataFrame,
    analyzed_emails_df: pd.DataFrame,
    action_suggestions_df: pd.DataFrame,
    event_log_df: pd.DataFrame,
) -> Dict[str, Any]:
```

### Responsabilidad del domain

| Bloque | DataFrame fuente | Métricas |
|---|---|---|
| `overview` | Todos | POs procesadas, emails resumidos, emails analizados, acciones sugeridas, eventos de Walle. |
| `email_summary` | `summarized_emails_df` | Emails por tipo, vendors, estados de PO, última fecha de email. |
| `email_analysis` | `analyzed_emails_df` | Promedio de confidence, distribución por scenario, versiones, flags/actions si son parseables. |
| `actions` | `action_suggestions_df` | Acciones por PO, registros de sugerencias, última creación/actualización. |
| `api_usage` | `event_log_df` | Uso por endpoint, status codes, error rate, duración promedio, p50, p95 y máxima. |

### Ejemplos de cálculo en Pandas

```python
processed_po_count = int(
    pd.concat([
        summarized_emails_df.get("po_name", pd.Series(dtype="object")),
        analyzed_emails_df.get("po_name", pd.Series(dtype="object")),
        action_suggestions_df.get("po_name", pd.Series(dtype="object")),
    ]).dropna().nunique()
)

summarized_email_count = int(summarized_emails_df["email_id"].nunique()) if "email_id" in summarized_emails_df else 0
analyzed_email_count = int(analyzed_emails_df["email_id"].nunique()) if "email_id" in analyzed_emails_df else 0

endpoint_usage = (
    event_log_df.groupby("endpoint", dropna=False)
    .agg(
        event_count=("endpoint", "size"),
        avg_duration_ms=("duration_ms", "mean"),
        max_duration_ms=("duration_ms", "max"),
    )
    .reset_index()
)
```

> Si en el futuro el volumen de datos crece demasiado, se puede agregar una optimización con queries agregadas SQL. Esa optimización queda fuera del alcance inicial porque la regla actual es construir métricas en Pandas desde los datasets originales.

## Métricas esperadas

La respuesta debe agrupar los resultados en bloques fáciles de consumir.

```json
{
  "overview": {
    "processed_po_count": 0,
    "summarized_email_count": 0,
    "analyzed_email_count": 0,
    "suggested_action_records": 0,
    "walle_event_count": 0
  },
  "email_summary": {
    "emails_by_type": [],
    "latest_email_date": null
  },
  "email_analysis": {
    "avg_confidence": 0,
    "scenario_distribution": [],
    "version_distribution": []
  },
  "actions": {
    "actions_by_po": [],
    "total_action_records": 0
  },
  "api_usage": {
    "events_by_endpoint": [],
    "status_code_distribution": [],
    "duration_ms": {
      "avg": 0,
      "p50": 0,
      "p95": 0,
      "max": 0
    },
    "error_count": 0,
    "error_rate_pct": 0
  }
}
```

## Contrato de respuesta

La tool debe usar el envelope estándar del proyecto:

```python
build_tool_response(
    tool_name="get_walle_usage_metrics",
    summary=summary,
    filters={
        "initial_date": initial_date,
        "final_date": final_date,
        "po_name": po_name,
        "limit": limit,
    },
    source_systems=["postgresql"],
    columns=[],
    rows=[],
    dataset_reference=dataset_reference,
    details=details,
)
```

El `dataset_reference` principal puede apuntar a un archivo consolidado con los cuatro datasets crudos. Adicionalmente, `details.dataset_references` debe identificar cada dataset individual para facilitar solicitudes posteriores de detalle.

`details` puede incluir referencias de dataset y muestras limitadas:

```json
{
  "dataset_references": {
    "summarized_emails": "walle_summarized_emails_data.json",
    "analyzed_emails": "walle_analyzed_emails_data.json",
    "action_suggestions": "walle_action_suggestions_data.json",
    "event_log": "walle_event_log_data.json"
  },
  "summarized_emails_sample": [],
  "analyzed_emails_sample": [],
  "action_suggestions_sample": [],
  "event_log_sample": []
}
```

## Requerimientos funcionales

- [ ] La tool debe ejecutar los cuatro bloques de consulta de Walle.
- [ ] La tool debe aceptar `po_name`, `initial_date`, `final_date` y `limit`.
- [ ] Las cuatro queries originales deben conservarse como datasets de detalle mediante `dataset_reference`.
- [ ] Si llega `po_name`, las queries por PO deben filtrar solo por `po.po_name = %s`.
- [ ] Si llega `po_name`, las queries por PO no deben incluir condiciones de fecha.
- [ ] Si no llega `po_name`, las queries por PO deben usar el rango de fechas definido para cada tabla.
- [ ] La query de uso de Walle debe filtrar siempre por `el."createdAt"::date BETWEEN %s AND %s`.
- [ ] Los valores del usuario deben pasarse siempre como parámetros, no interpolados.
- [ ] Los KPIs deben calcularse en `features/operations/domain/walle_usage.py` usando Pandas.
- [ ] Los KPIs deben calcularse con los DataFrames completos retornados por las queries base, no con muestras limitadas.
- [ ] `limit` debe aplicarse solo para muestras en `details`, no para el dataset usado por Pandas.

## Requerimientos no funcionales

- [ ] Validar formato de fechas `YYYY-MM-DD`.
- [ ] Validar que `initial_date <= final_date` cuando se usen fechas.
- [ ] Normalizar `po_name` con `strip()`.
- [ ] Limitar `limit` a un máximo seguro.
- [ ] Manejar resultados vacíos sin error.
- [ ] Retornar errores controlados si PostgreSQL falla.
- [ ] Mantener las queries en funciones pequeñas y testeables.
- [ ] Mantener las funciones de métricas en domain puras y testeables con DataFrames sintéticos.

## Tests esperados

### Query builders

- [ ] Con `po_name`, la query de emails resumidos contiene `po.po_name = %s`.
- [ ] Con `po_name`, la query de emails resumidos no contiene `BETWEEN`.
- [ ] Sin `po_name`, la query de emails resumidos contiene `poe.email_date::date BETWEEN %s AND %s`.
- [ ] Con `po_name`, la query de emails analizados contiene `po.po_name = %s`.
- [ ] Con `po_name`, la query de emails analizados no contiene `mea."createdAt"::date BETWEEN`.
- [ ] Sin `po_name`, la query de emails analizados contiene `mea."createdAt"::date BETWEEN %s AND %s`.
- [ ] Con `po_name`, la query de acciones contiene `po.po_name = %s`.
- [ ] Con `po_name`, la query de acciones no contiene `eas."createdAt"::date BETWEEN`.
- [ ] Sin `po_name`, la query de acciones contiene `eas."createdAt"::date BETWEEN %s AND %s`.
- [ ] La query de uso de Walle siempre contiene `el."createdAt"::date BETWEEN %s AND %s`.
- [ ] Las queries base usadas para métricas no contienen `LIMIT`.

### Use case

- [ ] Aplica fechas por defecto cuando no llega `po_name`.
- [ ] No exige fechas para las queries por PO cuando llega `po_name`.
- [ ] Sigue ejecutando uso de Walle con fechas aunque llegue `po_name`.
- [ ] Retorna `kpi_metrics` con los bloques `overview`, `email_summary`, `email_analysis`, `actions` y `api_usage`.
- [ ] Guarda referencias a los cuatro datasets crudos.

### Domain Pandas

- [ ] Calcula `processed_po_count` usando `po_name` único disponible en los DataFrames.
- [ ] Calcula `summarized_email_count` usando `email_id` único del dataset de emails resumidos.
- [ ] Calcula `analyzed_email_count` usando `email_id` único del dataset de emails analizados.
- [ ] Calcula distribuciones de `email_type`, `scenario`, `version`, `endpoint` y `status_code` con Pandas.
- [ ] Calcula duración promedio, p50, p95 y máxima desde `event_log_df.duration_ms`.
- [ ] Maneja DataFrames vacíos sin lanzar error.

## Fuera de alcance inicial

- Escritura o modificación de datos en Walle.
- Filtros por endpoint, status code, vendor o email type.
- Exportación a Excel.
- Un dashboard visual.
