# Implementación: topic `walle` en `get_purchase_orders`

Este documento baja a nivel de implementación la extensión de `get_purchase_orders` para soportar `topic="walle"` con métricas operativas de Walle, sin tocar la query existente de `vendors/items`.

## Quick path

1. Agregar una **query PostgreSQL nueva** para `topic="walle"`.
2. Agregar un **builder domain nuevo** para KPIs operativos.
3. Enrutar desde el use case según `topic`.
4. Mantener intacto el flujo actual de `vendors/items`.
5. Agregar tests de compatibilidad, query y envelope.

## Decisiones cerradas

| Tema | Decisión |
|---|---|
| Tool pública | Se mantiene `get_purchase_orders` |
| Nombre del nuevo topic | `walle` |
| Query actual | No se modifica |
| Nueva fuente operativa | Query PG separada |
| Granularidad | Una fila final por `po_id` |
| Regla de sugerencia procesada | `manual IS NOT NULL` |
| Cálculo de KPIs | Capa `domain` con Pandas |

## Archivos a tocar

| Archivo | Cambio |
|---|---|
| `features/operations/queries/purchase_orders.py` | Nueva función SQL para `topic="walle"` |
| `features/operations/domain/purchase_orders.py` | Nuevo builder de análisis `walle` |
| `features/operations/use_cases/purchase_orders.py` | Branch nuevo por topic |
| `tests/test_purchase_orders_analysis.py` | Nuevos tests de selección de query y builder |
| `tests/test_query_parameterization.py` | Tests de SQL parametrizada |
| `tests/test_response_envelope_contract.py` | Validación del envelope para `topic="walle"` |

## Nombres propuestos

### Query layer

```python
def get_purchase_orders_walle_query_pg(
    initial_date: str,
    final_date: str,
    vendor: str | None = None,
    status: str | None = None,
    brand: str | None = None,
) -> tuple[str, list[Any]]:
```

### Domain layer

```python
def build_walle_analysis(df: pd.DataFrame) -> dict[str, Any]:
```

### Use case imports

```python
from features.operations.domain.purchase_orders import (
    build_items_analysis,
    build_vendors_analysis,
    build_walle_analysis,
    normalize_topic,
)

from features.operations.queries.purchase_orders import (
    get_purchase_orders_query_pg,
    get_purchase_orders_walle_query_pg,
)
```

## Ajuste mínimo en `normalize_topic`

El normalizador actual debe aceptar también:

```python
{"vendors", "items", "walle"}
```

Si llega otro valor, debe mantener la estrategia actual de validación o fallback explícito del módulo.

## Flujo del use case

## Happy path

```python
def execute(..., topic: Optional[str] = "vendors") -> Dict[str, Any]:
    if not initial_date or not final_date:
        initial_date, final_date = get_month_start_and_today()

    normalized_topic = normalize_topic(topic)
    normalized_vendor = (vendor or "").strip() or None
    normalized_status = (status or "").strip() or None
    normalized_brand = (brand or "").strip() or None

    if normalized_status and normalized_status not in ALLOWED_STATUS:
        raise ValueError(...)

    if normalized_topic == "walle":
        sql, params = get_purchase_orders_walle_query_pg(
            initial_date=initial_date,
            final_date=final_date,
            vendor=normalized_vendor,
            status=normalized_status,
            brand=normalized_brand,
        )
    else:
        sql, params = get_purchase_orders_query_pg(
            initial_date=initial_date,
            final_date=final_date,
            vendor=normalized_vendor,
            status=normalized_status,
            brand=normalized_brand,
        )

    columns, rows = execute_pg_query(sql, params)
    dataset_reference = save_result_to_json(...)
    df = tuple_to_dataframe(columns, rows)

    if normalized_topic == "items":
        summary = build_items_analysis(df)
    elif normalized_topic == "walle":
        summary = build_walle_analysis(df)
    else:
        summary = build_vendors_analysis(df)

    summary.pop("full_data_reference", None)

    return build_tool_response(...)
```

## Query `walle`: shape recomendado

## Idea central

NO reutilizar la query actual de ítems. El topic `walle` tiene otra granularidad: PO + workflow operativo.

## Estructura sugerida

```sql
WITH po_base AS (
    SELECT
        po.po_id,
        po.po_name AS po_number,
        po.created_date::date,
        po.po_date,
        po.po_status AS status,
        po.approval_status,
        po.expediting_status,
        po.payment_status,
        po.vendor_name AS vendor,
        po.vendor_country,
        po.subsidiary,
        po.amount_usd,
        po.receive_by,
        po.new_receive_by,
        po.last_est_delivery_date_informed
    FROM ods.walle.purchaseorder po
    WHERE po.po_date BETWEEN %s::date AND %s::date
      AND po.po_status NOT IN ('Undefined', 'Closed', 'Planned')
      -- vendor/status/brand dinámicos
),
brand_filter AS (
    SELECT DISTINCT poi.po_id
    FROM ods.walle.purchaseorder_items poi
    WHERE poi.brand ILIKE %s
),
email_agg AS (
    SELECT
        poe.po_id,
        COUNT(*) AS email_count,
        COUNT(*) FILTER (WHERE poe.email_type = 'INBOUND') AS inbound_email_count,
        COUNT(*) FILTER (WHERE poe.email_type = 'OUTBOUND') AS outbound_email_count,
        COUNT(*) FILTER (WHERE poe.summarized IS TRUE) AS ai_processed_email_count,
        MIN(poe.email_date) AS first_email_date,
        MAX(poe.email_date) AS last_email_date
    FROM ods.walle.purchaseorder_email poe
    GROUP BY poe.po_id
),
suggestion_agg AS (
    SELECT
        eas.po_id,
        COUNT(*) AS suggestion_row_count,
        COUNT(*) FILTER (WHERE eas.manual IS NOT NULL) AS processed_suggestion_count,
        COUNT(*) FILTER (WHERE eas.manual IS NULL) AS unprocessed_suggestion_count,
        COUNT(*) FILTER (WHERE eas.manual IS TRUE) AS suggestion_true_count,
        COUNT(*) FILTER (WHERE eas.manual IS FALSE) AS suggestion_false_count,
        COALESCE(SUM(jsonb_array_length(eas.actions)), 0) AS total_actions_suggested,
        AVG(jsonb_array_length(eas.actions)::numeric) AS avg_actions_per_suggestion,
        MIN(eas."createdAt") AS first_suggestion_at,
        MAX(eas."createdAt") AS last_suggestion_at
    FROM ods.walle.expediting_action_suggestions eas
    GROUP BY eas.po_id
),
finance_agg AS (
    SELECT
        fi.po_id,
        COUNT(*) AS finance_request_count,
        COUNT(*) FILTER (WHERE fi.status = 'PENDING') AS finance_pending_count,
        COUNT(*) FILTER (WHERE fi.status = 'COMPLETED') AS finance_completed_count,
        COUNT(*) FILTER (WHERE fi.status = 'CANCELLED') AS finance_cancelled_count,
        COUNT(*) FILTER (WHERE jsonb_array_length(fi.payment_files) > 0) AS finance_executed_count,
        COUNT(*) FILTER (WHERE jsonb_array_length(fi.files) > 0) AS finance_with_support_files_count,
        COUNT(*) FILTER (WHERE fi.razon_rejection IS NOT NULL) AS finance_rejection_event_count,
        COUNT(*) FILTER (WHERE fi.razon_anulation IS NOT NULL) AS finance_anulation_event_count,
        COUNT(*) FILTER (WHERE fi.type = 'ANTICIPO') AS anticipo_count,
        COUNT(*) FILTER (WHERE fi.type = 'GASTO_IMPORTACION') AS gasto_importacion_count,
        MIN(fi."createdAt") AS first_finance_created_at,
        MAX(fi."updatedAt") AS last_finance_updated_at
    FROM ods.walle.finanzas_items fi
    GROUP BY fi.po_id
)
SELECT
    pb.po_id,
    pb.po_number,
    pb.created_date,
    pb.po_date,
    pb.status,
    pb.approval_status,
    pb.expediting_status,
    pb.payment_status,
    pb.vendor,
    pb.vendor_country,
    pb.subsidiary,
    pb.amount_usd,
    pb.receive_by,
    pb.new_receive_by,
    pb.last_est_delivery_date_informed,
    COALESCE(ea.email_count, 0) AS email_count,
    COALESCE(ea.inbound_email_count, 0) AS inbound_email_count,
    COALESCE(ea.outbound_email_count, 0) AS outbound_email_count,
    COALESCE(ea.ai_processed_email_count, 0) AS ai_processed_email_count,
    ea.first_email_date,
    ea.last_email_date,
    COALESCE(sa.suggestion_row_count, 0) AS suggestion_row_count,
    COALESCE(sa.processed_suggestion_count, 0) AS processed_suggestion_count,
    COALESCE(sa.unprocessed_suggestion_count, 0) AS unprocessed_suggestion_count,
    COALESCE(sa.suggestion_true_count, 0) AS suggestion_true_count,
    COALESCE(sa.suggestion_false_count, 0) AS suggestion_false_count,
    COALESCE(sa.total_actions_suggested, 0) AS total_actions_suggested,
    sa.avg_actions_per_suggestion,
    sa.first_suggestion_at,
    sa.last_suggestion_at,
    COALESCE(fa.finance_request_count, 0) AS finance_request_count,
    COALESCE(fa.finance_pending_count, 0) AS finance_pending_count,
    COALESCE(fa.finance_completed_count, 0) AS finance_completed_count,
    COALESCE(fa.finance_cancelled_count, 0) AS finance_cancelled_count,
    COALESCE(fa.finance_executed_count, 0) AS finance_executed_count,
    COALESCE(fa.finance_with_support_files_count, 0) AS finance_with_support_files_count,
    COALESCE(fa.finance_rejection_event_count, 0) AS finance_rejection_event_count,
    COALESCE(fa.finance_anulation_event_count, 0) AS finance_anulation_event_count,
    COALESCE(fa.anticipo_count, 0) AS anticipo_count,
    COALESCE(fa.gasto_importacion_count, 0) AS gasto_importacion_count,
    fa.first_finance_created_at,
    fa.last_finance_updated_at
FROM po_base pb
LEFT JOIN email_agg ea ON ea.po_id = pb.po_id
LEFT JOIN suggestion_agg sa ON sa.po_id = pb.po_id
LEFT JOIN finance_agg fa ON fa.po_id = pb.po_id
-- JOIN/EXISTS de brand_filter solo si brand viene con valor
ORDER BY pb.po_date DESC, pb.po_number;
```

## Construcción dinámica del `WHERE`

### Base filters

```python
where_clauses = [
    "po.po_date BETWEEN %s::date AND %s::date",
    "po.po_status NOT IN ('Undefined', 'Closed', 'Planned')",
]
params = [initial_date, final_date]

if vendor:
    where_clauses.append("po.vendor_name ILIKE %s")
    params.append(f"%{vendor}%")

if status:
    where_clauses.append("po.po_status = %s")
    params.append(status)
```

### `brand` sin romper granularidad

Implementación recomendada:

```python
brand_join = ""

if brand:
    brand_join = "INNER JOIN brand_filter bf ON bf.po_id = po.po_id"
    brand_params = [f"%{brand}%"]
else:
    brand_params = []
```

El orden final de parámetros debe respetar el orden de aparición real en el SQL.

## `build_walle_analysis(df)`: contrato sugerido

## Entrada esperada

DataFrame con una fila por `po_id` y columnas ya agregadas por la query.

## Salida esperada

```python
{
    "overview": {...},
    "coverage": {...},
    "workflow_stage_breakdown": [...],
    "email_metrics": {...},
    "suggestion_metrics": {...},
    "finance_metrics": {...},
    "top_purchase_orders_by_email_volume": [...],
    "top_purchase_orders_by_suggestion_volume": [...],
    "top_purchase_orders_with_pending_finance": [...],
    "top_purchase_orders_with_finance_exceptions": [...],
    "purchase_orders_without_operational_activity": [...],
}
```

## Derivadas dentro del builder

### Ratios seguros

Todos los ratios deben manejar división por cero.

```python
def safe_ratio(numerator: float, denominator: float) -> float:
    return 0.0 if not denominator else round(numerator / denominator, 4)
```

### Flags por PO

```python
df["has_email_activity"] = df["email_count"] > 0
df["has_ai_email_processing"] = df["ai_processed_email_count"] > 0
df["has_suggestions"] = df["suggestion_row_count"] > 0
df["has_processed_suggestions"] = df["processed_suggestion_count"] > 0
df["has_unprocessed_suggestions"] = df["unprocessed_suggestion_count"] > 0
df["has_finance_activity"] = df["finance_request_count"] > 0
df["has_pending_finance"] = df["finance_pending_count"] > 0
df["has_finance_exception"] = (
    (df["finance_rejection_event_count"] > 0)
    | (df["finance_anulation_event_count"] > 0)
)
```

### Touchpoints

```python
df["operational_touchpoints"] = (
    df["email_count"]
    + df["suggestion_row_count"]
    + df["finance_request_count"]
)
```

### Workflow stage

Implementarlo con prioridad explícita, no con `if` sueltos desordenados.

## `kpi_metrics`: shape recomendado

```python
{
    "total_purchase_orders": int,
    "purchase_orders_with_email_activity": int,
    "purchase_orders_with_ai_processed_emails": int,
    "purchase_orders_with_suggestions": int,
    "purchase_orders_with_processed_suggestions": int,
    "purchase_orders_with_finance_requests": int,
    "purchase_orders_with_pending_finance": int,
    "purchase_orders_with_finance_executed": int,
    "avg_emails_per_po": float,
    "avg_suggestions_per_po": float,
    "avg_finance_requests_per_po": float,
    "workflow_stage_breakdown": list[dict[str, Any]],
}
```

## `details`: shape recomendado

```python
{
    "top_purchase_orders_by_email_volume": list[dict[str, Any]],
    "top_purchase_orders_by_suggestion_volume": list[dict[str, Any]],
    "top_purchase_orders_with_pending_finance": list[dict[str, Any]],
    "top_purchase_orders_with_finance_exceptions": list[dict[str, Any]],
    "purchase_orders_without_operational_activity": list[dict[str, Any]],
}
```

## Testing plan

## Unit tests de query

- arma SQL base con `%s`
- agrega filtro `vendor`
- agrega filtro `status`
- resuelve `brand` sin pasar a granularidad de ítems
- mantiene orden correcto de parámetros

## Unit tests de use case

- `topic="walle"` usa `get_purchase_orders_walle_query_pg`
- `topic="vendors"` sigue usando `get_purchase_orders_query_pg`
- `topic="items"` sigue usando `get_purchase_orders_query_pg`
- `status` inválido sigue fallando antes de consultar

## Unit tests de domain

- cuenta sugerencias procesadas con `manual IS NOT NULL`
- clasifica `workflow_stage` con prioridad correcta
- calcula ratios sin dividir por cero
- detecta excepciones financieras por `razon_rejection` y `razon_anulation`

## Envelope contract

- el response mantiene `meta`, `kpi_metrics`, `artifacts`, `details`
- `meta.filters.topic` debe ser `walle`
- `source_systems` sigue siendo `postgresql`

## Riesgos y mitigación

| Riesgo | Mitigación |
|---|---|
| Duplicación de métricas por joins | Agregar todo por `po_id` antes del join final |
| Filtro `brand` rompe granularidad | Resolver con `EXISTS` o CTE `brand_filter` |
| Interpretar mal `manual` | Fijar regla de negocio: `manual IS NOT NULL` = procesada |
| Inferir rechazo solo por `status` | Leer también `razon_rejection` y `razon_anulation` |

## Checklist de implementación

- [ ] Aceptar `topic="walle"` en normalización de topic
- [ ] Crear query PG nueva separada
- [ ] Mantener query actual sin cambios funcionales
- [ ] Implementar builder `build_walle_analysis`
- [ ] Rutar en el use case según topic
- [ ] Guardar dataset de salida como hoy
- [ ] Agregar tests de query, use case, domain y envelope
