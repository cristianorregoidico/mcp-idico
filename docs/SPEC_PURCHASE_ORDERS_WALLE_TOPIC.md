# Spec: Topic `walle` para `get_purchase_orders`

Esta especificación define una extensión del análisis de Purchase Orders para incorporar métricas operativas de Walle dentro de la tool existente `get_purchase_orders`, sin modificar el comportamiento actual de los topics `vendors` e `items`.

## Decisión principal

La implementación será una extensión controlada de la tool actual:

1. **Se mantiene una sola tool pública**: `get_purchase_orders`.
2. **Se agrega un nuevo topic**: `walle`.
3. **La query SQL actual no se modifica** para `vendors` e `items`.
4. **`topic="walle"` usa una query PostgreSQL nueva e independiente**.
5. **La nueva query debe trabajar a nivel `po_id`**, no a nivel ítem.
6. **Las métricas operativas se calculan con agregados por PO**, evitando duplicaciones por joins con líneas de ítems.
7. **La capa domain consolida KPIs y tablas de detalle** a partir del dataset de POs enriquecido.

## Alcance

Este requerimiento agrega análisis operativo sobre Purchase Orders usando estas tablas de PostgreSQL:

- `ods.walle.purchaseorder`
- `ods.walle.purchaseorder_email`
- `ods.walle.expediting_action_suggestions`
- `ods.walle.finanzas_items`

## Objetivo funcional

El topic `walle` debe responder, por PO y en forma agregada:

- Si hubo actividad operativa sobre la PO.
- Si hubo procesamiento por IA sobre emails.
- Si hubo sugerencias de gestión y si fueron procesadas.
- Si hubo gestión financiera asociada.
- En qué etapa operativa quedó la PO.

## Regla crítica de negocio

### Procesamiento de sugerencias

Para `ods.walle.expediting_action_suggestions` se debe usar esta regla:

| Valor de `manual` | Interpretación |
|---|---|
| `NULL` | Sugerencia no procesada |
| `TRUE` | Sugerencia procesada |
| `FALSE` | Sugerencia procesada |

Por lo tanto:

- **Sugerencia procesada** = `manual IS NOT NULL`
- **Sugerencia no procesada** = `manual IS NULL`

> Esta regla reemplaza cualquier interpretación previa donde solo `manual = true` significaba ejecución.

## Compatibilidad hacia atrás

Debe preservarse el comportamiento actual:

| Topic | Fuente SQL | Cambio requerido |
|---|---|---|
| `vendors` | Query actual de purchase orders | Ninguno |
| `items` | Query actual de purchase orders | Ninguno |
| `walle` | Nueva query PostgreSQL agregada por `po_id` | Nuevo |

## Ubicación esperada

| Responsabilidad | Archivo / módulo |
|---|---|
| Tool MCP pública | `features/operations/tools.py` |
| Caso de uso | `features/operations/use_cases/purchase_orders.py` |
| Query actual de `vendors/items` | `features/operations/queries/purchase_orders.py` |
| Nueva query de `topic="walle"` | `features/operations/queries/purchase_orders.py` |
| Lógica de análisis | `features/operations/domain/purchase_orders.py` |
| Tests | `tests/` |

## Firma pública esperada

La firma pública no cambia; solo se amplían los valores válidos de `topic`.

```python
def get_purchase_orders(
    initial_date: Optional[str] = None,
    final_date: Optional[str] = None,
    vendor: Optional[str] = None,
    status: Optional[str] = None,
    brand: Optional[str] = None,
    topic: Optional[str] = "vendors",
) -> Dict[str, Any]:
```

## Parámetros

| Parámetro | Requerido | Default | Descripción |
|---|---:|---|---|
| `initial_date` | No | Inicio del mes actual | Fecha inicial del análisis. |
| `final_date` | No | Fecha actual | Fecha final del análisis. |
| `vendor` | No | `None` | Filtro parcial por vendor. |
| `status` | No | `None` | Filtro exacto por estado de la PO. |
| `brand` | No | `None` | Filtro parcial por marca. |
| `topic` | No | `vendors` | Valores válidos: `vendors`, `items`, `walle`. |

## Resolución de fechas

La resolución de fechas debe mantenerse igual que hoy:

```python
if not initial_date or not final_date:
    initial_date, final_date = get_month_start_and_today()
```

## Requerimientos de la query `walle`

### Reglas obligatorias

- Debe ser una función nueva, separada de la query actual.
- Debe usar placeholders `%s` de `psycopg`.
- Debe construirse con filtros dinámicos controlados por código.
- No debe interpolar valores del usuario en el SQL.
- Debe trabajar con una fila final por `po_id`.
- Debe usar `LEFT JOIN` sobre agregados por PO.
- Debe evitar cualquier join directo entre:
  - ítems de la PO,
  - emails,
  - sugerencias,
  - finanzas.

### Filtros esperados

La nueva query debe respetar los mismos filtros funcionales actuales:

- rango de fechas por `po.po_date`
- `vendor`
- `status`
- `brand`

### Regla de marca (`brand`)

Como `brand` vive a nivel de ítem y el topic `walle` trabaja a nivel PO, el filtro por marca debe resolverse sin romper la granularidad. Se acepta cualquiera de estas implementaciones seguras:

1. `EXISTS` sobre `purchaseorder_items`
2. CTE previa con `DISTINCT po_id` filtrada por marca

No se permite volver la query principal al nivel línea para resolver `brand`.

## Estructura lógica esperada de la query

La implementación debe organizarse con CTEs similares a estas:

### 1. `po_base`

POs filtradas del período, una fila por `po_id`.

Campos mínimos sugeridos:

- `po_id`
- `po_number`
- `created_date`
- `po_date`
- `status`
- `approval_status`
- `expediting_status`
- `payment_status`
- `vendor`
- `vendor_country`
- `subsidiary`
- `amount_usd`
- `receive_by`
- `new_receive_by`
- `last_est_delivery_date_informed`

### 2. `email_agg`

Agregado por `po_id` desde `purchaseorder_email`.

Campos requeridos:

- `email_count`
- `inbound_email_count`
- `outbound_email_count`
- `ai_processed_email_count`
- `first_email_date`
- `last_email_date`

### 3. `suggestion_agg`

Agregado por `po_id` desde `expediting_action_suggestions`.

Campos requeridos:

- `suggestion_row_count`
- `processed_suggestion_count` (`manual IS NOT NULL`)
- `unprocessed_suggestion_count` (`manual IS NULL`)
- `suggestion_true_count`
- `suggestion_false_count`
- `total_actions_suggested`
- `avg_actions_per_suggestion`
- `first_suggestion_at`
- `last_suggestion_at`

### 4. `finance_agg`

Agregado por `po_id` desde `finanzas_items`.

Campos requeridos:

- `finance_request_count`
- `finance_pending_count`
- `finance_completed_count`
- `finance_cancelled_count`
- `finance_executed_count` (`jsonb_array_length(payment_files) > 0`)
- `finance_with_support_files_count`
- `finance_rejection_event_count`
- `finance_anulation_event_count`
- `anticipo_count`
- `gasto_importacion_count`
- `first_finance_created_at`
- `last_finance_updated_at`

## Dataset final esperado por PO

La query `walle` debe devolver al menos estos campos por PO:

### Identidad y contexto

- `po_id`
- `po_number`
- `po_date`
- `status`
- `vendor`
- `amount_usd`

### Actividad de emails

- `email_count`
- `inbound_email_count`
- `outbound_email_count`
- `ai_processed_email_count`
- `first_email_date`
- `last_email_date`

### Gestión sugerida

- `suggestion_row_count`
- `processed_suggestion_count`
- `unprocessed_suggestion_count`
- `suggestion_true_count`
- `suggestion_false_count`
- `total_actions_suggested`
- `avg_actions_per_suggestion`
- `last_suggestion_at`

### Gestión financiera

- `finance_request_count`
- `finance_pending_count`
- `finance_completed_count`
- `finance_cancelled_count`
- `finance_executed_count`
- `finance_rejection_event_count`
- `finance_anulation_event_count`
- `anticipo_count`
- `gasto_importacion_count`
- `last_finance_updated_at`

## Métricas derivadas obligatorias en `domain`

La capa `domain` debe calcular y devolver al menos:

### Flags por PO

- `has_email_activity`
- `has_ai_email_processing`
- `has_suggestions`
- `has_processed_suggestions`
- `has_unprocessed_suggestions`
- `has_finance_activity`
- `has_pending_finance`
- `has_finance_exception`

### Ratios por PO

- `ai_processed_email_ratio`
- `suggestion_processing_rate`
- `finance_execution_rate`

### Aging por PO

- `days_since_last_email`
- `days_since_last_suggestion`
- `days_since_last_finance_update`

### Consolidado por PO

- `operational_touchpoints`
- `workflow_stage`

## Regla para `workflow_stage`

La etapa debe derivarse con esta prioridad:

1. `finance_pending`
2. `finance_exception`
3. `finance_completed`
4. `suggestions_processed`
5. `suggestions_pending_review`
6. `ai_email_processed`
7. `email_activity_only`
8. `no_activity`

### Definiciones sugeridas

| Stage | Condición |
|---|---|
| `finance_pending` | `finance_pending_count > 0` |
| `finance_exception` | `finance_rejection_event_count > 0 OR finance_anulation_event_count > 0` |
| `finance_completed` | `finance_completed_count > 0 OR finance_executed_count > 0` |
| `suggestions_processed` | `processed_suggestion_count > 0` |
| `suggestions_pending_review` | `unprocessed_suggestion_count > 0` |
| `ai_email_processed` | `ai_processed_email_count > 0` |
| `email_activity_only` | `email_count > 0` |
| `no_activity` | sin actividad operativa |

## Salida esperada en el response envelope

La tool debe seguir usando el envelope estándar del proyecto.

### `kpi_metrics`

Debe incluir un resumen ejecutivo del topic `walle`, por ejemplo:

- `total_purchase_orders`
- `purchase_orders_with_email_activity`
- `purchase_orders_with_ai_processed_emails`
- `purchase_orders_with_suggestions`
- `purchase_orders_with_processed_suggestions`
- `purchase_orders_with_finance_requests`
- `purchase_orders_with_pending_finance`
- `purchase_orders_with_finance_executed`
- `avg_emails_per_po`
- `avg_suggestions_per_po`
- `avg_finance_requests_per_po`
- `workflow_stage_breakdown`

### `details`

Debe incluir tablas útiles para inspección, como:

- `top_purchase_orders_by_email_volume`
- `top_purchase_orders_by_suggestion_volume`
- `top_purchase_orders_with_pending_finance`
- `top_purchase_orders_with_finance_exceptions`
- `purchase_orders_without_operational_activity`

## Casos especiales y decisiones de modelado

### `finanzas_items.status`

La especificación debe modelarse según los estados observados actualmente:

- `PENDING`
- `COMPLETED`
- `CANCELLED`

Los eventos de rechazo o anulación no deben inferirse solo por `status`; también deben leerse desde:

- `razon_rejection`
- `razon_anulation`

### `payment_files`

Debe tratarse como señal de ejecución financiera:

- ejecutada si `jsonb_array_length(payment_files) > 0`

## Tests requeridos

Se deben agregar tests para cubrir:

1. **Resolución de topic**: `walle` dispara la nueva query y el nuevo builder.
2. **Compatibilidad**: `vendors` e `items` siguen usando la query actual.
3. **Parámetros SQL**: uso correcto de `%s` y filtros dinámicos.
4. **Filtro de brand**: no rompe la granularidad por PO.
5. **Regla `manual IS NOT NULL`**: cuenta sugerencias procesadas correctamente.
6. **Envelope contract**: el topic `walle` mantiene la estructura estándar.
7. **Workflow stage**: prioridad correcta de clasificación.

## Fuera de alcance

Queda fuera de esta iteración:

- agregar nuevos parámetros públicos específicos de `walle`
- cambiar la firma pública de la tool
- reemplazar los topics actuales
- reescribir el análisis actual de `vendors` e `items`
- construir dashboards o endpoints separados
