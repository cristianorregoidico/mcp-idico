# Spec: Purchase Orders Analysis Tool

Esta feature agrega una nueva tool MCP dentro de `features/operations` para consultar Purchase Orders de NetSuite y devolver análisis Pandas según dos topics: `vendors` e `items`.

## Decisión principal

La implementación será híbrida:

1. **Una sola tool pública**.
2. **Una sola query base de NetSuite**.
3. **Filtros SQL dinámicos**.
4. **Parámetros SQL con placeholders `?`**, no interpolación directa con f-strings.
5. **Dos topics de análisis**:
   - `vendors`
   - `items`

## Ubicación esperada

| Responsabilidad | Archivo / módulo |
|---|---|
| Tool MCP pública | `features/operations/tools.py` |
| Orquestación del caso de uso | `features/operations/use_cases/purchase_orders.py` |
| Query SQL parametrizada | `features/operations/queries/purchase_orders.py` |
| Análisis Pandas | `features/operations/domain/purchase_orders.py` |
| Tests | `tests/` |

> La query **no** debe vivir en `connections/netsuite/queries.py`. Para esta feature debe quedar bajo `features/operations/queries`.

## Firma de la tool

```python
def get_purchase_orders_analysis(
    initial_date: Optional[str] = None,
    final_date: Optional[str] = None,
    vendor: Optional[str] = None,
    status: Optional[str] = None,
    brand: Optional[str] = None,
    topic: str = "vendors",
) -> Dict[str, Any]:
```

## Parámetros

| Parámetro | Requerido | Default | Descripción |
|---|---:|---|---|
| `initial_date` | No | Inicio del mes actual | Fecha inicial para `t.trandate`. Formato `YYYY-MM-DD`. |
| `final_date` | No | Fecha actual | Fecha final para `t.trandate`. Formato `YYYY-MM-DD`. |
| `vendor` | No | `None` | Filtro parcial por nombre del vendor. |
| `status` | No | `None` | Filtro exacto o parcial por estado de la PO. |
| `brand` | No | `None` | Filtro parcial por marca del item. |
| `topic` | No | `vendors` | Define el análisis retornado. Valores: `vendors`, `items`. |

## Valores permitidos para `status`

La tool debe aceptar estos estados:

- `Pending Supervisor Approval`
- `Pending Receipt`
- `Rejected by Supervisor`
- `Partially Received`
- `Pending Billing/Partially Received`
- `Pending Bill`
- `Fully Billed`

Si llega un `status` fuera de esta lista, la implementación debe retornar un error controlado o una respuesta de validación clara antes de ejecutar la query.

## Resolución de fechas

La query siempre debe recibir `initial_date` y `final_date`.

Regla:

```python
if not initial_date or not final_date:
    initial_date, final_date = get_month_start_and_today()
```

Con esto, si el usuario no envía fechas, la consulta usa:

- `initial_date`: primer día del mes actual.
- `final_date`: fecha actual.

## Requerimientos de query SQL

### Reglas obligatorias

- La query debe estar en `features/operations/queries/purchase_orders.py`.
- La query debe construirse con filtros dinámicos.
- Los valores variables deben pasarse con placeholders `?`.
- No se deben interpolar valores del usuario dentro del SQL con f-strings.
- La query debe retornar `sql` y `params`.
- La única condición obligatoria de negocio debe ser el rango de fechas.
- Los filtros de `vendor`, `status` y `brand` solo deben agregarse al `WHERE` si vienen con valor.
- Debe eliminarse cualquier filtro temporal de desarrollo, por ejemplo:

```sql
AND t.id = 870647
```

### Forma esperada

```python
def get_purchase_orders_query(
    initial_date: str,
    final_date: str,
    vendor: str | None = None,
    status: str | None = None,
    brand: str | None = None,
) -> tuple[str, list[Any]]:
    where_clauses = [
        "t.type = 'PurchOrd'",
        "tl.mainline = 'F'",
        "tl.itemtype IN ('InvtPart', 'Service')",
        "TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN ? AND ?",
    ]
    params = [initial_date, final_date]

    if vendor:
        where_clauses.append("BUILTIN.DF(t.entity) LIKE '%' || ? || '%'")
        params.append(vendor)

    if status:
        where_clauses.append("ts.name LIKE '%' || ? || '%'")
        params.append(status)

    if brand:
        where_clauses.append("BUILTIN.DF(i.custitem13) LIKE '%' || ? || '%'")
        params.append(brand)

    sql = f"""
    SELECT
        ...
    FROM transaction t
    ...
    WHERE {' AND '.join(where_clauses)}
    ORDER BY
        t.trandate DESC,
        t.tranid,
        tl.id
    """

    return sql, params
```

> Nota: el f-string solo se permite para ensamblar fragmentos SQL controlados por el código, como `where_clauses`. No se permite para insertar valores del usuario.

## Query base

La query debe seleccionar estas columnas:

```sql
SELECT
    -- PO header
    t.id AS po_id,
    t.tranid AS po_number,
    TO_CHAR(t.createddate, 'YYYY-MM-DD') AS created_date,
    TO_CHAR(t.trandate, 'YYYY-MM-DD') AS po_date,
    TO_CHAR(t.trandate, 'YYYY-MM') AS po_period,
    TO_CHAR(t.duedate, 'YYYY-MM-DD') AS receive_by,
    TO_CHAR(t.custbody_evol_new_receive_by, 'YYYY-MM-DD') AS new_receive_by,
    TO_CHAR(t.custbody_evol_last_delivery_date, 'YYYY-MM-DD') AS last_est_delivery_date_informed,
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS today,
    ts.name AS status,
    BUILTIN.DF(t.approvalstatus) AS approval_status,
    BUILTIN.DF(t.custbody_evol_expediting_status) AS expediting_status,
    BUILTIN.DF(t.custbody13) AS payment_status,
    BUILTIN.DF(t.entity) AS vendor,
    BUILTIN.DF(t.terms) AS terms,
    BUILTIN.DF(t.custbody41) AS incoterms,
    BUILTIN.DF(ea.country) AS vendor_country,
    BUILTIN.DF(tl.subsidiary) AS subsidiary,
    BUILTIN.DF(t.currency) AS currency,
    -t.foreigntotal * t.exchangerate AS amount_usd,

    -- Sales Order
    so.tranid AS so_number,
    BUILTIN.DF(so.entity) AS customer,

    -- Line detail
    BUILTIN.DF(tl.item) AS item,
    SUBSTR(i.purchasedescription, 1, 100) AS item_description,
    tl.itemtype AS item_type,
    tl.quantity AS quantity,
    tl.quantityshiprecv AS quantity_received,
    tl.rate AS unit_rate,
    tl.foreignamount * t.exchangerate AS line_amount,

    -- Item classification
    BUILTIN.DF(i.custitem13) AS brand,
    BUILTIN.DF(i.class) AS product_group
FROM transaction t
INNER JOIN transactionline tl
    ON tl.transaction = t.id
LEFT JOIN transactionStatus ts
    ON ts.id = t.status
    AND ts.trantype = 'PurchOrd'
LEFT JOIN item i
    ON i.id = tl.item
LEFT JOIN entityAddressBook eab
    ON eab.entity = t.entity
    AND eab.defaultbilling = 'T'
LEFT JOIN EntityAddress ea
    ON ea.nkey = eab.addressbookaddress
LEFT JOIN PreviousTransactionLink po_to_so
    ON po_to_so.nextdoc = t.id
    AND po_to_so.linktype = 'SpecOrd'
LEFT JOIN transaction so
    ON so.id = po_to_so.previousdoc
    AND so.type = 'SalesOrd'
```

## Topic `vendors`

Este topic analiza la PO como documento operativo, financiero y de seguimiento.

### Regla de DataFrame

Como el dataset viene a nivel línea, para métricas de PO se debe deduplicar:

```python
df_po = df.drop_duplicates("po_id")
```

### Análisis requeridos

#### 1. Overview

- Total de purchase orders.
- Total de vendors.
- Total de clientes.
- Monto total USD.
- Monto promedio por PO.

#### 2. Estados

- Distribución por `status`.
- Distribución por `approval_status`.
- Distribución por `expediting_status`.
- Distribución por `payment_status`.
- Monto por `status`.
- Monto por `payment_status`.

#### 3. Vendors

- Top vendors por cantidad de POs.
- Top vendors por monto.
- Distribución por país del vendor.
- Monto por país del vendor.
- Distribución por `terms`.
- Distribución por `incoterms`.

#### 4. Subsidiarias

- Cantidad de POs por subsidiaria.
- Monto por subsidiaria.
- Estados por subsidiaria.

#### 5. Fechas y entregas

Campos derivados:

```python
effective_receive_by = new_receive_by if exists else receive_by
days_to_receive = effective_receive_by - today
is_overdue = days_to_receive < 0
```

Métricas:

- POs vencidas.
- POs que vencen esta semana.
- POs que vencen en los próximos 30 días.
- Promedio de días para recibir.
- Promedio de días vencidos.
- Monto vencido USD.
- POs vencidas por vendor.
- POs vencidas por país.
- Aging buckets:
  - `overdue_30_plus`
  - `overdue_15_30`
  - `overdue_1_14`
  - `due_0_7`
  - `due_8_30`
  - `due_31_plus`

#### 6. Reprogramaciones

Usar:

- `receive_by`
- `new_receive_by`
- `last_est_delivery_date_informed`

Métricas:

- Cantidad de POs reprogramadas.
- Porcentaje de POs reprogramadas.
- Promedio de días reprogramados.
- Monto USD reprogramado.
- Top vendors con más reprogramaciones.

#### 7. Tendencia mensual

- Cantidad de POs por mes.
- Monto USD por mes.

## Topic `items`

Este topic analiza la PO a nivel línea, con foco en marcas, grupos, recepción y pendientes.

### Regla de DataFrame

Usar el dataset completo:

```python
df_lines = df.copy()
```

### Análisis requeridos

#### 1. Overview

- Total de líneas.
- Total de marcas.
- Total de product groups.
- Monto total por línea.
- Cantidad total ordenada.
- Cantidad total recibida.
- Cantidad total pendiente.

#### 2. Recepción

Campos derivados:

```python
pending_quantity = quantity - quantity_received
received_pct = quantity_received / quantity
```

Clasificación:

- `not_received`
- `partially_received`
- `fully_received`

Métricas:

- Cantidad ordenada vs recibida vs pendiente.
- Porcentaje recibido global.
- Distribución por estado de recepción.
- Líneas pendientes.
- Líneas parcialmente recibidas.
- Líneas totalmente recibidas.

#### 3. Marcas

- Distribución por marca.
- Monto por marca.
- Cantidad ordenada por marca.
- Cantidad recibida por marca.
- Cantidad pendiente por marca.
- Porcentaje recibido por marca.

#### 4. Product groups

- Distribución por product group.
- Monto por product group.
- Cantidad ordenada por product group.
- Cantidad recibida por product group.
- Cantidad pendiente por product group.
- Porcentaje recibido por product group.

#### 5. Marcas por vendor

- Distribución de marcas por vendor.
- Monto por combinación vendor + marca.
- Cantidad pendiente por combinación vendor + marca.
- Porcentaje recibido por combinación vendor + marca.
- Top combinaciones vendor + marca por monto.
- Top combinaciones vendor + marca por cantidad pendiente.

#### 6. Marcas por cliente

- Distribución de marcas por cliente.
- Monto por combinación cliente + marca.
- Cantidad pendiente por combinación cliente + marca.
- Porcentaje recibido por combinación cliente + marca.
- Top combinaciones cliente + marca por monto.
- Top combinaciones cliente + marca por cantidad pendiente.

#### 7. Pendientes

- Monto pendiente.
- Cantidad pendiente por marca.
- Cantidad pendiente por product group.
- Cantidad pendiente por vendor.
- Cantidad pendiente por customer.
- Monto pendiente por marca.
- Monto pendiente por vendor.
- Monto pendiente por customer.

## Respuesta esperada

### Para `topic="vendors"`

```python
{
    "topic": "vendors",
    "overview": {...},
    "status_analysis": {...},
    "vendor_analysis": {...},
    "subsidiary_analysis": {...},
    "delivery_analysis": {...},
    "rescheduling_analysis": {...},
    "monthly_trend": {...}
}
```

### Para `topic="items"`

```python
{
    "topic": "items",
    "overview": {...},
    "receipt_analysis": {...},
    "brand_analysis": {...},
    "product_group_analysis": {...},
    "brand_vendor_analysis": {...},
    "brand_customer_analysis": {...},
    "pending_analysis": {...}
}
```

## Envelope MCP

La tool debe usar el envelope estándar del proyecto:

```python
build_tool_response(
    tool_name="get_purchase_orders_analysis",
    summary=summary,
    filters={
        "initial_date": initial_date,
        "final_date": final_date,
        "vendor": vendor,
        "status": status,
        "brand": brand,
        "topic": topic,
    },
    source_systems=["netsuite"],
    columns=columns,
    rows=rows,
    dataset_reference=dataset_reference,
)
```

## Criterios de aceptación

- [ ] La tool existe en `features/operations/tools.py`.
- [ ] La query vive en `features/operations/queries/purchase_orders.py`.
- [ ] La query retorna `sql` y `params`.
- [ ] La query usa placeholders `?`.
- [ ] Los filtros de `vendor`, `status` y `brand` son dinámicos.
- [ ] No hay interpolación de inputs del usuario dentro del SQL.
- [ ] `initial_date` y `final_date` se resuelven por defecto cuando no vienen.
- [ ] `topic` acepta únicamente `vendors` e `items`.
- [ ] `topic` usa `vendors` por defecto.
- [ ] `status` se valida contra la lista permitida.
- [ ] `vendors` deduplica por `po_id` para métricas de PO.
- [ ] `items` usa el detalle completo de líneas.
- [ ] La respuesta usa el envelope estándar.
- [ ] Hay tests para query dinámica, defaults, validaciones y ambos topics.

## Tests recomendados

1. `get_purchase_orders_query` con solo fechas debe retornar una query con solo filtros obligatorios y dos params.
2. `get_purchase_orders_query` con `vendor`, `status` y `brand` debe agregar tres filtros dinámicos y cinco params.
3. La query no debe contener valores del usuario interpolados.
4. `topic=None` o vacío debe resolverse a `vendors`.
5. `topic="items"` debe retornar análisis de items.
6. `topic` inválido debe devolver error controlado.
7. `status` inválido debe devolver error controlado.
8. Si no vienen fechas, se debe usar inicio del mes actual y fecha actual.
9. El análisis `vendors` no debe duplicar `amount_usd` por líneas.
10. El análisis `items` debe calcular `pending_quantity` y `received_pct`.

## Fuera de alcance inicial

- Crear múltiples tools para PO.
- Crear múltiples queries para cada topic.
- Incluir análisis individual por item.
- Incluir `po_number` como filtro.
- Persistir resultados en base de datos.
