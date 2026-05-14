# Spec: IDRA Usage Metrics Tool

Esta feature agrega una nueva tool MCP en `features/operations` para consultar uso de tools IDRA desde PostgreSQL. La tool debe ejecutar una query parametrizada por rango de fechas, cargar los resultados en Pandas y construir métricas de adopción, uso, performance, temporalidad y errores.

## Quick path

1. Crear la tool pública `get_idra_usage(initial_date, final_date)`.
2. Ejecutar la query base con placeholders `%s` usando `psycopg` usando execute_pg_query.
3. Clasificar `response` con una normalización tolerante y calcular KPIs en Pandas.

## Decisión principal

La implementación será una sola tool pública de operaciones:

1. **Una sola tool MCP** para analítica de uso IDRA.
2. **Una sola query PostgreSQL parametrizada por fechas**.
3. **Sin filtros adicionales** en esta primera versión.
4. **Placeholders `%s` de `psycopg`**, nunca valores interpolados.
5. **Pandas en la capa domain** para todas las métricas iniciales.
6. **Clasificación de `response`** en `success`, `error` y `unknown`.

## Ubicación esperada

| Responsabilidad | Archivo / módulo |
|---|---|
| Tool MCP pública | `features/operations/tools.py` |
| Orquestación del caso de uso | `features/operations/use_cases/idra_usage.py` |
| Query SQL parametrizada | `features/operations/queries/idra_usage.py` |
| Cálculo de métricas | `features/operations/domain/idra_usage.py` |
| Tests | `tests/` |

## Firma propuesta de la tool

```python
def get_idra_usage(
    initial_date: str,
    final_date: str,
) -> Dict[str, Any]:
```

## Parámetros

| Parámetro | Requerido | Descripción |
|---|---:|---|
| `initial_date` | Sí | Fecha inicial en formato `YYYY-MM-DD`. |
| `final_date` | Sí | Fecha final en formato `YYYY-MM-DD`. |

## Reglas de validación

- `initial_date` y `final_date` deben venir en formato `YYYY-MM-DD`.
- `initial_date` no puede ser mayor que `final_date`.
- La query debe usar un límite superior exclusivo para evitar errores de borde con timestamps.

## Query base requerida

```sql
SELECT
    tool_name,
    username,
    response,
    duration_ms,
    created_at
FROM ods.analytics.idra_tool_calls
WHERE username <> 'Not Identified'
  AND created_at >= %s
  AND created_at < %s
ORDER BY created_at DESC;
```

## Parámetros esperados

```python
start_ts = initial_date
end_ts_exclusive = final_date + 1 day
params = [start_ts, end_ts_exclusive]
```

> No se permiten filtros adicionales por `tool_name`, `username` ni ningún otro campo en esta versión.

## Construcción de métricas con Pandas

Los KPIs deben calcularse en `features/operations/domain/idra_usage.py`, no con queries agregadas SQL en la implementación inicial.

### Transformaciones mínimas

1. Convertir `created_at` a datetime.
2. Derivar columnas auxiliares:
   - `date`
   - `hour`
   - `weekday`
3. Normalizar `response` para clasificar cada fila.
4. Derivar flags:
   - `is_success`
   - `is_error`
   - `is_unknown`

## Normalización de `response`

## Contexto real de almacenamiento

En el código actual, `response` se persiste como texto:

- errores: `ERROR: ...`
- éxitos: `str(response)`

Eso implica que puede venir:

1. como texto plano de error
2. como string con dict Python usando comillas simples
3. truncado a 4000 caracteres

## Clasificación requerida

Cada fila debe enriquecerse con:

- `response_kind`: `error_text | python_dict_text | json_text | unknown_text | empty`
- `execution_status`: `success | error | unknown`
- `error_message`: `str | None`

## Reglas de clasificación

1. Si `response` es nulo o vacío → `unknown`.
2. Si `response` empieza con `ERROR:` → `error`.
3. Si parsea con `json.loads(...)` → `success` si la estructura es válida.
4. Si no parsea como JSON, intentar `ast.literal_eval(...)`.
5. Si el parseo devuelve `dict` o `list`, clasificar como `success`.
6. Si no se puede parsear y no empieza con `ERROR:` → `unknown`.

## Nota crítica

Como `log_tool_call()` trunca `response` a 4000 caracteres, algunos payloads exitosos pueden quedar incompletos. Esos casos **no deben forzarse a success**; deben quedar en `unknown`.

## Métricas obligatorias

## 1. Resumen general

- `total_calls`
- `unique_tools`
- `unique_users`
- `avg_calls_per_day`

## 2. Tools más usadas

- ranking por `tool_name`
- porcentaje sobre el total
- usuarios únicos por tool
- duración promedio por tool
- duración mediana por tool
- error rate por tool

## 3. Usuarios más activos

- ranking por `username`
- calls por usuario
- tools distintas usadas por usuario
- duración promedio por usuario
- error rate por usuario

## 4. Performance

- `avg_duration_ms`
- `median_duration_ms`
- `p95_duration_ms`
- `p99_duration_ms`
- tools más lentas por mediana
- tools más lentas por p95

## 5. Uso temporal

- calls por día
- calls por día de semana
- calls por hora
- heatmap `weekday x hour`
- día pico de uso
- hora pico de uso

## 6. Calidad / errores

- `success_count`
- `error_count`
- `unknown_count`
- `error_rate`
- top mensajes de error
- errores por día
- errores por hora

## Estructura de respuesta esperada

La respuesta debe seguir el envelope estándar del proyecto:

```python
{
  "meta": {
    "tool_name": "get_idra_usage",
    "generated_at": "...",
    "source_systems": ["postgresql"],
    "filters": {
      "initial_date": "...",
      "final_date": "..."
    },
    "time_range": {
      "start_date": "...",
      "end_date": "..."
    }
  },
  "kpi_metrics": {
    "summary": {},
    "tools": {},
    "users": {},
    "performance": {},
    "usage_patterns": {},
    "quality": {}
  },
  "details": {
    "notes": [
      "response is stored as text and classified heuristically",
      "unknown responses are kept separate from errors"
    ]
  }
}
```

## Checklist de implementación

- [ ] Tool pública agregada en `features/operations/tools.py`
- [ ] Caso de uso creado en `features/operations/use_cases/idra_usage.py`
- [ ] Query parametrizada creada en `features/operations/queries/idra_usage.py`
- [ ] Métricas Pandas implementadas en `features/operations/domain/idra_usage.py`
- [ ] Clasificación robusta de `response` implementada
- [ ] Tests de validación, query y métricas agregados

## Tests mínimos

1. Validación de formato de fechas.
2. Error cuando `initial_date > final_date`.
3. Query parametrizada con rango exclusivo superior.
4. Clasificación correcta de `ERROR: ...`.
5. Parseo correcto de string con dict Python.
6. Clasificación `unknown` para responses truncados o inválidos.
7. Cálculo correcto de top tools.
8. Cálculo correcto de métricas de latencia.
9. Agrupación temporal por día y hora.
10. Cálculo correcto de error rate por tool.

## Resultado esperado

La tool queda correcta si:

- acepta `initial_date` y `final_date`
- usa SQL parametrizado
- calcula métricas con Pandas
- diferencia `success`, `error` y `unknown`
- entrega métricas accionables de uso, performance y calidad
