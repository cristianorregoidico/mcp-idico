# Guía para desarrolladores — contexto del proyecto MCP IDICO

Esta guía está pensada para una reunión de contexto técnico con un equipo nuevo. No busca definir una verdad absoluta ni imponer una única forma de hacer las cosas; busca explicar **cómo está construido hoy el proyecto**, cómo fluye una request y cuál es el camino recomendado para desarrollar una nueva tool sin romper la arquitectura actual.

---

## 1. Qué es este proyecto

Este repositorio implementa un **servidor MCP** para IDICO/IDRA.

Su función es exponer tools que permiten consultar, resumir y devolver información de negocio desde distintos orígenes, principalmente:

- **NetSuite**
- **PostgreSQL**
- algunos flujos auxiliares como datasets guardados o notificaciones

En términos simples, el servidor permite que un cliente MCP o un asistente consulte cosas como:

- cotizaciones
- bookings
- oportunidades
- items vendidos o cotizados
- scorecards y performance
- OTD, guías e importaciones

y reciba una respuesta estructurada, consistente y reutilizable.

---

## 2. Cómo pensar el proyecto

La forma más fácil de entenderlo es esta:

> una tool recibe parámetros, ejecuta un caso de uso, consulta una fuente de datos, procesa los resultados y devuelve un envelope MCP homogéneo.

No es una API REST tradicional. Tampoco es solo un repo de queries. Es más bien una **capa de orquestación de herramientas analíticas**.

---

## 3. Estructura general del repositorio

## Quick path

Si el equipo tiene que entender el repo rápido, estas son las carpetas más importantes:

| Ruta | Para qué sirve |
|---|---|
| `main.py` | compone y arranca el servidor MCP |
| `middleware.py` | aplica wrapper transversal a todas las tools |
| `auth/` | autenticación, identidad y Redis para OAuth |
| `features/` | lógica del negocio separada por dominio |
| `connections/` | acceso a NetSuite y PostgreSQL |
| `utils/` | helpers compartidos |
| `data/` | datasets persistidos |
| `tests/` | pruebas de regresión y validaciones focalizadas |

---

## 4. Cómo está organizada la carpeta `features/`

`features/` es el corazón del proyecto.

Cada dominio tiene su propia carpeta. Hoy los principales son:

- `sales/`
- `operations/`
- `performance/`
- `files/`
- `notifications/`

Los tres primeros siguen el mismo patrón interno:

```text
features/<dominio>/
  tools.py
  use_cases/
  queries/
  domain/
```

### Qué significa cada cosa

| Componente | Responsabilidad |
|---|---|
| `tools.py` | entrada MCP del dominio |
| `use_cases/` | orquestación del flujo de cada tool |
| `queries/` | extracción de datos por capability |
| `domain/` | lógica de negocio, KPIs, resúmenes, reglas |

### Regla simple

- si algo es **entrada MCP** → `tools.py`
- si algo es **coordinación del flujo** → `use_cases/`
- si algo es **query builder / extracción del capability** → `queries/`
- si algo es **regla, cálculo o resumen de negocio** → `domain/`

---

## 5. Flujo de una request

## Vista de alto nivel

Cuando llega una request a una tool, el flujo actual es aproximadamente este:

1. el cliente MCP invoca una tool
2. `main.py` ya registró esa tool en el servidor
3. `middleware.py` envuelve la ejecución
4. se resuelve identidad/auth si corresponde
5. la tool normaliza parámetros
6. la tool llama a un use case
7. el use case consulta datos en `connections/`
8. el use case procesa los resultados con `domain/` y/o `pandas`
9. opcionalmente se persiste un dataset en `data/`
10. se arma la respuesta final con `utils/envelope.py`
11. `middleware.py` registra auditoría y duración

---

## 6. Flujo detallado por capas

### 6.1 `main.py`

Responsabilidad:

- crea el `FastMCP`
- compone auth provider y Redis
- registra tools por dominio

No debería tener lógica de negocio.

---

### 6.2 `middleware.py`

Responsabilidad:

- envolver todas las tools con un comportamiento común
- resolver el usuario autenticado
- medir duración
- registrar la llamada en PostgreSQL

Esto evita duplicar esa lógica en cada tool.

---

### 6.3 `tools.py`

Responsabilidad esperada:

- recibir parámetros de la tool
- aplicar defaults y normalización liviana
- llamar a un único use case

Ejemplos típicos:

- setear rango de fechas por defecto
- normalizar `customer_name` a uppercase
- convertir filtros vacíos a `""` o `None` según el flujo

Lo importante es que `tools.py` sea una capa **liviana**, no un lugar para meter reglas complejas.

---

### 6.4 `use_cases/`

Responsabilidad:

- orquestar el caso de uso completo
- pedir `sql, params` o consumir APIs/fuentes externas
- ejecutar queries
- transformar resultados tabulares
- llamar a lógica de negocio en `domain/`
- persistir datasets si aplica
- construir la respuesta final

Pensalo como el director del flujo.

No debería contener lógica de negocio pesada si esa lógica puede vivir en `domain/`.

---

### 6.5 `domain/`

Responsabilidad:

- métricas
- agregaciones
- reglas
- resúmenes
- shape semántico del resultado de negocio

Acá suele aparecer `pandas`, porque muchas tools trabajan sobre DataFrames para resumir resultados tabulares.

Ejemplos:

- `features/sales/domain/bookings.py`
- `features/sales/domain/opportunities.py`
- `features/operations/domain/otd.py`
- `features/performance/domain/inside_sales.py`

---

### 6.6 `connections/`

Responsabilidad:

- conectarse a la fuente externa
- ejecutar queries
- devolver columnas y filas

No debería tomar decisiones de negocio.

Estructura actual:

```text
connections/
  netsuite/
    client.py
  postgresql/
    client.py

features/
  sales/
    queries/
  operations/
    queries/
  performance/
    queries/
```

### Importante hoy

Las queries activas ya están migradas para usar parámetros binded:

- **NetSuite / jaydebeapi** → placeholders `?`
- **PostgreSQL / psycopg** → placeholders `%s`

Esto reduce riesgo de interpolación insegura y separa mejor SQL de datos de entrada.

---

## 7. Cómo se desarrolla una nueva tool

Esta es la parte más importante para el equipo.

La forma recomendada de sumar una tool nueva hoy es:

## Paso 1 — definir qué problema resuelve

Antes de escribir código, conviene tener claro:

- qué pregunta de negocio responde
- qué fuente de datos necesita
- qué filtros recibe
- qué resumen final espera el usuario

Preguntas útiles:

- ¿esto es `sales`, `operations` o `performance`?
- ¿la respuesta es un detalle tabular o un resumen ejecutivo?
- ¿vale la pena guardar dataset reutilizable?

---

## Paso 2 — definir la extracción de datos

Acá se define la query o integración con la fuente.

### Convención actual del proyecto

Los clients siguen centralizados en `connections/*/client.py`, pero los query builders nuevos viven dentro del feature que los usa:

- `features/<dominio>/queries/<capacidad>.py`

Esto permite que la extracción de datos quede organizada por capability y no por datasource solamente.

### Si usa NetSuite

- crear o extender una función en `features/<dominio>/queries/<capacidad>.py`
- retornar `sql, params`
- usar placeholders `?`

### Si usa PostgreSQL

- crear o extender una función en `features/<dominio>/queries/<capacidad>.py`
- retornar `sql, params`
- usar placeholders `%s`

### Regla actual

Para filtros opcionales, el proyecto viene usando mejor el enfoque de:

> **dynamic WHERE builder**

es decir:

- armar una lista de cláusulas base
- agregar filtros solo si el valor viene informado
- mantener `params` en el mismo orden que los placeholders

---

## Paso 3 — construir el use case

Crear un archivo en `features/<dominio>/use_cases/`.

El use case suele hacer esto:

1. pedir `sql, params`
2. ejecutar query con el client adecuado
3. recibir `columns, rows`
4. convertir a DataFrame si hace falta
5. pasar el DataFrame a `domain/`
6. guardar dataset si aplica
7. construir la respuesta con `build_tool_response(...)`

---

## Paso 4 — procesar datos con `pandas` / domain

Si la tool necesita resumen, KPI o agrupaciones, eso debería vivir en `domain/`.

### Ejemplo de responsabilidades típicas de `domain/`

- agrupar por cliente
- calcular KPIs
- detectar outliers
- generar distribuciones
- devolver estructura de negocio consistente

La idea no es obligar a usar `pandas` siempre, sino reconocer que hoy el proyecto está construido mucho alrededor de:

- `columns, rows`
- `tuple_to_dataframe(...)`
- reglas de resumen en DataFrame

Entonces, si una tool va a resumir datos tabulares, `pandas` suele ser el camino natural en este repo.

---

## Paso 5 — definir si guarda dataset

Muchas tools del proyecto guardan el resultado tabular completo en `data/`.

Eso no es una verdad absoluta ni una obligación, pero **sí es un patrón frecuente hoy**.

### Cuándo suele tener sentido guardar dataset

- cuando la consulta devuelve detalle útil para revisar después
- cuando el resumen final no alcanza y conviene permitir recuperación del dataset
- cuando otra tool o análisis posterior puede reutilizarlo

### Cómo se hace hoy

El use case suele usar:

- `save_result_to_json(columns, rows, description, name=...)`

y luego incluir la referencia en:

- `meta.dataset_reference`
- `artifacts.dataset`

### Qué no asumir

No todas las tools deberían guardar dataset.
No hay que convertir esto en dogma.

La regla sana es:

> si el detalle tabular tiene valor de reutilización, persistilo; si no, no hace falta.

---

## Paso 6 — construir la respuesta final

La respuesta MCP se arma hoy con `utils/envelope.py`.

El shape común es:

```json
{
  "meta": {},
  "kpi_metrics": {},
  "artifacts": {
    "dataset": {}
  },
  "details": {}
}
```

### Lectura rápida de cada bloque

| Campo | Propósito |
|---|---|
| `meta` | trazabilidad, filtros, columnas, rangos, fuente |
| `kpi_metrics` | resumen principal para el usuario MCP |
| `artifacts.dataset` | referencia al dataset persistido si existe |
| `details` | bloques auxiliares, tablas secundarias o detalle extendido |

### Regla práctica

- lo más importante para el usuario final debería vivir en `kpi_metrics`
- el detalle extra va en `details`
- la trazabilidad técnica va en `meta`

---

## Paso 7 — exponer la tool en `tools.py`

Una vez que el use case está listo:

1. se crea o actualiza la función pública en `tools.py`
2. se agregan defaults y normalización de parámetros
3. se llama al use case
4. se agrega la función al arreglo `*_TOOLS`

Ejemplo conceptual:

```python
def get_xxx(...):
    # normalización
    return use_case.execute(...)
```

Esa función es la que finalmente `main.py` registra vía `register_tools(...)`.

---

## 8. Checklist recomendado para una nueva tool

## Antes de codear

- [ ] está claro el dominio (`sales`, `operations`, `performance`, etc.)
- [ ] está clara la fuente de datos
- [ ] están claros los filtros
- [ ] está claro si devuelve resumen, detalle o ambos
- [ ] está claro si conviene persistir dataset

## Durante la implementación

- [ ] query/API definida en `features/<dominio>/queries/` (y cliente en `connections/*/client.py`)
- [ ] parámetros binded correctamente (`?` o `%s`)
- [ ] orquestación en `use_cases/`
- [ ] reglas de negocio en `domain/`
- [ ] respuesta armada con `build_tool_response(...)`
- [ ] tool expuesta en `tools.py`

## Antes de cerrar

- [ ] probar caso sin filtros
- [ ] probar caso con filtros
- [ ] probar caso sin resultados
- [ ] verificar que no se rompa el shape del envelope
- [ ] si aplica, agregar o actualizar tests

---

## 9. Qué cosas conviene evitar

### Evitar lógica pesada en `tools.py`

`tools.py` no debería convertirse en un archivo monstruo.

### Evitar lógica de negocio en `connections/`

`connections/` provee infraestructura de conexión; no decide negocio.

### Evitar volver a centralizar queries por datasource

La convención actual es que las queries nuevas vivan dentro de `features/<dominio>/queries/` y no en un único archivo compartido por tecnología.

### Evitar meter reglas en `utils/`

`utils/` debería quedar para helpers realmente genéricos.

### Evitar interpolación de SQL

Hoy el estándar recomendado del proyecto es:

- NetSuite → `?`
- PostgreSQL → `%s`

con parámetros binded y no string interpolation.

### Evitar asumir que siempre hay datos

Ya vimos casos donde una tool no devuelve filas o las fechas no son válidas. Los resúmenes de `domain/` deberían tolerar:

- DataFrame vacío
- fechas inválidas
- listas vacías

sin tirar excepción innecesaria.

---

## 10. Ejemplo resumido de implementación

## Caso conceptual

Queremos una tool nueva: `get_top_customers_by_brand`

### A. extracción

- crear query en `features/sales/queries/<capacidad>.py`
- retornar `sql, params`

### B. orquestación

- crear `features/sales/use_cases/top_customers_by_brand.py`
- ejecutar query
- transformar `columns, rows`

### C. procesamiento

- crear `features/sales/domain/top_customers_by_brand.py`
- calcular ranking y distribución

### D. respuesta

- construir `build_tool_response(...)`
- decidir si se guarda dataset

### E. exposición MCP

- agregar tool en `features/sales/tools.py`
- incluirla en `SALES_TOOLS`

Ese es el camino que mejor calza con la arquitectura actual.

---

## 11. Cómo explicar esto en la reunión

Una forma simple de decirlo al equipo es:

> `tools.py` recibe, `use_cases/` coordina, `queries/` extrae, `domain/` decide, `connections/` conecta y `utils/` ayuda.

Y para una nueva tool:

> primero definimos la extracción, después el procesamiento, después la respuesta, y recién al final la exponemos como tool.

---

## 12. Cierre

La arquitectura actual no pretende ser la única posible, pero sí ofrece una convención clara:

- separar entrada MCP
- separar orquestación
- separar lógica de negocio
- separar acceso a datos
- devolver respuestas consistentes

Si el equipo entiende esas separaciones, ya tiene el 80% del contexto necesario para trabajar bien en este proyecto.
