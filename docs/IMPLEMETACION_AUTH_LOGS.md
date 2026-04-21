# Reporte de cambios — IDRA MCP Server
**Fecha base del reporte:** 20 de abril de 2026  
**Actualizado:** 21 de abril de 2026

---

## 1. Autenticación migrada a Azure / Microsoft Entra ID

La autenticación OAuth 2.0 del servidor MCP ya no usa Auth0. Ahora se utiliza `AzureProvider` de FastMCP con credenciales del tenant de Azure.

**Qué se hizo:**
- Se reemplazó `Auth0Provider` por `AzureProvider`.
- El servidor exige un token válido emitido dentro del flujo configurado en Azure.
- Se mantiene el scope requerido `mcp-access`.

**Implementación actual:**
- `auth/provider.py` centraliza la creación del provider.
- `main.py` solo compone el servidor y consume `create_auth_provider(...)`.

**Código clave:**
```python
def create_auth_provider(redis_client):
    return AzureProvider(
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
        tenant_id=os.environ["AZURE_TENANT_ID"],
        base_url=os.environ["BASE_URL"],
        required_scopes=["mcp-access"],
        jwt_signing_key=os.environ["JWT_SIGNING_KEY"],
        client_storage=FernetEncryptionWrapper(
            key_value=RedisStore(client=redis_client),
            fernet=Fernet(os.environ["STORAGE_ENCRYPTION_KEY"])
        )
    )
```

---

## 2. Almacenamiento OAuth migrado a Azure Redis

El estado OAuth y el almacenamiento usado por `client_storage` ya no se soportan con Upstash. Ahora se usa Redis de Azure.

**Qué se hizo:**
- Se cambió la conexión para usar `AZURE_REDIS_URL`.
- Se mantuvo `decode_responses=True`, necesario para que `RedisStore` trabaje con `str` y no con `bytes`.
- Se sigue cifrando la información persistida con `FernetEncryptionWrapper`.

**Implementación actual:**
- `auth/redis_client.py` centraliza la creación del cliente Redis.

```python
def create_redis_client():
    return aioredis.from_url(
        os.environ["AZURE_REDIS_URL"],
        ssl_cert_reqs=None,
        decode_responses=True,
    )
```

**Nota importante:**  
Azure Redis persiste el estado de autenticación y soporte del flujo OAuth, pero **no es la fuente de identidad del usuario**. La identidad se obtiene desde los claims del access token validado por Azure.

---

## 3. Captura del usuario que realiza cada petición

Se agregó una capa de resolución de identidad para identificar al usuario autenticado que ejecuta cada tool.

**Qué se hace actualmente:**
- Se intenta leer `preferred_username` desde los claims del token.
- Si `preferred_username` no existe o viene vacío, se usa `name`.
- Si ocurre cualquier error o no hay contexto autenticado, se retorna `"Not Identified"`.

**Implementación actual:**
- `auth/debug.py` encapsula esta lógica.

```python
def resolve_authenticated_identity() -> str:
    try:
        token = get_access_token()
        if token is None:
            return "Not Identified"

        claims = token.claims or {}
        return (
            claims.get("preferred_username")
            or claims.get("name")
            or "Not Identified"
        )
    except Exception:
        return "Not Identified"
```

**Comportamiento observado en Azure:**
- En el tenant actual, `preferred_username` está llegando como correo corporativo del usuario.
- Ejemplo real observado:
  - `preferred_username = cristianorrego@idico.com`
  - `name = Cristian Orrego Duque`

---

## 4. Auth debug por petición

Antes de ejecutar cada tool, el sistema imprime en consola el usuario detectado para facilitar validación operativa.

**Implementación actual:**
```python
def log_auth_debug(tool_name: str) -> str:
    identity = resolve_authenticated_identity()
    print(f"[AUTH-DEBUG] tool={tool_name} auth={identity}")
    return identity
```

**Salida esperada:**
```text
[AUTH-DEBUG] tool=get_bookings auth=cristianorrego@idico.com
```

Esto permite validar rápidamente qué identidad está propagando Azure en cada request sin depender todavía de consultas a base de datos.

---

## 5. Logging de llamadas a tools en PostgreSQL

Se mantiene la persistencia de cada invocación de tool en PostgreSQL, pero ahora también se envía el usuario resuelto para esa petición.

### 5.1 Nueva firma de `log_tool_call`

La función async ahora recibe:
- `tool_name`
- `username`
- `params`
- `response`
- `duration_ms`

```python
async def log_tool_call(
    tool_name: str,
    username: str,
    params: dict,
    response: str,
    duration_ms: int,
) -> None:
    ...
    INSERT INTO ods.analytics.idra_tool_calls
        (tool_name, username, params, response, duration_ms)
    VALUES (%s, %s, %s, %s, %s)
```

### 5.2 Wrapper centralizado para tools

La lógica de registro de tools fue extraída de `main.py` y movida a `server/tool_registry.py`.

**Qué hace el wrapper actual:**
- resuelve el usuario autenticado vía `log_auth_debug(...)`
- ejecuta el tool en `asyncio.to_thread(...)`
- calcula duración
- persiste nombre del tool, usuario, parámetros, respuesta y tiempo

```python
def register_tool(app, fn):
    @functools.wraps(fn)
    async def wrapper(**kwargs):
        start = time.monotonic()
        error = None
        response = None

        authenticated_user = log_auth_debug(fn.__name__)

        try:
            response = await asyncio.to_thread(fn, **kwargs)
            return response
        except Exception as e:
            error = e
            raise
        finally:
            duration_ms = int((time.monotonic() - start) * 1000)
            resp_str = f"ERROR: {error}" if error else str(response)
            asyncio.create_task(
                log_tool_call(
                    fn.__name__,
                    authenticated_user,
                    kwargs,
                    resp_str,
                    duration_ms,
                )
            )

    app.tool(annotations=DEFAULT_ANNOTATIONS)(wrapper)
```

**DDL actualizado sugerido en Postgres:**
```sql
CREATE TABLE ods.analytics.idra_tool_calls (
    id          SERIAL PRIMARY KEY,
    tool_name   TEXT        NOT NULL,
    username    TEXT,
    params      TEXT,
    response    TEXT,
    duration_ms INTEGER,
    created_at  TIMESTAMPTZ DEFAULT now()
);
```

---

## 6. Refactor de estructura

La lógica que antes estaba concentrada en `main.py` fue separada en módulos pequeños para mejorar mantenibilidad.

**Nueva estructura relevante:**

| Archivo | Responsabilidad |
|---|---|
| `main.py` | Composición del servidor FastMCP |
| `auth/redis_client.py` | Crear cliente Redis de Azure |
| `auth/provider.py` | Crear `AzureProvider` |
| `auth/debug.py` | Resolver e imprimir identidad del usuario |
| `server/tool_registry.py` | Registrar tools con wrapper común |
| `connections/postgresql.py` | Persistir auditoría de tools |

**Resultado:**
- `main.py` quedó más limpio
- la autenticación quedó desacoplada
- la lógica de identificación del usuario quedó reutilizable
- el wrapper de tools quedó centralizado

---

## Resumen de archivos modificados / relevantes

| Archivo | Tipo de cambio |
|---|---|
| `main.py` | Composición del servidor y registro de tools |
| `auth/provider.py` | Nuevo módulo para `AzureProvider` |
| `auth/redis_client.py` | Nuevo módulo para Redis de Azure |
| `auth/debug.py` | Nuevo módulo para resolver/capturar usuario autenticado |
| `server/tool_registry.py` | Nuevo wrapper centralizado para tools |
| `connections/postgresql.py` | Logging async con columna `username` |
