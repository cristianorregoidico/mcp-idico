import os
import psycopg
from typing import Any, List, Tuple, Optional
import traceback


def execute_pg_query(sql: str) -> List[Tuple[Any, ...]]:
    """
    Ejecuta una consulta SQL en PostgreSQL y devuelve los resultados.

    - Si la consulta es un SELECT, devuelve (columns, rows).
    - Si la consulta no devuelve filas (INSERT/UPDATE/DELETE), devuelve ([], []).

    Los parámetros de conexión se leen de variables de entorno:
    PGHOST_DEV, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.
    """

    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db   = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER", "postgres")

    print(f"[PG-CONNECT] Conectando a PostgreSQL → host={host}, port={port}, db={db}, user={user}")

    # Log de error en la conexión
    try:
        conn = psycopg.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=os.getenv("PGPASSWORD", ""),
        )
        print("[PG-CONNECT] Conexión establecida correctamente.")
    except Exception as e:
        print(f"[PG-ERROR] Error al conectar a PostgreSQL: {e}")
        traceback.print_exc()
        # Re-lanzamos la excepción para que el caller sepa que falló
        raise

    try:
        print(f"[PG-QUERY] Ejecutando SQL: {sql[:200]}{'...' if len(sql) > 200 else ''}")

        with conn.cursor() as cur:
            cur.execute(sql)

            if cur.description is None:
                # No hay resultado (por ejemplo INSERT/UPDATE/DELETE)
                conn.commit()
                print("[PG-RESULT] Query sin retorno (INSERT/UPDATE/DELETE). Commit realizado.")
                return [], []

            # Nombres de columnas
            columns = [col.name for col in cur.description]

            # Filas como lista de tuplas
            rows = cur.fetchall()

            print(f"[PG-RESULT] Filas retornadas: {len(rows)}")
            return columns, rows

    except Exception as e:
        # Log de error en ejecución de query
        print(f"[PG-ERROR] Error ejecutando SQL: {e}")
        traceback.print_exc()
        # Intentamos rollback por si la transacción quedó abierta
        try:
            conn.rollback()
            print("[PG-ERROR] Rollback realizado.")
        except Exception as rollback_err:
            print(f"[PG-ERROR] Error al hacer rollback: {rollback_err}")
        # Re-lanzamos la excepción para que el caller pueda manejarla
        raise

    finally:
        conn.close()
        print("[PG-CONNECT] Conexión cerrada.")
        
def execute_pg_query_dev(sql: str) -> List[Tuple[Any, ...]]:
    """
    Ejecuta una consulta SQL en PostgreSQL y devuelve los resultados.

    - Si la consulta es un SELECT, devuelve (columns, rows).
    - Si la consulta no devuelve filas (INSERT/UPDATE/DELETE), devuelve ([], []).

    Los parámetros de conexión se leen de variables de entorno:
    PGHOST_DEV, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.
    """

    host = os.getenv("PGHOST_DEV", "localhost")
    port = os.getenv("PGPORT", "5432")
    db   = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER", "postgres")

    print(f"[PG-CONNECT] Conectando a PostgreSQL → host={host}, port={port}, db={db}, user={user}")

    # Log de error en la conexión
    try:
        conn = psycopg.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=os.getenv("PGPASSWORD", ""),
        )
        print("[PG-CONNECT] Conexión establecida correctamente.")
    except Exception as e:
        print(f"[PG-ERROR] Error al conectar a PostgreSQL: {e}")
        traceback.print_exc()
        # Re-lanzamos la excepción para que el caller sepa que falló
        raise

    try:
        print(f"[PG-QUERY] Ejecutando SQL: {sql[:200]}{'...' if len(sql) > 200 else ''}")

        with conn.cursor() as cur:
            cur.execute(sql)

            if cur.description is None:
                # No hay resultado (por ejemplo INSERT/UPDATE/DELETE)
                conn.commit()
                print("[PG-RESULT] Query sin retorno (INSERT/UPDATE/DELETE). Commit realizado.")
                return [], []

            # Nombres de columnas
            columns = [col.name for col in cur.description]

            # Filas como lista de tuplas
            rows = cur.fetchall()

            print(f"[PG-RESULT] Filas retornadas: {len(rows)}")
            return columns, rows

    except Exception as e:
        # Log de error en ejecución de query
        print(f"[PG-ERROR] Error ejecutando SQL: {e}")
        traceback.print_exc()
        # Intentamos rollback por si la transacción quedó abierta
        try:
            conn.rollback()
            print("[PG-ERROR] Rollback realizado.")
        except Exception as rollback_err:
            print(f"[PG-ERROR] Error al hacer rollback: {rollback_err}")
        # Re-lanzamos la excepción para que el caller pueda manejarla
        raise

    finally:
        conn.close()
        print("[PG-CONNECT] Conexión cerrada.")
