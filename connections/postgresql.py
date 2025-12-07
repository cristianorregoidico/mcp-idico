import os
import psycopg
from typing import Any, List, Tuple, Optional


def execute_pg_query(sql: str) -> List[Tuple[Any, ...]]:
    """
    Ejecuta una consulta SQL en PostgreSQL y devuelve los resultados.

    - Si la consulta es un SELECT, devuelve una lista de tuplas con las filas.
    - Si la consulta no devuelve filas (INSERT/UPDATE/DELETE), devuelve una lista vacía.

    Los parámetros de conexión se leen de variables de entorno:
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.
    """

    conn = psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "postgres"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )

    try:
        with conn.cursor() as cur:
            cur.execute(sql)

            if cur.description is None:
                # No hay resultado (por ejemplo INSERT/UPDATE/DELETE)
                conn.commit()
                return [], []

            # Nombres de columnas
            columns = [col.name for col in cur.description]

            # Filas como lista de tuplas
            rows = cur.fetchall()

            return columns, rows

    finally:
        conn.close()
