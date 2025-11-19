import os
import traceback
from contextlib import contextmanager
from dotenv import load_dotenv
import jaydebeapi as jd
# from netsuite_querys import get_bookings_by_period
# from typing import Any, Dict, List, Optional


# Load environment variables from .env file (if present)
load_dotenv()


class NetSuiteConnection:
    """Wrapper around jaydebeapi connection for NetSuite.

    Usage:
        with NetSuiteConnection() as conn:
            rows = conn.execute_query(sql, params)

    This class exposes a context manager, an execute_query helper that returns
    rows and column names, and robust error logging.
    """

    def __init__(self):
        self._conn = None
        self.driver = os.environ.get("DRIVER_NETSUITE")
        self.url = os.environ.get("URL_NETSUITE")
        self.usr = os.environ.get("USER_NETSUITE")
        self.pwd = os.environ.get("PWD_NETSUITE")
        # Keep driver jar next to this file under lib/NQjc.jar
        self.path_driver = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib", "NQjc.jar")

    def connect(self) -> bool:
        """Establish the JDBC connection. Returns True on success, False otherwise."""
        try:
            if not all([self.driver, self.url, self.usr, self.pwd]):
                raise ValueError("Missing one or more NetSuite connection environment variables")

            # jaydebeapi.connect takes (classname, url, [user, password], jarpath)
            self._conn = jd.connect(self.driver, self.url, [self.usr, self.pwd], self.path_driver)
            return True
        except Exception as e:
            print(f"Failed to connect to NetSuite: {e}")
            print("Error:", type(e).__name__)
            print("Mensaje:", str(e))
            print("Traza:")
            traceback.print_exc()
            self._conn = None
            return False

    def cursor(self):
        if not self._conn:
            raise RuntimeError("Connection not established. Call connect() first or use the context manager.")
        return self._conn.cursor()

    def close(self):
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            traceback.print_exc()
        finally:
            self._conn = None

    @contextmanager
    def managed(self):
        """Context manager wrapper to use the connection with `with`."""
        try:
            ok = self.connect()
            if not ok:
                raise RuntimeError("Could not establish NetSuite connection")
            yield self
        finally:
            self.close()

    def execute_query(self, sql: str, params=None):
        """Execute a query and return (columns, rows).

        columns is a list of column names; rows is a list of tuples.
        """
        cur = self.cursor()
        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            # cursor.description may be None for non-selects
            desc = cur.description or []
            columns = [d[0] for d in desc]
            rows = cur.fetchall()
            return columns, rows
        finally:
            try:
                cur.close()
            except Exception:
                pass
        
# sql = get_bookings_by_period("'2025-07-01'", "'2025-09-30'")
# print("sql",sql)
# conn = NetSuiteConnection()
# with conn.managed() as ns:
#     columns, rows = ns.execute_query(sql)

# # Map rows (tuples) to dicts using column names
# results: List[Dict[str, Any]] = []
# for row in rows:
#     if columns:
#         results.append({col: val for col, val in zip(columns, row)})
#     else:
#         # If no column names provided, return tuple under 'row'
#         results.append({"row": row})
        
# print("results",results)

    