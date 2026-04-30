import os
import traceback
from contextlib import contextmanager
from dotenv import load_dotenv
import jaydebeapi as jd
import random
import string
import time
import hmac
import hashlib
import base64

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
        self.pwd = self.generate_tba_token()  # Generate TBA token for password
        # JAR lives at connections/lib/NQjc.jar — one level up from this subpackage
        self.path_driver = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib", "NQjc.jar")

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
    
    def generate_tba_token(self):
        """Genera un token TBA (Token-Based Authentication) para NetSuite utilizando HMAC-SHA256."""
       

        # Generar nonce
        #nonce = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(20))
        nonce = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10))


        # PROD NS SS
        account = '11012044'  # Cuenta Netsuite Legaxy
        consumer_key = '57df4ad8829fa3973b28e570201d4f5f87ffa112d4c7d0b445c3e59376c66145'
        consumer_secret = 'e2047c5fb143a69d8c3612a0c28c21de22528ae712ff4aeec87b0aca72cd51fc'
        token = 'f4f5ea8f427f21a709ec701fcba64276e46308edb5eaeffedc009b4c1810ab32'
        token_secret = '2b86fe37c5c05b3ceb7895663178fafeb94f554eee3dd0857b50ea017817d631'


        # Obtener timestamp
        timestamp = str(int(time.time()))

        # Crear el mensaje para la firma
        msg = f"{account}&{consumer_key}&{token}&{nonce}&{timestamp}"
        secret = f"{consumer_secret}&{token_secret}"

        key = f"{consumer_secret}&{token_secret}"
        baseString = f"{account}&{consumer_key}&{token}&{nonce}&{timestamp}"

        # Crear la firma HMAC-SHA256
        #hmacsha = hmac.new(secret.encode('ascii'), msg.encode('ascii'), hashlib.sha256)
        #signature = base64.b64encode(hmacsha.digest()).decode('ascii')
        sha256 = hashlib.sha256(key.encode()).digest()
        signature = base64.b64encode(hmac.new(sha256, baseString.encode(), hashlib.sha256).digest()).decode()

        # Algoritmo de firma
        signature_algorithm = 'HMAC-SHA256'

        # Imprimir la estructura final
        output = f"{account}&{consumer_key}&{token}&{nonce}&{timestamp}&{signature}&{signature_algorithm}"
        print("output", output)
        return output
        
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

    