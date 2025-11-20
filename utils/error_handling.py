import logging
from functools import wraps

log = logging.getLogger(__name__)

def safe_tool(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            log.exception("Tool failed: %s", fn.__name__)
            return {"error": "Unexpected error. Please try again later."}
    return wrapper

