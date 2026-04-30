import asyncio
import functools
import time
from auth.identity import log_auth_debug
from connections.postgresql.client import log_tool_call

DEFAULT_ANNOTATIONS = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "openWorldHint": False,
}

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
            asyncio.create_task(log_tool_call(fn.__name__, authenticated_user, kwargs, resp_str, duration_ms))

    annotations = getattr(fn, "MCP_ANNOTATIONS", DEFAULT_ANNOTATIONS)
    app.tool(annotations=annotations)(wrapper)


def register_tools(app, tools):
    for tool in tools:
        register_tool(app, tool)
