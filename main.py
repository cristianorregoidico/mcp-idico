from fastmcp import FastMCP
from mcp.types import Icon
import asyncio

from auth.redis_client import create_redis_client
from auth.provider import create_auth_provider
from middleware import register_tools

from features.sales.tools import SALES_TOOLS
from features.files.tools import FILES_TOOLS
from features.operations.tools import OPS_TOOLS
from features.performance.tools import PERFORMANCE_TOOLS
from features.notifications.tools import NOTIFICATION_TOOLS

redis_client = create_redis_client()
auth_provider = create_auth_provider(redis_client)

app = FastMCP(
    "IDRA IDICO AI",
    #auth=auth_provider,
    instructions="Accede a datos en tiempo real y genera análisis claros para apoyar decisiones.",
    icons=[Icon(src="https://i.ibb.co/svxz9ZcR/idra-logo.png", mimeType="image/png", sizes=["48x48"])],
)

register_tools(app, SALES_TOOLS)
register_tools(app, FILES_TOOLS)
register_tools(app, PERFORMANCE_TOOLS)
register_tools(app, OPS_TOOLS)
register_tools(app, NOTIFICATION_TOOLS)


if __name__ == "__main__":

    try:
        app.run(
            transport="streamable-http",
            host="0.0.0.0",
            port=8000,
            #path="/mcp",  # endpoint HTTP del servidor MCP
        )
    except asyncio.CancelledError:
        pass
    
