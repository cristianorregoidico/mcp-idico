from servers.base import build_app
from tools.operations import OPS_TOOLS
from tools.shared.files import FILES_TOOLS  # opcional

app = build_app("idico-operations", OPS_TOOLS + FILES_TOOLS)

if __name__ == "__main__":
    import asyncio
    try:
        app.run(
            transport="streamable-http", 
            host="0.0.0.0", 
            port=8040, 
            path="/mcp"
        )
    except asyncio.CancelledError:
        pass