import os
from typing import Dict, List, Optional, Any
from fastmcp import FastMCP
from tools.netsuite import NETSUITE_TOOLS
from tools.files import FILES_TOOLS
from tools.postgres import POSTGRES_TOOLS

app = FastMCP("idico-sales")

for tool_netsuite in NETSUITE_TOOLS:
    app.tool(tool_netsuite)

for tool_files in FILES_TOOLS:
    app.tool(tool_files)
    
for tool_postgres in POSTGRES_TOOLS:
    app.tool(tool_postgres)


if __name__ == "__main__":
    import asyncio
    try:
        app.run(
            transport="streamable-http",
            host="0.0.0.0",
            port=8000,
            path="/mcp",  # endpoint HTTP del servidor MCP
        )
    except asyncio.CancelledError:
        pass
