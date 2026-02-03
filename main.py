from fastmcp import FastMCP
from tools.sales import SALES_TOOLS
from tools.files import FILES_TOOLS
from tools.operations import OPS_TOOLS
from tools.performance import PERFORMANCE_TOOLS


app = FastMCP("idico-sales")

DEFAULT_ANNOTATIONS = {
    "readOnlyHint": True,      # ✅ solo consulta
    "destructiveHint": False,  # ✅ no modifica nada
    "openWorldHint": False,    # ✅ no “mundo abierto” (si es SQL predefinido, no debería ser open world)
}

for tool_sales in SALES_TOOLS:
    app.tool(tool_sales, enabled=True, annotations=DEFAULT_ANNOTATIONS)

for tool_files in FILES_TOOLS:
    app.tool(tool_files, enabled=True, annotations=DEFAULT_ANNOTATIONS)
    
for tool_performance in PERFORMANCE_TOOLS:
    app.tool(tool_performance, enabled=True, annotations=DEFAULT_ANNOTATIONS)
    
for tool_ops in OPS_TOOLS:
    app.tool(tool_ops , enabled=True, annotations=DEFAULT_ANNOTATIONS)
    




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
