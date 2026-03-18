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

def tool_register(fn):
    app.tool(
        enabled=True,
        annotations=DEFAULT_ANNOTATIONS,
    )(fn)
    
for tool_sales in SALES_TOOLS:
    tool_register(tool_sales)

for tool_files in FILES_TOOLS:
    tool_register(tool_files)
    
for tool_performance in PERFORMANCE_TOOLS:
    tool_register(tool_performance)
    
for tool_ops in OPS_TOOLS:
    tool_register(tool_ops)
    



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
