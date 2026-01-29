from fastmcp import FastMCP
from tools.sales import SALES_TOOLS
from tools.files import FILES_TOOLS
from tools.operations import OPS_TOOLS
from tools.performance import PERFORMANCE_TOOLS


app = FastMCP("idico-sales")

for tool_sales in SALES_TOOLS:
    app.tool(tool_sales)

for tool_files in FILES_TOOLS:
    app.tool(tool_files)
    
for tool_performance in PERFORMANCE_TOOLS:
    app.tool(tool_performance)
    
for tool_ops in OPS_TOOLS:
    app.tool(tool_ops)
    




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
