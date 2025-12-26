from fastmcp import FastMCP

def build_app(name: str, tools):
    app = FastMCP(name)
    for tool in tools:
        app.tool(tool)
    return app