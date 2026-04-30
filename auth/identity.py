from fastmcp.server.dependencies import get_access_token

def resolve_authenticated_identity() -> str:
    try:
        token = get_access_token()
        if token is None:
            return "Not Identified"

        claims = token.claims or {}
        return (
            claims.get("preferred_username")
            or claims.get("name")
            or "Not Identified"
        )
    except Exception:
        return "Not Identified"


def log_auth_debug(tool_name: str) -> str:
    identity = resolve_authenticated_identity()
    print(f"[AUTH-DEBUG] tool={tool_name} auth={identity}")
    return identity
