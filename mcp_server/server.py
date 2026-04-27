import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import subprocess
from mcp.server.fastmcp import FastMCP

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with open(os.path.join(PROJECT_ROOT, "storage", "secrets.json"), "r") as f:
    _secrets = json.load(f)

mcp = FastMCP("weekly-reports")


@mcp.tool()
def list_clients() -> str:
    """List all available clients for weekly report generation."""
    config_path = os.path.join(PROJECT_ROOT, "storage", "config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        clients = json.load(f)
    return json.dumps([c["name"] for c in clients])


@mcp.tool()
def fetch_client_data(client_name: str) -> str:
    """Fetch all performance data for a client. Returns the full data JSON needed to generate commentary."""
    script = os.path.join(PROJECT_ROOT, "weekly_reports", "fetch_data.py")
    result = subprocess.run(
        [sys.executable, script, "--client", client_name],
        capture_output=True, text=True, cwd=PROJECT_ROOT
    )
    if result.returncode != 0:
        raise Exception(f"Data fetch failed: {result.stderr}")

    data_path = os.path.join(PROJECT_ROOT, "storage", f"{client_name}_data.json")
    with open(data_path, "r", encoding="utf-8") as f:
        return f.read()


@mcp.tool()
def send_weekly_report(client_name: str, commentary: str) -> str:
    """Send the weekly report email for a client. commentary must be a JSON string matching the report schema."""
    commentary_path = os.path.join(PROJECT_ROOT, "storage", f"{client_name}_commentary.json")
    with open(commentary_path, "w", encoding="utf-8") as f:
        json.dump(json.loads(commentary), f, ensure_ascii=False, indent=2)

    script = os.path.join(PROJECT_ROOT, "weekly_reports", "send_email.py")
    result = subprocess.run(
        [sys.executable, script, "--client", client_name],
        capture_output=True, text=True, cwd=PROJECT_ROOT
    )
    if result.returncode != 0:
        raise Exception(f"Email send failed: {result.stderr}")

    return f"Weekly report sent successfully for {client_name}"


if __name__ == "__main__":
    import uvicorn
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.middleware import Middleware
    from starlette.applications import Starlette
    from starlette.routing import Mount
    from starlette.responses import Response

    SECRET = _secrets.get("mcp_secret_key", "")
    if not SECRET:
        raise RuntimeError("mcp_secret_key missing from secrets.json")

    class AuthMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            auth = request.headers.get("Authorization", "")
            print(f"[auth] path={request.url.path} header={repr(auth)}")
            if auth != f"Bearer {SECRET}":
                return Response("Unauthorized", status_code=401)
            return await call_next(request)

    app = Starlette(
        routes=[Mount("/", app=mcp.sse_app())],
        middleware=[Middleware(AuthMiddleware)]
    )
    uvicorn.run(app, host="0.0.0.0", port=8000)
