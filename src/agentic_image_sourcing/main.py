from __future__ import annotations

import argparse

import uvicorn

from .api import create_app
from .mcp_server import create_mcp_server


def main() -> None:
    parser = argparse.ArgumentParser(prog="ais")
    subparsers = parser.add_subparsers(dest="command", required=True)

    api_parser = subparsers.add_parser("api", help="Run the HTTP API")
    api_parser.add_argument("--host", default="127.0.0.1")
    api_parser.add_argument("--port", type=int, default=8000)

    subparsers.add_parser("mcp", help="Run the MCP server over stdio")

    args = parser.parse_args()
    if args.command == "api":
        uvicorn.run(create_app(), host=args.host, port=args.port)
        return
    if args.command == "mcp":
        create_mcp_server().run()


if __name__ == "__main__":
    main()
