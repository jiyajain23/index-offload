"""Unified runner for LIFELINE: Fullstack Edge Backend + Frontend.

Runs the FastAPI edge backend and optionally launches the Vite frontend dev server.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def main():
    parser = argparse.ArgumentParser(description="LIFELINE Fullstack Unified Runner")
    parser.add_argument("--host", default="127.0.0.1", help="API host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="API port (default: 8000)")
    parser.add_argument("--dev", action="store_true", help="Also start frontend Vite dev server concurrently")
    parser.add_argument("--demo", action="store_true", default=True, help="Enable LIFELINE demo mode (default: True)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    frontend_dir = project_root / "frontend"

    if args.demo:
        os.environ["LIFELINE_DEMO"] = "1"

    print("=" * 60)
    print("           LIFELINE FULLSTACK PLATFORM")
    print("=" * 60)
    print(f"Backend API:       http://{args.host}:{args.port}")
    print(f"Interactive Docs:  http://{args.host}:{args.port}/docs")
    print(f"Frontend Root:     http://{args.host}:{args.port}/")
    print(f"Operational View:  http://{args.host}:{args.port}/dashboard")
    print("=" * 60)

    processes = []

    try:
        if args.dev and frontend_dir.exists():
            print("\nStarting Vite Frontend Dev Server...")
            npm_cmd = "npm.cmd" if sys.platform == "win32" else "npm"
            dev_env = os.environ.copy()
            dev_env["VITE_API_BASE_URL"] = f"http://{args.host}:{args.port}/api/v1"
            p_fe = subprocess.Popen([npm_cmd, "run", "dev"], cwd=str(frontend_dir), env=dev_env)
            processes.append(p_fe)
            time.sleep(2)
            print("Frontend Dev Server active at http://localhost:5173")

        import uvicorn
        from app.main import app
        uvicorn.run(app, host=args.host, port=args.port, log_level="info")

    except KeyboardInterrupt:
        print("\nShutting down fullstack processes...")
    finally:
        for p in processes:
            p.terminate()
            try:
                p.wait(timeout=3)
            except Exception:
                p.kill()
        print("All processes stopped.")


if __name__ == "__main__":
    main()
