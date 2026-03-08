import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CSS_INPUT = PROJECT_ROOT / "src" / "teelo" / "web" / "static" / "css" / "input.css"
CSS_OUTPUT = PROJECT_ROOT / "src" / "teelo" / "web" / "static" / "css" / "styles.css"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="teelo")
    subparsers = parser.add_subparsers(dest="command")

    web_parser = subparsers.add_parser("web", help="Run the Teelo web server")
    web_parser.add_argument("--host", default="0.0.0.0", help="Host to bind (default: 0.0.0.0)")
    web_parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")

    css_parser = subparsers.add_parser("css", help="Build Tailwind CSS")
    css_parser.add_argument("--watch", action="store_true", help="Watch for changes")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "web":
        import uvicorn
        uvicorn.run("teelo.web.main:app", host=args.host, port=args.port, reload=True)
        return 0

    if args.command == "css":
        cmd = ["npx", "tailwindcss", "-i", str(CSS_INPUT), "-o", str(CSS_OUTPUT), "--minify"]
        if args.watch:
            cmd.append("--watch")
        return subprocess.call(cmd, cwd=PROJECT_ROOT)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
