import argparse
import subprocess
from pathlib import Path
from shutil import which

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CSS_INPUT = PROJECT_ROOT / "src" / "teelo" / "web" / "static" / "css" / "input.css"
CSS_OUTPUT = PROJECT_ROOT / "src" / "teelo" / "web" / "static" / "css" / "styles.css"
TAILWIND_BINARY = PROJECT_ROOT / "tailwindcss"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="teelo")
    subparsers = parser.add_subparsers(dest="command")

    web_parser = subparsers.add_parser("web", help="Run the Teelo web server")
    web_parser.add_argument("--host", default="0.0.0.0", help="Host to bind (default: 0.0.0.0)")
    web_parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")

    css_parser = subparsers.add_parser("css", help="Build Tailwind CSS")
    css_parser.add_argument("--watch", action="store_true", help="Watch for changes")

    model_parser = subparsers.add_parser("model", help="Manage ML models in S3")
    model_parser.set_defaults(model_parser=model_parser)
    model_subparsers = model_parser.add_subparsers(dest="model_command")

    model_push_parser = model_subparsers.add_parser(
        "push", help="Upload model and metadata to S3",
    )
    model_push_parser.add_argument("local_path", help="Local model file path")

    model_pull_parser = model_subparsers.add_parser(
        "pull", help="Download model and metadata from S3",
    )
    model_pull_parser.add_argument("remote_name", help="Remote model filename")

    model_subparsers.add_parser("list", help="List available model files in S3")

    features_backfill_parser = subparsers.add_parser(
        "features-backfill", help="Recompute features for all matches using the latest preset"
    )
    features_backfill_parser.add_argument("--preset", default=None, help="Override preset name")
    features_backfill_parser.add_argument("--feature-set", default=None, help="Override feature set name")

    predictions_backfill_parser = subparsers.add_parser(
        "predictions-backfill", help="Regenerate predictions for all completed matches"
    )
    predictions_backfill_parser.add_argument("--model", default=None, help="Model file path")
    predictions_backfill_parser.add_argument("--feature-set", default=None, help="Override feature set name")

    predictions_live_parser = subparsers.add_parser(
        "predictions-live", help="Generate predictions for upcoming/scheduled matches"
    )
    predictions_live_parser.add_argument("--model", default=None, help="Model file path")
    predictions_live_parser.add_argument("--feature-set", default=None, help="Override feature set name")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "web":
        import uvicorn
        uvicorn.run("teelo.web.main:app", host=args.host, port=args.port, reload=True)
        return 0

    if args.command == "css":
        if which("npx"):
            cmd = ["npx", "tailwindcss"]
        elif TAILWIND_BINARY.exists():
            cmd = [str(TAILWIND_BINARY)]
        else:
            print("Tailwind CSS build tool not found. Install Node/npm or restore the local tailwindcss binary.")
            return 1

        cmd.extend(["-i", str(CSS_INPUT), "-o", str(CSS_OUTPUT), "--minify"])
        if args.watch:
            cmd.append("--watch")
        return subprocess.call(cmd, cwd=PROJECT_ROOT)

    if args.command == "model":
        from teelo.storage import download_model, list_models, upload_model

        if args.model_command == "push":
            model_key, meta_key = upload_model(args.local_path)
            print(f"Uploaded: {model_key}")
            print(f"Uploaded: {meta_key}")
            return 0

        if args.model_command == "pull":
            model_path, meta_path = download_model(args.remote_name)
            print(f"Downloaded: {model_path}")
            print(f"Downloaded: {meta_path}")
            return 0

        if args.model_command == "list":
            models = list_models()
            if not models:
                print("No models found in S3 bucket.")
                return 0
            for model_name in models:
                print(model_name)
            return 0

        args.model_parser.print_help()
        return 1

    if args.command == "features-backfill":
        from teelo.features import build_registry, latest_preset
        from teelo.features.engine import FeatureEngine

        preset = args.preset or latest_preset()
        feature_set_name = args.feature_set or preset

        registry = build_registry(preset)
        engine = FeatureEngine(registry=registry, feature_set_name=feature_set_name)
        engine.run(backfill=True)
        return 0

    if args.command == "predictions-backfill":
        from teelo.ml.predictor import BatchPredictor

        predictor = BatchPredictor(args.model, args.feature_set, backfill=True)
        count = predictor.predict()
        print(f"Predicted {count} matches")
        return 0

    if args.command == "predictions-live":
        from teelo.ml.predictor import BatchPredictor

        predictor = BatchPredictor(args.model, args.feature_set, backfill=False)
        count = predictor.predict()
        print(f"Predicted {count} matches")
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
