"""One-time script to backfill predictions on historical completed matches."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from teelo.ml.predictor import BatchPredictor
from teelo.ml.metrics import compute_snapshot


def main():
    print("Running backfill predictions on historical matches...")
    predictor = BatchPredictor(backfill=True)
    count = predictor.predict()
    print(f"Backfilled {count} matches")

    print("Computing metrics snapshots...")
    for source in ("live", "backfill", "all"):
        compute_snapshot(source_filter=source)
    print("Done.")


if __name__ == "__main__":
    main()
