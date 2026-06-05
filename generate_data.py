"""CLI to generate the FMCG training datasets.

Examples::

    uv run python generate_data.py                 # default ~500k sales rows
    uv run python generate_data.py --scale 2.0     # ~1M sales rows
    uv run python generate_data.py --clean         # no injected data issues
    uv run python generate_data.py --out data2     # custom output directory
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Make `utils` and `src/fmcg` importable when run as a plain script.
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from fmcg.catalog import GenConfig  # noqa: E402
from fmcg.generator import generate  # noqa: E402
from utils.spark import build_session  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate FMCG training datasets.")
    p.add_argument("--scale", type=float, default=1.0,
                   help="Volume multiplier for fact_sales (default 1.0 ~ 500k rows).")
    p.add_argument("--seed", type=int, default=42, help="Random seed.")
    p.add_argument("--out", default="data", help="Output directory (default: data).")
    p.add_argument("--clean", action="store_true",
                   help="Generate clean data with no injected quality issues.")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    out_dir = ROOT / args.out if not Path(args.out).is_absolute() else Path(args.out)

    print(f"Generating FMCG data (scale={args.scale}, seed={args.seed}, "
          f"clean={args.clean}) -> {out_dir}")
    spark = build_session("fmcg-generate")
    start = time.time()
    cfg = GenConfig(scale=args.scale, seed=args.seed)
    try:
        counts = generate(spark, out_dir, cfg, clean=args.clean)
    finally:
        spark.stop()

    total = sum(counts.values())
    elapsed = time.time() - start
    print(f"\nDone: {total:,} rows across {len(counts)} datasets in {elapsed:.1f}s.")
    print(f"Output: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
