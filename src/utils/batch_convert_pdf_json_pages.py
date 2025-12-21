#!/usr/bin/env python3
# src/study_pdf/batch_convert_pdf_json_pages.py

import sys
from pathlib import Path
import argparse

# импортируем нашу функцию
from src.utils.convert_pdf_json_pages import dump_pdf_pages, DATA_DIR


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Batch: walk a directory, find all PDFs, and dump per-page JSONL "
            "to DATA_DIR/converted_jsons."
        )
    )
    ap.add_argument(
        "root",
        help="Root directory to scan for PDFs (e.g. 'data' or 'data/bcc')",
    )
    ap.add_argument(
        "--pattern",
        default="*.pdf",
        help="Glob pattern for PDFs (default: *.pdf). Used with rglob.",
    )
    ap.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional limit on number of PDFs to process (for testing).",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    root = Path(args.root)
    if not root.is_dir():
        raise SystemExit(f"Root is not a directory: {root}")

    pdf_paths = sorted(root.rglob(args.pattern))
    if not pdf_paths:
        print(f"⚠️ No PDFs found in {root} (pattern={args.pattern})")
        return

    if args.max_files is not None:
        pdf_paths = pdf_paths[: args.max_files]

    print(f"Found {len(pdf_paths)} PDF file(s) under {root}")
    print(f"JSONL will be written to: {DATA_DIR / 'converted_jsons'}")

    for i, pdf_path in enumerate(pdf_paths, start=1):
        try:
            print(f"\n[{i}/{len(pdf_paths)}] Processing: {pdf_path}")
            written = dump_pdf_pages(pdf_path=pdf_path, out_path=None)
            print(f"   → Written: {written}")
        except Exception as e:
            print(f"   ❌ Failed for {pdf_path}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
# python -m src.utils.batch_convert_pdf_json_pages data