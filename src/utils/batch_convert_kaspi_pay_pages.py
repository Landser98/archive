#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch-конвертер для Kaspi Pay:
- ищет PDF в указанной папке
- для каждого PDF делает *_pages.jsonl
- кладёт результат в отдельную папку:
    DATA_DIR / "converted_jsons" / "kaspi_pay"
"""

import sys
from pathlib import Path
import argparse

from src.utils.convert_pdf_json_pages import dump_pdf_pages, DATA_DIR


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Batch for Kaspi Pay: scan a directory for PDFs and dump per-page JSONL "
            "to DATA_DIR/converted_jsons/kaspi_pay."
        )
    )
    ap.add_argument(
        "root",
        help="Root directory to scan for PDFs (e.g. 'data/kaspi_pay')",
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

    out_dir = DATA_DIR / "converted_jsons" / "kaspi_pay"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(pdf_paths)} PDF file(s) under {root}")
    print(f"JSONL will be written to: {out_dir}")

    for i, pdf_path in enumerate(pdf_paths, start=1):
        try:
            print(f"\n[{i}/{len(pdf_paths)}] Processing: {pdf_path}")
            out_path = out_dir / f"{pdf_path.stem}_pages.jsonl"
            written = dump_pdf_pages(pdf_path=pdf_path, out_path=out_path)
            print(f"   → Written: {written}")
        except Exception as e:
            print(f"   ❌ Failed for {pdf_path}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
