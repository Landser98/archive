# src/halyk_ind/batch_parse.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Batch / single-file parser for Halyk Bank type B (individual) statements."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .parser import parse_halyk_b_statement
from src.utils.statement_validation import (
    validate_statement_generic,
    HALYK_INDIVIDUAL_SCHEMA,
)
from src.utils.income_calc import compute_ip_income
# ✅ NEW: импортируем генератор JSONL
from src.utils.convert_pdf_json_pages import dump_pdf_pages



def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _stem_key(pdf_path: Path) -> str:
    """Возвращаем базовый stem для файлов: '1 (1).pdf' -> '1 (1)'"""
    return pdf_path.stem


def _json_default(obj: Any) -> Any:
    """Safe JSON encoder for exotic types (Decimal, etc.)."""
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def process_one_pdf(
    pdf_path: Path,
    *,
    jsonl_dir: Path,
    out_dir: Path,
    months_back: Optional[int] = None,
    verbose: bool = True,
) -> None:
    stem = _stem_key(pdf_path)

    # ✅ JSONL target path for this PDF
    jsonl_path = jsonl_dir / f"{stem}_pages.jsonl"

    # ✅ If JSONL is missing – generate it via dump_pdf_pages()
    if not jsonl_path.is_file():
        if verbose:
            print(f"    [INFO] JSONL not found for {pdf_path.name}, generating...")
            print(f"          -> {jsonl_path}")
        try:
            # jsonl_dir уже существует (проверяется в main),
            # просто просим dump_pdf_pages писать ровно туда
            dump_pdf_pages(pdf_path=pdf_path, out_path=jsonl_path)
        except Exception as e:
            print(f"    [ERROR] Failed to generate JSONL for {pdf_path.name}: {e}")
            return

    if verbose:
        print(f"=== Processing Halyk individual: {pdf_path.name} ===")
        print(f"    JSONL: {jsonl_path}")

    # 1) parse statement (with description enrichment from PDF)
    header_df, tx_df, footer_df = parse_halyk_b_statement(
        str(jsonl_path),
        pdf_path=str(pdf_path),
    )

    # ✅ footer_df может быть list[dict] из parse_footers → приводим к DataFrame
    if isinstance(footer_df, list):
        footer_df = pd.DataFrame(footer_df)
    elif not isinstance(footer_df, pd.DataFrame):
        footer_df = pd.DataFrame()

    # 2) numeric validation
    flags, debug_info = validate_statement_generic(
        header_df,
        tx_df,
        footer_df,
        schema=HALYK_INDIVIDUAL_SCHEMA,
    )

    # 3) ensure KNP column exists
    if "КНП" not in tx_df.columns:
        tx_df["КНП"] = ""

    # 4) compute IP income
    # ⚠️ Дату не переопределяем паттернами — используем дефолты из income_calc.
    enriched_tx, monthly_income, income_summary = compute_ip_income(
        tx_df,
        col_op_date="Дата проведения операции",
        col_credit="Приход в валюте счета",
        col_knp="КНП",
        col_purpose="Описание операции",
        col_counterparty="Описание операции",
        months_back=months_back,
        verbose=verbose,
        max_examples=5,
    )

    # 5) build meta
    meta: Dict[str, Any] = {
        "pdf_name": pdf_path.name,
        "pdf_path": str(pdf_path),
        "jsonl_path": str(jsonl_path),
        "flags": flags,
        "validation_debug": debug_info,
    }

    # 6) save outputs
    _ensure_dir(out_dir)
    out_header = out_dir / f"{stem}_header.csv"
    out_tx = out_dir / f"{stem}_tx.csv"
    out_footer = out_dir / f"{stem}_footer.csv"
    out_meta = out_dir / f"{stem}_meta.json"
    out_tx_ip = out_dir / f"{stem}_tx_ip.csv"
    out_ip_monthly = out_dir / f"{stem}_ip_income_monthly.csv"
    out_income_summary = out_dir / f"{stem}_income_summary.csv"
    income_summary_df = pd.DataFrame([income_summary])

    header_df.to_csv(out_header, index=False)
    tx_df.to_csv(out_tx, index=False)
    footer_df.to_csv(out_footer, index=False)
    enriched_tx.to_csv(out_tx_ip, index=False)
    monthly_income.to_csv(out_ip_monthly, index=False)
    income_summary_df.to_csv(out_income_summary, index=False)

    with open(out_meta, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    if verbose:
        print(f"    ✅ header:    {out_header}")
        print(f"    ✅ tx:        {out_tx}")
        print(f"    ✅ footer:    {out_footer}")
        print(f"    ✅ meta:      {out_meta}")
        print(f"    ✅ tx_ip:     {out_tx_ip}")
        print(f"    ✅ ip_month:  {out_ip_monthly}")
        print(f"    ✅ income summary: {out_income_summary}")
        print(f"    ✅ Adjusted income: {income_summary['total_income_adjusted']:,.2f}")


def _cli_parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Batch parser for Halyk Bank type B (individual) statements.",
    )
    ap.add_argument("root", help="Root dir with PDFs or a single PDF file")
    ap.add_argument(
        "--jsonl-dir",
        help="Folder to store/load *_pages.jsonl (default: <base_root>/converted_jsons)",
    )
    ap.add_argument(
        "--out-dir",
        help="Output folder for CSVs (default: <base_root>_out)",
    )
    ap.add_argument("--months-back", type=int, default=12)
    ap.add_argument("--no-verbose", action="store_true")

    return ap.parse_args()


def main() -> None:
    args = _cli_parse_args()
    from pathlib import Path

    root = Path(args.root)

    # Decide what is "base_dir" and which PDFs to process
    if root.is_file():
        pdf_files = [root]
        base_dir = root.parent
    else:
        if not root.is_dir():
            raise SystemExit(f"Root path not found or not a directory: {root}")
        # Adjust glob if your script used something else originally
        pdf_files = sorted(root.rglob("*.pdf"))
        base_dir = root

    if not pdf_files:
        raise SystemExit(f"No PDF files found under {root}")

    # Always use the folder name as base_name
    base_name = base_dir.name

    # Smart defaults:
    #   out_dir     = <parent_of_base_dir>/<base_name>_out
    #   jsonl_dir   = <base_dir>/converted_jsons
    out_dir = Path(args.out_dir) if args.out_dir else base_dir.parent / f"{base_name}_out"
    jsonl_dir = Path(args.jsonl_dir) if args.jsonl_dir else base_dir / "converted_jsons"

    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(pdf_files)} Halyk IND PDF(s) under {root}")
    print(f"CSV output dir:    {out_dir}")
    print(f"Pages JSONL dir:   {jsonl_dir}")

    for pdf in pdf_files:
        process_one_pdf(
            pdf,
            out_dir=out_dir,
            jsonl_dir=jsonl_dir,
            months_back=args.months_back,
            verbose=not args.no_verbose,
        )


if __name__ == "__main__":
    main()
