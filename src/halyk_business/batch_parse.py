# src/halyk_business/batch_parse.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch / single-file parser for Halyk Bank type A (business) statements + IP income.

Pipeline per PDF:
  1) ensure *_pages.jsonl (pdfplumber pages dump) exists
  2) ensure <stem>.json (PDF metadata + pages structure) exists
  3) parse header / tx / footer via parse_halyk_statement
  4) numeric checks via generic statement_validation + HALYK_BUSINESS schema (если настроен)
  5) PDF metadata validation (creation/mod dates, creator/producer, etc.)
  6) ensure KNP column (если нет), compute IP income
  7) save CSVs:
       <stem>_header.csv
       <stem>_tx.csv
       <stem>_footer.csv
       <stem>_meta.csv
       <stem>_tx_ip.csv
       <stem>_ip_income_monthly.csv
"""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import pikepdf

from src.utils import warnings_setup  # noqa: F401  (side-effect: suppress warnings)

from src.halyk_business.parser import parse_halyk_statement
from src.utils.income_calc import compute_ip_income
from src.utils.statement_validation import (
    BANK_SCHEMAS,
    validate_statement_generic,
    validate_pdf_metadata_from_json,
)
from src.utils.convert_pdf_json_pages import dump_pdf_pages
from src.utils.convert_pdf_json_page import dump_catalog, dump_pages

from src.core.analysis import get_last_full_12m_window
# ---------------------------------------------------------------------------
# Helpers: JSONL pages + PDF meta JSON
# ---------------------------------------------------------------------------


def _json_default(obj: Any) -> Any:
    """
    Safe JSON encoder for pikepdf/Decimal/etc.
    """
    if isinstance(obj, Decimal):
        return float(obj)
    # add more types if pikepdf returns something exotic
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def ensure_jsonl_for_pdf(
    pdf_path: Path,
    jsonl_dir: Path,
    suffix: str = "_pages.jsonl",
) -> Path:
    """
    Ensure we have pdfplumber-style pages JSONL for this PDF.

    If missing, create it via dump_pdf_pages().
    """
    jsonl_dir.mkdir(parents=True, exist_ok=True)
    out_path = jsonl_dir / f"{pdf_path.stem}{suffix}"

    if out_path.exists():
        return out_path

    print(f"[jsonl] Creating {out_path.name} from {pdf_path.name}")
    dump_pdf_pages(
        pdf_path=pdf_path,
        out_path=out_path,
        stream_preview_len=4000,
        include_full_stream=False,
    )
    return out_path


def ensure_pdf_meta_json(pdf_path: Path, meta_dir: Path) -> Path:
    """
    Ensure we have a single JSON with PDF metadata + pages structure
    for pdf_path in meta_dir.

    Uses pikepdf + dump_catalog + dump_pages from convert_pdf_json_page.py.
    """
    meta_dir.mkdir(parents=True, exist_ok=True)
    json_path = meta_dir / f"{pdf_path.stem}.json"

    if json_path.exists():
        return json_path

    print(f"[meta-json] Creating {json_path.name} from {pdf_path.name}")

    with pikepdf.open(str(pdf_path)) as pdf:
        out: Dict[str, Any] = {
            "file": str(pdf_path),
            "num_pages": len(pdf.pages),
        }
        out.update(
            dump_catalog(
                pdf,
                max_depth=6,
                include_streams=False,
                stream_max_bytes=0,
            )
        )
        out.update(
            dump_pages(
                pdf,
                max_depth=6,
                include_streams=False,
                stream_max_bytes=0,
            )
        )

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=_json_default)

    return json_path


# ---------------------------------------------------------------------------
# Per-PDF processing
# ---------------------------------------------------------------------------


def process_one_halyk_a(
    pdf_path: Path,
    jsonl_dir: Path,
    out_dir: Path,
    pdf_meta_dir: Path,
    jsonl_suffix: str = "_pages.jsonl",
    months_back: Optional[int] = 12,
    verbose: bool = True,
) -> None:
    """
    Full pipeline for a single Halyk Bank business statement PDF.
    """
    print(f"\n=== Processing Halyk business: {pdf_path.name} ===")

    # 1) ensure pages JSONL
    jsonl_path = ensure_jsonl_for_pdf(pdf_path, jsonl_dir, suffix=jsonl_suffix)

    # 2) parse header / tx / footer from JSONL
    header_df, tx_df, footer_df = parse_halyk_statement(str(jsonl_path))

    # чтобы generic-валидатор видел closing в header_df
    if "Исходящий_остаток" in footer_df.columns and "Исходящий_остаток" not in header_df.columns:
        header_df = header_df.copy()
        header_df["Исходящий_остаток"] = footer_df["Исходящий_остаток"]

    # header / footer должны быть всегда
    if header_df.empty:
        raise ValueError(f"Empty header_df for {pdf_path.name}")
    if footer_df.empty:
        raise ValueError(f"Empty footer_df for {pdf_path.name}")

    # дополнительные флаги на уровень meta (для нестандартных кейсов)
    extra_flags: List[str] = []

    # единственный проблемный стейтмент → tx_df пустой
    # вместо падения — мягкое предупреждение и флаг
    if tx_df.empty:
        print(f"[WARN] No transactions parsed for {pdf_path.name} (tx_df is empty)")
        tx_df = pd.DataFrame(
            columns=[
                "Дата",
                "Дебет",
                "Кредит",
                "Детали платежа",
                "Контрагент (имя)",
            ]
        )
        extra_flags.append("no_tx_rows_parsed")

    # quick sanity for tx columns
    required_cols = [
        "Дата",
        "Дебет",
        "Кредит",
        "Детали платежа",
        "Контрагент (имя)",
    ]
    missing = [c for c in required_cols if c not in tx_df.columns]
    if missing:
        raise ValueError(f"Missing columns in tx_df for {pdf_path.name}: {missing}")

    # 3) numeric validation через generic schema (если сконфигурирован)
    num_flags: List[str] = []
    num_debug: Dict[str, Any] = {}

    schema = BANK_SCHEMAS.get("HALYK_BUSINESS")
    num_flags, num_debug = validate_statement_generic(
        header_df,
        tx_df,
        footer_df,
        schema,
    )

    # 4) PDF metadata validation
    pdf_flags: List[str] = []
    pdf_debug: Dict[str, Any] = {}

    meta_json_path = ensure_pdf_meta_json(pdf_path, pdf_meta_dir)
    try:
        with meta_json_path.open("r", encoding="utf-8") as f:
            pdf_json = json.load(f)

        # подстрой, если в header другое имя для конца периода
        period_end = header_df.iloc[0].get("period_end")

        pdf_flags, pdf_debug = validate_pdf_metadata_from_json(
            pdf_json,
            bank="HALYK_BUSINESS",
            period_end=period_end,
            period_date_format="%d.%m.%Y",
            max_days_after_period_end=7,
            allowed_creators=None,   # можно сузить, когда увидим реальные значения
            allowed_producers=None,
        )
    except Exception as e:
        pdf_flags = ["pdf_meta_validation_error"]
        pdf_debug = {"error": str(e), "meta_json_path": str(meta_json_path)}

    # 5) КНП (если нет — пустая строка)
    if "КНП" not in tx_df.columns:
        tx_df["КНП"] = ""
    # pick a reliable statement date to define the "last full month" window
    hdr = header_df.iloc[0]

    # 1) try explicit statement generation date
    stmt_dt_raw = (
            hdr.get("Дата выписки")
            or hdr.get("Дата формирования")
            or hdr.get("statement_generation_date")
            or hdr.get("statement_date")
    )

    stmt_dt = pd.to_datetime(stmt_dt_raw, dayfirst=True, errors="coerce") if stmt_dt_raw else pd.NaT

    # 2) fallback: period end (your parsers often store it under one of these)
    if pd.isna(stmt_dt):
        period_end_raw = (
                hdr.get("Период по")
                or hdr.get("Период (конец)")
                or hdr.get("period_to")
                or hdr.get("period_end")
        )
        stmt_dt = pd.to_datetime(period_end_raw, dayfirst=True, errors="coerce") if period_end_raw else pd.NaT

    # 3) fallback: latest txn date (most reliable if header is messy)
    if pd.isna(stmt_dt):
        tmp_dates = pd.to_datetime(tx_df["Дата"], dayfirst=True, errors="coerce")
        if tmp_dates.notna().any():
            stmt_dt = tmp_dates.max()

    if pd.isna(stmt_dt):
        raise ValueError(
            "Cannot determine anchor date: header has no statement date/period_end and tx_df dates are empty."
        )

    anchor = stmt_dt.date()
    window_start, window_end = get_last_full_12m_window(anchor)

    # 6) IP income
    # filter tx_df to the window (IMPORTANT)
    tx_df = tx_df.copy()
    tx_raw_df = tx_df.copy()

    tx_df["txn_date"] = pd.to_datetime(tx_df["Дата"], dayfirst=True, errors="coerce")
    tx_df = tx_df[tx_df["txn_date"].notna()]
    tx_df = tx_df[(tx_df["txn_date"] >= pd.Timestamp(window_start)) & (tx_df["txn_date"] <= pd.Timestamp(window_end))]

    enriched_tx, monthly_income, income_summary = compute_ip_income(
        tx_df,
        col_op_date="Дата",
        col_credit="Кредит",
        col_knp="КНП",
        col_purpose="Детали платежа",
        col_counterparty="Контрагент (имя)",
        months_back=None,  # ✅ window already applied
        statement_generation_date=None,  # ✅ don’t re-filter inside compute_ip_income
        verbose=False,
    )

    # 7) meta_df (numeric + pdf flags + debug_info)
    all_flags = num_flags + pdf_flags + extra_flags
    all_debug: Dict[str, Any] = {
        "numeric": num_debug,
        "pdf_meta": pdf_debug,
        "jsonl_file": str(jsonl_path),
    }
    if extra_flags:
        all_debug["extra_flags"] = extra_flags

    meta_df = pd.DataFrame(
        [{
            "bank": "HALYK_BUSINESS",
            "pdf_file": pdf_path.name,
            "jsonl_file": jsonl_path.name,
            "flags": ";".join(all_flags),
            "debug_info": json.dumps(all_debug, ensure_ascii=False),
        }]
    )

    # 8) paths
    stem = pdf_path.stem
    header_path = out_dir / f"{stem}_header.csv"
    tx_path = out_dir / f"{stem}_tx.csv"
    footer_path = out_dir / f"{stem}_footer.csv"
    meta_path = out_dir / f"{stem}_meta.csv"
    enriched_path = out_dir / f"{stem}_tx_ip.csv"
    monthly_path = out_dir / f"{stem}_ip_income_monthly.csv"
    income_summary_path = out_dir / f"{stem}_income_summary.csv"
    # dict → one-row DataFrame
    income_summary_df = pd.DataFrame([income_summary])

    # 9) save CSVs
    out_dir.mkdir(parents=True, exist_ok=True)

    header_df.to_csv(header_path, index=False, encoding="utf-8-sig")
    tx_df.to_csv(tx_path, index=False, encoding="utf-8-sig")
    tx_raw_df.to_csv(tx_path, index=False, encoding="utf-8-sig")  # raw, full parsed tx

    footer_df.to_csv(footer_path, index=False, encoding="utf-8-sig")
    meta_df.to_csv(meta_path, index=False, encoding="utf-8-sig")
    enriched_tx.to_csv(enriched_path, index=False, encoding="utf-8-sig")
    monthly_income.to_csv(monthly_path, index=False, encoding="utf-8-sig")
    income_summary_df.to_csv(income_summary_path, index=False, encoding="utf-8-sig")

    print(f"  → Header:      {header_df.shape[0]} row   → {header_path}")
    print(f"  → Tx:          {tx_df.shape[0]} rows → {tx_path}")
    print(f"  → Footer:      {footer_df.shape[0]} row   → {footer_path}")
    print(f"  → Meta:        {meta_df.shape[0]} row   → {meta_path}")
    print(f"  → Tx+IP flags: {enriched_tx.shape[0]} rows → {enriched_path}")
    print(f"  → IP monthly:  {monthly_income.shape[0]} rows → {monthly_path}")
    print(f"  → Income summary: {income_summary_path}")
    adjusted = float(income_summary_df.loc[0, "total_income_adjusted"])
    print(f"✅ Adjusted income: {adjusted:,.2f} KZT")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Parse Halyk Bank type A (business) statements (single file or folder), "
            "auto-create pages JSONL and PDF meta JSON, validate, and compute IP income."
        )
    )
    ap.add_argument(
        "path",
        help="PDF file or directory with PDFs (e.g. data/halyk_business)",
    )
    ap.add_argument(
        "--pattern",
        default="*.pdf",
        help="Glob pattern when path is a directory (default: '*.pdf')",
    )
    ap.add_argument(
        "--jsonl-dir",
        help=(
            "Directory to store/load *_pages.jsonl "
            "(default: <path>/converted_jsons or <file_dir>/converted_jsons)"
        ),
    )
    ap.add_argument(
        "--pdf-meta-dir",
        help=(
            "Directory to store/load PDF metadata JSONs "
            "(default: <path>/pdf_meta or <file_dir>/pdf_meta)"
        ),
    )
    ap.add_argument(
        "--out-dir",
        help="Output directory for CSVs (default: <path>/out or <file_dir>/out)",
    )
    ap.add_argument(
        "--jsonl-suffix",
        default="_pages.jsonl",
        help="Suffix for JSONL filenames (default: '_pages.jsonl')",
    )
    ap.add_argument(
        "--months-back",
        type=int,
        default=12,
        help="How many last months to consider for IP income (default: 12)",
    )
    ap.add_argument(
        "--no-verbose",
        action="store_true",
        help="Disable detailed income_calc logging",
    )

    args = ap.parse_args()

    in_path = Path(args.path)

    if in_path.is_file():
        pdf_files = [in_path]
        base_dir = in_path.parent
        base_name = in_path.stem
    else:
        if not in_path.is_dir():
            raise SystemExit(f"Path not found or not a directory: {in_path}")
        pdf_files = sorted(in_path.rglob(args.pattern))
        base_dir = in_path
        base_name = in_path.name

    if not pdf_files:
        raise SystemExit(f"No PDF files found under {in_path} with pattern {args.pattern}")

    default_out_dir = base_dir.parent / f"{base_name}_out"
    out_dir = Path(args.out_dir) if args.out_dir else default_out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    jsonl_dir = Path(args.jsonl_dir) if args.jsonl_dir else base_dir / "converted_jsons"
    jsonl_dir.mkdir(parents=True, exist_ok=True)

    pdf_meta_dir = Path(args.pdf_meta_dir) if args.pdf_meta_dir else base_dir / "pdf_meta"
    pdf_meta_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(pdf_files)} Halyk business statement(s).")
    print(f"CSV output dir:   {out_dir}")
    print(f"Pages JSONL dir:  {jsonl_dir}")
    print(f"PDF meta dir:     {pdf_meta_dir}")

    for pdf in pdf_files:
        try:
            process_one_halyk_a(
                pdf_path=pdf,
                jsonl_dir=jsonl_dir,
                out_dir=out_dir,
                pdf_meta_dir=pdf_meta_dir,
                jsonl_suffix=args.jsonl_suffix,
                months_back=args.months_back,
                verbose=not args.no_verbose,
            )
        except Exception as e:
            print(f"❌ Failed to process {pdf.name}: {e}")


if __name__ == "__main__":
    main()
