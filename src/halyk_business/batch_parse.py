#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch / single-file parser for Halyk Bank type A (business) statements + IP income.

Pipeline per PDF:
  1) ensure *_pages.jsonl (pdfplumber pages dump) exists
  2) ensure <stem>.json (PDF metadata + pages structure) exists
  3) parse header / tx / footer via parse_halyk_statement
  4) numeric checks via generic statement_validation + HALYK_BUSINESS schema
  5) PDF metadata validation
  6) UI Analysis Tables: Top-9 debit/credit, Related Parties (Net)
  7) ensure KNP column, compute IP income
  8) save CSVs:
       <stem>_header.csv
       <stem>_tx.csv
       <stem>_footer.csv
       <stem>_meta.csv
       <stem>_tx_ip.csv
       <stem>_ip_income_monthly.csv
       <stem>_ui_debit_top_9.csv
       <stem>_ui_credit_top_9.csv
       <stem>_ui_related_parties_net.csv
"""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import pikepdf

from src.utils import warnings_setup  # noqa: F401
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
from src.ui.ui_analysis_report_generator import get_ui_analysis_tables


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def ensure_jsonl_for_pdf(pdf_path: Path, jsonl_dir: Path, suffix: str = "_pages.jsonl") -> Path:
    jsonl_dir.mkdir(parents=True, exist_ok=True)
    out_path = jsonl_dir / f"{pdf_path.stem}{suffix}"
    if out_path.exists():
        return out_path
    print(f"[jsonl] Creating {out_path.name} from {pdf_path.name}")
    dump_pdf_pages(pdf_path=pdf_path, out_path=out_path)
    return out_path


def ensure_pdf_meta_json(pdf_path: Path, meta_dir: Path) -> Path:
    meta_dir.mkdir(parents=True, exist_ok=True)
    json_path = meta_dir / f"{pdf_path.stem}.json"
    if json_path.exists():
        return json_path
    print(f"[meta-json] Creating {json_path.name} from {pdf_path.name}")
    with pikepdf.open(str(pdf_path)) as pdf:
        out = {"file": str(pdf_path), "num_pages": len(pdf.pages)}
        out.update(dump_catalog(pdf, max_depth=6, include_streams=False, stream_max_bytes=0))
        out.update(dump_pages(pdf, max_depth=6, include_streams=False, stream_max_bytes=0))
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=_json_default)
    return json_path


# ---------------------------------------------------------------------------
# Processing Logic
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
    print(f"\n=== Processing Halyk business: {pdf_path.name} ===")

    # 1) ensure pages JSONL
    jsonl_path = ensure_jsonl_for_pdf(pdf_path, jsonl_dir, suffix=jsonl_suffix)

    # 2) parse header / tx / footer
    header_df, tx_df, footer_df = parse_halyk_statement(str(jsonl_path))

    if "Исходящий_остаток" in footer_df.columns and "Исходящий_остаток" not in header_df.columns:
        header_df = header_df.copy()
        header_df["Исходящий_остаток"] = footer_df["Исходящий_остаток"]

    if header_df.empty or footer_df.empty:
        raise ValueError(f"Empty header or footer for {pdf_path.name}")

    extra_flags: List[str] = []
    if tx_df.empty:
        print(f"[WARN] No transactions parsed for {pdf_path.name}")
        tx_df = pd.DataFrame(columns=["Дата", "Дебет", "Кредит", "Детали платежа", "Контрагент (имя)"])
        extra_flags.append("no_tx_rows_parsed")

    # 3) numeric validation
    schema = BANK_SCHEMAS.get("HALYK_BUSINESS")
    num_flags, num_debug = validate_statement_generic(header_df, tx_df, footer_df, schema)

    # 4) PDF metadata validation
    meta_json_path = ensure_pdf_meta_json(pdf_path, pdf_meta_dir)
    try:
        with meta_json_path.open("r", encoding="utf-8") as f:
            pdf_json = json.load(f)
        period_end = header_df.iloc[0].get("period_end")
        pdf_flags, pdf_debug = validate_pdf_metadata_from_json(
            pdf_json, bank="HALYK_BUSINESS", period_end=period_end, period_date_format="%d.%m.%Y"
        )
    except Exception as e:
        pdf_flags, pdf_debug = ["pdf_meta_validation_error"], {"error": str(e)}

    # 5) Time Window
    hdr = header_df.iloc[0]
    stmt_dt_raw = hdr.get("Дата выписки") or hdr.get("period_end")
    stmt_dt = pd.to_datetime(stmt_dt_raw, dayfirst=True, errors="coerce")
    if pd.isna(stmt_dt) and not tx_df.empty:
        stmt_dt = pd.to_datetime(tx_df["Дата"], dayfirst=True, errors="coerce").max()

    if pd.isna(stmt_dt):
        raise ValueError("Cannot determine anchor date.")

    window_start, window_end = get_last_full_12m_window(stmt_dt.date())

    # 6) IP income & Filtering
    tx_raw_df = tx_df.copy()
    tx_df = tx_df.copy()
    tx_df["txn_date"] = pd.to_datetime(tx_df["Дата"], dayfirst=True, errors="coerce")
    tx_df = tx_df[tx_df["txn_date"].notna()]
    tx_df = tx_df[(tx_df["txn_date"] >= pd.Timestamp(window_start)) & (tx_df["txn_date"] <= pd.Timestamp(window_end))]

    if "КНП" not in tx_df.columns:
        tx_df["КНП"] = ""

    enriched_tx, monthly_income, income_summary = compute_ip_income(
        tx_df, col_op_date="Дата", col_credit="Кредит", col_knp="КНП",
        col_purpose="Детали платежа", col_counterparty="Контрагент (имя)",
        verbose=False
    )

    # 7) UI ANALYSIS TABLES (Top-9 & Related Parties Net)
    ui_input_df = tx_df.copy()

    def clean_val(v):
        if pd.isna(v) or v == '': return 0.0
        if isinstance(v, (int, float)): return float(v)
        return float(str(v).replace(',', '').replace(' ', '').replace('\xa0', '').strip())

    # Генерируем 'amount' (Кредит - Дебет)
    ui_input_df['amount'] = ui_input_df['Кредит'].apply(clean_val) - ui_input_df['Дебет'].apply(clean_val)

    # Мапим БИН и Имя
    if 'Контрагент (БИН/ИИН)' in ui_input_df.columns:
        ui_input_df['counterparty_id'] = ui_input_df['Контрагент (БИН/ИИН)'].fillna('N/A')
    elif 'БИН' in ui_input_df.columns:
        ui_input_df['counterparty_id'] = ui_input_df['БИН'].fillna('N/A')
    else:
        ui_input_df['counterparty_id'] = ui_input_df['Контрагент (имя)'].fillna('N/A')

    ui_input_df['counterparty_name'] = ui_input_df['Контрагент (имя)'].fillna('N/A')
    ui_input_df['details'] = ui_input_df['Детали платежа'].fillna('')

    ui_tables = get_ui_analysis_tables(ui_input_df)

    # 8) Paths and Saving
    stem = pdf_path.stem
    out_dir.mkdir(parents=True, exist_ok=True)

    header_df.to_csv(out_dir / f"{stem}_header.csv", index=False, encoding="utf-8-sig")
    tx_raw_df.to_csv(out_dir / f"{stem}_tx.csv", index=False, encoding="utf-8-sig")
    footer_df.to_csv(out_dir / f"{stem}_footer.csv", index=False, encoding="utf-8-sig")

    all_flags = num_flags + pdf_flags + extra_flags
    meta_df = pd.DataFrame([{"bank": "HALYK_BUSINESS", "pdf_file": pdf_path.name, "flags": ";".join(all_flags)}])
    meta_df.to_csv(out_dir / f"{stem}_meta.csv", index=False, encoding="utf-8-sig")

    enriched_tx.to_csv(out_dir / f"{stem}_tx_ip.csv", index=False, encoding="utf-8-sig")
    monthly_income.to_csv(out_dir / f"{stem}_ip_income_monthly.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame([income_summary]).to_csv(out_dir / f"{stem}_income_summary.csv", index=False, encoding="utf-8-sig")

    # Save UI Tables
    pd.DataFrame(ui_tables["debit_top"]).to_csv(out_dir / f"{stem}_ui_debit_top_9.csv", index=False,
                                                encoding="utf-8-sig")
    pd.DataFrame(ui_tables["credit_top"]).to_csv(out_dir / f"{stem}_ui_credit_top_9.csv", index=False,
                                                 encoding="utf-8-sig")
    pd.DataFrame(ui_tables["related_parties"]).to_csv(out_dir / f"{stem}_ui_related_parties_net.csv", index=False,
                                                      encoding="utf-8-sig")

    print(f"  → UI Tables: Top-9 & Related Net CSVs generated")
    print(f"✅ Adjusted income: {income_summary.get('total_income_adjusted', 0):,.2f} KZT")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Batch-parse Halyk Business statements.")
    ap.add_argument("path", help="PDF file or directory")
    ap.add_argument("--pattern", default="*.pdf")
    ap.add_argument("--out-dir", help="Output directory")
    ap.add_argument("--jsonl-dir", help="JSONL directory")
    ap.add_argument("--pdf-meta-dir", help="PDF meta directory")
    ap.add_argument("--months-back", type=int, default=12)
    ap.add_argument("--no-verbose", action="store_true")

    args = ap.parse_args()
    in_path = Path(args.path)
    pdf_files = [in_path] if in_path.is_file() else sorted(in_path.rglob(args.pattern))

    base_dir = in_path.parent if in_path.is_file() else in_path
    out_dir = Path(args.out_dir) if args.out_dir else base_dir / "out"
    jsonl_dir = Path(args.jsonl_dir) if args.jsonl_dir else base_dir / "converted_jsons"
    pdf_meta_dir = Path(args.pdf_meta_dir) if args.pdf_meta_dir else base_dir / "pdf_meta"

    for pdf in pdf_files:
        try:
            process_one_halyk_a(
                pdf_path=pdf, jsonl_dir=jsonl_dir, out_dir=out_dir,
                pdf_meta_dir=pdf_meta_dir, months_back=args.months_back, verbose=not args.no_verbose
            )
        except Exception as e:
            print(f"❌ Failed to process {pdf.name}: {e}")


if __name__ == "__main__":
    main()