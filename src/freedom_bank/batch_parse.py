# src/freedom_bank/batch_parse.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch / single-file parser for Freedom Bank statements + IP income.

Pipeline per PDF:
  1) ensure <stem>.json (PDF metadata + pages structure) exists
  2) parse header / tx / footer via freedom_bank.parser
  3) numeric checks (optional, via statement_validation + FREEDOM schema)
  4) PDF metadata validation (creation/mod dates, creator/producer, etc.)
  5) create KNP column (Freedom doesn't have KNP in the statement)
  6) compute IP income via compute_ip_income
  7) save CSVs:
       <stem>_header.csv
       <stem>_tx.csv
       <stem>_footer.csv
       <stem>_meta.csv
       <stem>_tx_ip.csv
       <stem>_ip_income_monthly.csv
"""

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import pikepdf
from decimal import Decimal

from src.utils import warnings_setup  # noqa: F401  (side-effect: suppress warnings)

from src.freedom_bank.parser import extract_header_footer, extract_transactions
from src.utils.income_calc import compute_ip_income
from src.utils.statement_validation import (
    BANK_SCHEMAS,
    validate_statement_generic,
    validate_pdf_metadata_from_json,
)
from src.utils.convert_pdf_json_page import dump_catalog, dump_pages


# ---------------------------------------------------------------------------
# Helper: PDF meta JSON
# ---------------------------------------------------------------------------


def _json_safe(obj):
    """
    Recursively convert objects (e.g. Decimal) into JSON-serializable types.
    """
    if isinstance(obj, Decimal):
        # or: return str(obj) if you prefer strings
        return float(obj)
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    return obj



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

    # make sure there are no Decimal (or other non-JSON) types
    out_clean = _json_safe(out)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out_clean, f, ensure_ascii=False, indent=2)

    return json_path



# ---------------------------------------------------------------------------
# Per-PDF processing
# ---------------------------------------------------------------------------

def process_one_freedom(
    pdf_path: Path,
    out_dir: Path,
    pdf_meta_dir: Path,
    months_back: int | None = 12,
    verbose: bool = True,
) -> None:
    """
    Process a single Freedom Bank PDF:
      - parse header / tx / footer
      - numeric validation (if FREEDOM schema configured)
      - PDF metadata validation
      - KNP column + IP income
      - save CSVs
    """
    print(f"\n=== Processing Freedom: {pdf_path.name} ===")

    # 1) parse header + footer
    header_df, footer_df = extract_header_footer(str(pdf_path))

    # 2) parse transactions
    tx_df = extract_transactions(str(pdf_path))

    # --- basic sanity for tx columns ---
    required_cols = [
        "Дата",
        "Кредит",
        "Назначение платежа",
        "Корреспондент",
    ]
    missing = [c for c in required_cols if c not in tx_df.columns]
    if missing:
        raise ValueError(f"Missing columns in tx_df for {pdf_path.name}: {missing}")

    # 3) numeric validation via generic schema (if configured)
    num_flags: List[str] = []
    num_debug: Dict[str, Any] = {}

    schema = BANK_SCHEMAS.get("FREEDOM")
    if schema is not None:
        # If schema expects turnover columns in header but we only have them in footer,
        # you can pre-fill them here:
        header_for_val = header_df.copy()
        if "credit_turnover" not in header_for_val.columns and "credit_total" in footer_df.columns:
            header_for_val.loc[0, "credit_turnover"] = footer_df.loc[0, "credit_total"]
        if "debit_turnover" not in header_for_val.columns and "debit_total" in footer_df.columns:
            header_for_val.loc[0, "debit_turnover"] = footer_df.loc[0, "debit_total"]

        try:
            num_flags, num_debug = validate_statement_generic(
                header_for_val,
                tx_df,
                footer_df,
                schema,
            )
        except Exception as e:
            num_flags = ["numeric_validation_error"]
            num_debug = {"error": str(e)}
    else:
        num_debug = {
            "note": "No 'FREEDOM' schema in BANK_SCHEMAS; numeric validation skipped."
        }

    # 4) PDF metadata validation
    pdf_flags: List[str] = []
    pdf_debug: Dict[str, Any] = {}

    meta_json_path = ensure_pdf_meta_json(pdf_path, pdf_meta_dir)
    try:
        with open(meta_json_path, "r", encoding="utf-8") as f:
            pdf_json = json.load(f)

        period_end = header_df.iloc[0].get("period_end")

        pdf_flags, pdf_debug = validate_pdf_metadata_from_json(
            pdf_json,
            bank="FREEDOM",
            period_end=period_end,
            period_date_format="%d.%m.%Y",
            max_days_after_period_end=7,
            allowed_creators=None,   # tighten once you know real values
            allowed_producers=None,
        )
    except Exception as e:
        pdf_flags = ["pdf_meta_validation_error"]
        pdf_debug = {"error": str(e), "meta_json_path": str(meta_json_path)}

    # 5) KNP column (Freedom doesn't have it in the statement)
    if "КНП" not in tx_df.columns:
        tx_df["КНП"] = ""

    # 6) IP income

    enriched_tx, monthly_income, income_summary = compute_ip_income(
        tx_df,
        col_op_date="Дата",
        col_credit="Кредит",
        col_knp="КНП",
        col_purpose="Назначение платежа",
        col_counterparty="Корреспондент",
        months_back=months_back,
        op_date_pattern=r"(\d{2}\.\d{2}\.\d{2})",  # 20.06.25
        op_date_format="%d.%m.%y",
        verbose=verbose,
        max_examples=5,
    )


    # 7) meta_df (numeric + pdf flags + debug_info)
    all_flags = num_flags + pdf_flags
    all_debug = {
        "numeric": num_debug,
        "pdf_meta": pdf_debug,
    }

    meta_df = pd.DataFrame(
        [{
            "bank": "FREEDOM",
            "pdf_file": pdf_path.name,
            "flags": ";".join(all_flags),
            "debug_info": json.dumps(all_debug, ensure_ascii=False),
        }]
    )

    # 8) paths
    stem = pdf_path.stem
    header_path   = out_dir / f"{stem}_header.csv"
    tx_path       = out_dir / f"{stem}_tx.csv"
    footer_path   = out_dir / f"{stem}_footer.csv"
    meta_path     = out_dir / f"{stem}_meta.csv"
    enriched_path = out_dir / f"{stem}_tx_ip.csv"
    monthly_path  = out_dir / f"{stem}_ip_income_monthly.csv"
    income_summary_path = out_dir / f"{stem}_income_summary.csv"
    # dict → one-row DataFrame for CSV
    income_summary_df = pd.DataFrame([income_summary])

    # 9) save CSVs
    out_dir.mkdir(parents=True, exist_ok=True)

    header_df.to_csv(header_path, index=False, encoding="utf-8-sig")
    tx_df.to_csv(tx_path, index=False, encoding="utf-8-sig")
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
    print(f"  → IP monthly:  {monthly_income.shape[0]} rows → {monthly_path}")
    print(f"  → Income summary: {income_summary_path}")
    print(f"✅ Adjusted income: {income_summary['total_income_adjusted']:,.2f}")



# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Parse Freedom Bank statements (single file or folder), "
            "create PDF meta JSON, validate, and compute IP income."
        )
    )
    ap.add_argument(
        "path",
        help="PDF file or directory with PDFs (e.g. data/freedom)",
    )
    ap.add_argument(
        "--pattern",
        default="*.pdf",
        help="Glob pattern when path is a directory (default: '*.pdf')",
    )
    ap.add_argument(
        "--out-dir",
        help="Output directory for CSVs (default: <path>/out or <file_dir>/out)",
    )
    ap.add_argument(
        "--pdf-meta-dir",
        help=(
            "Directory to store/load PDF metadata JSONs "
            "(default: <path>/pdf_meta or <file_dir>/pdf_meta)"
        ),
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

    pdf_meta_dir = Path(args.pdf_meta_dir) if args.pdf_meta_dir else base_dir / "pdf_meta"
    pdf_meta_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(pdf_files)} Freedom statement(s).")
    print(f"CSV output dir:  {out_dir}")
    print(f"PDF meta dir:    {pdf_meta_dir}")

    for pdf in pdf_files:
        try:
            process_one_freedom(
                pdf_path=pdf,
                out_dir=out_dir,
                pdf_meta_dir=pdf_meta_dir,
                months_back=args.months_back,
                verbose=not args.no_verbose,
            )
        except Exception as e:
            print(f"❌ Failed to process {pdf.name}: {e}")


if __name__ == "__main__":
    main()
