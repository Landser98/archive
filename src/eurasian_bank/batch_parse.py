# src/eurasian_bank/batch_parse.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch / single-file parser for Eurasian Bank statements + IP income.

For each PDF:
  - create per-PDF metadata JSON (convert_pdf_json_page.dump_pages)
  - parse via parse_eurasian_statement (header + tx + footer)
  - validate PDF metadata (creation/mod dates, creator/producer, etc.)
  - compute IP income via compute_ip_income
  - save CSVs:
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

import pandas as pd
import pikepdf

from src.utils import warnings_setup  # noqa: F401  (side-effect: suppress warnings)
from src.eurasian_bank.parser import parse_eurasian_statement
from src.utils.income_calc import compute_ip_income
from src.utils.statement_validation import validate_pdf_metadata_from_json, validate_statement_generic, BANK_SCHEMAS
from src.utils.convert_pdf_json_page import dump_catalog, dump_pages


# ---------- helpers ----------

def ensure_pdf_meta_json(pdf_path: Path, meta_dir: Path) -> Path:
    """
    Ensure we have a JSON with PDF metadata + pages structure
    for pdf_path in meta_dir.

    Uses pikepdf + dump_catalog + dump_pages from convert_pdf_json_page.py.
    """
    meta_dir.mkdir(parents=True, exist_ok=True)
    json_path = meta_dir / f"{pdf_path.stem}.json"

    if json_path.exists():
        return json_path

    print(f"[meta-json] Creating {json_path.name} from {pdf_path.name}")

    with pikepdf.open(str(pdf_path)) as pdf:
        out: dict = {
            "file": str(pdf_path),
            "num_pages": len(pdf.pages),
        }

        # same style as convert_pdf_json_page.main()
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

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    return json_path



def process_one_pdf(
    pdf_path: Path,
    out_dir: Path,
    pdf_meta_dir: Path,
    pages: str = "1-end",
    flavor: str = "lattice",
    months_back: int | None = 12,
    verbose: bool = True,
) -> None:
    """
    Process a single Eurasian Bank PDF:
      - parse header / tx / footer
      - validate numeric consistency
      - validate PDF metadata via JSON
      - compute IP income
      - save CSVs into out_dir
    """
    print(f"\n=== Processing Eurasian: {pdf_path.name} ===")

    # 1) parse statement
    header_df, tx_df, footer_df = parse_eurasian_statement(
        str(pdf_path),
        pages=pages,
        flavor=flavor,
    )
    header_df = header_df.copy()
    if "closing_balance" not in header_df.columns:
        # choose what you consider "true closing":
        # here I assume final_balance = closing balance of the account
        header_df["closing_balance"] = footer_df["final_balance"].iloc[0]

    if "credit_turnover" not in header_df.columns:
        header_df["credit_turnover"] = footer_df["turnover_credit"].iloc[0]

    if "debit_turnover" not in header_df.columns:
        header_df["debit_turnover"] = footer_df["turnover_debit"].iloc[0]

    # 2) numeric validation (if schema configured)
    schema = BANK_SCHEMAS.get("EURASIAN")
    num_flags: list[str] = []
    num_debug: dict[str, object] = {}

    if schema is not None:
        num_flags, num_debug = validate_statement_generic(
            header_df,
            tx_df,
            footer_df,
            schema,
        )
    else:
        num_debug = {
            "note": "No 'EURASIAN' schema in BANK_SCHEMAS; numeric validation skipped."
        }

    # 3) PDF metadata validation (via JSON from convert_pdf_json_page)
    pdf_flags: list[str] = []
    pdf_debug: dict[str, object] = {}

    meta_json_path = ensure_pdf_meta_json(pdf_path, pdf_meta_dir)
    try:
        with open(meta_json_path, "r", encoding="utf-8") as f:
            pdf_json = json.load(f)

        period_end = header_df.iloc[0].get("period_end")

        pdf_flags, pdf_debug = validate_pdf_metadata_from_json(
            pdf_json,
            bank="EURASIAN",
            period_end=period_end,
            period_date_format="%d.%m.%Y",   # adjust if needed
            max_days_after_period_end=7,
            allowed_creators=None,           # e.g. ["Eurasian Bank"]
            allowed_producers=None,
        )
    except Exception as e:
        pdf_flags = ["pdf_meta_validation_error"]
        pdf_debug = {
            "error": str(e),
            "meta_json_path": str(meta_json_path),
        }

    # 4) ensure KNP column exists (just in case)
    if "КНП" not in tx_df.columns:
        tx_df["КНП"] = ""

    # 5) compute IP income
    enriched_tx, monthly_income, income_summary = compute_ip_income(
        tx_df,
        col_op_date="Дата проводки",
        col_credit="Кредит",
        col_knp="КНП",
        col_purpose="Назначение платежа",
        col_counterparty="Наименование Бенефициара/Отправителя",
        months_back=months_back,
        verbose=verbose,
        max_examples=5,
    )

    # 6) combine flags + debug (numeric + pdf_meta)
    all_flags = num_flags + pdf_flags
    all_debug = {
        "numeric": num_debug,
        "pdf_meta": pdf_debug,
    }

    meta_df = pd.DataFrame(
        [{
            "bank": "EURASIAN",
            "pdf_file": pdf_path.name,
            "flags": ";".join(all_flags),
            "debug_info": json.dumps(all_debug, ensure_ascii=False),
        }]
    )

    # 7) build paths
    stem = pdf_path.stem
    header_path   = out_dir / f"{stem}_header.csv"
    tx_path       = out_dir / f"{stem}_tx.csv"
    footer_path   = out_dir / f"{stem}_footer.csv"
    meta_path     = out_dir / f"{stem}_meta.csv"
    enriched_path = out_dir / f"{stem}_tx_ip.csv"
    monthly_path  = out_dir / f"{stem}_ip_income_monthly.csv"
    income_summary_path = out_dir / f"{stem}_income_summary.csv"
    income_summary_df = pd.DataFrame([income_summary])

    # 8) save CSVs
    header_df.to_csv(header_path, index=False, encoding="utf-8-sig")
    tx_df.to_csv(tx_path, index=False, encoding="utf-8-sig")
    footer_df.to_csv(footer_path, index=False, encoding="utf-8-sig")
    meta_df.to_csv(meta_path, index=False, encoding="utf-8-sig")
    enriched_tx.to_csv(enriched_path, index=False, encoding="utf-8-sig")
    monthly_income.to_csv(monthly_path, index=False, encoding="utf-8-sig")
    income_summary_df.to_csv(income_summary_path, index=False, encoding="utf-8-sig")

    # 9) summary
    print(f"  → Header:      {header_df.shape[0]} row  → {header_path}")
    print(f"  → Tx:          {tx_df.shape[0]} rows → {tx_path}")
    print(f"  → Footer:      {footer_df.shape[0]} row  → {footer_path}")
    print(f"  → Meta:        {meta_df.shape[0]} row  → {meta_path}")
    print(f"  → Tx+IP flags: {enriched_tx.shape[0]} rows → {enriched_path}")
    print(f"  → IP monthly:  {monthly_income.shape[0]} rows → {monthly_path}")
    print(f"  → Income summary: {income_summary_path}")
    print(f"✅ Adjusted income: {income_summary['total_income_adjusted']:,.2f} KZT")

# ---------- CLI ----------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Parse Eurasian Bank statements (single file or folder), "
            "validate PDF metadata via JSON, and compute IP income."
        )
    )
    ap.add_argument(
        "path",
        help="PDF file or directory with PDFs (e.g. data/eurasian_bank)",
    )
    ap.add_argument(
        "--pattern",
        default="*.pdf",
        help="Glob pattern when path is a directory (default: '*.pdf')",
    )
    ap.add_argument(
        "--out-dir",
        help="Output folder for CSVs (default: <path>/out or <file_dir>/out)",
    )
    ap.add_argument(
        "--pdf-meta-dir",
        help=(
            "Folder for per-PDF metadata JSONs "
            "(default: <path>/pdf_meta or <file_dir>/pdf_meta)"
        ),
    )
    ap.add_argument(
        "--pages",
        default="1-end",
        help="Camelot pages spec (default: '1-end')",
    )
    ap.add_argument(
        "--flavor",
        default="lattice",
        choices=["lattice", "stream"],
        help="Camelot flavor (default: 'lattice')",
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

    print(f"Found {len(pdf_files)} Eurasian statement(s).")
    print(f"CSV output dir:  {out_dir}")
    print(f"PDF meta dir:    {pdf_meta_dir}")

    for pdf in pdf_files:
        try:
            process_one_pdf(
                pdf_path=pdf,
                out_dir=out_dir,
                pdf_meta_dir=pdf_meta_dir,
                pages=args.pages,
                flavor=args.flavor,
                months_back=args.months_back,
                verbose=not args.no_verbose,
            )
        except Exception as e:
            print(f"❌ Failed to process {pdf.name}: {e}")


if __name__ == "__main__":
    main()
