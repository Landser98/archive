# src/bcc/batch_parse.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch parser for BCC (Bank CenterCredit) statements + IP income.

For each PDF:
  - ensure *_pages.jsonl exists (via convert_pdf_json_pages.dump_pdf_pages)
  - ensure single metadata JSON exists (via convert_pdf_json_page + pikepdf)
  - parse via parse_bcc_statement (header + tx + footer)
  - run numeric validation + PDF metadata validation
  - compute IP income via compute_ip_income
  - save CSVs:
      <stem>_header.csv
      <stem>_transactions.csv
      <stem>_footer.csv
      <stem>_meta.csv
      <stem>_tx_ip_enriched.csv
      <stem>_ip_income_monthly.csv
"""

import argparse
import json
from pathlib import Path

import pandas as pd
import pikepdf  # <— needed to open PDFs for metadata dump

from src.utils import warnings_setup  # noqa: F401  # side-effect: filters warnings

from src.bcc.parser import parse_bcc_statement
from src.utils.income_calc import compute_ip_income
from src.utils.statement_validation import (
    BANK_SCHEMAS,
    validate_statement_generic,
    validate_pdf_metadata_from_json,
)
from src.utils.convert_pdf_json_pages import dump_pdf_pages
from src.utils.convert_pdf_json_page import dump_catalog, dump_pages


def ensure_pdf_meta_json(pdf_path: Path, meta_dir: Path) -> Path:
    """
    Ensure a single JSON with PDF metadata+pages exists for this PDF.

    Format matches convert_pdf_json_page.py main():
    {
      "file": "...",
      "num_pages": ...,
      "pdf_version": "...",
      "trailer_keys": [...],
      "metadata": {...},
      "Root": {...},
      "Pages": [...],
      "XRef": [...]   # (we skip this part here)
    }
    """
    meta_dir.mkdir(parents=True, exist_ok=True)
    json_path = meta_dir / f"{pdf_path.stem}.json"
    if json_path.exists():
        return json_path

    print(f"[meta-json] Creating {json_path.name} from {pdf_path.name}")

    # replicate convert_pdf_json_page.main() logic
    with pikepdf.open(str(pdf_path)) as pdf:
        kw = dict(
            max_depth=6,
            include_streams=False,
            stream_max_bytes=0,  # we only care about metadata, not stream previews
        )
        out: dict = {
            "file": str(pdf_path),
            "num_pages": len(pdf.pages),
        }
        out.update(dump_catalog(pdf, **kw))
        out.update(dump_pages(pdf, **kw))
        # we skip XRef for now – not needed for metadata validation

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    return json_path


def ensure_jsonl_for_pdf(
    pdf_path: Path,
    jsonl_dir: Path,
    jsonl_suffix: str = "_pages.jsonl",
) -> Path:
    """
    Ensure we have a pdfplumber-style pages JSONL for this PDF in jsonl_dir.
    If missing, create it with dump_pdf_pages().
    """
    jsonl_dir.mkdir(parents=True, exist_ok=True)
    out_path = jsonl_dir / f"{pdf_path.stem}{jsonl_suffix}"

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


def process_one_bcc(
    pdf_path: Path,
    jsonl_dir: Path,
    out_dir: Path,
    pdf_meta_dir: Path,
    jsonl_suffix: str = "_pages.jsonl",
    months_back: int | None = 12,
    verbose: bool = True,
) -> None:
    """
    Обработка одного BCC-выписки:
      - создаём/находим JSONL
      - создаём/находим JSON с метаданными PDF
      - парсим header / tx / footer
      - валидируем числовые суммы и PDF metadata
      - считаем доход ИП
      - сохраняем CSV.
    """
    print(f"\n=== Processing BCC: {pdf_path.name} ===")

    # 1) ensure JSONL exists
    jsonl_path = ensure_jsonl_for_pdf(pdf_path, jsonl_dir, jsonl_suffix=jsonl_suffix)

    # 2) parse statement
    header_df, tx_df, footer_df = parse_bcc_statement(str(pdf_path), str(jsonl_path))

    # 3) basic sanity check for required columns
    required_cols = [
        "Күні / Дата",
        "Кредит / Кредит",
        "ТМК /КНП",
        "Төлемнің мақсаты / Назначение платежа",
        "Корреспондент / Корреспондент",
    ]
    missing = [c for c in required_cols if c not in tx_df.columns]
    if missing:
        raise ValueError(f"Missing columns in tx_df for {pdf_path.name}: {missing}")

    # 4) numeric validation (optional, only if BCC schema registered)
    num_flags: list[str] = []
    num_debug: dict[str, object] = {}

    schema = BANK_SCHEMAS.get("BCC")
    if schema is not None:
        num_flags, num_debug = validate_statement_generic(
            header_df,
            tx_df,
            footer_df,
            schema,
        )
    else:
        num_debug = {
            "note": "No 'BCC' schema in BANK_SCHEMAS; numeric validation skipped."
        }

    # 5) PDF metadata validation
    pdf_flags: list[str] = []
    pdf_debug: dict[str, object] = {}

    meta_json_path = ensure_pdf_meta_json(pdf_path, pdf_meta_dir)
    try:
        with open(meta_json_path, "r", encoding="utf-8") as f:
            pdf_json = json.load(f)

        period_end = header_df.iloc[0].get("Период (конец)")
        pdf_flags, pdf_debug = validate_pdf_metadata_from_json(
            pdf_json,
            bank="BCC",
            period_end=period_end,
            period_date_format="%d.%m.%Y",
            max_days_after_period_end=7,
            allowed_creators=["Bank CenterCredit"],  # TODO: adjust to real values
            allowed_producers=None,
        )
    except Exception as e:
        pdf_flags = ["pdf_meta_validation_error"]
        pdf_debug = {"error": str(e), "meta_json_path": str(meta_json_path)}

    # 6) IP income
    enriched_tx, monthly_income, income_summary = compute_ip_income(
        tx_df,
        col_op_date="Күні / Дата",
        col_credit="Кредит / Кредит",
        col_knp="ТМК /КНП",
        col_purpose="Төлемнің мақсаты / Назначение платежа",
        col_counterparty="Корреспондент / Корреспондент",
        months_back=months_back,
        verbose=verbose,
        max_examples=5,
    )

    # 7) meta row
    all_flags = num_flags + pdf_flags
    all_debug = {"numeric": num_debug, "pdf_meta": pdf_debug}

    meta_df = pd.DataFrame(
        [{
            "bank": "BCC",
            "pdf_file": pdf_path.name,
            "jsonl_file": jsonl_path.name,
            "flags": ";".join(all_flags),
            "debug_info": json.dumps(all_debug, ensure_ascii=False),
        }]
    )

    # 8) save CSVs
    stem = pdf_path.stem

    header_path = out_dir / f"{stem}_header.csv"
    tx_path = out_dir / f"{stem}_transactions.csv"
    footer_path = out_dir / f"{stem}_footer.csv"
    meta_path = out_dir / f"{stem}_meta.csv"
    enriched_path = out_dir / f"{stem}_tx_ip_enriched.csv"
    monthly_path = out_dir / f"{stem}_ip_income_monthly.csv"

    header_df.to_csv(header_path, index=False, encoding="utf-8-sig")
    tx_df.to_csv(tx_path, index=False, encoding="utf-8-sig")
    footer_df.to_csv(footer_path, index=False, encoding="utf-8-sig")
    meta_df.to_csv(meta_path, index=False, encoding="utf-8-sig")
    enriched_tx.to_csv(enriched_path, index=False, encoding="utf-8-sig")
    monthly_income.to_csv(monthly_path, index=False, encoding="utf-8-sig")
    income_summary_path = out_dir / f"{stem}_income_summary.csv"
    income_summary_df = pd.DataFrame([income_summary])
    income_summary_df.to_csv(
        income_summary_path,
        index=False,
        encoding="utf-8-sig"
    )

    print(f"  → Header:      {header_df.shape[0]} row   → {header_path}")
    print(f"  → Tx:          {tx_df.shape[0]} rows  → {tx_path}")
    print(f"  → Footer:      {footer_df.shape[0]} row   → {footer_path}")
    print(f"  → Meta:        {meta_df.shape[0]} row   → {meta_path}")
    print(f"  → Tx+IP flags: {enriched_tx.shape[0]} rows → {enriched_path}")
    print(f"  → IP monthly:  {monthly_income.shape[0]} rows → {monthly_path}")
    print(f"✅ Adjusted income: {income_summary['total_income_adjusted']:,.2f}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Batch parse BCC statements, auto-create pages JSONL & metadata JSON, "
            "validate, and compute IP income."
        )
    )
    ap.add_argument(
        "path",                      # 👈 was pdf_dir
        help="PDF file or directory with BCC PDFs (e.g. data/bcc or data/bcc/file.pdf)",
    )
    ap.add_argument(
        "--jsonl-dir",
        help=(
            "Directory to store/load *_pages.jsonl "
            "(default: <pdf_dir>/converted_jsons)"
        ),
    )
    ap.add_argument(
        "--out-dir",
        help="Output directory for CSVs (default: <pdf_dir>/out)",
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
    ap.add_argument(
        "--pdf-meta-dir",
        help="Folder for single JSON metadata files (default: <pdf_dir>/pdf_meta)",
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
        pdf_files = sorted(in_path.glob("*.pdf"))
        base_dir = in_path
        base_name = in_path.name

    default_out_dir = base_dir.parent / f"{base_name}_out"
    out_dir = Path(args.out_dir) if args.out_dir else default_out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    jsonl_dir = Path(args.jsonl_dir) if args.jsonl_dir else base_dir / "converted_jsons"
    jsonl_dir.mkdir(parents=True, exist_ok=True)

    pdf_meta_dir = Path(args.pdf_meta_dir) if args.pdf_meta_dir else base_dir / "pdf_meta"
    pdf_meta_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(pdf_files)} BCC statement(s).")
    print(f"CSV output dir:     {out_dir}")
    print(f"Pages JSONL dir:    {jsonl_dir}")
    print(f"PDF meta dir:       {pdf_meta_dir}")

    for pdf_path in pdf_files:
        try:
            process_one_bcc(
                pdf_path=pdf_path,
                jsonl_dir=jsonl_dir,
                out_dir=out_dir,
                pdf_meta_dir=pdf_meta_dir,
                jsonl_suffix=args.jsonl_suffix,
                months_back=args.months_back,
                verbose=not args.no_verbose,
            )
        except Exception as e:
            print(f"❌ Failed to process {pdf_path.name}: {e}")


if __name__ == "__main__":
    main()
