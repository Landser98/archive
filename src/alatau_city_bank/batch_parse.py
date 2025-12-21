# src/alatau_city_bank/batch_parse.py
#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

import pandas as pd
import pikepdf

from src.alatau_city_bank.parser import parse_acb_pdf_with_camelot
from src.utils.statement_validation import (
    BANK_SCHEMAS,
    validate_statement_generic,
    validate_pdf_metadata_from_json,
)
from src.utils.income_calc import compute_ip_income
from src.utils.convert_pdf_json_page import dump_catalog, dump_pages
from src.utils import warnings_setup  # noqa: F401


def ensure_pdf_json(
    pdf_path: Path,
    json_dir: Path,
    *,
    max_depth: int = 6,
    include_streams: bool = False,
    stream_max_bytes: int = 4096,
    include_xref: bool = False,
) -> Path:
    """
    Ensure we have a JSON dump for this PDF in json_dir.
    Uses the same logic as convert_pdf_json_page.py but with a tunable destination.
    """
    json_dir.mkdir(parents=True, exist_ok=True)
    out_path = json_dir / f"{pdf_path.stem}.json"

    if out_path.exists():
        return out_path

    with pikepdf.open(str(pdf_path)) as pdf:
        out: dict = {
            "file": str(pdf_path),
            "num_pages": len(pdf.pages),
        }

        # reuse helpers from convert_pdf_json_page.py
        out.update(
            dump_catalog(
                pdf,
                max_depth=max_depth,
                include_streams=include_streams,
                stream_max_bytes=stream_max_bytes,
            )
        )
        out.update(
            dump_pages(
                pdf,
                max_depth=max_depth,
                include_streams=include_streams,
                stream_max_bytes=stream_max_bytes,
            )
        )

        if include_xref:
            xref = []
            try:
                for obj in pdf.objects:
                    try:
                        og = obj.objgen
                        xref.append(
                            {
                                "obj": og[0],
                                "gen": og[1],
                                "type": type(obj.get_object()).__name__,
                            }
                        )
                    except Exception:
                        pass
            except Exception as e:
                xref = {"error": f"{type(e).__name__}: {e}"}
            out["XRef"] = xref

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"[json] Dumped PDF internals to {out_path}")
    return out_path


def process_one_pdf(
    pdf_path: Path,
    out_dir: Path,
    json_dir: Path,
    months_back: int | None = 12,
    verbose: bool = True,
) -> None:
    print(f"\n=== Processing {pdf_path.name} ===")

    header_df, tx_df, footer_df = parse_acb_pdf_with_camelot(str(pdf_path))

    # ---- numeric validation ----
    num_schema = BANK_SCHEMAS["ALATAU_CITY"]
    num_flags, num_debug = validate_statement_generic(
        header_df, tx_df, footer_df, num_schema
    )

    # ---- ensure & load PDF JSON (from convert_pdf_json_page.py logic) ----
    json_path = ensure_pdf_json(
        pdf_path,
        json_dir=json_dir,
        max_depth=6,
        include_streams=False,      # set True if you want stream previews
        stream_max_bytes=2048,
        include_xref=False,         # True if you want XRef info
    )

    with open(json_path, "r", encoding="utf-8") as f:
        pdf_json = json.load(f)

    closing_date = header_df.iloc[0].get("closing_balance_date")

    pdf_flags, pdf_debug = validate_pdf_metadata_from_json(
        pdf_json,
        bank="ALATAU_CITY",
        period_end=closing_date,
        period_date_format="%d.%m.%Y",
        max_days_after_period_end=7,
        allowed_creators=None,   # e.g. ["Alatau City Bank"] when you know real values
        allowed_producers=None,
    )

    # ---- merge flags/debug ----
    all_flags = num_flags + pdf_flags
    all_debug = {"numeric": num_debug, "pdf_meta": pdf_debug}

    meta_df = pd.DataFrame(
        [
            {
                "bank": "ALATAU_CITY",
                "pdf_file": pdf_path.name,
                "json_file": json_path.name,
                "flags": ";".join(all_flags),
                "debug_info": json.dumps(all_debug, ensure_ascii=False),
            }
        ]
    )

    # make sure KNP exists
    if "КНП" not in tx_df.columns:
        tx_df["КНП"] = ""

    # compute IP income
    enriched_tx, monthly_income, income_summary = compute_ip_income(
        tx_df,
        col_op_date="Дата операции",
        col_credit="Кредит",
        col_knp="КНП",
        col_purpose="Назначение платежа",
        col_counterparty="Корреспондент",
        months_back=months_back,
        verbose=verbose,
        max_examples=5,
    )

    stem = pdf_path.stem
    header_path = out_dir / f"{stem}_header.csv"
    tx_path = out_dir / f"{stem}_tx.csv"
    footer_path = out_dir / f"{stem}_footer.csv"
    meta_path = out_dir / f"{stem}_meta.csv"
    enriched_path = out_dir / f"{stem}_tx_ip.csv"
    monthly_path = out_dir / f"{stem}_ip_income_monthly.csv"
    income_summary_path = out_dir/ f"{stem}_income_summary.csv"

    header_df.to_csv(header_path, index=False, encoding="utf-8-sig")
    tx_df.to_csv(tx_path, index=False, encoding="utf-8-sig")
    footer_df.to_csv(footer_path, index=False, encoding="utf-8-sig")
    meta_df.to_csv(meta_path, index=False, encoding="utf-8-sig")
    enriched_tx.to_csv(enriched_path, index=False, encoding="utf-8-sig")
    monthly_income.to_csv(monthly_path, index=False, encoding="utf-8-sig")
    income_summary_df = pd.DataFrame([income_summary])

    income_summary_df.to_csv(income_summary_path, index=False, encoding="utf-8-sig")

    print(f"  → Header:      {header_df.shape[0]} row  → {header_path}")
    print(f"  → Tx:          {tx_df.shape[0]} rows → {tx_path}")
    print(f"  → Footer:      {footer_df.shape[0]} row  → {footer_path}")
    print(f"  → Meta:        {meta_df.shape[0]} row  → {meta_path}")
    print(f"  → Tx+IP flags: {enriched_tx.shape[0]} rows → {enriched_path}")
    print(f"  → IP monthly:  {monthly_income.shape[0]} rows → {monthly_path}")
    print(f"✅ Adjusted income: {income_summary['total_income_adjusted']:,.2f}")


def main():
    ap = argparse.ArgumentParser(
        description="Batch parse Alatau City Bank PDFs, validate, and compute IP income."
    )
    ap.add_argument("in_dir", help="Folder with PDFs or a single PDF file")
    ap.add_argument("--out-dir", help="Output folder for CSVs")
    ap.add_argument(
        "--json-dir",
        help="Folder to store/load PDF JSON dumps "
             "(default: <base_in_dir>/converted_jsons)",
    )
    ap.add_argument("--months-back", type=int, default=12)
    ap.add_argument("--no-verbose", action="store_true")
    args = ap.parse_args()

    in_path = Path(args.in_dir)
    if in_path.is_file():
        pdf_files = [in_path]
        base_dir = in_path.parent
        base_name = in_path.stem
    else:
        pdf_files = sorted(in_path.glob("*.pdf"))
        base_dir = in_path
        base_name = in_path.name

    if not pdf_files:
        raise SystemExit(f"No PDF files found in {in_path}")

    default_out_dir = base_dir.parent / f"{base_name}_out"
    out_dir = Path(args.out_dir) if args.out_dir else default_out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    json_dir = Path(args.json_dir) if args.json_dir else base_dir / "converted_jsons"
    json_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(pdf_files)} PDF(s) in {in_path}")
    print(f"CSV output dir:    {out_dir}")
    print(f"PDF JSON dump dir: {json_dir}")

    for pdf in pdf_files:
        process_one_pdf(
            pdf,
            out_dir=out_dir,
            json_dir=json_dir,
            months_back=args.months_back,
            verbose=not args.no_verbose,
        )


if __name__ == "__main__":
    main()
