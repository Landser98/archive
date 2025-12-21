#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kaspi Pay / Kaspi Gold statement parser orchestrator.

Takes:
  - pages JSONL (output of convert_pdf_json_pages.py)

Uses:
  - header.parse_header_page(page)              → header_df (1 row)
  - transactions.parse_transactions_from_pages(pages) → tx_df (many rows)
  - footer.parse_footer_from_pages(pages)       → footer_df (0 or 1 row)

Exports three CSVs:
  - header:  --out-header
  - tx:      --out-tx
  - footer:  --out-footer
"""

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any, Tuple

import pandas as pd

from src.kaspi_pay.header import parse_header_page
from src.kaspi_pay.transactions import parse_transactions_from_pages
from src.kaspi_pay.footer import parse_footer_from_pages
from src.utils.income_calc import compute_ip_income



# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------
def _read_pages_jsonl(path: str) -> List[Dict[str, Any]]:
    pages: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            pages.append(json.loads(line))
    return pages

def _pick_first_existing(cols, candidates, fallback=None):
    """
    Возвращает первый кандидат, который реально есть в DataFrame.columns.
    Если нет ни одного — возвращает fallback.
    """
    for c in candidates:
        if c in cols:
            return c
    return fallback

# ---------------------------------------------------------------------
# High-level wrapper
# ---------------------------------------------------------------------
def parse_kaspi_pay_statement(jsonl_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    High-level wrapper for Kaspi Pay / Kaspi Gold:
    returns (header_df, tx_df, footer_df).
    """
    pages = _read_pages_jsonl(jsonl_path)
    if not pages:
        raise ValueError(f"No pages found in JSONL: {jsonl_path}")

    # 1) HEADER — from first page
    first_page = pages[0]
    header_df = parse_header_page(first_page)

    # 2) TRANSACTIONS — from all pages
    tx_df = parse_transactions_from_pages(pages)

    # 3) FOOTER — totals, meta info (may be empty)
    try:
        footer_df = parse_footer_from_pages(pages)
    except Exception as e:
        print(f"⚠️ Footer parsing failed: {e}")
        footer_df = pd.DataFrame()

    return header_df, tx_df, footer_df


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Kaspi Pay / Kaspi Gold JSONL → header / transactions / footer CSVs"
    )
    ap.add_argument(
        "jsonl",
        help="Path to pages JSONL (output of convert_pdf_json_pages.py)",
    )

    ap.add_argument(
        "--out-header",
        default=None,
        help="Output CSV for header (default: <jsonl_stem>_header.csv)",
    )
    ap.add_argument(
        "--out-tx",
        default=None,
        help="Output CSV for transactions (default: <jsonl_stem>_tx.csv)",
    )
    ap.add_argument(
        "--out-footer",
        default=None,
        help="Output CSV for footer totals (default: <jsonl_stem>_footer.csv)",
    )
    # новые файлы для дохода ИП
    ap.add_argument(
        "--out-tx-ip",
        default=None,
        help="Output CSV for tx with IP flags (default: <jsonl_stem>_tx_ip.csv)",
    )
    ap.add_argument(
        "--out-ip-monthly",
        default=None,
        help="Output CSV for monthly IP income (default: <jsonl_stem>_ip_income_monthly.csv)",
    )

    args = ap.parse_args()

    header_df, tx_df, footer_df = parse_kaspi_pay_statement(args.jsonl)

    in_path = Path(args.jsonl)
    out_header = Path(args.out_header) if args.out_header else in_path.with_name(in_path.stem + "_header.csv")
    out_tx     = Path(args.out_tx)     if args.out_tx     else in_path.with_name(in_path.stem + "_tx.csv")
    out_footer = Path(args.out_footer) if args.out_footer else in_path.with_name(in_path.stem + "_footer.csv")
    out_tx_ip  = Path(args.out_tx_ip)  if args.out_tx_ip  else in_path.with_name(in_path.stem + "_tx_ip.csv")
    out_ip_monthly = Path(args.out_ip_monthly) if args.out_ip_monthly else in_path.with_name(in_path.stem + "_ip_income_monthly.csv")

    # --- write header & tx ---
    header_df.to_csv(out_header, index=False, encoding="utf-8-sig")
    tx_df.to_csv(out_tx, index=False, encoding="utf-8-sig")

    # --- normalize footer_df type & write if non-empty ---
    df_footer: pd.DataFrame | None
    if footer_df is None:
        df_footer = None
    elif isinstance(footer_df, pd.DataFrame):
        df_footer = footer_df
    elif isinstance(footer_df, list):
        df_footer = pd.DataFrame(footer_df)
    elif isinstance(footer_df, dict):
        df_footer = pd.DataFrame([footer_df])
    else:
        df_footer = pd.DataFrame()

    if df_footer is not None and not df_footer.empty:
        df_footer.to_csv(out_footer, index=False, encoding="utf-8-sig")

    # --- logs по базовым CSV ---
    print(f"✅ Header:        {header_df.shape[0]} rows → {out_header}")
    print(f"✅ Transactions:  {tx_df.shape[0]} rows → {out_tx}")
    if df_footer is not None and not df_footer.empty:
        print(f"✅ Footer:        {df_footer.shape[0]} rows → {out_footer}")
    else:
        print("⚠️ Footer: empty (not written)")

    # === расчёт дохода ИП по Kaspi Pay ===

    cols = list(tx_df.columns)

    col_op_date = _pick_first_existing(cols, ["Дата операции", "Дата"], fallback=cols[1])
    col_credit  = _pick_first_existing(cols, ["Кредит"], fallback=cols[3])
    col_knp     = _pick_first_existing(cols, ["КНП"], fallback=None)
    col_purpose = _pick_first_existing(cols, ["Назначение платежа"], fallback=cols[-1])
    col_counterparty = _pick_first_existing(
        cols,
        [
            "Наименование получателя",
            "Наименование получателя (бенеф)",
            "Наименование получателя (отправителя денег)",
        ],
        fallback=cols[4] if len(cols) > 4 else cols[-1],
    )

    if col_knp is None:
        # На всякий случай, но у Kaspi Pay КНП есть
        tx_df["КНП"] = ""
        col_knp = "КНП"

    enriched_tx, monthly_income, avg_income = compute_ip_income(
        tx_df,
        col_op_date=col_op_date,
        col_credit=col_credit,
        col_knp=col_knp,
        col_purpose=col_purpose,
        col_counterparty=col_counterparty,
        months_back=12,
        # Kaspi обычно в формате dd.mm.yyyy
        op_date_pattern=r"(\d{2}\.\d{2}\.\d{4})",
        op_date_format="%d.%m.%Y",
        verbose=True,
        max_examples=5,
    )

    enriched_tx.to_csv(out_tx_ip, index=False, encoding="utf-8-sig")
    monthly_income.to_csv(out_ip_monthly, index=False, encoding="utf-8-sig")

    print(f"✅ Tx+IP flags:   {enriched_tx.shape[0]} rows → {out_tx_ip}")
    print(f"✅ IP monthly:    {monthly_income.shape[0]} rows → {out_ip_monthly}")
    print(f"✅ Avg monthly IP income: {avg_income:,.2f}")


if __name__ == "__main__":
    main()



# python src/kaspi_pay/parser.py \
# "data/converted_jsons/Vypiska_po_scetu_KZ98722S000033980379_pages.jsonl" \
# --out-header data/converted_jsons/kaspi_pay_header.csv \
# --out-tx     data/converted_jsons/kaspi_pay_tx.csv \
# --out-footer data/converted_jsons/kaspi_pay_footer.csv
