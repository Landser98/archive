#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
High-level parser for a BCC (Bank CenterCredit) statement.

Public API:
    load_jsonl(path: str) -> list[dict]
    parse_bcc_statement(pdf_path: str, jsonl_path: str)
        -> (header_df, tx_df, footer_df)

- header_df: 1 row (opening/closing balances, turnovers, period dates, etc.)
- tx_df:     transaction table (Camelot, columns like 'Күні / Дата', 'Кредит / Кредит', ...)
- footer_df: 1 row with totals parsed from JSONL.
"""

from typing import List, Dict, Any, Tuple
import json

import pandas as pd

from src.bcc.header import parse_bcc_header
from src.bcc.footer import parse_bcc_footer
from src.bcc.transactions import parse_bcc_transactions_camelot


# ---------- helpers ----------

def load_jsonl(path: str) -> List[Dict[str, Any]]:
    """
    Read pdfplumber-style pages.jsonl:
    one JSON object per line → list of dicts.
    """
    pages: List[Dict[str, Any]] = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pages.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Invalid JSON in {path!r}: {line[:200]!r}"
                ) from e

    if not pages:
        raise ValueError(f"No pages found in {path}")

    return pages


def parse_bcc_statement(
    pdf_path: str,
    jsonl_path: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    High-level function: parse full BCC statement.

    Parameters
    ----------
    pdf_path : str
        Original statement PDF (for Camelot).
    jsonl_path : str
        pdfplumber-style pages.jsonl for the same statement (for header/footer).

    Returns
    -------
    header_df : DataFrame (1 row)
    tx_df     : DataFrame (transactions table)
    footer_df : DataFrame (1 row)
    """
    # 1) HEADER: текст первой страницы из JSONL
    pages = load_jsonl(jsonl_path)
    page1 = next((p for p in pages if p.get("page_num") in (1, "1")), pages[0])
    page1_text = page1.get("text") or ""
    header_df = parse_bcc_header(page1_text)

    # 2) TRANSACTIONS: тянем Camelot-ом из PDF
    tx_df = parse_bcc_transactions_camelot(pdf_path)

    # 3) FOOTER: из JSONL
    footer_dict = parse_bcc_footer(jsonl_path)
    footer_df = pd.DataFrame([footer_dict])

    return header_df, tx_df, footer_df
