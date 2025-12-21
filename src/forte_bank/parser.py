#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ForteBank statement parser orchestrator.

Calls:
  - header.parse_header(pdf_path)
  - transactions.parse_transactions(pdf_path)
  - footer.parse_forte_footer(jsonl_path)

and writes three CSVs:
  - header:  --out-header
  - tx:      --out-tx
  - footer:  --out-footer
"""

import argparse
import pandas as pd

from src.forte_bank.header import parse_header          # your existing module
from src.forte_bank.transactions import parse_fortebank_pdf  # your existing module
from src.forte_bank.footer import parse_forte_footer    # JSONL footer parser (returns dict)
import re

def _extract_knp_from_purpose(purpose: str) -> str:
    """
    Пытаемся достать КНП из строки типа '... КНП 841 ...' или 'КНП_841'.
    Если нет — возвращаем "".
    """
    s = str(purpose)
    m = re.search(r"КНП[_\s:-]*([0-9]{2,3})", s, flags=re.IGNORECASE)
    if m:
        return m.group(1).lstrip("0") or m.group(1)
    return ""


def parse_forte_statement(pdf_path: str, jsonl_path: str):
    """
    High-level wrapper: returns (header_df, tx_df, footer_df)
    """
    # 1) HEADER from PDF
    header_df = parse_header(pdf_path)

    # 2) TRANSACTIONS from PDF (Camelot inside)
    tx_df = parse_fortebank_pdf(pdf_path)

    # 3) FOOTER from JSONL (pdfplumber-style pages.jsonl)
    footer_dict = parse_forte_footer(jsonl_path)   # -> dict with totals/closing_balance
    footer_df = pd.DataFrame([footer_dict])

    return header_df, tx_df, footer_df
