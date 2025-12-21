# src/halyk_business/parser.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Halyk Bank type A (business) parser.

Public API:
    load_jsonl(path: str) -> list[dict]
    parse_halyk_statement(jsonl_path: str) -> (header_df, tx_df, footer_df)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from src.halyk_business.header import parse_halyk_header
from src.halyk_business.transactions import parse_halyk_transactions_from_pages
from src.halyk_business.footer import parse_halyk_footer


def load_jsonl(path: str | Path) -> List[Dict[str, Any]]:
    """
    Read pdfplumber-style pages JSONL and return list of page dicts.
    """
    pages: List[Dict[str, Any]] = []
    path = Path(path)

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            pages.append(json.loads(line))

    return pages


def parse_halyk_statement(
    jsonl_path: str | Path,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    High-level parser for Halyk Bank business statement.

    Parameters
    ----------
    jsonl_path : str | Path
        Path to *_pages.jsonl produced by convert_pdf_json_pages.

    Returns
    -------
    header_df : DataFrame
        Single-row header with period, balances, account info, etc.
    tx_df : DataFrame
        Transactions table. Expected columns (at least):
        ['Дата', 'Номер документа', 'Дебет', 'Кредит',
         'Контрагент (имя)', 'Контрагент ИИН/БИН', 'Детали платежа']
    footer_df : DataFrame
        Single-row footer with totals, closing balance, etc.
    """
    jsonl_path = Path(jsonl_path)
    pages = load_jsonl(jsonl_path)

    header_df = parse_halyk_header(pages)
    tx_df = parse_halyk_transactions_from_pages(pages)
    footer_df = parse_halyk_footer(pages)

    return header_df, tx_df, footer_df
