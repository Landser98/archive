# We'll load the JSONL, pick page 1, and parse header fields into a tidy DataFrame.
import json
import re
from typing import Optional, Tuple, Dict, Any, List
import pandas as pd


SPACE_CHARS = r"\u00A0\u202F"  # NBSP + narrow NBSP
S = SPACE_CHARS  # alias for compact f-strings

def _re_get(pattern: str, text: str, flags=0) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None

def _extract_period(text: str) -> Tuple[Optional[str], Optional[str]]:
    # e.g., "Период: 29.09.2024 - 29.09.2025"
    m = re.search(r"Период:\s*(\d{2}\.\d{2}\.\d{4})\s*[-–]\s*(\d{2}\.\d{2}\.\d{4})", text)
    if m:
        return m.group(1), m.group(2)
    return None, None

def _extract_balance(label: str, text: str):
    """
    Find the first amount+ccy that appears shortly after `label`.
    Returns a string "2 201 173,4 KZT" or None.
    """
    # Collapse tabs/multiple spaces but keep newlines so we can search across them
    t = re.sub(r"[ \t]+", " ", text)

    pat = (
        rf"{re.escape(label)}:?[\s\S]{{0,120}}?"         # up to 120 chars after label, including newlines
        rf"([\d {SPACE_CHARS}.,]+)\s*([A-Z]{{3}})"       # number with spaces/commas + 3-letter ccy
    )
    m = re.search(pat, t)
    if m:
        amount_raw = m.group(1).strip()
        ccy = m.group(2).strip()
        return f"{amount_raw} {ccy}"
    return None

def _normalize_amount_to_float(amount_str: str):
    """
    Optional helper: "2 201 173,4 KZT" -> (2201173.4, "KZT")
    """
    if not amount_str:
        return None, None
    m = re.search(r"([\d \u00A0\u202F.,]+)\s*([A-Z]{3})", amount_str)
    if not m:
        return None, None
    num = m.group(1)
    ccy = m.group(2)
    num = re.sub(r"[ \u00A0\u202F]", "", num).replace(",", ".")
    try:
        return float(num), ccy
    except ValueError:
        return None, ccy

def parse_header_page(page):
    text: str = page.get("text") or ""
    text_norm = re.sub(r"[ \t]+", " ", text)

    account = _re_get(r"Лицевой\s+счет:\s*([A-Z0-9]+)", text_norm)
    currency = _re_get(r"Валюта\s+счета:\s*([A-Z]{3})", text_norm)
    period_start, period_end = _extract_period(text_norm)
    last_move = _re_get(r"Дата\s+последнего\s+движения:\s*([0-9.: \-]+)", text_norm)
    iin_bin = _re_get(r"ИИН/БИН:\s*([0-9]+)", text_norm)
    client = _re_get(r"Наименование\s+клиента:\s*(.+)", text_norm)

    # ← use the relaxed extractor on the RAW text
    opening_balance = _extract_balance("Входящий остаток", text)
    closing_balance = _extract_balance("Исходящий остаток", text)

    return pd.DataFrame([{
        "Лицевой счет": account,
        "Валюта счета": currency,
        "Период (начало)": period_start,
        "Период (конец)": period_end,
        "Дата последнего движения": last_move,
        "ИИН/БИН": iin_bin,
        "Наименование клиента": client,
        "Входящий остаток": opening_balance,
        "Исходящий остаток": closing_balance,
    }])

