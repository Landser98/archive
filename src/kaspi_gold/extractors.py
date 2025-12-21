# src/kaspi_parser/extractors.py
import re
from typing import Dict, Tuple, Optional, List
from datetime import datetime

import pandas as pd
import fitz

from src.kaspi_gold.utils import parse_amount, AMOUNT_ROW_REGEX, to_ddmmy_date

OP_TO_SUMMARY_KEY = {
    "покупка": "Покупки",
    "перевод": "Переводы",
    "пополнение": "Пополнения",
    "разное": "Разное",
    "снятие": "Снятия",
}


def find_period(text: str) -> Tuple[str, str]:
    m = re.search(
        r"\bс\s+(\d{2}\.\d{2}\.\d{2,4})\s+по\s+(\d{2}\.\d{2}\.\d{2,4})\b",
        text,
        re.I
    )
    return (m.group(1), m.group(2)) if m else ("", "")


def find_iban(text: str) -> str:
    m = re.search(r"\bKZ[0-9A-Z]{18,}\b", text)
    return m.group(0) if m else ""


def find_currency(text: str) -> str:
    m = re.search(r"(?:Валюта\s*счета|Валюта)\s*[:\-]?\s*([^\n\r]+)", text, re.I)
    return (m.group(1).strip() if m else "").strip()


def find_cardlast4(text: str) -> str:
    m = re.search(r"(?:Номер\s*карты)\s*[:\-]?\s*\*?(\d{4})", text, re.I)
    return m.group(1) if m else ""


def extract_summary_reported_from_page(page: fitz.Page) -> Dict[str, float]:
    """
    Parse the short summary box (Покупки / Переводы / ... ) on page 1.

    We scan spans in y0,x0 order. For each label we know
    ("Покупки", "Переводы", etc.) we look ahead a few spans to find
    the first thing that looks like a currency amount and parse it.

    Returns:
        {
            "Покупки": -1023995.00,
            "Переводы": -1199436.00,
            ...
        }
    """
    wanted_labels = ["Покупки", "Переводы", "Пополнения", "Разное", "Снятия"]
    reported: Dict[str, float] = {}

    spans_list: List[Dict[str, float]] = []
    d = page.get_text("dict")
    for b in d.get("blocks", []):
        if b.get("type") != 0:
            continue
        for ln in b.get("lines", []):
            for sp in ln.get("spans", []):
                txt = (sp.get("text") or "").strip()
                if not txt:
                    continue
                x0, y0, x1, y1 = sp["bbox"]
                spans_list.append({
                    "text": txt,
                    "x0": x0,
                    "y0": y0,
                    "x1": x1,
                    "y1": y1,
                })

    if not spans_list:
        return reported

    spans_df = pd.DataFrame(spans_list).sort_values(["y0", "x0"]).reset_index(drop=True)

    for i, row in spans_df.iterrows():
        label = row["text"]
        if label in wanted_labels:
            # look ahead up to 5-6 spans for the numeric amount
            for j in range(i + 1, min(i + 6, len(spans_df))):
                cand = spans_df.loc[j, "text"]
                m = AMOUNT_ROW_REGEX.search(cand)
                if m:
                    reported[label] = parse_amount(m.group(1))
                    break

    return reported


def extract_balances_from_page(page: fitz.Page) -> Dict[str, Optional[float]]:
    """
    Kaspi summary layout example:

        Доступно на 01.09.24    + 1 877,62 ₸
        ...
        Доступно на 30.09.24    + 3 726,62 ₸

    We:
    - find all rows starting with 'Доступно на <date>'
    - pull the amount after each one
    - sort by date
    - first is opening_balance, last is closing_balance

    Returns:
        {
            "opening_balance": <float or None>,
            "closing_balance": <float or None>
        }
    """
    balances_by_date: List[Tuple[str, float]] = []

    spans_list: List[Dict[str, float]] = []
    d = page.get_text("dict")
    for b in d.get("blocks", []):
        if b.get("type") != 0:
            continue
        for ln in b.get("lines", []):
            for sp in ln.get("spans", []):
                txt = (sp.get("text") or "").strip()
                if not txt:
                    continue
                x0, y0, x1, y1 = sp["bbox"]
                spans_list.append({
                    "text": txt,
                    "x0": x0,
                    "y0": y0,
                    "x1": x1,
                    "y1": y1,
                })

    if not spans_list:
        return {"opening_balance": None, "closing_balance": None}

    spans_df = pd.DataFrame(spans_list).sort_values(["y0", "x0"]).reset_index(drop=True)

    # e.g. "Доступно на 01.09.24"
    avail_re = re.compile(
        r"доступно\s+на\s+(\d{2}\.\d{2}\.\d{2,4})",
        re.IGNORECASE
    )

    for i, row in spans_df.iterrows():
        txt = row["text"]
        m_av = avail_re.search(txt)
        if not m_av:
            continue

        date_str = m_av.group(1)

        # look ahead for amount span
        found_amount_val = None
        for j in range(i + 1, min(i + 8, len(spans_df))):
            cand = spans_df.loc[j, "text"]
            m_amt = AMOUNT_ROW_REGEX.search(cand)
            if m_amt:
                found_amount_val = parse_amount(m_amt.group(1))
                break

        if found_amount_val is not None:
            balances_by_date.append((date_str, found_amount_val))

    if not balances_by_date:
        return {"opening_balance": None, "closing_balance": None}

    # Sort balances_by_date by real parsed date
    parsed: List[Tuple[datetime, float]] = []
    for date_str, amt in balances_by_date:
        dt = to_ddmmy_date(date_str)
        if dt is not None:
            parsed.append((dt, amt))

    if not parsed:
        return {"opening_balance": None, "closing_balance": None}

    parsed.sort(key=lambda x: x[0])
    opening_balance = parsed[0][1]
    closing_balance = parsed[-1][1]

    return {
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
    }


def compute_category_sums_simple(tx_df: pd.DataFrame) -> Dict[str, float]:
    """
    Roll up tx_df amounts by tx_df['operation'].
    Then normalize operation names into Kaspi summary buckets.

    Returns:
        {
          "Покупки": float,
          "Переводы": float,
          ...
        }
    """
    totals = {
        "Покупки": 0.0,
        "Переводы": 0.0,
        "Пополнения": 0.0,
        "Разное": 0.0,
        "Снятия": 0.0,
    }

    if tx_df.empty:
        return totals

    gb = tx_df.groupby("operation")["amount"].sum()

    for op_name, amount_sum in gb.items():
        key = OP_TO_SUMMARY_KEY.get(op_name.strip().lower(), "Разное")
        totals[key] += float(amount_sum)

    return totals
