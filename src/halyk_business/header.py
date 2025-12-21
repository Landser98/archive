import re
import pandas as pd
from typing import Optional, Tuple, Dict, Any, List
import json

SPACE_CHARS = r"\u00A0\u202F"  # NBSP + narrow NBSP
S = SPACE_CHARS

def _re_get(pattern: str, text: str, flags=0) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None

def _extract_period_halyk(text: str) -> Tuple[Optional[str], Optional[str]]:
    # "За период с 15-09-2023 по15-09-2025" (space after "по" may be missing)
    m = re.search(r"За\s+период\s+с\s*([0-9.\-]+)\s*по\s*([0-9.\-]+)", text, flags=re.IGNORECASE)
    return (m.group(1), m.group(2)) if m else (None, None)

def _extract_account_currency(text: str) -> Tuple[Optional[str], Optional[str]]:
    # "Счет(Валюта) KZ02601A221000086711 (KZT)"
    m = re.search(r"Счет\(Валюта\)\s*([A-Z0-9]+)\s*\(([A-Z]{3})\)", text)
    if m:
        return m.group(1), m.group(2)
    return None, None

def _extract_opening_and_date(text: str) -> Tuple[Optional[str], Optional[str]]:
    # "Входящий остаток: 0.00 Дата остатка: 15-09-2023"
    # Be liberal with spaces/newlines
    m = re.search(
        rf"Входящий\s+остаток:\s*([0-9{S}\s.,]+)\s*(?:[A-Z]{{3}})?[\s\S]*?Дата\s+остатка:\s*([0-9.\-]+)",
        text,
        flags=re.IGNORECASE,
    )
    if m:
        return m.group(1).strip(), m.group(2).strip()
    # If split across lines differently, try separate grabs
    val = _re_get(rf"Входящий\s+остаток:\s*([0-9{S}\s.,]+)", text, flags=re.IGNORECASE)
    dte = _re_get(r"Дата\s+остатка:\s*([0-9.\-]+)", text, flags=re.IGNORECASE)
    return val, dte

def parse_halyk_header_page(page: Dict[str, Any]) -> pd.DataFrame:
    """
    Parse Halyk Bank header (page 1) into a tidy one-row DataFrame.
    Works on the `text` field from your JSONL page record.
    """
    raw = page.get("text") or ""
    # normalize runs of spaces/tabs but keep newlines to allow cross-line matches
    norm = re.sub(r"[ \t]+", " ", raw)

    # Title is present but not required; we parse concrete fields
    period_start, period_end = _extract_period_halyk(norm)

    bank = _re_get(r"Банк[: ]+\s*(.+?)\s*$", norm, flags=re.MULTILINE)  # line with "Банк ..."
    bic = _re_get(r"БИК[: ]+\s*([A-Z0-9]{8,11})", norm)
    iin_bin = _re_get(r"ИИН/БИН[: ]+\s*([0-9]+)", norm)
    client = _re_get(r"Клиент[: ]+\s*(.+?)\s*$", norm, flags=re.MULTILINE)

    account, currency = _extract_account_currency(norm)

    dt_recv = _re_get(r"Дата\s+получения\s+выписки:\s*([0-9.\-]+)", norm)
    dt_prev = _re_get(r"Дата\s+предыдущей\s+операции:\s*([0-9.\-]+)", norm)
    dt_last = _re_get(r"Дата\s+последней\s+операции:\s*([0-9.\-]+)", norm)

    opening_balance, opening_date = _extract_opening_and_date(raw)  # use RAW for robustness

    return pd.DataFrame([{
        "Банк": bank,
        "БИК": bic,
        "ИИН/БИН": iin_bin,
        "Клиент": client,
        "Счет": account,
        "Валюта": currency,
        "Период (начало)": period_start,
        "Период (конец)": period_end,
        "Дата получения выписки": dt_recv,
        "Дата предыдущей операции": dt_prev,
        "Дата последней операции": dt_last,
        "Входящий остаток": opening_balance,
        "Дата остатка": opening_date,
    }])

# в начале файла уже есть:
# from typing import Optional, Tuple, Dict, Any, List

def parse_halyk_header(pages: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Обёртка над parse_halyk_header_page:
    берёт первую страницу (или ту, у которой page_num == 1)
    и парсит из неё шапку.
    """
    if not pages:
        # на всякий случай — пустой DF, чтобы не падало
        return pd.DataFrame([{
            "Банк": None,
            "БИК": None,
            "ИИН/БИН": None,
            "Клиент": None,
            "Счет": None,
            "Валюта": None,
            "Период (начало)": None,
            "Период (конец)": None,
            "Дата получения выписки": None,
            "Дата предыдущей операции": None,
            "Дата последней операции": None,
            "Входящий остаток": None,
            "Дата остатка": None,
        }])

    # ищем страницу с page_num == 1, иначе берём pages[0]
    first_page = next(
        (p for p in pages if str(p.get("page_num")) in ("1", "01")),
        pages[0],
    )
    return parse_halyk_header_page(first_page)
