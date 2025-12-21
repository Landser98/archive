# -*- coding: utf-8 -*-
# bcc_header_parser_v1.py
#
# Usage (example at bottom):
#   df_bcc_header = parse_bcc_header(page1.get("text") or "")
#   print(df_bcc_header.T)

from __future__ import annotations
import re
import pandas as pd
from typing import Optional

# ---------- Common helpers/regex ----------
WS = r" \t\r\n\u00A0\u202F"
S0 = rf"[{WS}]*"
S1 = rf"[{WS}]+"

# number like: 403480.88  |  403 480,88  |  403 480.88  |  +1,00 / -1.00
NUM_RE = r"[+-]?\d[\d \u00A0\u202F]*[.,]\d{2}"

def _last(text: str, pat: str, flags: int = 0) -> Optional[str]:
    """Return last captured group(1) match trimmed, else None."""
    m = None
    for m in re.finditer(pat, text, flags):
        pass
    return m.group(1).strip() if m else None

def _norm_spaces(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s.replace("\u00A0", " ").replace("\u202F", " ")).strip()

def _to_float(num: Optional[str]) -> Optional[float]:
    if num is None:
        return None
    t = num.replace("\u00A0", " ").replace("\u202F", " ")
    t = t.replace(" ", "")
    # prefer comma as decimal if present; else dot
    if "," in t and "." in t:
        # If both exist, assume thousand-sep is the one appearing first
        # Normalize by removing thousands and keeping last separator as decimal
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "")
            t = t.replace(",", ".")
        else:
            t = t.replace(",", "")
    else:
        t = t.replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None

# ---------- Main parser ----------
def parse_bcc_header(page_text: str) -> pd.DataFrame:
    t = page_text or ""

    # Bank name (accept lines like: АО «Банк ЦентрКредит»)
    bank_name = _last(t, rf'(АО{S1}«?Банк{S1}ЦентрКредит»?)', re.IGNORECASE)

    # Registration / outgoing number (may appear as "Регистрационный номер Исх. № 15201")
    reg_no = _last(t, rf'Регистрационный{S1}номер{S1}Исх\.?{S0}№{S0}([0-9A-Za-z\-\/]+)')

    # Formed at (date & optional time)
    formed_dt = _last(
        t,
        rf'(?:Құрылған{S1}күні|Дата{S1}формирования):{S0}([0-9]{{2}}\.[0-9]{{2}}\.[0-9]{{4}}(?:{S1}[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}})?)',
        re.IGNORECASE,
    )

    # Support phones (optional; grab the right side after colon)
    support_phones = _last(
        t,
        rf'(?:Тегін{S1}қолдау{S1}телефондары|Бесплатные{S1}телефоны{S1}поддержки):{S0}([^\n]+)'
    )

    # Client
    # Client
    # Пытаемся поймать:
    #   "Клиент: ТОО Ромашка"
    #   "Клиент / Клиент: ИП ..."
    #   "Клиент / Client: ТОО ..."
    client = _last(
        t,
        rf'Клиент(?:{S1}/{S1}(?:Клиент|Client))?{S0}:{S0}(.+?)(?:\r?\n|$)'
    )

    # Если не нашли – очень широкий фоллбэк:
    if not client:
        m = re.search(r'Клиент[^\n\r:]*:(.+)', t)
        if m:
            client = m.group(1)

    # Нормализуем пробелы и кавычки
    if client:
        client = _norm_spaces(
            client
            .replace('\"', '"')
            .replace('“', '"')
            .replace('”', '"')
        )

    # IIN/BIN (ИИН/БИН)

    iin_bin = _last(
        t,
        rf'(?:ЖСН{S0}/{S0}ИИН|ЖСН|ИИН|БИН){S0}:{S0}([0-9]{{9,12}})',
        re.IGNORECASE,
    )

    # IIK/IBAN (ЖСК/ИИК)
    iban = _last(
        t,
        rf'(?:ЖСК|ИИК|IBAN){S0}:{S0}([Kk][Zz][0-9A-Za-z]{{16,30}})',
    )
    if iban:
        iban = iban.upper()

    # BIC (БСК/БИК)
    bic = _last(
        t,
        rf'(?:БСК|БИК|BIC){S0}:{S0}([A-Z0-9]{{8,11}})'
    )

    # Currency
    ccy = _last(
        t,
        rf'(?:Валютасы|Валюта){S0}:{S0}([A-Z]{{3}})'
    )

    # Period: try Russian first: "Движения по счету c 01.06.2023 по 31.05.2024"
    period_start = _last(
        t,
        rf'Движения{S1}по{S1}счету{S1}c{S1}([0-9]{{2}}\.[0-9]{{2}}\.[0-9]{{4}})'
    )
    period_end = _last(
        t,
        rf'Движения{S1}по{S1}счету{S1}c{S1}[0-9]{{2}}\.[0-9]{{2}}\.[0-9]{{4}}{S1}по{S1}([0-9]{{2}}\.[0-9]{{2}}\.[0-9]{{4}})'
    )
    # Kazakh variant (if Russian didn’t hit): "Есепшот бойынша ... бастап ... дейінгі қозғалыс"
    if not period_start:
        period_start = _last(
            t,
            rf'Есепшот{S1}бойынша{S1}([0-9]{{2}}\.[0-9]{{2}}\.[0-9]{{4}}){S1}бастап',
        )
    if not period_end:
        period_end = _last(
            t,
            rf'Есепшот{S1}бойынша{S1}[0-9]{{2}}\.[0-9]{{2}}\.[0-9]{{4}}{S1}бастап{S1}([0-9]{{2}}\.[0-9]{{2}}\.[0-9]{{4}}){S1}дейінгі{S1}қозғалыс',
        )

    # Credit limit
    credit_limit_raw = _last(
        t,
        rf'(?:Несие{S1}лимиті|Кредитный{S1}лимит):{S0}({NUM_RE}|0(?:[.,]00)?)',
        re.IGNORECASE,
    )
    credit_limit = _to_float(credit_limit_raw)

    # Opening balance (Входящий остаток)
    # Opening balance (Входящий остаток)
    opening_raw = _last(
        t,
        rf'(?:Кіріс{S1}қалдық|Входящий{S1}остаток):{S0}({NUM_RE}|[+-]?\d+)',
        re.IGNORECASE,
    )
    opening_balance = _to_float(opening_raw)


    # Incoming saldo (Входящее сальдо) — sometimes duplicated: keep as separate, but we’ll backfill if needed
    incoming_saldo_raw = _last(
        t,
        rf'(?:Кіріс{S1}сальдо|Входящее{S1}сальдо):{S0}({NUM_RE}|[+-]?\d+)',
        re.IGNORECASE,
    )
    incoming_saldo = _to_float(incoming_saldo_raw)

    # Real balance
    real_balance_raw = _last(
        t,
        rf'(?:Нақты{S1}қалдық|Реальный{S1}баланс):{S0}({NUM_RE})',
        re.IGNORECASE,
    )
    real_balance = _to_float(real_balance_raw)

    # Blocked funds
    blocked_raw = _last(
        t,
        rf'(?:Қаражатқа{S1}тосқауыл{S1}қою|Блокированные{S1}средства):{S0}({NUM_RE}|0(?:[.,]00)?)',
        re.IGNORECASE,
    )
    blocked_funds = _to_float(blocked_raw)

    # Backfill: if opening missing but incoming saldo present, use it
    if opening_balance is None and incoming_saldo is not None:
        opening_balance = incoming_saldo

    df = pd.DataFrame([{
        "Банк": _norm_spaces(bank_name),
        "Регистрационный номер (Исх. №)": _norm_spaces(reg_no),
        "Дата формирования": _norm_spaces(formed_dt),
        "Телефоны поддержки": _norm_spaces(support_phones),
        "Клиент": client,
        "ИИК/IBAN": iban,
        "БИК": bic,
        "Валюта": ccy,
        "Период (начало)": period_start,
        "Период (конец)": period_end,
        "Кредитный лимит": credit_limit,
        "Входящий остаток": opening_balance,
        "Входящее сальдо": incoming_saldo,
        "Реальный баланс": real_balance,
        "Блокированные средства": blocked_funds,
        "ИИН/БИН": iin_bin,          # ← NEW
    }])


    # доп. колонки для UI
    df["account_holder_name"] = df["Клиент"]
    df["account_number"] = df["ИИК/IBAN"]
    df["iin_bin"] = df["ИИН/БИН"]
    return df
