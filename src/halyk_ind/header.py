# halyk_b_header_parser_v3.py
import re
import pandas as pd
from typing import Optional, Dict, Any, List
import json

WS = r" \t\r\n\u00A0\u202F"
S0 = rf"[{WS}]*"
NUM_RE = r"[+-]?\d[\d \u00A0\u202F]*,\d{2}"

def _last(text: str, pat: str, flags=0) -> Optional[str]:
    m = None
    for m in re.finditer(pat, text, flags):
        pass
    return m.group(1).strip() if m else None

def parse_header_type_b(page_text: str) -> pd.DataFrame:
    t = page_text

    # Bank / branch / BIC
    bank   = _last(t, rf'(АО{S0}"?Народный{S0}Банк{S0}Казахстана"?)', re.IGNORECASE)
    branch = _last(t, rf"Филиал:{S0}([^\n]+)")
    bic    = _last(t, rf"БИК:{S0}([A-Z0-9]{{8,11}})")

    # Client
    fio = _last(t, rf"ФИО:{S0}(.+?){S0}(?:(?:ИИН|$))", re.DOTALL)
    iin = _last(t, rf"ИИН:{S0}([0-9]{{12}})")

    # Dates / period
    dt_report    = _last(t, rf"Дата{S0}формирования{S0}выписки:{S0}([0-9.]+)")
    period_start = _last(t, rf"Период{S0}выписки:{S0}с{S0}([0-9.]+)")
    period_end   = _last(t, rf"Период{S0}выписки:{S0}с{S0}[0-9.]+{S0}по{S0}([0-9.]+)")

    # Account / currency / card
    acc_type = _last(t, rf"Тип{S0}счета:{S0}([^\n]+)")
    acc_no   = _last(t, rf"(?:Номер{S0}счета|Выписка{S0}по{S0}счету):{S0}([A-Z0-9]+)")
    ccy      = _last(t, rf"Валюта{S0}счета:{S0}([A-Z]{{3}})")
    card_no  = _last(t, rf"(?:Номер{S0}карточки|№{S0}карточки):{S0}([0-9*]+)")

    # Opening/closing balances & limits
    opening  = _last(t, rf"Входящий{S0}остаток:{S0}({NUM_RE})", re.IGNORECASE)
    closing  = _last(t, rf"Исходящий{S0}остаток:{S0}({NUM_RE})", re.IGNORECASE)
    avail    = _last(t, rf"Доступная{S0}сумма.*?:{S0}({NUM_RE})", re.IGNORECASE | re.DOTALL)
    lim_set  = _last(t, rf"Установленный{S0}кредитный{S0}лимит:{S0}({NUM_RE}|0,00)", re.IGNORECASE)
    lim_free = _last(t, rf"Доступный{S0}кредитный{S0}лимит:{S0}({NUM_RE}|0,00)", re.IGNORECASE)
    lim_pay  = _last(t, rf"Платеж{S0}по{S0}кредитному{S0}лимиту:{S0}({NUM_RE}|0,00)", re.IGNORECASE)
    pay_date = _last(t, rf"Дата{S0}платежа:{S0}([0-9.]*)", re.IGNORECASE)

    # Blocked funds
    blocked_ops   = _last(t, rf"По{S0}операциям{S0}({NUM_RE}|0,00)", re.IGNORECASE)
    blocked_legal = _last(t, rf"По{S0}требованиям{S0}третьих{S0}лиц{S0}({NUM_RE}|0,00)", re.IGNORECASE)

    # Account open/close dates (new)
    open_date  = _last(t, rf"Дата{S0}открытия{S0}счета:{S0}([0-9.]+)")
    close_date = _last(t, rf"Дата{S0}закрытия{S0}счета:{S0}([0-9.]*)")  # may be empty

    df = pd.DataFrame([{
        "Банк": bank,
        "Филиал": branch,
        "БИК": bic,
        "ФИО": fio,
        "ИИН": iin,
        "Дата формирования выписки": dt_report,
        "Период (начало)": period_start,
        "Период (конец)": period_end,
        "Тип счета": acc_type,
        "Счет": acc_no,
        "Валюта": ccy,
        "Номер карточки": card_no,
        "Входящий остаток": opening,
        "Исходящий остаток": closing,
        "Доступная сумма": avail,
        "Установленный кредитный лимит": lim_set,
        "Доступный кредитный лимит": lim_free,
        "Платеж по кредитному лимиту": lim_pay,
        "Дата платежа": pay_date,
        "Блокировки (по операциям)": blocked_ops,
        "Блокировки (по требованиям третьих лиц)": blocked_legal,
        "Дата открытия счета": open_date,
        "Дата закрытия счета": close_date,
    }])

    return df

