# src/utils/statement_validation.py

from dataclasses import dataclass
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

import pandas as pd


@dataclass
class BankValidationSchema:
    opening_col: str
    closing_col: str
    credit_turnover_col: Optional[str]
    debit_turnover_col: Optional[str]
    tx_credit_col: str
    tx_debit_col: str
    footer_credit_col: str
    footer_debit_col: str
    period_start_col: Optional[str] = None
    period_end_col: Optional[str] = None


def _to_float_ru_generic(val: Any) -> float:
    """
    Convert '5 576 876,37' / '0,00' / 5.0 / None → float.
    Returns 0.0 if it can't parse.
    """
    if val is None or (isinstance(val, float) or isinstance(val, int)):
        try:
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    if pd.isna(val):
        return 0.0

    s = str(val)
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    s = s.replace(" ", "")
    s = s.replace(",", ".")
    if not s:
        return 0.0

    try:
        return float(s)
    except ValueError:
        return 0.0



def _parse_number(val) -> float:
    if pd.isna(val):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)

    s = str(val)
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    s = s.replace(" ", "")
    # выкидываем всё, что не цифра / точка / запятая / минус
    s = re.sub(r"[^0-9,.\-]", "", s)
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        # если совсем мусор — считаем 0 для валидации
        return 0.0


def _col_as_float(df: pd.DataFrame, col: str) -> float:
    val = df.iloc[0][col]
    return _parse_number(val)

def _maybe_col_as_float(df: pd.DataFrame, col: Optional[str]) -> Optional[float]:
    if col is None:
        return None
    if col not in df.columns:
        return None
    return _col_as_float(df, col)



PDF_DATE_RE = re.compile(
    r"^D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})"
)


def _parse_pdf_date(date_str: str) -> Optional[datetime]:
    """
    Parse PDF date format like:
      D:20240922143000+05'00'
    into datetime(2024, 9, 22, 14, 30, 0).
    Timezone is ignored (treated as naive).
    """
    if not date_str:
        return None
    m = PDF_DATE_RE.match(date_str)
    if not m:
        return None
    y, mo, d, h, mi, s = map(int, m.groups())
    return datetime(y, mo, d, h, mi, s)


def validate_pdf_metadata_from_json(
    pdf_json: Dict[str, Any],
    *,
    bank: str,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    period_date_format: str = "%d.%m.%Y",
    max_days_after_period_end: int = 7,
    allowed_creators: Optional[List[str]] = None,
    allowed_producers: Optional[List[str]] = None,
) -> Tuple[List[str], Dict[str, Any]]:
    """
    Validate PDF metadata from convert_pdf_json_page output.

    Checks:
      - presence of CreationDate / ModDate
      - Creator / Producer in whitelists (if given)
      - ModDate close to CreationDate
      - CreationDate not too far from statement period_end
    """
    flags: List[str] = []
    debug: Dict[str, Any] = {"bank": bank}

    meta = pdf_json.get("metadata") or {}

    creation_raw = meta.get("/CreationDate")
    mod_raw = meta.get("/ModDate")
    creator = meta.get("/Creator")
    producer = meta.get("/Producer")

    creation_dt = _parse_pdf_date(creation_raw) if creation_raw else None
    mod_dt = _parse_pdf_date(mod_raw) if mod_raw else None

    debug.update(
        pdf_creation_raw=creation_raw,
        pdf_mod_raw=mod_raw,
        pdf_creator=creator,
        pdf_producer=producer,
        pdf_creation_dt=creation_dt.isoformat() if creation_dt else None,
        pdf_mod_dt=mod_dt.isoformat() if mod_dt else None,
    )

    # --- presence checks ---
    if not creation_raw:
        flags.append("pdf_missing_creation_date")
    if not mod_raw:
        flags.append("pdf_missing_mod_date")

    # --- whitelist checks ---
    if allowed_creators is not None:
        if creator not in allowed_creators:
            flags.append("pdf_creator_not_whitelisted")

    if allowed_producers is not None:
        if producer not in allowed_producers:
            flags.append("pdf_producer_not_whitelisted")

    # --- modification vs creation (suspicious edits) ---
    if creation_dt and mod_dt:
        # more than 60 seconds difference -> treat as "modified"
        if abs((mod_dt - creation_dt).total_seconds()) > 60:
            flags.append("pdf_modified_after_creation")

    # --- link to statement period ---
    if period_end and creation_dt:
        try:
            period_end_dt = datetime.strptime(period_end, period_date_format)
            debug["period_end_dt"] = period_end_dt.isoformat()

            # creation must be >= period_end
            if creation_dt.date() < period_end_dt.date():
                flags.append("pdf_creation_before_period_end")

            # creation not too far after period_end
            latest_ok = period_end_dt + timedelta(days=max_days_after_period_end)
            if creation_dt.date() > latest_ok.date():
                flags.append("pdf_creation_too_late_after_period_end")

        except ValueError as e:
            debug["period_end_parse_error"] = str(e)

    return flags, debug


def validate_statement_generic(
    header_df: pd.DataFrame,
    tx_df: pd.DataFrame,
    footer_df: pd.DataFrame,
    schema: BankValidationSchema,
    tol: float = 0.01,
) -> Tuple[List[str], Dict[str, Any]]:

    """
    Generic numeric validation:
    - opening + Σ(credit) - Σ(debit) == closing
    - header turnovers vs tx sums
    - footer totals vs tx sums
    """
    flags: List[str] = []
    debug: Dict[str, Any] = {}

    # --- header ---
    opening = _col_as_float(header_df, schema.opening_col)
    closing_pdf = _col_as_float(header_df, schema.closing_col)

    credit_turnover_pdf = None
    debit_turnover_pdf = None
    if schema.credit_turnover_col is not None:
        credit_turnover_pdf = _col_as_float(header_df, schema.credit_turnover_col)
    if schema.debit_turnover_col is not None:
        debit_turnover_pdf = _col_as_float(header_df, schema.debit_turnover_col)




    # --- tx sums ---
    total_credit = (
        pd.to_numeric(tx_df[schema.tx_credit_col], errors="coerce")
        .fillna(0.0)
        .sum()
    )
    total_debit = (
        pd.to_numeric(tx_df[schema.tx_debit_col], errors="coerce")
        .fillna(0.0)
        .sum()
    )

    closing_calc = opening + total_credit - total_debit

    # --- footer ---
    footer_credit = None
    footer_debit = None
    if schema.footer_credit_col is not None:
        footer_credit = _col_as_float(footer_df, schema.footer_credit_col)
    if schema.footer_debit_col is not None:
        footer_debit = _col_as_float(footer_df, schema.footer_debit_col)


    # --- checks ---
    if credit_turnover_pdf is not None and abs(total_credit - credit_turnover_pdf) > tol:
        flags.append("credit_turnover_mismatch_header_vs_tx")
    if debit_turnover_pdf is not None and abs(total_debit - debit_turnover_pdf) > tol:
        flags.append("debit_turnover_mismatch_header_vs_tx")

    if abs(closing_calc - closing_pdf) > tol:
        flags.append("closing_balance_mismatch")

    if footer_credit is not None and abs(footer_credit - total_credit) > tol:
        flags.append("footer_credit_mismatch_footer_vs_tx")
    if footer_debit is not None and abs(footer_debit - total_debit) > tol:
        flags.append("footer_debit_mismatch_footer_vs_tx")


    if schema.period_start_col:
        debug["period_start"] = header_df.iloc[0][schema.period_start_col]
    if schema.period_end_col:
        debug["period_end"] = header_df.iloc[0][schema.period_end_col]

    debug.update(
        dict(
            opening=opening,
            closing_pdf=closing_pdf,
            closing_calc=closing_calc,
            total_credit=total_credit,
            total_debit=total_debit,
            header_credit_turnover=credit_turnover_pdf,
            header_debit_turnover=debit_turnover_pdf,
            footer_credit=footer_credit,
            footer_debit=footer_debit,
            tolerance=tol,
        )
    )

    return flags, debug

# ---- Per-bank schemas ----

ALATAU_SCHEMA = BankValidationSchema(
    opening_col="opening_balance",
    closing_col="closing_balance",
    credit_turnover_col="credit_turnover",
    debit_turnover_col="debit_turnover",
    tx_credit_col="Кредит",
    tx_debit_col="Дебет",
    # 👇 match the names produced by _parse_acb_footer_from_tx
    footer_credit_col="total_credit_footer",
    footer_debit_col="total_debit_footer",
    period_start_col="opening_balance_date",  # or whatever you want
    period_end_col="closing_balance_date",
)


BCC_SCHEMA = BankValidationSchema(
    opening_col="Входящее сальдо",
    closing_col="Реальный баланс",
    credit_turnover_col=None,              # no such columns in header
    debit_turnover_col=None,
    tx_credit_col="Кредит / Кредит",
    tx_debit_col="Дебет / Дебет",
    footer_credit_col="total_credit",
    footer_debit_col="total_debit",
    period_start_col="Период (начало)",
    period_end_col="Период (конец)",
)

EURASIAN_SCHEMA = BankValidationSchema(
    opening_col="opening_balance",
    closing_col="closing_balance",      # we just added this into header_df
    credit_turnover_col="credit_turnover",
    debit_turnover_col="debit_turnover",
    tx_credit_col="Кредит",
    tx_debit_col="Дебет",
    footer_credit_col="turnover_credit",
    footer_debit_col="turnover_debit",
    period_start_col="period_start",
    period_end_col="period_end",
)

FORTE_SCHEMA = BankValidationSchema(
    opening_col="opening_balance",   # header_df
    closing_col="closing_balance",   # we'll copy from footer into header
    credit_turnover_col="total_credit",  # copied from footer into header
    debit_turnover_col="total_debit",    # copied from footer into header
    tx_credit_col="Кредит",         # tx_df
    tx_debit_col="Дебет",           # tx_df
    footer_credit_col="total_credit",    # footer_df
    footer_debit_col="total_debit",      # footer_df
    period_start_col="period_start",     # header_df
    period_end_col="period_end",         # header_df
)


FREEDOM_SCHEMA = BankValidationSchema(
    opening_col="opening_balance",
    closing_col="closing_balance",
    credit_turnover_col="credit_turnover",   # we fill this from footer.debit_total/credit_total in batch_parse
    debit_turnover_col="debit_turnover",
    tx_credit_col="Кредит",
    tx_debit_col="Дебет",
    footer_credit_col="credit_total",
    footer_debit_col="debit_total",
    period_start_col="period_start",
    period_end_col="period_end",
)


HALYK_BUSINESS_SCHEMA = BankValidationSchema(
    opening_col="Входящий остаток",
    closing_col="Исходящий_остаток",     # см. пункт 3 ниже
    credit_turnover_col=None,            # в header нет оборотов — пропускаем
    debit_turnover_col=None,
    tx_credit_col="Кредит",
    tx_debit_col="Дебет",
    footer_credit_col="Обороты_Кредит",
    footer_debit_col="Обороты_Дебет",
    period_start_col="Период (начало)",
    period_end_col="Период (конец)",
)


HALYK_INDIVIDUAL_SCHEMA = BankValidationSchema(
    opening_col="Входящий остаток",
    closing_col="Исходящий остаток",
    credit_turnover_col=None,               # нет оборотов в header
    debit_turnover_col=None,
    tx_credit_col="Приход в валюте счета",
    tx_debit_col="Расход в валюте счета",
    footer_credit_col=None,                 # футер без сумм – не валидируем
    footer_debit_col=None,
    period_start_col="Период (начало)",
    period_end_col="Период (конец)",
)

KASPI_PAY_SCHEMA = BankValidationSchema(
    opening_col="Входящий остаток",
    closing_col="Исходящий остаток",
    credit_turnover_col=None,            # в header оборотов нет
    debit_turnover_col=None,
    tx_credit_col="Кредит",              # из kaspi_pay.transactions
    tx_debit_col="Дебет",
    footer_credit_col="total_credit_turnover",   # из kaspi_pay.footer
    footer_debit_col="total_debit_turnover",
    period_start_col="Период (начало)",  # из kaspi_pay.header
    period_end_col="Период (конец)",
)



BANK_SCHEMAS: Dict[str, BankValidationSchema] = {
    "ALATAU_CITY": ALATAU_SCHEMA,
    "BCC": BCC_SCHEMA,
    "EURASIAN": EURASIAN_SCHEMA,
    "FORTE": FORTE_SCHEMA,
    "FREEDOM": FREEDOM_SCHEMA,
    "HALYK_BUSINESS": HALYK_BUSINESS_SCHEMA,
    "HALYK_INDIVIDUAL": HALYK_INDIVIDUAL_SCHEMA,
    "KASPI_PAY": KASPI_PAY_SCHEMA,   # <-- добавить это
}
