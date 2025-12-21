# src/utils/kaspi_gold_utils.py

from __future__ import annotations

import re
from typing import Optional
import numpy as np

import pandas as pd


# --- Regex helpers ---

# Cyrillic + Kazakh ranges (uppercase / lowercase)
CYR_UP = "А-ЯЁӘІҢҒҮҰҚӨҺ"
CYR_LOW = "а-яёәіңғүұқөһ"

# One name word: "Сағынтқан", "Нургуль", "Махаббат-Меруерт"
#  - first letter uppercase
#  - rest lowercase
#  - optional inner hyphen with same pattern
WORD = rf"[{CYR_UP}][{CYR_LOW}]+(?:-[{CYR_UP}][{CYR_LOW}]+)*"

# Full pattern:
#   1) one WORD
#   2) optional second WORD (separated by space)
#   3) space + single capital initial + "."
#
# Examples:
#   "Олжас А."
#   "Гульзипа А."
#   "Сағынтқан Ш."
#   "Магия К."
#   "Алия Меруерт С."
FULL_NAME_RE = re.compile(
    rf"({WORD}(?:\s+{WORD})?\s+[{CYR_UP}]\.)"
)


def _extract_person_name_from_details(details: object) -> Optional[str]:
    """
    Extracts a name like 'Олжас А.' or 'Алия Меруерт С.' from the details string.

    Returns the matched string (trimmed) or None if no match.
    """
    if not isinstance(details, str):
        return None
    m = FULL_NAME_RE.search(details)
    if not m:
        return None
    return m.group(1).strip()


def summarize_kaspi_gold_persons(
    tx_df: pd.DataFrame,
    details_col: str = "details",
    amount_col: str = "amount",
    date_col: str = "txn_date",
    fallback_date_col: str = "date",
    fallback_date_format: str = "%d.%m.%y",
    low_impact_threshold: float = 0.01,   # 1%
) -> pd.DataFrame:
    """
    Для выписки Kaspi Gold:

    Для каждого найденного ФИО (person_name) считаем:
      - incoming_total   — сумма всех входящих операций (amount > 0)
      - outgoing_total   — сумма всех исходящих операций (amount < 0, по модулю)
      - total_amount     — чистый результат: incoming_total - outgoing_total (со знаком)
      - incoming_count   — количество входящих операций
      - outgoing_count   — количество исходящих операций
      - txn_count        — всего операций
      - begin_date       — дата первой операции
      - end_date         — дата последней операции
    """

    if details_col not in tx_df.columns:
        raise ValueError(f"Column '{details_col}' not found in tx_df")

    if amount_col not in tx_df.columns:
        raise ValueError(f"Column '{amount_col}' not found in tx_df")

    df = tx_df.copy()

    # --- ensure date col exists and is datetime ---

    if date_col not in df.columns:
        if fallback_date_col not in df.columns:
            raise ValueError(
                f"Neither '{date_col}' nor '{fallback_date_col}' found in tx_df"
            )
        df[date_col] = pd.to_datetime(
            df[fallback_date_col].astype(str),
            format=fallback_date_format,
            errors="coerce",
        )

    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    df = df.dropna(subset=[date_col]).copy()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "person_name",
                "incoming_total",
                "outgoing_total",
                "total_amount",
                "incoming_count",
                "outgoing_count",
                "txn_count",
                "begin_date",
                "end_date",
            ]
        )

    # --- extract person_name from details ---

    df["person_name"] = df[details_col].apply(_extract_person_name_from_details)
    df = df[~df["person_name"].isna()].copy()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "person_name",
                "incoming_total",
                "outgoing_total",
                "total_amount",
                "incoming_count",
                "outgoing_count",
                "txn_count",
                "begin_date",
                "end_date",
            ]
        )

    # --- numeric amount and incoming / outgoing splits ---

    df["_amount_num"] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0)

    # incoming = credit (поступления на карту)
    df["_incoming"] = df["_amount_num"].where(df["_amount_num"] > 0, 0.0)

    # outgoing = debit (списания с карты), берём модуль
    df["_outgoing"] = -df["_amount_num"].where(df["_amount_num"] < 0, 0.0)

    # --- group by person_name ---

    grouped = (
        df.groupby("person_name")
        .agg(
            incoming_total=("_incoming", "sum"),
            outgoing_total=("_outgoing", "sum"),
            total_amount=("_amount_num", "sum"),
            incoming_count=("_incoming", lambda s: (s > 0).sum()),
            outgoing_count=("_outgoing", lambda s: (s > 0).sum()),
            begin_date=(date_col, "min"),
            end_date=(date_col, "max"),
            txn_count=(date_col, "count"),
        )
        .reset_index()
    )

    grouped["turnover_total"] = grouped["incoming_total"] + grouped["outgoing_total"]
    grouped["is_key_supplier"] = grouped["outgoing_total"] > grouped["incoming_total"]

    # доля исходящих в обороте этого контрагента
    turnover = grouped["turnover_total"].replace(0, np.nan)
    share_outgoing = (grouped["outgoing_total"] / turnover).fillna(0.0)

    # сохраняем и в доле, и в процентах
    grouped["outgoing_share_of_turnover"] = share_outgoing  # 0.0–1.0
    grouped["outgoing_share_pct"] = share_outgoing * 100.0  # 0.0–100.0

    # 2) LOW IMPACT RELATED:
    grouped["is_low_impact_related"] = (
            ~grouped["is_key_supplier"]
            & (share_outgoing <= low_impact_threshold)
            & grouped["turnover_total"].gt(0)
    )

    # Итоговый флаг
    grouped["exclude_from_income"] = grouped["is_key_supplier"] | grouped["is_low_impact_related"]

    # Причина исключения
    grouped["exclude_reason"] = ""
    grouped.loc[grouped["is_key_supplier"], "exclude_reason"] = "key_supplier"
    grouped.loc[grouped["is_low_impact_related"], "exclude_reason"] = "low_outgoing_le_1pct"

    # аккуратный порядок колонок
    cols = [
        "person_name",
        "incoming_total",
        "outgoing_total",
        "total_amount",
        "turnover_total",
        "outgoing_share_of_turnover",
        "outgoing_share_pct",
        "incoming_count",
        "outgoing_count",
        "txn_count",
        "begin_date",
        "end_date",
        "is_key_supplier",
        "is_low_impact_related",
        "exclude_from_income",
        "exclude_reason",
    ]
    return grouped[cols]
