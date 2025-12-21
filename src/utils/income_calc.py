# src/utils/income_calc.py

from __future__ import annotations

import re
from typing import Iterable, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# ====== ДЕФОЛТНЫЕ НАСТРОЙКИ БИЗНЕС-ПРАВИЛ ====================================

DEFAULT_EXCLUDED_KNP_BASE: set[str] = {
    "10", "12", "121", "131", "132", "192", "193", "194", "195",
    "211", "213", "221", "223", "230", "290", "342", "343", "344",
    "345", "350", "361", "390", "411", "413", "419", "421", "423",
    "424", "429", "430", "911", "912",
}

DEFAULT_EXCLUDED_KNP_EXTRA: set[str] = {
    "310", "312", "314", "315", "316", "317",
    "320", "321", "322", "324", "329",
}

DEFAULT_EXTRA_KNP_CUTOFF_DATE = pd.Timestamp(2025, 7, 22)


DEFAULT_NON_BUSINESS_KEYWORDS: list[str] = [
    "возврат",
    "отмена",
    # МКО
    "money-express",
    "tengeda",
    "solva lite",
    "acredit",
    "cashdrive",
    "честное слово",
    "tomi.",
    "tengebai",
    "i-credit",
    "kviku",
    "lime",
    "деньги-клик",
    "alacredit деньги",
    "quick money",
    "мани мен",
    "ccloan",
    "gmoney",
    "смартолет",
    "creditplus",
    "vivus",
    "вивус",
    "solva",
    "кредитбар",
    "qanat",
    "turbomoney",
    "займер",
    "koke",
    "tengo",
    "onecredit",
    "credit365",
    # прочее
    "несие",
    "социальный счет",
    "cash-in",
    "проданный автомобиль",
    "кошельк",
    "зарплата",
    "жалақы",
    "арест",
    "қайтар",
    "пенсионные",
    "конверт",
    "банкомат",
    "терминал",
    "popolnenie depozita",
    "зейнетақы",
    "социаль",
    "командировочные",
    # букмекеры
    "1xbet",
    "pin-up",
    "olimpbet",
    "parimatch",
    "winline",
    "ubet",
    "tennisi",
    "fonbet",
    "ringobet",
]

DEFAULT_KEYWORDS_KEEP_IF_KNP_099: list[str] = [
    "возмещение",
    "возмещ.",
    "гарант",
]

# Варианты написания Банка ЦентрКредит для whitelist
BCC_KEYWORDS: list[str] = [
    "банк центр кредит",
    "банкцентркредит",
    "банк центркредит",
    "банкцентр кредит",
    "бцк",
    "bcc",
    "bank center credit",
    "bankcentrcredit",
]

# ====== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==============================================

def _to_float_ru(s: str):
    if pd.isna(s):
        return np.nan
    s = str(s)
    s = s.replace("\xa0", " ").replace("\u202f", " ").replace(" ", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    if s in ("", "."):
        return np.nan
    try:
        return float(s)
    except Exception:
        return np.nan

def _normalize_knp_series(knp: pd.Series) -> pd.Series:
    """
    Оставляем только цифры, обрезаем лидирующие нули.
    Пустые/битые значения -> "".
    """
    knp_raw = (
        knp.astype(str)
           .str.extract(r"(\d+)", expand=False)
           .fillna("")
    )
    return knp_raw.str.lstrip("0")


def _parse_op_date_series(
    s: pd.Series,
    date_pattern: str = r"(\d{2}\.\d{2}\.\d{4})",
    date_format: str = "%d.%m.%Y",
) -> pd.Series:
    """
    Достаёт дату операции (без времени) как Timestamp из строки.
    """
    date_str = s.astype(str).str.extract(date_pattern, expand=False)
    return pd.to_datetime(date_str, format=date_format, errors="coerce")


# ====== ОСНОВНАЯ РАЗМЕТКА ТРАНЗАКЦИЙ =========================================

def mark_business_income_transactions(
    tx: pd.DataFrame,
    *,
    col_op_date: str,
    col_credit: str,
    col_knp: str,
    col_purpose: str,
    col_counterparty: str,
    excluded_knp_base: Optional[Iterable[str]] = None,
    excluded_knp_extra: Optional[Iterable[str]] = None,
    extra_knp_cutoff_date: pd.Timestamp = DEFAULT_EXTRA_KNP_CUTOFF_DATE,
    non_business_keywords: Optional[Sequence[str]] = None,
    keywords_keep_if_knp_099: Optional[Sequence[str]] = None,
    op_date_pattern: str = r"(\d{2}\.\d{2}\.\d{4})",
    op_date_format: str = "%d.%m.%Y",
    verbose: bool = False,
    max_examples: int = 5,
) -> pd.DataFrame:
    """
    Помечает транзакции флагами "бизнес / небизнес".
    Ничего не дропает, только добавляет технические колонки с префиксом ip_.

    Возвращаемый df содержит:
      - ip_knp_norm
      - ip_op_date
      - ip_is_non_business_by_knp
      - ip_is_non_business_by_keywords
      - ip_is_non_business
      - ip_is_business_income
    """
    df = tx.copy()

    # --- дефолтные параметры, если не переданы ---
    if excluded_knp_base is None:
        excluded_knp_base = DEFAULT_EXCLUDED_KNP_BASE
    else:
        excluded_knp_base = set(excluded_knp_base)

    if excluded_knp_extra is None:
        excluded_knp_extra = DEFAULT_EXCLUDED_KNP_EXTRA
    else:
        excluded_knp_extra = set(excluded_knp_extra)

    if non_business_keywords is None:
        non_business_keywords = DEFAULT_NON_BUSINESS_KEYWORDS

    if keywords_keep_if_knp_099 is None:
        keywords_keep_if_knp_099 = DEFAULT_KEYWORDS_KEEP_IF_KNP_099

    # --- нормализованный КНП, дата операции ---
    df["ip_knp_norm"] = _normalize_knp_series(df[col_knp])
    df["ip_op_date"] = _parse_op_date_series(
        df[col_op_date],
        date_pattern=op_date_pattern,
        date_format=op_date_format,
    )

    # --- логика по КНП ---
    base_mask = df["ip_knp_norm"].isin(excluded_knp_base)
    extra_mask = (
        (df["ip_op_date"] >= extra_knp_cutoff_date)
        & df["ip_knp_norm"].isin(excluded_knp_extra)
    )
    df["ip_is_non_business_by_knp"] = base_mask | extra_mask

    # --- текст для поиска ключевых слов ---
    purpose = df[col_purpose].fillna("").astype(str)
    counterparty = df[col_counterparty].fillna("").astype(str)
    text = (purpose + " " + counterparty).str.lower()

    if non_business_keywords:
        pattern_excl = r"(" + "|".join(re.escape(k.lower()) for k in non_business_keywords) + r")"
        df["ip_is_non_business_by_keywords"] = text.str.contains(
            pattern_excl, case=False, na=False
        )
    else:
        df["ip_is_non_business_by_keywords"] = False

    # --- override: если это Банк ЦентрКредит, НЕ считаем как небизнес по словам
    if BCC_KEYWORDS:
        pattern_bcc = r"(" + "|".join(re.escape(k.lower()) for k in BCC_KEYWORDS) + r")"
        bcc_mask = text.str.contains(pattern_bcc, case=False, na=False)
        df.loc[bcc_mask, "ip_is_non_business_by_keywords"] = False


    # --- правило для КНП 099 (возмещение/гарант) ---
    if keywords_keep_if_knp_099:
        pattern_keep = r"(" + "|".join(re.escape(k.lower()) for k in keywords_keep_if_knp_099) + r")"
        knp_str = (
            df[col_knp]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .fillna("")
            .str.zfill(3)
        )
        knp099_mask = knp_str.eq("099")
        kw_keep_mask = text.str.contains(pattern_keep, case=False, na=False)
        override_keep_mask = knp099_mask & kw_keep_mask
    else:
        override_keep_mask = False  # scalar bool, нормально комбинируется с Series

    # --- итоговый флаг небизнесовой операции ---
    df["ip_is_non_business"] = (
        (df["ip_is_non_business_by_knp"] | df["ip_is_non_business_by_keywords"])
        & ~override_keep_mask
    )

    # --- бизнес-доход (кредит > 0 и не небизнес) ---
    # --- бизнес-доход (кредит > 0 и не небизнес) ---
    credit = df[col_credit].apply(_to_float_ru).fillna(0.0)
    df["ip_credit_amount"] = credit
    df["ip_is_business_income"] = (~df["ip_is_non_business"]) & (df["ip_credit_amount"] > 0)

    # ======================= DEBUG / VERBOSE ==================================
    if verbose:
        total = len(df)
        n_knp = int(df["ip_is_non_business_by_knp"].sum())
        n_kw = int(df["ip_is_non_business_by_keywords"].sum())
        n_nonbiz = int(df["ip_is_non_business"].sum())
        n_biz = int(df["ip_is_business_income"].sum())
        if keywords_keep_if_knp_099:
            n_override = int(getattr(override_keep_mask, "sum", lambda: 0)())
        else:
            n_override = 0

        print("\n[income_calc] ===== IP income marking summary =====")
        print(f"[income_calc] total rows:              {total}")
        print(f"[income_calc] non-business by KNP:     {n_knp}")
        print(f"[income_calc] non-business by keywords:{n_kw}")
        print(f"[income_calc] overrides (KNP=099):     {n_override}")
        print(f"[income_calc] total non-business:      {n_nonbiz}")
        print(f"[income_calc] business income rows:    {n_biz}")

        # Примеры по КНП
        if n_knp > 0:
            print("\n[income_calc] examples excluded by KNP:")
            print(
                df.loc[df["ip_is_non_business_by_knp"],
                       [col_op_date, col_knp, col_credit, col_purpose]]
                  .head(max_examples)
            )

        # Примеры по ключевым словам
        if n_kw > 0:
            print("\n[income_calc] examples excluded by keywords:")
            print(
                df.loc[df["ip_is_non_business_by_keywords"],
                       [col_op_date, col_knp, col_credit, col_purpose, col_counterparty]]
                  .head(max_examples)
            )

        # Примеры override
        if n_override > 0 and hasattr(override_keep_mask, "any") and override_keep_mask.any():
            print("\n[income_calc] examples KEPT due to KNP=099 + 'возмещение/гарант':")
            print(
                df.loc[override_keep_mask,
                       [col_op_date, col_knp, col_credit, col_purpose, col_counterparty]]
                  .head(max_examples)
            )

    return df


# ====== РАСЧЁТ ДОХОДА ИП =====================================================

def compute_ip_income(
    tx: pd.DataFrame,
    *,
    col_op_date: str,
    col_credit: str,
    col_knp: str,
    col_purpose: str,
    col_counterparty: str,
    months_back: Optional[int] = None,
    statement_generation_date: Optional[pd.Timestamp] = None,
    excluded_knp_base: Optional[Iterable[str]] = None,
    excluded_knp_extra: Optional[Iterable[str]] = None,
    extra_knp_cutoff_date: pd.Timestamp = DEFAULT_EXTRA_KNP_CUTOFF_DATE,
    non_business_keywords: Optional[Sequence[str]] = None,
    keywords_keep_if_knp_099: Optional[Sequence[str]] = None,
    op_date_pattern: str = r"(\d{2}\.\d{2}\.\d{4})",
    op_date_format: str = "%d.%m.%Y",
    verbose: bool = False,
    max_examples: int = 5,
    extra_candidate_mask: Optional[pd.Series] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Универсальный расчёт дохода ИП.

    Возвращает:
      enriched_tx      – исходный tx с добавленными флагами ip_*
      monthly_income   – DataFrame с доходом по месяцам:
                         колонки ['month', 'business_income']
      summary          – dict с ключами:
                         total_income_adjusted, total_sum, max_transaction, ...
    """
    # 1) Базовая разметка по КНП/ключевым словам
    enriched = mark_business_income_transactions(
        tx,
        col_op_date=col_op_date,
        col_credit=col_credit,
        col_knp=col_knp,
        col_purpose=col_purpose,
        col_counterparty=col_counterparty,
        excluded_knp_base=excluded_knp_base,
        excluded_knp_extra=excluded_knp_extra,
        extra_knp_cutoff_date=extra_knp_cutoff_date,
        non_business_keywords=non_business_keywords,
        keywords_keep_if_knp_099=keywords_keep_if_knp_099,
        op_date_pattern=op_date_pattern,
        op_date_format=op_date_format,
        verbose=verbose,
        max_examples=max_examples,
    )

    # 2) Дополнительный фильтр: внешняя маска допустимых кандидатов
    #    (например, valid_for_ip_income у Kaspi Gold)
    if extra_candidate_mask is not None:
        extra = pd.Series(extra_candidate_mask, index=tx.index)
        extra = extra.reindex(enriched.index)
        extra = extra.fillna(False).astype(bool)
        enriched["ip_is_business_income"] = enriched["ip_is_business_income"] & extra

    # 3) Оставляем только бизнес-доход
    df_inc = enriched[enriched["ip_is_business_income"]].copy()

    if df_inc.empty:
        if verbose:
            print("\n[income_calc] no business income rows after filtering.")
        monthly_income = pd.DataFrame(columns=["month", "business_income"])
        summary = {
            "total_income_adjusted": 0.0,
            "formula": "sum - max - min + sum/6",
            "total_sum": 0.0,
            "max_transaction": 0.0,
            "min_transaction": 0.0,
            "mean_transaction": 0.0,
            "transactions_used": 0,
        }
        return enriched, monthly_income, summary

    # 4) Ограничение по последним N месяцев (если задано)
    if months_back is not None:
        min_dt = None
        max_dt = None

        if statement_generation_date is not None and pd.notna(statement_generation_date):
            stmt_dt = pd.to_datetime(statement_generation_date)

            # конец периода: последний день предыдущего месяца
            first_of_stmt_month = stmt_dt.replace(day=1)
            end_dt = first_of_stmt_month - pd.Timedelta(days=1)

            # начало периода: первый день месяца, который на (months_back-1) месяцев раньше end_dt
            start_of_prev_month = end_dt + pd.offsets.MonthBegin(0)
            start_dt = start_of_prev_month - pd.DateOffset(months=months_back - 1)

            min_dt, max_dt = start_dt.normalize(), end_dt.normalize()
        else:
            # fallback: по последней дате операции
            max_dt = enriched["ip_op_date"].max()
            if pd.notna(max_dt):
                min_dt = (max_dt + pd.offsets.MonthBegin(1)) - pd.DateOffset(months=months_back)
                min_dt = min_dt.normalize()
                max_dt = max_dt.normalize()

        if min_dt is not None and max_dt is not None:
            df_inc = df_inc[
                (df_inc["ip_op_date"] >= min_dt) & (df_inc["ip_op_date"] <= max_dt)
            ]
            if verbose:
                print(f"\n[income_calc] limiting to last {months_back} months:")
                print(f"[income_calc] from {min_dt.date()} to {max_dt.date()}")

    if df_inc.empty:
        if verbose:
            print("\n[income_calc] no business income rows after months_back filter.")
        monthly_income = pd.DataFrame(columns=["month", "business_income"])
        summary = {
            "total_income_adjusted": 0.0,
            "formula": "sum - max - min + sum/6",
            "total_sum": 0.0,
            "max_transaction": 0.0,
            "min_transaction": 0.0,
            "mean_transaction": 0.0,
            "transactions_used": 0,
        }
        return enriched, monthly_income, summary

    # 5) Группировка по месяцу
    df_inc["month"] = df_inc["ip_op_date"].dt.to_period("M")
    monthly_income = (
        df_inc.groupby("month")["ip_credit_amount"]
        .sum()
        .reset_index(name="business_income")
    )

    # 6) Новая формула Adjusted income
    amounts = df_inc["ip_credit_amount"].astype(float)

    if amounts.empty:
        total_sum = max_val = min_val = mean_val = 0.0
        income_adjusted = 0.0
    else:
        total_sum = float(amounts.sum())
        max_val = float(amounts.max())
        min_val = float(amounts.min())
        mean_val = float(amounts.mean())
        income_adjusted = total_sum - max_val - min_val + total_sum / 6.0

    summary = {
        "total_income_adjusted": income_adjusted,
        "formula": "sum - max - min + sum/6",
        "total_sum": total_sum,
        "max_transaction": max_val,
        "min_transaction": min_val,
        "mean_transaction": mean_val,
        "transactions_used": len(amounts),
    }

    return enriched, monthly_income, summary
