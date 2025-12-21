# src/kaspi_parser/checks_consistency.py
from typing import Dict, Tuple, List, Optional
import pandas as pd
from src.kaspi_gold.extractors import compute_category_sums_simple
from datetime import datetime, timedelta


def check_summary_mismatch_simple(
    summary_reported: Dict[str, float],
    tx_df: pd.DataFrame,
    tol: float = 1.0
) -> Tuple[List[str], Dict[str, float]]:
    """
    Return (flags, diffs)
    diffs[cat] = computed - reported
    """
    flags = []
    diffs: Dict[str, float] = {}

    computed = compute_category_sums_simple(tx_df)
    cats = set(list(summary_reported.keys()) + list(computed.keys()))
    for cat in cats:
        rep_val = float(summary_reported.get(cat, 0.0))
        comp_val = float(computed.get(cat, 0.0))
        diff = comp_val - rep_val
        diffs[cat] = diff
        if abs(diff) > tol:
            flags.append(f"SUMMARY_MISMATCH_{cat}")
    return flags, diffs

def check_balance_rollforward(
    opening_balance: Optional[float],
    closing_balance: Optional[float],
    tx_df: pd.DataFrame,
    tol: float = 1.0
) -> Optional[str]:
    """
    Verify that opening_balance + sum(tx_df.amount) ~= closing_balance.
    """
    if opening_balance is None or closing_balance is None:
        return None
    total_flow = float(tx_df["amount"].sum()) if not tx_df.empty else 0.0
    expected_closing = opening_balance + total_flow
    if abs(expected_closing - closing_balance) > tol:
        return "BALANCE_ROLLFORWARD_MISMATCH"
    return None






def _parse_ddmmy(date_str: str) -> Optional[datetime]:
    """
    Strict Kaspi-style date parser. Accepts 'dd.mm.yy' or 'dd.mm.yyyy'.
    Returns datetime or None.
    """
    if not isinstance(date_str, str):
        return None
    for fmt in ("%d.%m.%y", "%d.%m.%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None





def _is_monotonic_nondecreasing(seq: list[datetime]) -> bool:
    # allow equal consecutive dates
    for i in range(1, len(seq)):
        if seq[i] < seq[i - 1]:
            return False
    return True


def _is_monotonic_nonincreasing(seq: list[datetime]) -> bool:
    # allow equal consecutive dates
    for i in range(1, len(seq)):
        if seq[i] > seq[i - 1]:
            return False
    return True


def check_tx_date_sorting(
    tx_df: pd.DataFrame,
    period_start_str: str,
    period_end_str: str,
) -> Optional[str]:
    """
    Check if transaction dates are in a consistent chronological order.

    Acceptable:
    - ascending by date (oldest -> newest)
    - descending by date (newest -> oldest)
    - ties (same date repeated) anywhere

    We IGNORE rows with dates outside the statement period ±2 days,
    so random garbage rows don't break the check.

    Returns:
        "UNSORTED_TRANSACTIONS" or None
    """
    if tx_df.empty or "date" not in tx_df.columns:
        return None

    period_start_dt = _parse_ddmmy(period_start_str)
    period_end_dt = _parse_ddmmy(period_end_str)

    # fallback if header period didn't parse:
    # just parse all dates and test monotonic.
    if period_start_dt is None or period_end_dt is None:
        parsed = [d for d in ( _parse_ddmmy(x) for x in tx_df["date"] ) if d is not None]
        if len(parsed) <= 2:
            return None
        if _is_monotonic_nondecreasing(parsed):
            return None
        if _is_monotonic_nonincreasing(parsed):
            return None
        return "UNSORTED_TRANSACTIONS"

    # define plausible window with cushion
    lo = period_start_dt - timedelta(days=2)
    hi = period_end_dt + timedelta(days=2)

    cleaned_dates: list[datetime] = []
    for raw in tx_df["date"]:
        dt = _parse_ddmmy(raw)
        if dt is None:
            continue
        if lo <= dt <= hi:
            cleaned_dates.append(dt)

    # can't judge with 0-2 rows
    if len(cleaned_dates) <= 2:
        return None

    # accept ascending OR descending
    if _is_monotonic_nondecreasing(cleaned_dates):
        return None
    if _is_monotonic_nonincreasing(cleaned_dates):
        return None

    # neither ascending nor descending => suspicious splice
    return "UNSORTED_TRANSACTIONS"

def check_summary_sign_rules(
    summary_reported: Dict[str, float],
    tx_df: pd.DataFrame
) -> Tuple[List[str], Dict[str, Dict[str, float]]]:
    """
    Validate expected sign conventions for categories:
      - 'Пополнения' should be >= 0 (money in)
      - 'Покупки' should be <= 0 (money out)
      - 'Снятия'  should be <= 0 (cash withdrawal = money out)

    We will check BOTH:
      1) reported summary box (what the PDF claims)
      2) recomputed totals from tx_df

    Returns:
        (
            flags,
            debug
        )

        flags: [
            'SUMMARY_SIGN_MISMATCH_Пополнения',
            ...
        ]

        debug: {
            'Пополнения': {
                'reported':  2496280.00,
                'computed':  2496280.00,
                'expected': '>= 0'
            },
            'Покупки': {
                'reported': -1023995.00,
                'computed': -1023995.00,
                'expected': '<= 0'
            },
            ...
        }
    """

    # 1. define expected sign rules
    rules = {
        "Пополнения": ">=0",
        "Покупки": "<=0",
        "Снятия": "<=0",
        # we don't enforce for 'Переводы' or 'Разное' because
        # they can go either direction
    }

    # 2. recompute category totals from tx_df using your existing logic
    #    (import this at top of the file)

    computed_totals = compute_category_sums_simple(tx_df)

    flags: List[str] = []
    debug: Dict[str, Dict[str, float]] = {}

    for cat, rule in rules.items():
        rep_val = float(summary_reported.get(cat, 0.0))
        comp_val = float(computed_totals.get(cat, 0.0))

        if rule == ">=0":
            reported_ok = rep_val >= -1e-9
            computed_ok = comp_val >= -1e-9
            expected_desc = ">= 0"

        elif rule == "<=0":
            reported_ok = rep_val <= 1e-9
            computed_ok = comp_val <= 1e-9
            expected_desc = "<= 0"

        else:
            # shouldn't happen, but default to "ok"
            reported_ok = True
            computed_ok = True
            expected_desc = rule

        # prepare debug info for this category
        debug[cat] = {
            "reported": rep_val,
            "computed": comp_val,
            "expected": expected_desc,
        }

        # If either reported OR computed violates the rule -> flag
        if not reported_ok or not computed_ok:
            flags.append(f"SUMMARY_SIGN_MISMATCH_{cat}")

    return flags, debug
