# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
src/ui/app.py

Streamlit UI for bank statement parsing + 12m window analysis.

Run:
  streamlit run src/ui/app.py
"""
from __future__ import annotations

from pathlib import Path
import sys
import math
from datetime import date
from typing import List, Optional, Dict, Any

import pandas as pd
import streamlit as st

# --- ensure project root on sys.path (BEFORE importing src.*) ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# -----------------------------
# Helpers (UI-only)
# -----------------------------
def init_session_state() -> None:
    if "client_name" not in st.session_state:
        st.session_state.client_name = ""
    if "anchor_date" not in st.session_state:
        st.session_state.anchor_date = date.today()
    if "session_iin_bin" not in st.session_state:
        st.session_state.session_iin_bin = None
    if "statements" not in st.session_state:
        st.session_state.statements = []
    if "allow_iin_mismatch" not in st.session_state:
        st.session_state.allow_iin_mismatch = False


def _format_bank_label(bank_key: str) -> str:
    return {
        "kaspi_gold": "Kaspi Gold",
        "kaspi_pay": "Kaspi Pay",
        "halyk_business": "Halyk (Business)",
        "halyk_individual": "Halyk (Individual)",
        "freedom_bank": "Freedom Bank",
        "forte_bank": "ForteBank",
        "eurasian_bank": "Eurasian Bank",
        "bcc_bank": "BCC (CenterCredit)",
        "alatau_city_bank": "Alatau City Bank",
    }.get(bank_key, bank_key)


def build_metadata_df(statements) -> pd.DataFrame:
    if not statements:
        return pd.DataFrame()

    rows = []
    for s in statements:
        row = {
            "pdf_name": getattr(s, "pdf_name", None),
            "bank": getattr(s, "bank", None),
            "account_holder_name": getattr(s, "account_holder_name", None),
            "iin_bin": getattr(s, "iin_bin", None),
            "account_number": getattr(s, "account_number", None),
            "period_from": getattr(s, "period_from", None),
            "period_to": getattr(s, "period_to", None),
            "statement_generation_date": getattr(s, "statement_generation_date", None),
        }

        hdr_df = getattr(s, "header_df", None)
        if hdr_df is not None and hasattr(hdr_df, "empty") and not hdr_df.empty:
            hdr = hdr_df.iloc[0]
            mapping = {
                "Валюта": "currency",
                "БИК": "bic",
                "Кредитный лимит": "credit_limit",
                "Входящий остаток": "opening_balance",
                "Входящее сальдо": "incoming_saldo",
                "Реальный баланс": "real_balance",
                "Блокированные средства": "blocked_funds",
            }
            for src_col, out_col in mapping.items():
                if src_col in hdr.index:
                    row[out_col] = hdr.get(src_col)

        rows.append(row)

    return pd.DataFrame(rows)


def ensure_txn_date(statement) -> None:
    """
    Guarantee statement.tx_df has datetime column 'txn_date'.
    Uses IP_INCOME_CONFIG date column if possible, else fallbacks.
    """
    df = getattr(statement, "tx_df", None)
    if df is None or df.empty:
        return

    if "txn_date" in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df["txn_date"]):
            df["txn_date"] = pd.to_datetime(df["txn_date"], errors="coerce", dayfirst=True)
        statement.tx_df = df
        return

    # lazy import to avoid hanging at app startup
    from src.core.ip_config import IP_INCOME_CONFIG

    cfg = IP_INCOME_CONFIG.get(getattr(statement, "bank", ""), {})
    date_col = cfg.get("col_op_date")

    candidates = [
        date_col,
        "Дата",
        "date",
        "Дата операции",
        "Дата проводки",
        "Дата отражения по счету",
        "Operation date",
    ]
    candidates = [c for c in candidates if c and c in df.columns]
    if not candidates:
        return

    df["txn_date"] = pd.to_datetime(df[candidates[0]], errors="coerce", dayfirst=True)
    statement.tx_df = df


# -----------------------------
# Streamlit App
# -----------------------------
def main() -> None:
    st.set_page_config(page_title="Bank Statement Analyzer", layout="wide")
    st.title("Bank Statement Analyzer (Prototype)")
    st.caption("If you see this line, Streamlit UI is rendering ✅")

    init_session_state()

    # ===== Step 1 =====
    st.header("1. Client info & anchor date")
    with st.form("client_form", clear_on_submit=False):
        st.session_state.client_name = st.text_input(
            "Client name (ФИО / Название компании)",
            value=st.session_state.client_name,
        )
        st.session_state.anchor_date = st.date_input(
            "Anchor date (for tests; prod = today)",
            value=st.session_state.anchor_date,
        )
        submitted = st.form_submit_button("Save / Update session")
        if submitted:
            st.success("Session updated")

    # lazy import core logic (safe after UI renders)
    from src.core.analysis import get_last_full_12m_window

    window_start, window_end = get_last_full_12m_window(st.session_state.anchor_date)
    st.info(f"12-month window: **{window_start} → {window_end}** (inclusive)")

    st.checkbox(
        "Allow mixing ИИН/БИН across statements (testing only)",
        key="allow_iin_mismatch",
    )

    # ===== Step 2–3 =====
    st.header("2–3. Upload statements")
    col_bank, col_file = st.columns([1, 3])

    with col_bank:
        bank_key = st.selectbox(
            "Choose bank",
            options=[
                "kaspi_gold",
                "kaspi_pay",
                "halyk_business",
                "halyk_individual",
                "freedom_bank",
                "forte_bank",
                "eurasian_bank",
                "bcc_bank",
                "alatau_city_bank",
            ],
            format_func=_format_bank_label,
        )

    with col_file:
        uploaded_file = st.file_uploader("Upload PDF", type=["pdf"])

    if uploaded_file is not None:
        st.write(f"Selected: `{uploaded_file.name}`")

    if uploaded_file and st.button("Parse & add statement", type="primary"):
        try:
            # lazy import to avoid startup hang
            from src.core.service import parse_statement

            stmnt = parse_statement(
                bank_key=bank_key,
                pdf_name=uploaded_file.name,
                pdf_bytes=uploaded_file.read(),
            )

            ensure_txn_date(stmnt)

            session_iin = st.session_state.session_iin_bin
            if session_iin is None:
                st.session_state.session_iin_bin = stmnt.iin_bin
                st.session_state.statements.append(stmnt)
                st.success(f"First statement added. Session ИИН/БИН = `{stmnt.iin_bin}`")
            else:
                if stmnt.iin_bin == session_iin or st.session_state.allow_iin_mismatch:
                    st.session_state.statements.append(stmnt)
                    st.success("Statement added")
                else:
                    st.error(
                        f"ИИН/БИН mismatch: statement=`{stmnt.iin_bin}` vs session=`{session_iin}`"
                    )

        except Exception as e:
            st.exception(e)

    # ===== Session overview =====
    st.subheader("Uploaded statements")
    if not st.session_state.statements:
        st.info("No statements yet")
        return

    st.dataframe(build_metadata_df(st.session_state.statements), use_container_width=True)

    # ===== Step 4 =====
    st.header("4. Аналитика по оборотам и аффилированным лицам")

    from src.core.analysis import combine_transactions, compute_ip_income_for_statement

    tx_12m = combine_transactions(
        st.session_state.statements,
        window_start=window_start,
        window_end=window_end,
    )

    if tx_12m.empty:
        st.warning("No transactions found in the 12-month window.")
    else:
        # НОВЫЙ БЛОК: ТАБЛИЦЫ ТОП-9 И АФФИЛИРОВАННЫЕ ЛИЦА
        from src.ui.ui_analysis_report_generator import get_ui_analysis_tables

        # Подготовка данных для анализа (колонки могут отличаться в зависимости от банка)
        df_analysis = tx_12m.copy()
        if 'counterparty_id' not in df_analysis.columns:
            df_analysis['counterparty_id'] = df_analysis['details'].fillna('N/A')
        if 'counterparty_name' not in df_analysis.columns:
            df_analysis['counterparty_name'] = df_analysis['details'].fillna('N/A')

        analysis_results = get_ui_analysis_tables(df_analysis)

        # Визуализация Топ-9
        col_debit, col_credit = st.columns(2)

        with col_debit:
            st.write("**Расходы (Дебет)**")
            if analysis_results["debit_top"]:
                st.dataframe(pd.DataFrame(analysis_results["debit_top"]), use_container_width=True, hide_index=True)
            else:
                st.info("Нет данных по расходам")

        with col_credit:
            st.write("**Приходы (Кредит)**")
            if analysis_results["credit_top"]:
                st.dataframe(pd.DataFrame(analysis_results["credit_top"]), use_container_width=True, hide_index=True)
            else:
                st.info("Нет данных по приходам")

        # Визуализация Аффилированных лиц
        st.subheader("Аффилированные лица (Net расчет)")
        if analysis_results["related_parties"]:
            st.dataframe(pd.DataFrame(analysis_results["related_parties"]), use_container_width=True, hide_index=True)
        else:
            st.info("Нет данных по аффилированным лицам")

        # Конец нового блока
        st.divider()

        with st.expander("Посмотреть все транзакции (12 месяцев)"):
            st.dataframe(tx_12m, use_container_width=True)

    # IP flags + summary
    all_enriched = []
    summary_rows = []

    for stmnt in st.session_state.statements:
        enriched, summary = compute_ip_income_for_statement(stmnt, window_start, window_end)
        if enriched is not None and not enriched.empty:
            all_enriched.append(enriched)
        if summary is not None:
            summary_rows.append(summary)

    st.subheader("Transactions with IP flags")
    if all_enriched:
        tx_ip = pd.concat(all_enriched, ignore_index=True)
        st.dataframe(tx_ip, use_container_width=True)
    else:
        st.info("No IP transactions in window / or config not matched.")

    st.subheader("Income summary")
    if summary_rows:
        st.dataframe(pd.DataFrame(summary_rows), use_container_width=True)
    else:
        st.info("No income summary.")

    # Kaspi related parties (старый блок, оставляем для совместимости или убираем)
    if not tx_12m.empty and "bank" in tx_12m.columns and (tx_12m["bank"] == "Kaspi Gold").any():
        from src.utils.kaspi_gold_related_parties import summarize_kaspi_gold_persons

        kaspi_tx = tx_12m.loc[tx_12m["bank"] == "Kaspi Gold"].copy()
        if {"details", "amount", "txn_date"}.issubset(kaspi_tx.columns):
            with st.expander("Kaspi Gold – related parties (Legacy view)"):
                rel_df = summarize_kaspi_gold_persons(
                    kaspi_tx,
                    details_col="details",
                    amount_col="amount",
                    date_col="txn_date",
                    fallback_date_col="txn_date",
                    fallback_date_format="%Y-%m-%d",
                )
                st.dataframe(rel_df, use_container_width=True)


if __name__ == "__main__":
    main()