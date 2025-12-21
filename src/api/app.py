#!/usr/bin/env python3
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
        rows.append(row)

    return pd.DataFrame(rows)


def ensure_txn_date(statement) -> None:
    df = getattr(statement, "tx_df", None)
    if df is None or df.empty:
        return

    if "txn_date" in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df["txn_date"]):
            df["txn_date"] = pd.to_datetime(df["txn_date"], errors="coerce", dayfirst=True)
        return

    from src.core.ip_config import IP_INCOME_CONFIG
    cfg = IP_INCOME_CONFIG.get(getattr(statement, "bank", ""), {})
    date_col = cfg.get("col_op_date")

    candidates = [date_col, "Дата", "date", "Дата операции", "Дата проводки"]
    candidates = [c for c in candidates if c and c in df.columns]
    if candidates:
        df["txn_date"] = pd.to_datetime(df[candidates[0]], errors="coerce", dayfirst=True)
    statement.tx_df = df


# -----------------------------
# Streamlit App
# -----------------------------
def main() -> None:
    st.set_page_config(page_title="Bank Statement Analyzer", layout="wide")
    st.title("Bank Statement Analyzer")

    init_session_state()

    # ===== Step 1 =====
    st.header("1. Client info & anchor date")
    with st.form("client_form", clear_on_submit=False):
        st.session_state.client_name = st.text_input("Client name", value=st.session_state.client_name)
        st.session_state.anchor_date = st.date_input("Anchor date", value=st.session_state.anchor_date)
        if st.form_submit_button("Update session"):
            st.success("Session updated")

    from src.core.analysis import get_last_full_12m_window
    window_start, window_end = get_last_full_12m_window(st.session_state.anchor_date)
    st.info(f"12-month window: **{window_start}** → **{window_end}**")

    st.checkbox("Allow mixing ИИН/БИН", key="allow_iin_mismatch")

    # ===== Step 2–3 =====
    st.header("2–3. Upload statements")
    col_bank, col_file = st.columns([1, 3])

    with col_bank:
        bank_key = st.selectbox("Choose bank", options=["kaspi_gold", "kaspi_pay", "halyk_business", "halyk_individual",
                                                        "freedom_bank", "forte_bank", "eurasian_bank", "bcc_bank",
                                                        "alatau_city_bank"], format_func=_format_bank_label)

    with col_file:
        uploaded_file = st.file_uploader("Upload PDF", type=["pdf"])

    if uploaded_file and st.button("Parse & add statement", type="primary"):
        try:
            from src.core.service import parse_statement
            stmnt = parse_statement(bank_key=bank_key, pdf_name=uploaded_file.name, pdf_bytes=uploaded_file.read())
            ensure_txn_date(stmnt)

            session_iin = st.session_state.session_iin_bin
            if session_iin is None or stmnt.iin_bin == session_iin or st.session_state.allow_iin_mismatch:
                if session_iin is None: st.session_state.session_iin_bin = stmnt.iin_bin
                st.session_state.statements.append(stmnt)
                st.success(f"Statement added. IIN: {stmnt.iin_bin}")
            else:
                st.error(f"IIN mismatch: {stmnt.iin_bin} vs {session_iin}")
        except Exception as e:
            st.exception(e)

    if not st.session_state.statements:
        st.info("No statements uploaded yet.")
        return

    st.subheader("Uploaded statements")
    st.dataframe(build_metadata_df(st.session_state.statements), use_container_width=True)

    # ===== Step 4: Analysis =====
    st.header("4. UI Analysis Tables (Top-9 & Related Parties)")

    from src.core.analysis import combine_transactions
    tx_12m = combine_transactions(st.session_state.statements, window_start, window_end)

    if not tx_12m.empty:
        from src.ui.ui_analysis_report_generator import get_ui_analysis_tables

        # Генерируем таблицы на основе всех транзакций за 12 месяцев
        analysis = get_ui_analysis_tables(tx_12m)

        # 1. Таблицы по оборотам (Дебет / Кредит)
        st.subheader("Обороты по контрагентам (Топ-9 + Прочие)")
        c1, c2 = st.columns(2)

        with c1:
            st.write("**Расходы (Дебет)**")
            if analysis["debit_top"]:
                st.dataframe(pd.DataFrame(analysis["debit_top"]), use_container_width=True, hide_index=True)
            else:
                st.write("Нет данных по расходам")

        with c2:
            st.write("**Приходы (Кредит)**")
            if analysis["credit_top"]:
                st.dataframe(pd.DataFrame(analysis["credit_top"]), use_container_width=True, hide_index=True)
            else:
                st.write("Нет данных по приходам")

        # 2. Таблица аффилированных лиц
        st.subheader("Аффилированные лица (Net расчет)")
        if analysis["related_parties"]:
            rp_df = pd.DataFrame(analysis["related_parties"])
            # Сортируем по абсолютному значению сальдо или обороту для наглядности
            st.dataframe(rp_df.sort_values("turnover", ascending=False), use_container_width=True, hide_index=True)
        else:
            st.write("Данные отсутствуют")

    # Стандартные разделы (для совместимости)
    with st.expander("Посмотреть все транзакции (12 месяцев)"):
        st.dataframe(tx_12m, use_container_width=True)


if __name__ == "__main__":
    main()