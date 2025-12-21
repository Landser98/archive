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
                if session_iin is None:
                    st.session_state.session_iin_bin = stmnt.iin_bin
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

        # --- НОРМАЛИЗАЦИЯ ДАННЫХ ПЕРЕД ГЕНЕРАЦИЕЙ ТАБЛИЦ ---
        df_analysis = tx_12m.copy()

        # 1. Поиск колонки с описанием
        desc_candidates = ['Описание операции', 'details', 'Назначение платежа', 'operation', 'Назначение']
        actual_desc_col = next((c for c in desc_candidates if c in df_analysis.columns), None)

        # 2. Обработка сумм (обязательно создаем колонку 'amount')
        if 'amount' not in df_analysis.columns:
            # Специфика Kaspi Pay: Дебет и Кредит в разных колонках
            if 'Дебет' in df_analysis.columns and 'Кредит' in df_analysis.columns:
                df_analysis['amount'] = (
                        pd.to_numeric(df_analysis['Кредит'], errors='coerce').fillna(0.0) -
                        pd.to_numeric(df_analysis['Дебет'], errors='coerce').fillna(0.0)
                )
            else:
                # Для Халыка сумма часто в 'Сумма операции' или 'Сумма в KZT'
                amt_candidates = ['Сумма операции', 'Сумма в KZT', 'Сумма', 'Расход']
                found_amt_col = next((c for c in amt_candidates if c in df_analysis.columns), None)

                if found_amt_col:
                    df_analysis['amount'] = pd.to_numeric(df_analysis[found_amt_col], errors='coerce').fillna(0.0)
                elif 'Доход' in df_analysis.columns and 'Расход' in df_analysis.columns:
                    df_analysis['amount'] = (
                            pd.to_numeric(df_analysis['Доход'], errors='coerce').fillna(0.0) -
                            pd.to_numeric(df_analysis['Расход'], errors='coerce').fillna(0.0).abs()
                    )
                else:
                    df_analysis['amount'] = 0.0

        # 3. Специфическая логика Халыка (очистка имен согласно ТЗ)
        is_halyk = df_analysis.get('bank', pd.Series([])).str.contains('Halyk', na=False)
        if is_halyk.any():
            h_desc = "Описание операции" if "Описание операции" in df_analysis.columns else actual_desc_col
            if h_desc:
                def extract_halyk_name(text):
                    if not isinstance(text, str): return "N/A"
                    prefixes = [
                        "Операция оплаты у коммерсанта ",
                        "Поступление перевода ",
                        "Перевод на другую карту "
                    ]
                    for p in prefixes:
                        if text.startswith(p):
                            return text[len(p):].strip()
                    return text

                df_analysis.loc[is_halyk, 'counterparty_name'] = df_analysis.loc[is_halyk, h_desc].apply(
                    extract_halyk_name)
                df_analysis.loc[is_halyk, 'counterparty_id'] = df_analysis.loc[is_halyk, 'counterparty_name']

        # 4. Fallback для counterparty и обязательная колонка 'details'
        # Для Kaspi Pay контрагент в колонке 'Наименование получателя'
        cp_candidates = ['Наименование получателя', 'counterparty_name', 'Контрагент', 'Получатель']
        actual_cp_col = next((c for c in cp_candidates if c in df_analysis.columns), None)

        if actual_cp_col and 'counterparty_name' not in df_analysis.columns:
            df_analysis['counterparty_name'] = df_analysis[actual_cp_col].fillna('N/A')
            df_analysis['counterparty_id'] = df_analysis[actual_cp_col].fillna('N/A')

        # Если имя так и не определено, берем из описания (Kaspi Gold)
        if 'counterparty_name' not in df_analysis.columns and actual_desc_col:
            df_analysis['counterparty_name'] = df_analysis[actual_desc_col].fillna('N/A')
            df_analysis['counterparty_id'] = df_analysis[actual_desc_col].fillna('N/A')

        if 'details' not in df_analysis.columns and actual_desc_col:
            df_analysis['details'] = df_analysis[actual_desc_col]

        # Генерируем таблицы
        analysis = get_ui_analysis_tables(df_analysis)

        # 1. Таблицы по оборотам (Дебет / Кредит)
        st.subheader("Обороты по контрагентам (Топ-9 + Прочие)")
        c1, c2 = st.columns(2)

        with c1:
            st.write("**Расходы (Дебет)**")
            if analysis["debit_top"]:
                st.dataframe(pd.DataFrame(analysis["debit_top"]), use_container_width=True, hide_index=True)
            else:
                st.info("Нет данных по расходам")

        with c2:
            st.write("**Приходы (Кредит)**")
            if analysis["credit_top"]:
                st.dataframe(pd.DataFrame(analysis["credit_top"]), use_container_width=True, hide_index=True)
            else:
                st.info("Нет данных по приходам")

        # 2. Таблица аффилированных лиц
        st.subheader("Аффилированные лица (Net расчет)")
        if analysis["related_parties"]:
            rp_df = pd.DataFrame(analysis["related_parties"])
            st.dataframe(rp_df.sort_values("Оборот", ascending=False), use_container_width=True, hide_index=True)
        else:
            st.info("Данные отсутствуют")

    # Стандартный просмотр транзакций
    with st.expander("Посмотреть все транзакции (12 месяцев)"):
        st.dataframe(tx_12m, use_container_width=True)


if __name__ == "__main__":
    main()