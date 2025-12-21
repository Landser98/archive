#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional

import pandas as pd
import streamlit as st


# ---------- Dataclass ----------

@dataclass
class Statement:
    bank: str
    pdf_name: str

    account_holder_name: Optional[str]
    iin_bin: Optional[str]
    account_number: Optional[str]

    period_from: Optional[date]
    period_to: Optional[date]
    statement_generation_date: Optional[date]

    tx_df: pd.DataFrame  # должен содержать хотя бы колонку txn_date (datetime64)


# ---------- Parsing adapter ----------

def parse_statement(bank_key: str, pdf_name: str, pdf_bytes: bytes) -> Statement:
    """
    Adapter вокруг конкретных парсеров.
    Сейчас реализован только Kaspi Gold.
    """

    if bank_key == "kaspi_gold":
        # наш адаптер поверх parse_kaspi_statement_v6b
        from src.kaspi_gold.adapter import parse_kaspi_gold_pdf_bytes

        header_df, tx_df, meta_df = parse_kaspi_gold_pdf_bytes(
            pdf_bytes=pdf_bytes,
            pdf_name=pdf_name,
        )

        header_row = header_df.iloc[0]

        # --- МЕТА из header_df ---
        # см. твой header = {...} в parser.py
        account_holder_name = header_row.get("client_name")

        account_number = header_row.get("account_number")
        card_mask = header_row.get("card_mask")

        # В Kaspi Gold в header нет ИИН/БИН -> временно используем номер счёта/карты
        iin_bin = account_number or card_mask or None

        period_start_raw = header_row.get("period_start")
        period_end_raw = header_row.get("period_end")

        period_from = (
            pd.to_datetime(period_start_raw, dayfirst=True, errors="coerce").date()
            if period_start_raw
            else None
        )
        period_to = (
            pd.to_datetime(period_end_raw, dayfirst=True, errors="coerce").date()
            if period_end_raw
            else None
        )

        available_date_raw = header_row.get("available_date")
        statement_generation_date = (
            pd.to_datetime(available_date_raw, dayfirst=True, errors="coerce").date()
            if available_date_raw
            else None
        )

        # --- tx_df: делаем колонку txn_date из 'date' (%d.%m.%y) ---
        if "date" not in tx_df.columns:
            raise ValueError(
                "Kaspi Gold tx_df has no 'date' column, "
                "но batch_calc использует col_op_date='date'."
            )

        tx_df = tx_df.copy()
        tx_df["txn_date"] = pd.to_datetime(
            tx_df["date"], format="%d.%m.%y", errors="coerce"
        )

        return Statement(
            bank="Kaspi Gold",
            pdf_name=pdf_name,
            account_holder_name=str(account_holder_name) if account_holder_name is not None else None,
            iin_bin=str(iin_bin) if iin_bin is not None else None,
            account_number=str(account_number) if account_number is not None else None,
            period_from=period_from,
            period_to=period_to,
            statement_generation_date=statement_generation_date,
            tx_df=tx_df,
        )

    # другие банки — потом
    raise NotImplementedError(f"parse_statement() not implemented for bank_key={bank_key}")


# ---------- Session helpers ----------

def init_session_state() -> None:
    if "client_name" not in st.session_state:
        st.session_state.client_name = ""
    if "anchor_date" not in st.session_state:
        st.session_state.anchor_date = date.today()
    if "statements" not in st.session_state:
        st.session_state.statements: List[Statement] = []


def get_12m_window(anchor: date) -> tuple[date, date]:
    end_ = anchor
    start_ = anchor - timedelta(days=365)  # можно потом заменить на relativedelta
    return start_, end_


def build_metadata_df(statements: List[Statement]) -> pd.DataFrame:
    if not statements:
        return pd.DataFrame()

    rows = []
    for s in statements:
        rows.append(
            {
                "pdf_name": s.pdf_name,
                "bank": s.bank,
                "account_holder_name": s.account_holder_name,
                "iin_bin": s.iin_bin,
                "account_number": s.account_number,
                "period_from": s.period_from,
                "period_to": s.period_to,
                "statement_generation_date": s.statement_generation_date,
            }
        )
    return pd.DataFrame(rows)


def combine_transactions(
    statements: List[Statement],
    window_start: date,
    window_end: date,
) -> pd.DataFrame:
    if not statements:
        return pd.DataFrame()

    all_rows = []
    for s in statements:
        df = s.tx_df.copy()

        if "txn_date" not in df.columns:
            raise ValueError(
                f"tx_df from {s.bank} / {s.pdf_name} "
                f"does not have required column 'txn_date'"
            )

        if not pd.api.types.is_datetime64_any_dtype(df["txn_date"]):
            df["txn_date"] = pd.to_datetime(df["txn_date"], errors="coerce")

        df["bank"] = s.bank
        df["account_number"] = s.account_number
        df["source_pdf"] = s.pdf_name
        all_rows.append(df)

    all_tx = pd.concat(all_rows, ignore_index=True)

    mask = (all_tx["txn_date"] >= pd.Timestamp(window_start)) & (
        all_tx["txn_date"] <= pd.Timestamp(window_end)
    )
    filtered = all_tx.loc[mask].copy()

    # простая дедупликация по разумному ключу, если колонки есть
    subset = [c for c in ["bank", "account_number", "txn_date", "amount", "operation", "details"] if c in filtered.columns]
    if subset:
        filtered = filtered.drop_duplicates(subset=subset)

    return filtered


# ---------- Streamlit UI ----------

def main() -> None:
    st.set_page_config(page_title="Bank Statement Analyzer", layout="wide")
    st.title("Bank Statement Analyzer (Prototype)")

    init_session_state()

    # 1. Client + 1.1. Anchor date
    st.header("1. Клиент и дата среза")

    with st.form("client_form", clear_on_submit=False):
        client_name = st.text_input(
            "Имя клиента (ФИО / Наименование)",
            value=st.session_state.client_name,
        )
        anchor_date = st.date_input(
            "Дата анализа (для теста; в проде будет 'сегодня')",
            value=st.session_state.anchor_date,
        )
        submitted = st.form_submit_button("Сохранить сессию")
        if submitted:
            st.session_state.client_name = client_name
            st.session_state.anchor_date = anchor_date
            st.success(f"Сессия обновлена: клиент = '{client_name}', дата среза = {anchor_date}")

    st.write(
        f"**Текущая сессия:** клиент = `{st.session_state.client_name or '—'}`, "
        f"дата среза = `{st.session_state.anchor_date}`"
    )

    window_start, window_end = get_12m_window(st.session_state.anchor_date)
    st.info(
        f"12-месячное окно анализа: **{window_start}** → **{window_end}** "
        f"(включительно, от выбранной даты)"
    )

    st.markdown("---")

    # 2–3. Upload PDFs (без валидации ИИН/имени)
    st.header("2–3. Загрузка выписок (без проверки ИИН/имени)")

    col_bank, col_file = st.columns([1, 3])

    with col_bank:
        bank_key = st.selectbox(
            "Банк",
            options=[
                "kaspi_gold",
                # позже добавишь сюда kaspi_pay / halyk_...
            ],
            format_func=lambda x: {
                "kaspi_gold": "Kaspi Gold",
            }.get(x, x),
        )

    with col_file:
        uploaded_file = st.file_uploader(
            "Загрузить PDF-выписку",
            type=["pdf"],
        )

    if uploaded_file is not None:
        st.write(f"**Файл:** `{uploaded_file.name}`")
        if st.button("Распарсить и добавить в сессию", type="primary"):
            try:
                pdf_bytes = uploaded_file.read()
                statement = parse_statement(
                    bank_key=bank_key,
                    pdf_name=uploaded_file.name,
                    pdf_bytes=pdf_bytes,
                )

                # ⚠️ НИКАКОЙ проверки ИИН/имени — просто добавляем
                st.session_state.statements.append(statement)
                st.success(
                    f"Выписка {statement.bank} ({uploaded_file.name}) добавлена в сессию."
                )

            except NotImplementedError as e:
                st.error(str(e))
            except Exception as e:
                st.exception(e)

    st.subheader("Загруженные выписки (текущая сессия)")
    if not st.session_state.statements:
        st.info("Ещё нет загруженных выписок.")
    else:
        meta_df = build_metadata_df(st.session_state.statements)
        st.dataframe(meta_df, use_container_width=True)
        st.caption(
            "Сейчас нет проверки, что все выписки принадлежат одному клиенту. "
            "В проде это будет делаться через внешний сервис (ИИН/БИН и т.п.)."
        )

    st.markdown("---")

    # 4. Produce output
    st.header("4. Сформировать результат за последние 12 месяцев")

    if st.button("Показать транзакции за 12 месяцев"):
        if not st.session_state.statements:
            st.warning("Нет выписок в сессии. Сначала загрузите хотя бы одну PDF.")
        else:
            try:
                window_start, window_end = get_12m_window(st.session_state.anchor_date)
                tx_12m = combine_transactions(
                    st.session_state.statements,
                    window_start=window_start,
                    window_end=window_end,
                )

                if tx_12m.empty:
                    st.warning(
                        "Не найдено транзакций в 12-месячном окне. "
                        "Либо периоды выписок не попадают в этот диапазон, "
                        "либо операций действительно нет."
                    )
                else:
                    st.success(
                        f"Найдено {len(tx_12m):,} транзакций "
                        f"между {window_start} и {window_end}."
                    )
                    st.dataframe(tx_12m, use_container_width=True)

                    csv_bytes = tx_12m.to_csv(index=False).encode("utf-8-sig")
                    st.download_button(
                        label="Скачать CSV (12 месяцев)",
                        data=csv_bytes,
                        file_name="transactions_12m.csv",
                        mime="text/csv",
                    )

            except Exception as e:
                st.exception(e)

    st.markdown("---")
    st.caption(
        "В прод-версии: дату среза можно будет всегда брать как сегодня, "
        "а ИИН/БИН/имя клиента валидировать через внешний сервис."
    )


if __name__ == "__main__":
    main()
