#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Alatau City Bank statement parser.

Public API:
    parse_acb_pdf_with_camelot(pdf_path: str) -> (header_df, tx_df, footer_df)

- header_df: 1 row with account, client, turnovers, balances, dates, etc.
- tx_df:     transaction table with TARGET_COLS_RU columns
- footer_df: 1 row with total_debit_footer / total_credit_footer and raw_footer_row
"""

import re

import camelot
import numpy as np
import pandas as pd

# ---------- CONSTANTS ----------

TARGET_COLS_RU = [
    "Дата операции",
    "Дата отражения по счету",
    "№ док",
    "Дебет",
    "Кредит",
    "Курс НБ РК",
    "Эквивалент в тенге по курсу НБ РК",
    "КНП",
    "Назначение платежа",
    "Корреспондент",
    "БИН/ИИН",
    "БИК корр.",
    "Счет",
]


# ---------- HELPERS ----------

def _norm_cell(x: object) -> str:
    s = "" if pd.isna(x) else str(x)
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    s = re.sub(r"\s+", " ", s.strip())  # collapse newlines & spaces
    # fix colon-dates like 30:09:2024 -> 30.09.2024
    s = re.sub(r"(\b\d{2}):(\d{2}):(\d{4}\b)", r"\1.\2.\3", s)
    return s


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


# ---------- HEADER PARSING (через PyMuPDF) ----------

def _parse_acb_header_from_text(text: str) -> pd.DataFrame:
    """
    Разбирает текст первой страницы выписки Alatau City Bank и
    возвращает 1-строчный DataFrame с реквизитами.
    """
    # нормализуем пробелы/переносы
    t = text.replace("\xa0", " ").replace("\u202f", " ")
    t = re.sub(r"[ \t]+", " ", t)

    meta = {
        "account": None,
        "currency": None,
        "client": None,
        "iin_bin": None,
        "credit_turnover": np.nan,
        "debit_turnover": np.nan,
        "opening_balance": np.nan,
        "opening_balance_date": None,
        "opening_balance_equiv_kzt_nb": np.nan,
        "closing_balance": np.nan,
        "closing_balance_date": None,
        "closing_balance_equiv_kzt_nb": np.nan,
        "raw_header_text": t,
    }

    # ----- Лицевой счёт + валюта -----
    m = re.search(
        r"Лицевой\s+счет:?[\s№]*([A-Z0-9]+)\s*([A-Z]{3})",
        t,
        flags=re.I,
    )
    if m:
        meta["account"] = m.group(1)
        meta["currency"] = m.group(2).upper()

    # ----- Клиент (может быть на 2 строках) -----
    # "Клиент: Индивидуальный\nпредприниматель "ОРИОН"\nИИН (БИН): ..."
    m = re.search(r"Клиент:\s*(.+?)\s+ИИН\s*\(БИН\):", t, flags=re.S)
    if m:
        client = re.sub(r"\s+", " ", m.group(1)).strip()
        meta["client"] = client

    # ----- ИИН / БИН -----
    m = re.search(r"ИИН\s*\(БИН\):\s*([0-9]{10,12})", t)
    if m:
        meta["iin_bin"] = m.group(1)

    # ----- Обороты по кредиту/дебету -----
    m = re.search(r"Обороты\s+по\s+кредиту:\s*([0-9\s,]+)", t)
    if m:
        meta["credit_turnover"] = _to_float_ru(m.group(1))

    m = re.search(r"Обороты\s+по\s+дебету:\s*([0-9\s,]+)", t)
    if m:
        meta["debit_turnover"] = _to_float_ru(m.group(1))

    # ----- Входящий остаток + дата -----
    m = re.search(
        r"Входящий\s+остаток:\s*([0-9\s,]+)\s*Дата\s+остатка:\s*(\d{2}\.\d{2}\.\d{4})",
        t,
    )
    if m:
        meta["opening_balance"] = _to_float_ru(m.group(1))
        meta["opening_balance_date"] = m.group(2)

    # ----- Исходящий остаток + дата -----
    m = re.search(
        r"Исходящий\s+остаток:\s*([0-9\s,]+)\s*Дата\s+остатка:\s*(\d{2}\.\d{2}\.\d{4})",
        t,
    )
    if m:
        meta["closing_balance"] = _to_float_ru(m.group(1))
        meta["closing_balance_date"] = m.group(2)

    # ----- Эквивалент в тенге по курсу НБ РК (2 строки) -----
    eq_matches = list(
        re.finditer(
            r"Эквивалент\s+в\s+тенге\s+по\s+курсу\s+НБ\s+РК:\s*([0-9\s,]*)",
            t,
        )
    )
    if len(eq_matches) >= 1:
        s1 = eq_matches[0].group(1).strip()
        if re.search(r"\d", s1):
            meta["opening_balance_equiv_kzt_nb"] = _to_float_ru(s1)

    if len(eq_matches) >= 2:
        s2 = eq_matches[1].group(1).strip()
        if re.search(r"\d", s2):
            meta["closing_balance_equiv_kzt_nb"] = _to_float_ru(s2)

    return pd.DataFrame([meta])


def _parse_acb_header_text(pdf_path: str) -> pd.DataFrame:
    """
    Достаёт текст 1-й страницы через PyMuPDF и передаёт в парсер выше.
    Если pymupdf не установлен — возвращает одну строку с None.
    """
    try:
        import fitz  # pymupdf
    except ImportError:
        cols = [
            "account",
            "currency",
            "client",
            "iin_bin",
            "credit_turnover",
            "debit_turnover",
            "opening_balance",
            "opening_balance_date",
            "opening_balance_equiv_kzt_nb",
            "closing_balance",
            "closing_balance_date",
            "closing_balance_equiv_kzt_nb",
            "raw_header_text",
        ]
        return pd.DataFrame([{c: None for c in cols}])

    with fitz.open(pdf_path) as doc:
        page0 = doc[0]
        text = page0.get_text("text")

    return _parse_acb_header_from_text(text)


def _parse_acb_footer_from_tx(tx: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Находит строку 'ИТОГО' в tx (уже с TARGET_COLS_RU) и возвращает:
    - footer_df: одна строка с total_debit_footer / total_credit_footer
    - tx_no_footer: tx без строки ИТОГО
    """
    # ищем строку футера
    mask_footer = tx["Дата операции"].str.match(r"^ИТОГО:?\s*$", na=False)
    footer_rows = tx[mask_footer].copy()

    footer_meta = {
        "total_debit_footer": np.nan,
        "total_credit_footer": np.nan,
        "raw_footer_row": None,
    }

    if not footer_rows.empty:
        row = footer_rows.iloc[0]
        footer_meta["total_debit_footer"] = _to_float_ru(row.get("Дебет", ""))
        footer_meta["total_credit_footer"] = _to_float_ru(row.get("Кредит", ""))
        footer_meta["raw_footer_row"] = " | ".join(
            str(v) for v in row.values if pd.notna(v)
        )

    footer_df = pd.DataFrame([footer_meta])
    tx_no_footer = tx[~mask_footer].copy()
    return footer_df, tx_no_footer


# ---------- PUBLIC PARSER ----------

def parse_acb_pdf_with_camelot(pdf_path: str):
    """
    Parse an Alatau City Bank PDF statement into header / tx / footer DataFrames.
    """
    # 0) Сначала парсим реквизиты из текста
    header_df = _parse_acb_header_text(pdf_path)

    # 1) Читаем все таблицы Camelot'ом
    tables = camelot.read_pdf(pdf_path, pages="1-end", flavor="lattice")
    if len(tables) == 0:
        raise RuntimeError(
            "Camelot не нашёл ни одной таблицы. Проверь Ghostscript/страницы."
        )

    raw = pd.concat([t.df for t in tables], ignore_index=True)

    # 2) Нормализуем ячейки (без applymap FutureWarning)
    raw = raw.apply(lambda col: col.map(_norm_cell))

    # 3) Ищем первую строку 'Дата операции' — это начало блока транзакций
    hdr_rows = raw.index[
        raw.iloc[:, 0].str.fullmatch(r"Дата операции", na=False)
    ].tolist()
    if not hdr_rows:
        raise RuntimeError(
            "Не найден заголовок 'Дата операции'. Проверь раскладку/страницы."
        )
    first_hdr_idx = hdr_rows[0]

    tx = raw.iloc[first_hdr_idx:].copy()

    # 4) Приводим к 13 колонкам и задаём TARGET_COLS_RU
    tx = tx.reindex(columns=list(range(max(13, tx.shape[1]))))
    tx = tx.iloc[:, :13].copy()
    tx.columns = TARGET_COLS_RU

    # 5) Вытащим футер ИТОГО и уберём его из tx
    footer_df, tx = _parse_acb_footer_from_tx(tx)

    # 6) Убираем повторяющиеся заголовки внутри таблицы
    mask_is_header = tx["Дата операции"].str.fullmatch("Дата операции", na=False)
    tx = tx[~mask_is_header].copy()

    # 7) Склеиваем дату и время в "Дата операции", если они развалились
    tx["Дата операции"] = tx["Дата операции"].str.replace(
        r"\b(\d{2}\.\d{2}\.\d{4})\s+(\d{2}:\d{2}(?::\d{2})?)\b",
        r"\1 \2",
        regex=True,
    )

    # 8) Числовые колонки → float
    for col in ["Дебет", "Кредит", "Курс НБ РК", "Эквивалент в тенге по курсу НБ РК"]:
        tx[col] = tx[col].apply(_to_float_ru)

    # 9) Текстовые колонки — trim + чистка nan
    for col in [
        "№ док",
        "КНП",
        "Назначение платежа",
        "Корреспондент",
        "БИН/ИИН",
        "БИК корр.",
        "Счет",
        "Дата отражения по счету",
        "Дата операции",
    ]:
        tx[col] = (
            tx[col].astype(str).str.strip().replace({"nan": "", "NaN": ""})
        )

    tx["БИК корр."] = tx["БИК корр."].str.upper().str.replace(" ", "", regex=False)
    tx["Счет"] = tx["Счет"].str.upper().str.replace(" ", "", regex=False)

    # 10) Удаляем полностью пустые строки по ключевым колонкам
    key_subset = [
        "Дата операции",
        "Дата отражения по счету",
        "№ док",
        "Дебет",
        "Кредит",
        "КНП",
        "Счет",
    ]
    tx = tx.dropna(how="all", subset=[c for c in key_subset if c in tx.columns])
    tx.reset_index(drop=True, inplace=True)

    return header_df, tx, footer_df
