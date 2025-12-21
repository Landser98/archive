#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parse BCC transactions table directly from PDF using Camelot.

Assumes relatively stable layout:
- one big table per page
- 12 columns (docno, date, BIC, IBAN, ... bank, purpose)
"""

import camelot
import pandas as pd
from typing import List

COLS = [
    "Реттік №/ № п/п",
    "Күні / Дата",
    "Корр-тің БСК / БИК корр-та",
    "Корр-тің ЖСК /ИИК корр-та",
    "Жіберушінің СН / ИН отправителя",
    "Корреспондент / Корреспондент",
    "Алушының СН / ИН получателя",
    "Дебет / Дебет",
    "Кредит / Кредит",
    "ТМК /КНП",
    "Банк корреспондент / Банк корреспондент",
    "Төлемнің мақсаты / Назначение платежа",
]

def parse_bcc_transactions_camelot(pdf_path: str) -> pd.DataFrame:
    """
    Extract the transaction table from BCC statement using Camelot.

    1. Use lattice (if there are visible grid lines).
    2. If that fails, fall back to stream with calibrated table area & columns.
    """
    # --- try lattice first ---
    tables: List[camelot.core.Table] = []
    try:
        tables = camelot.read_pdf(
            pdf_path,
            pages="all",
            flavor="lattice",
        )
    except Exception:
        tables = []

    if not tables:
        # fallback: stream + manual calibration
        tables = camelot.read_pdf(
            pdf_path,
            pages="all",
            flavor="stream",
            table_areas=["36,780,560,80"],  # top,left,bottom,right
            columns=["60,110,200,270,340,410,470,520"],
            strip_text="\n",
        )

    if not tables:
        raise RuntimeError(f"No tables parsed from {pdf_path}")

    # Склеиваем все страницы
    df_list = [t.df for t in tables]
    df_raw = pd.concat(df_list, ignore_index=True)

    # Первая строка почти всегда заголовок — дропаем
    df_raw = df_raw.iloc[1:].reset_index(drop=True)

    # Подрезаем до 12 колонок и даём им наши имена
    if df_raw.shape[1] != len(COLS):
        raise ValueError(f"Expected {len(COLS)} columns, got {df_raw.shape[1]}")

    df_raw.columns = COLS

    # Нормализация пробелов
    df = df_raw.applymap(lambda x: " ".join(str(x).split()) if isinstance(x, str) else x)

    # выбрасываем пустые по дате
    df = df.dropna(subset=["Күні / Дата"])

    # И **дропаем последний ряд** – там твой футер
    if not df.empty:
        df = df.iloc[:-1]

    return df
