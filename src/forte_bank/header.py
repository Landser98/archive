#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ForteBank PDF parser
→ Outputs header_df, tx_df, footer_df as separate DataFrames
"""

import re
import camelot
import pdfplumber
import pandas as pd
from pathlib import Path

# ------------------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------------------
def clean_text(s):
    if not isinstance(s, str):
        return ""
    return re.sub(r"\s+", " ", s.strip())


def extract_field(text, pattern):
    m = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else ""

def cut_before_table_header(full_text: str) -> str:
    """
    Оставляем только верхнюю часть страницы – до строки вида
    '№ Күні/ Құжат Нөмірі/ ...'.
    Если такую строку не нашли, возвращаем текст как есть.
    """
    if not full_text:
        return ""

    lines = full_text.splitlines()
    cutoff = None

    for i, raw_line in enumerate(lines):
        line = clean_text(raw_line)  # "№ Күні/ Құжат Нөмірі/ Жіберуші ..."
        low = line.lower()

        # достаточно надёжный признак твоей шапки:
        # начинается с № и в строке есть "күні" и "құжат"
        if low.startswith("№".lower()) and "күні" in low and "құжат" in low:
            cutoff = i
            break

    if cutoff is None:
        return full_text

    return "\n".join(lines[:cutoff])

# ------------------------------------------------------------------------------
# Header extraction
# ------------------------------------------------------------------------------
def parse_header(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0]
        full_text = first_page.extract_text() or ""

    # отрезаем всё, что ниже строки с колонками таблицы
    text = cut_before_table_header(full_text)

    header = {
        "statement_date": extract_field(text, r"Жасалған күні[:\s]+([\d\. :/]+)"),
        "client_name": extract_field(
            text,
            r"Клиент/Клиент[:\s]+(.+?)(?:Банк|Мекен\s*жайы/Адрес|$)"
        ),
        "address": extract_field(
            text,
            r"Мекен\s*жайы/Адрес[:\s]+(.+?)(?:БИН|БСН|ИИК|Шот|Валюта|$)"
        ),
        "BIN": extract_field(
            text,
            r"БИН.*?:\s*([0-9]{9,12})"
        ),
        "IIK": extract_field(
            text,
            r"ИИК.*?:\s*([A-Z0-9]{16,34})"
        ),

        "BIK": extract_field(text, r"БИК[:\s]*([A-Z0-9]+)"),
        "currency": extract_field(text, r"Валюта/Валюта[:\s]+([A-Z]+)"),
        "opening_balance": extract_field(text, r"Входящий остаток[:\s]*([\d,\.]+)"),
        "period_start": extract_field(
            text,
            r"([0-9]{2}\.[0-9]{2}\.[0-9]{4})\s+бастап"
        ),
        "period_end": extract_field(
            text,
            r"по\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})"
        ),
        "raw_header_text": text,          # уже ОЧИЩЕННЫЙ от шапки таблицы
    }

    return pd.DataFrame([header])

# ------------------------------------------------------------------------------
# Transaction table extraction
# ------------------------------------------------------------------------------
def parse_transactions(pdf_path):
    print("📄 Extracting transactions with Camelot...")

    tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")
    if len(tables) == 0:
        print("⚠️ Lattice failed — trying stream mode")
        tables = camelot.read_pdf(pdf_path, pages="all", flavor="stream", edge_tol=150)

    if len(tables) == 0:
        print("❌ No tables found.")
        return pd.DataFrame()

    df = pd.concat([t.df for t in tables], ignore_index=True)
    df = df.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all")
    df.columns = [clean_text(c) for c in df.iloc[0]]
    df = df.iloc[1:].reset_index(drop=True)
    df = df.applymap(clean_text)
    return df


# ------------------------------------------------------------------------------
# Footer extraction
# ------------------------------------------------------------------------------
def parse_footer(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        last_page = pdf.pages[-1]
        text = last_page.extract_text() or ""

    footer = {
        "doc_count": extract_field(text, r"Итого документов[:\s]*([0-9]+)"),
        "closing_balance": extract_field(text, r"Исходящий остаток[:\s]*([\d,\.]+)"),
        "raw_footer_text": text,
    }

    return pd.DataFrame([footer])
