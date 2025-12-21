#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Freedom Bank parser (stable + safe version)
- Camelot for transactions
- pdfplumber for header & footer
- Robust regex with fallbacks

CLI:
  python src/freedom/parser.py path/to/statement.pdf \
    --out-header freedom_header.csv \
    --out-tx freedom_tx.csv \
    --out-footer freedom_footer.csv
"""

import re
import argparse
import pandas as pd
import camelot
import pdfplumber

from src.utils.income_calc import compute_ip_income

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def clean_text(s):
    return re.sub(r"\s+", " ", s.strip()) if isinstance(s, str) else ""


def extract_field(text, pattern):
    """
    Safe regex extractor with group() fallback.
    Returns '' if nothing found or regex has no group.
    """
    try:
        m = re.search(pattern, text, re.IGNORECASE)
        if not m:
            return ""
        if m.groups():
            return m.group(1).strip()
        else:
            return m.group(0).strip()
    except Exception:
        return ""


def to_float(val):
    if isinstance(val, str):
        try:
            return float(val.replace(" ", "").replace(",", "."))
        except Exception:
            return None
    return val


# ---------------------------------------------------------------------
# Header & Footer extraction
# ---------------------------------------------------------------------
def extract_header_footer(pdf_path: str):
    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0]
        last_page = pdf.pages[-1]

        header_text = first_page.extract_text() or ""
        footer_text = last_page.extract_text() or ""

    header_text = re.sub(r"\s+", " ", header_text)

    header = {
        "account_number": extract_field(header_text, r"Выписка по счету\s+([A-Z0-9]+)"),
        "period_start": extract_field(header_text, r"с\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})"),
        "period_end": extract_field(header_text, r"по\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})"),
        "client_name": extract_field(header_text, r"Клиент\s+(.+?)БИН"),
        "BIN": extract_field(header_text, r"БИН/ИИН\s+(\d+)"),
        "account_type": extract_field(header_text, r"Тип счета\s+(.+?)Входящий"),
        "currency": extract_field(header_text, r"Валюта счета\s+([A-Z]+)"),
        "opening_balance": extract_field(header_text, r"Входящий остаток\s+([\d\s,\.]+)"),
        "closing_balance": extract_field(header_text, r"Исходящий остаток\s+([\d\s,\.]+)"),
        "bank_name": extract_field(header_text, r"АО\s+«?([^»]+)»?"),
        "bank_address": extract_field(header_text, r"(г\.[A-Za-zА-Яа-яЁё,\s0-9]+)"),
        "raw_header_text": header_text,
    }

    # --- Footer ---
    footer_lines = [l for l in footer_text.splitlines() if "Итого по счету" in l]
    footer_line = footer_lines[-1] if footer_lines else footer_text
    footer_numbers = re.findall(r"\d[\d\s,]*\d", footer_line)

    footer = {
        "footer_raw": footer_text,
        "debit_total": to_float(footer_numbers[0]) if len(footer_numbers) >= 1 else None,
        "credit_total": to_float(footer_numbers[1]) if len(footer_numbers) >= 2 else None,
    }

    return pd.DataFrame([header]), pd.DataFrame([footer])


# ---------------------------------------------------------------------
# Transaction table (Camelot)
# ---------------------------------------------------------------------
def extract_transactions(pdf_path: str) -> pd.DataFrame:
    print("📄 Extracting transaction table using Camelot...")

    tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")
    if len(tables) == 0:
        print("⚠️ Lattice failed — trying stream mode")
        tables = camelot.read_pdf(pdf_path, pages="all", flavor="stream", edge_tol=150)

    if len(tables) == 0:
        raise RuntimeError("Camelot did not find any tables.")

    df = pd.concat([t.df for t in tables], ignore_index=True)
    df = df.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all")

    df.columns = [clean_text(c) for c in df.iloc[0]]
    df = df.iloc[1:].reset_index(drop=True)
    df = df.applymap(clean_text)

    # Drop repeated headers
    df = df[df.iloc[:, 0].notna()]
    df = df[~df.iloc[:, 0].str.contains(r"Дата|Номер документа", na=False, case=False)]

    # Drop duplicates by № column if present
    if "№" in df.columns:
        df = df[df["№"].ne("№")]
        df = df[df["№"].ne("")]
        df = df.drop_duplicates(subset="№", keep="first")

    return df


