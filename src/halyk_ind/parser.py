#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Halyk Bank (type B / individual) statement parser.

What it does
------------
1. Берёт JSONL с распарсенными страницами (output convert_pdf_json_pages.py).
2. Через:
      - parse_header_type_b() парсит шапку;
      - parse_halyk_transactions() парсит транзакции;
      - parse_footers() парсит итоговый футер.
3. Если передан путь к PDF:
      - вытаскивает транзакционную таблицу из PDF (extract_halyk_ind_tx_from_pdf);
      - сшивает транзакции по дате + счёту и подставляет более длинное
        "Описание операции" из PDF.
4. Немного чистит "Описание операции" от шапки / мусора.
5. Возвращает (header_df, tx_df, footer_df).

CLI:
python -m src.halyk_ind.parser \
  "data/halyk_individual/converted_jsons/1 (1)_pages.jsonl" \
  --pdf "data/halyk_bank/halyk_individual/1 (1).pdf" \
  --out-header "data/halyk_individual/1 (1)_header.csv" \
  --out-tx     "data/halyk_individual/1 (1)_tx.csv" \
  --out-footer "data/halyk_individual/1 (1)_footer.csv"
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .header import parse_header_type_b
from .transactions import parse_halyk_transactions
from .footer import parse_footers
from .tx_from_pdf import extract_halyk_ind_tx_from_pdf


# ---------------------------------------------------------------------------
# JSONL reading
# ---------------------------------------------------------------------------

def _read_pages_jsonl(jsonl_path: str) -> List[Dict[str, Any]]:
    pages: List[Dict[str, Any]] = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                pages.append(obj)
    return pages


# ---------------------------------------------------------------------------
# Helpers from your working debug script
# ---------------------------------------------------------------------------

def _norm_date_for_match(value: Any) -> Optional[str]:
    """
    Приводим дату к единому виду (YYYY-MM-DD), чтобы можно было
    матчить строки из tx_df и tx_pdf, даже если форматы разные.

    Поддерживаем:
      - '2024-03-10'
      - '10.03.2024'
    """
    if value is None:
        return None

    s = str(value).strip()
    if not s:
        return None

    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue

    return None


HEADER_TRASH_RE = re.compile(
    r"АО \"Народный Банк Казахстана\".*?операции операции"
    r"|Дата Дата Сумма Валюта Приход в Расход в проведения обработки Описание операции "
    r"Комиссия (?:№ карточки/счета|Счет) операции операции валюте счета валюте счета операции операции",
    flags=re.DOTALL,
)


def _clean_descr(s: Any) -> Any:
    """Убрать повторяющуюся шапку / мусор из описания, нормализовать пробелы.

    ВАЖНО: здесь НЕТ обрезания по ключевым словам — только вычищаем
    явно распознанную шапку и нормализуем пробелы.
    """
    if not isinstance(s, str):
        return s

    s = s.replace("\xa0", " ").replace("\u202f", " ")
    s = re.sub(r"\s+", " ", s).strip()

    s = HEADER_TRASH_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _enrich_description(tx_df: pd.DataFrame, tx_pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Берём base tx_df (геометрический парсер) и таблицу tx_pdf из самого PDF
    и подставляем более длинное "Описание операции" там, где строки матчатся.
    Логика 1-в-1 из debug_halyk_descr.py.
    """
    if tx_df.empty or tx_pdf.empty:
        return tx_df

    if "Описание операции" not in tx_df.columns or "Описание операции" not in tx_pdf.columns:
        print("[WARN] no 'Описание операции' column in one of dataframes, skipping enrichment.")
        return tx_df

    # Определяем, как называются колонки со счётом
    acct_col_tx = (
        "Счет"
        if "Счет" in tx_df.columns
        else "№ карточки/счета"
        if "№ карточки/счета" in tx_df.columns
        else None
    )
    acct_col_pdf = (
        "Счет"
        if "Счет" in tx_pdf.columns
        else "№ карточки/счета"
        if "№ карточки/счета" in tx_pdf.columns
        else None
    )

    if acct_col_tx is None or acct_col_pdf is None:
        print("[WARN] no account column found, skipping enrichment.")
        return tx_df

    needed_cols_pdf = [
        "Дата проведения операции",
        "Описание операции",
        acct_col_pdf,
    ]
    if not all(col in tx_pdf.columns for col in needed_cols_pdf):
        print("[WARN] tx_pdf missing some expected columns, skipping enrichment.")
        return tx_df

    df = tx_df.copy()
    n = min(len(df), len(tx_pdf))
    desc_idx = df.columns.get_loc("Описание операции")

    replaced = 0
    mismatched = 0

    for i in range(n):
        row_old = df.iloc[i]
        row_new = tx_pdf.iloc[i]

        d_old = _norm_date_for_match(row_old.get("Дата проведения операции"))
        d_new = _norm_date_for_match(row_new.get("Дата проведения операции"))

        acc_old = str(row_old.get(acct_col_tx, "")).strip()
        acc_new = str(row_new.get(acct_col_pdf, "")).strip()

        if not d_old or not d_new or not acc_old:
            mismatched += 1
            continue
        if d_old != d_new or acc_old != acc_new:
            mismatched += 1
            continue

        old_descr = str(row_old.get("Описание операции") or "")
        new_descr = str(row_new.get("Описание операции") or "")

        # Берём более длинное описание из PDF-таблицы
        if len(new_descr) > len(old_descr):
            df.iat[i, desc_idx] = new_descr
            replaced += 1

    print(f"[INFO] description enrichment: {replaced} rows updated, {mismatched} rows skipped.")
    return df


def _clean_tx_df(df: pd.DataFrame) -> pd.DataFrame:
    """ffill дат и почистить описание. Логика из debug_halyk_descr.py."""
    df = df.copy()

    for col in ("Дата проведения операции", "Дата обработки операции"):
        if col in df.columns:
            df[col] = df[col].ffill()

    if "Описание операции" in df.columns:
        df["Описание операции"] = df["Описание операции"].apply(_clean_descr)

    return df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_halyk_b_statement(
    jsonl_path: str,
    pdf_path: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    High-level wrapper: returns (header_df, tx_df, footer_df) for Halyk type B.

    Если pdf_path не None:
      - дополнительно читаем таблицу из PDF и обогащаем 'Описание операции'
        (как в debug_halyk_descr.py).
    """
    pages = _read_pages_jsonl(jsonl_path)
    if not pages:
        raise ValueError(f"No pages found in JSONL: {jsonl_path}")

    # 1) HEADER from first page text
    first_text = pages[0].get("text") or ""
    header_df = parse_header_type_b(first_text)

    # 2) TRANSACTIONS from all pages (geometric parser)
    tx_df, totals = parse_halyk_transactions(pages)

    # 3) FOOTER totals from all pages (text-based)
    try:
        footer_df = parse_footers(pages)
    except Exception as e:
        print(f"⚠️ Footer parsing failed: {e}")
        footer_df = pd.DataFrame()

    # 4) If PDF is provided, enrich description via PDF table
    if pdf_path is not None:
        try:
            tx_pdf = extract_halyk_ind_tx_from_pdf(str(pdf_path))
            print(f"[INFO] tx_pdf rows: {len(tx_pdf)}")
            tx_df = _enrich_description(tx_df, tx_pdf)
        except Exception as e:
            print(f"[WARN] Failed to extract tx from PDF for description enrichment: {e}")

    # 5) Clean (ffill dates + remove header trash in description)
    tx_df = _clean_tx_df(tx_df)

    return header_df, tx_df, footer_df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli_parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Single-file parser for Halyk Bank type B (individual) statements.",
    )
    ap.add_argument(
        "jsonl",
        help="Path to pages JSONL (output of convert_pdf_json_pages.py).",
    )
    ap.add_argument(
        "--pdf",
        help="Path to original PDF (for better 'Описание операции').",
        default=None,
    )
    ap.add_argument(
        "--out-header",
        help="Where to save header CSV.",
        default=None,
    )
    ap.add_argument(
        "--out-tx",
        help="Where to save transactions CSV.",
        default=None,
    )
    ap.add_argument(
        "--out-footer",
        help="Where to save footer CSV.",
        default=None,
    )
    return ap.parse_args()


def main() -> None:
    args = _cli_parse_args()
    jsonl_path = Path(args.jsonl)
    if not jsonl_path.is_file():
        raise SystemExit(f"JSONL not found: {jsonl_path}")

    pdf_path: Optional[Path] = None
    if args.pdf:
        pdf_path = Path(args.pdf)
        if not pdf_path.is_file():
            raise SystemExit(f"PDF not found: {pdf_path}")

    header_df, tx_df, footer_df = parse_halyk_b_statement(
        str(jsonl_path),
        str(pdf_path) if pdf_path is not None else None,
    )

    print("=== HEADER ===")
    print(header_df.to_string(index=False))

    print("\n=== TX (first 20) ===")
    print(tx_df.head(20).to_string(index=False))

    print("\n=== FOOTER ===")
    if not footer_df.empty:
        print(footer_df.to_string(index=False))
    else:
        print("<empty>")

    if args.out_header:
        header_df.to_csv(args.out_header, index=False)
        print(f"[OK] header -> {Path(args.out_header).resolve()}")
    if args.out_tx:
        tx_df.to_csv(args.out_tx, index=False)
        print(f"[OK] tx     -> {Path(args.out_tx).resolve()}")
    if args.out_footer:
        footer_df.to_csv(args.out_footer, index=False)
        print(f"[OK] footer -> {Path(args.out_footer).resolve()}")


if __name__ == "__main__":
    main()
