#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Halyk Bank footer parser.

Из последней страницы выписки вытаскивает:
  - Обороты_Дебет
  - Обороты_Кредит
  - За_период_с
  - За_период_по
  - Исходящий_остаток
  - Дата_остатка

Использование:
  1) как часть пайплайна:
        footer_df = parse_halyk_footer(pages)   # pages = list[dict] из JSONL
  2) как CLI:
        python src/halyk_business/footer.py accountStatement 2.pdf \
            -o accountStatement_2_footer.csv \
            --out-json accountStatement_2_footer.json
"""

import argparse
import json
import re
from typing import Optional, Dict, Any, List

import pandas as pd
import pdfplumber

AMT_RE = re.compile(r"-?\d[\d\u00A0 '\u202F]*[.,]\d{2}")


def _parse_amount(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    t = str(s)
    for ch in ("\u00A0", "\u202F", " ", "'"):
        t = t.replace(ch, "")
    t = t.replace(",", ".")
    t = re.sub(r"[^0-9.\-]", "", t)
    if not t or t == "-":
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _parse_footer_text(text: str) -> Dict[str, Optional[float]]:
    """
    Общая логика парсинга футера по голому тексту.
    Используется и пайплайном, и CLI.
    """
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]

    turnover_debit = turnover_credit = None
    period_start = period_end = None
    closing_balance = None
    closing_date = None

    for idx, line in enumerate(lines):
        # Обороты + период
        if "Обороты:" in line:
            m_period = re.search(
                r"За период:\s*([0-9]{2}[.-][0-9]{2}[.-][0-9]{4})\s*[-–]\s*([0-9]{2}[.-][0-9]{2}[.-][0-9]{4})",
                line,
            )
            if m_period:
                period_start, period_end = m_period.group(1), m_period.group(2)

            # суммы обычно на следующей строке(ах)
            for j in range(idx + 1, min(idx + 5, len(lines))):
                nums = AMT_RE.findall(lines[j])
                if len(nums) >= 2:
                    turnover_debit = _parse_amount(nums[0])
                    turnover_credit = _parse_amount(nums[1])
                    break

        # Исходящий остаток + дата остатка
        if "Исходящий остаток" in line:
            m_amt = AMT_RE.search(line)
            if m_amt:
                closing_balance = _parse_amount(m_amt.group(0))
            m_date = re.search(
                r"Дата остатка:\s*([0-9]{2}[.-][0-9]{2}[.-][0-9]{4})",
                line,
            )
            if m_date:
                closing_date = m_date.group(1)

    return {
        "Обороты_Дебет": turnover_debit,
        "Обороты_Кредит": turnover_credit,
        "За_период_с": period_start,
        "За_период_по": period_end,
        "Исходящий_остаток": closing_balance,
        "Дата_остатка": closing_date,
    }


# ====== Функция, которую вызывает parse_halyk_statement(pages) ======

def parse_halyk_footer(pages: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Версия для пайплайна: принимает список страниц (JSONL → list[dict])
    и возвращает одно-строчный DataFrame с полями футера.
    """
    if not pages:
        return pd.DataFrame([{
            "Обороты_Дебет": None,
            "Обороты_Кредит": None,
            "За_период_с": None,
            "За_период_по": None,
            "Исходящий_остаток": None,
            "Дата_остатка": None,
        }])

    last_page = pages[-1]
    text = last_page.get("text") or last_page.get("page_text") or ""
    data = _parse_footer_text(text)
    return pd.DataFrame([data])


# ====== Вспомогательная функция для CLI ======

def _extract_text(source_path: str) -> str:
    """
    .pdf  -> последняя страница через pdfplumber
    .jsonl -> последний объект, поле "text" или "page_text"
    остальное -> читаем как обычный текстовый файл
    """
    if source_path.lower().endswith(".pdf"):
        with pdfplumber.open(source_path) as pdf:
            text = pdf.pages[-1].extract_text() or ""
        return text

    if source_path.lower().endswith(".jsonl"):
        last_obj: Dict[str, Any] = {}
        with open(source_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    last_obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
        return (last_obj.get("text")
                or last_obj.get("page_text")
                or "")

    with open(source_path, "r", encoding="utf-8") as f:
        return f.read()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Parse Halyk Bank footer (обороты и исходящий остаток)."
    )
    ap.add_argument(
        "source",
        help="Путь к выписке (.pdf, .jsonl или .txt).",
    )
    ap.add_argument(
        "-o",
        "--out",
        dest="out_csv",
        default="accountStatement_footer.csv",
        help="Выходной CSV (по умолчанию accountStatement_footer.csv).",
    )
    ap.add_argument(
        "--out-json",
        dest="out_json",
        help="Опциональный JSON с теми же полями.",
    )
    args = ap.parse_args()

    text = _extract_text(args.source)
    res = _parse_footer_text(text)
    df = pd.DataFrame([res])
    df.to_csv(args.out_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Footer parsed → {args.out_csv}")
    print(df.to_string(index=False))

    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=2)
        print(f"✅ JSON saved → {args.out_json}")


if __name__ == "__main__":
    main()
