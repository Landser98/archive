#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Eurasian Bank PDF → CSV (Camelot) — tolerant header detection & fuzzy column mapping.
"""
from __future__ import annotations

import argparse, re
from typing import List, Optional, Dict, Tuple
import pandas as pd
import re
from typing import Optional, Dict, Tuple
from src.utils.income_calc import compute_ip_income

from pathlib import Path

EURASIAN_DEBUG = False  # flip to True to dump Camelot head to CSV for debugging



EXPECTED = [
    "Дата проводки",
    "Вид операции",
    "Номер документа клиента",
    "Наименование Бенефициара/Отправителя",
    "ИИН/БИН Бенефициара/Отправителя",
    "ИИК Бенефициара/Отправителя денег",
    "Наименование банка Бенефициара/Отправителя денег",
    "БИК банка Бенефициара/Отправителя",
    "Назначение платежа",
    "Дебет",
    "Кредит",
    "Блокированная сумма",
]

ANCHORS = [
    "дата","проводк","операци","вид","тип","номер","док",
    "бенефициара","отправителя","получателя",
    "иин","бин","иик","iban","банк","бик","swift",
    "назначение","дебет","кредит","блокирован"
]

DATE_RE = re.compile(r"^\s*\d{2}\.\d{2}\.\d{4}\b", re.U)


AMT_RE = re.compile(r"[0-9][0-9' \u00A0\u202F.,]*[0-9]")


def clean_cell(x: Optional[str]) -> str:
    if x is None: return ""
    s = str(x).replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm(s: str) -> str:
    s = clean_cell(s).lower()
    s = s.replace("ё", "е")
    s = re.sub(r"[^a-zа-я0-9/ ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # soften endings/variants
    s = s.replace("денег", "").replace("клиента", "").replace("банка", "")
    s = s.replace("бенефициара / отправителя", "бенефициара/отправителя")
    return s

def tokens(s: str) -> set:
    return set(norm(s).split())

def header_candidate_score(row_vals: List[str]) -> int:
    text = " ".join(norm(v) for v in row_vals)
    return sum(1 for a in ANCHORS if a in text)

def looks_like_header(row_vals: list[str]) -> bool:
    # not a data row (no leading date), must contain 'дата' and at least one of дебет/кредит
    text = " ".join(norm(v) for v in row_vals)
    if DATE_RE.match((row_vals[0] or "")):
        return False
    score = sum(1 for a in ANCHORS if a in text)
    return ("дата" in text) and (("дебет" in text) or ("кредит" in text)) and score >= 4

def is_total_footer(row_vals: list[str]) -> bool:
    t = " ".join(norm(v) for v in row_vals)
    return ("итог" in t) or ("итого" in t)

def map_headers_fuzzy(raw_cols: list[str]) -> list[str]:
    # synonyms / normalizations per column
    synonyms = {
        "Дата проводки": ["дата провод", "дата операции", "дата транзакции"],
        "Вид операции": ["вид операции", "тип операции", "операция"],
        "Номер документа клиента": ["номер документа", "номер док", "№ док", "документ клиента"],
        "Наименование Бенефициара/Отправителя": [
            "наименование бенефициара/отправителя", "наименование получателя/отправителя",
            "получатель/отправитель", "контрагент", "бенефициар"
        ],
        "ИИН/БИН Бенефициара/Отправителя": ["иин/бин", "бин/иин", "идент номер"],
        "ИИК Бенефициара/Отправителя денег": ["иик", "iban", "счет получателя"],
        "Наименование банка Бенефициара/Отправителя денег": ["банк бенефициара", "банк получателя", "банк отправителя"],
        "БИК банка Бенефициара/Отправителя": ["бик", "swift", "bic"],
        "Назначение платежа": ["назначение платежа", "назначение"],
        "Дебет": ["дебет", "сумма по дебету"],
        "Кредит": ["кредит", "сумма по кредиту"],
        "Блокированная сумма": ["блокирован", "блокированная сумма", "холд", "зарезервированная"],
    }
    def best_expected(col: str) -> str|None:
        c = norm(col)
        # exact/contains match first
        for exp, alts in synonyms.items():
            if any(a in c for a in ([norm(exp)] + [norm(x) for x in alts])):
                return exp
        # fallback: jaccard-ish
        exp_tokens = {e: tokens(e) for e in EXPECTED}
        ct = tokens(col)
        best, best_score = None, 0.0
        for e, et in exp_tokens.items():
            inter = len(ct & et); union = len(ct | et) or 1
            score = inter / union
            if score > best_score:
                best, best_score = e, score
        return best
    mapped = []
    used = set()
    for rc in raw_cols:
        m = best_expected(rc)
        if m and m not in used:
            mapped.append(m); used.add(m)
        else:
            mapped.append(None)
    # fill remaining, preserving EXPECTED order
    remaining = [e for e in EXPECTED if e not in used]
    mapped = [m if m else remaining.pop(0) for m in mapped]
    return mapped

def find_header(df: pd.DataFrame) -> tuple[int, list[str]]:
    n = min(len(df), 80)
    # pass 1: strict
    for i in range(n):
        row = [clean_cell(x) for x in df.iloc[i].tolist()]
        if looks_like_header(row):
            header_lines = [row]
            # merge up to 3 continuation lines (until a date appears on first column)
            for k in (i+1, i+2, i+3):
                if k < len(df):
                    nxt = [clean_cell(x) for x in df.iloc[k].tolist()]
                    nxt_text = " ".join(norm(v) for v in nxt)
                    if (not DATE_RE.match(nxt[0] or "")) and sum(1 for a in ANCHORS if a in nxt_text) >= 2:
                        header_lines.append(nxt)
                    else:
                        break
            stitched = []
            for col_idx in range(df.shape[1]):
                parts = [clean_cell(h[col_idx]) for h in header_lines if col_idx < len(h)]
                stitched.append(clean_cell(" ".join(p for p in parts if p)))
            stitched = [re.sub(r"\s+", " ", s).strip() for s in stitched]
            mapped = map_headers_fuzzy(stitched)
            return i + len(header_lines) - 1, mapped
    # pass 2: fallback — first row that contains дата + (дебет|кредит), even if weak
    for i in range(n):
        row = [clean_cell(x) for x in df.iloc[i].tolist()]
        text = " ".join(norm(v) for v in row)
        if (not DATE_RE.match(row[0] or "")) and ("дата" in text) and (("дебет" in text) or ("кредит" in text)):
            header_lines = [row]
            for k in (i+1, i+2, i+3):
                if k < len(df):
                    nxt = [clean_cell(x) for x in df.iloc[k].tolist()]
                    if (not DATE_RE.match(nxt[0] or "")):
                        header_lines.append(nxt)
                    else:
                        break
            stitched = []
            for col_idx in range(df.shape[1]):
                parts = [clean_cell(h[col_idx]) for h in header_lines if col_idx < len(h)]
                stitched.append(clean_cell(" ".join(p for p in parts if p)))
            stitched = [re.sub(r"\s+", " ", s).strip() for s in stitched]
            mapped = map_headers_fuzzy(stitched)
            return i + len(header_lines) - 1, mapped
    raise RuntimeError("Не нашёл шапку таблицы даже по упрощённым правилам. Снимок того, что видит Camelot поможет.")

def parse_money(x: str) -> Optional[float]:
    s = clean_cell(x).replace("\u00a0", " ").replace("\u202f", " ")
    s = s.replace(" ", "").replace(",", ".")
    s = re.sub(r"[^\d.\-]", "", s)
    if s == "" or s == "-": return None
    try:
        return float(s)
    except ValueError:
        return None

def read_with_camelot(pdf_path: str, pages: str, flavor: str):
    import camelot
    if flavor == "auto":
        # try lattice with stronger line detection first
        try:
            t = camelot.read_pdf(pdf_path, pages=pages, flavor="lattice",
                                 line_scale=40, strip_text="\n")
            if len(t): return t
        except Exception:
            pass
        # then stream with a slightly tighter tolerance
        return camelot.read_pdf(pdf_path, pages=pages, flavor="stream",
                                row_tol=8, column_tol=8, strip_text="\n")
    elif flavor == "lattice":
        return camelot.read_pdf(pdf_path, pages=pages, flavor="lattice",
                                line_scale=40, strip_text="\n")
    else:
        return camelot.read_pdf(pdf_path, pages=pages, flavor="stream",
                                row_tol=8, column_tol=8, strip_text="\n")

def normalize_op_type(s: str) -> str:
    """
    Нормализуем 'Вид операции':
    - чистим пробелы
    - если это чисто цифры и длина 1–2 → дополняем слева нулями до 2 символов
    иначе возвращаем как есть.
    """
    s = clean_cell(s)
    if re.fullmatch(r"\d{1,2}", s):
        return s.zfill(2)   # '6' -> '06', '12' -> '12'
    return s

def parse_pdf(pdf_path: str, pages: str = "1-end", flavor: str = "auto") -> pd.DataFrame:
    tables = read_with_camelot(pdf_path, pages, flavor)
    if len(tables) == 0:
        raise RuntimeError("Camelot не нашёл таблиц. Проверьте Ghostscript/--flavor.")
    frames = []
    for t in tables:
        df = t.df.astype(str)
        # use .map over columns to avoid applymap deprecation
        for c in df.columns:
            df[c] = df[c].map(clean_cell)
        frames.append(df)
    raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if raw.empty:
        raise RuntimeError("Пустой результат после объединения таблиц.")

    # 1) find & build header
    hdr_end_idx, mapped_cols = find_header(raw)

    if EURASIAN_DEBUG:
        pdf_p = Path(pdf_path)
        debug_path = pdf_p.parent / f"{pdf_p.stem}_eurasian_debug_head.csv"
        raw.head(40).to_csv(debug_path, index=False)
        print(f"Wrote Eurasian debug head (first 40 rows) to {debug_path.resolve()}")

    # 2) slice data under the header
    data = raw.iloc[hdr_end_idx+1:, :].reset_index(drop=True)
    data.columns = mapped_cols + [f"extra_{i}" for i in range(len(data.columns)-len(mapped_cols))]

    # 3) drop repeated headers within pages
    def row_is_header_like(r) -> bool:
        return looks_like_header([str(x) for x in r.values])
    mask = ~data.apply(row_is_header_like, axis=1)
    data = data[mask].reset_index(drop=True)

    # 4) drop footers (ИТОГО)
    data = data[~data.apply(lambda r: is_total_footer([str(x) for x in r.values]), axis=1)].reset_index(drop=True)

    # 5) keep only our EXPECTED columns (in order); add empties if missing
    for col in EXPECTED:
        if col not in data.columns:
            data[col] = ""
    data = data[EXPECTED]

    # 6) prune obvious non-data rows (no date in first col)
    data = data[data["Дата проводки"].apply(lambda s: bool(DATE_RE.match(s or "")))].reset_index(drop=True)

    # 6.5) Вид операции – всегда строка, подправляем код
    if "Вид операции" in data.columns:
        data["Вид операции"] = (
            data["Вид операции"]
            .astype(str)
            .map(normalize_op_type)
        )

    # 7) types for money columns
    for mcol in ["Дебет", "Кредит", "Блокированная сумма"]:
        data[mcol] = data[mcol].apply(parse_money)

    return data

def _parse_amount(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    t = str(s)
    t = t.replace("\u00A0", " ").replace("\u202F", " ")
    t = t.replace(" ", "").replace("'", "")
    t = t.replace(",", ".")
    t = re.sub(r"[^\d.\-]", "", t)
    if not t or t == "-":
        return None
    try:
        return float(t)
    except ValueError:
        return None


# ---------------- HEADER ---------------- #

def parse_eurasian_header(pdf_path: str) -> pd.DataFrame:
    import pdfplumber
    with pdfplumber.open(pdf_path) as pdf:
        text = (pdf.pages[0].extract_text() or "")

    txt = text.replace("\r", "\n")

    def search(pattern: str) -> Optional[str]:
        m = re.search(pattern, txt, flags=re.I)
        if not m:
            return None
        if m.lastindex:
            return m.group(1).strip()
        return m.group(0).strip()

    bank_name = search(r"АО\s+ЕВРАЗИЙСКИЙ\s+БАНК") or "АО ЕВРАЗИЙСКИЙ БАНК"
    bank_bin  = search(r"БИН\s+(\d{10,12})")
    bank_bik  = search(r"БИК\s+([A-Z0-9]{6,11})")

    # e.g. "Дата выписки: 22.09.2025г."
    statement_date = search(r"Дата\s+выписки:\s*([\d.]+)\s*(?:г\.?)?")

    # e.g. "Выписка за период: 22.08.2024г. - 22.08.2025г."
    period = re.search(
        r"Выписка\s+за\s+период:\s*([\d.]+)\s*(?:г\.?)?\s*[–-]\s*([\d.]+)\s*(?:г\.?)?",
        txt,
        flags=re.I,
    )
    period_start = period.group(1).strip() if period else None
    period_end   = period.group(2).strip() if period else None

    client_name = search(r"Клиент:\s*(.+)")
    client_inn  = search(r"ИИН/БИН:\s*([\d]+)")
    iban        = search(r"ИИК.*?:\s*([A-Z0-9]+)")
    currency    = search(r"Валюта:\s*([A-Z]{3})")

    last_op_date = search(r"Дата\s+последней\s+операции\s+по\s+счету:\s*([\d.]+)")

    opening_str = search(r"Входящий\s+остаток\s+пассив[аы]?\s*([0-9' \u00A0\u202F.,]+)")
    opening_bal = _parse_amount(opening_str) if opening_str else None

    row = {
        "bank_name": bank_name,
        "bank_bin": bank_bin,
        "bank_bik": bank_bik,
        "statement_date": statement_date,
        "period_start": period_start,
        "period_end": period_end,
        "client_name": client_name,
        "client_inn_bin": client_inn,
        "iban": iban,
        "currency": currency,
        "last_operation_date": last_op_date,
        "opening_balance": opening_bal,
    }
    return pd.DataFrame([row])


# ---------------- FOOTER ---------------- #

def parse_eurasian_footer(pdf_path: str) -> pd.DataFrame:
    """
    Parses the footer of an Eurasian Bank statement from the LAST page(s).

    Returns 1-row DataFrame with:
      total_docs
      turnover_debit
      turnover_credit
      turnover_blocked
      closing_balance_passive
      note1_amount ... note6_amount
      final_balance   (остаток суммы денег на конец периода ...)
    """
    import pdfplumber

    with pdfplumber.open(pdf_path) as pdf:
        # sometimes footer spills to last 2 pages; concatenate
        pages_text = []
        if len(pdf.pages) >= 2:
            pages_text.append(pdf.pages[-2].extract_text() or "")
        pages_text.append(pdf.pages[-1].extract_text() or "")
        txt = "\n".join(pages_text)

    def search(pattern: str) -> Optional[str]:
        m = re.search(pattern, txt, flags=re.I | re.S)
        return m.group(1).strip() if m else None

    # Всего документов: 281
    total_docs_str = search(r"Всего\s+документов:\s*([0-9]+)")
    total_docs = int(total_docs_str) if total_docs_str is not None else None

    # Итого обороты: three numbers (дебет, кредит, блок / разница)
    itogo_match = re.search(
        r"Итого\s+обороты:?[^0-9]*"
        r"([0-9' \u00A0\u202F.,]+)\s+"
        r"([0-9' \u00A0\u202F.,]+)\s+"
        r"([0-9' \u00A0\u202F.,]+)",
        txt,
        flags=re.I,
    )
    if itogo_match:
        turnover_debit = _parse_amount(itogo_match.group(1))
        turnover_credit = _parse_amount(itogo_match.group(2))
        turnover_blocked = _parse_amount(itogo_match.group(3))
    else:
        turnover_debit = turnover_credit = turnover_blocked = None

    # Исходящий остаток пассив 245'061.92
    closing_str = search(r"Исходящ[ийая]\s+остаток\s+пассив[аы]?\s*([0-9' \u00A0\u202F.,]+)")
    closing_balance_passive = _parse_amount(closing_str) if closing_str else None

    # bullet 1..6 amounts (1) ... 0.00
    note_amounts: Dict[str, Optional[float]] = {}
    for i in range(1, 7):
        m = re.search(
            rf"\b{i}\)\s.*?({AMT_RE.pattern})",
            txt,
            flags=re.I | re.S,
        )
        note_amounts[f"note{i}_amount"] = _parse_amount(m.group(1)) if m else None

    # final “остаток суммы денег ...” amount (same as closing, but grab anyway)
    final_str = search(
        r"остаток\s+суммы\s+денег.*?:\s*([0-9' \u00A0\u202F.,]+)"
    )
    final_balance = _parse_amount(final_str) if final_str else None

    row = {
        "total_docs": total_docs,
        "turnover_debit": turnover_debit,
        "turnover_credit": turnover_credit,
        "turnover_blocked": turnover_blocked,
        "closing_balance_passive": closing_balance_passive,
        **note_amounts,
        "final_balance": final_balance,
    }
    return pd.DataFrame([row])


def parse_eurasian_statement(
    pdf_path: str,
    pages: str = "1-end",
    flavor: str = "auto",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Full pipeline for Eurasian Bank statement:
    - header   (pdfplumber)
    - tx table (Camelot)
    - footer   (pdfplumber)

    Returns: (header_df, tx_df, footer_df)
    """
    header_df = parse_eurasian_header(pdf_path)
    tx_df     = parse_pdf(pdf_path, pages=pages, flavor=flavor)
    footer_df = parse_eurasian_footer(pdf_path)
    return header_df, tx_df, footer_df


def _extract_knp_from_purpose(purpose: str) -> str:
    """
    Для Eurasian: КНП зашит в названии, например 'КНП_841 ...'.
    Достаём 2–3 цифры после 'КНП', если есть.
    """
    s = str(purpose)
    m = re.search(r"КНП[_\s-]*([0-9]{2,3})", s, flags=re.IGNORECASE)
    if m:
        return m.group(1).lstrip("0") or m.group(1)
    return ""


