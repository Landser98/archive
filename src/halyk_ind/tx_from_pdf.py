# src/halyk_ind/tx_from_pdf.py
import re
from typing import List, Dict

import fitz  # PyMuPDF
import pandas as pd

DATE_RE = re.compile(r"\d{2}\.\d{2}\.\d{4}")


def _normalize_text_block(text: str) -> str:
    text = text.replace("\xa0", " ").replace("\u202f", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_page_transactions(page) -> List[Dict]:
    """
    Берём пары блоков:
      левый блок (даты + длинное Описание операции)
      правый блок (суммы, валюта, счёт)
    """
    blocks = page.get_text("blocks")
    # сортируем по y, затем по x
    blocks_sorted = sorted(blocks, key=lambda b: (round(b[1]), b[0]))

    left_blocks = []
    right_blocks = []

    for x0, y0, x1, y1, txt, *_ in blocks_sorted:
        txt = txt.strip()
        if not txt:
            continue
        if x0 < 150:   # левая колонка
            left_blocks.append((x0, y0, x1, y1, txt))
        else:          # правая колонка
            right_blocks.append((x0, y0, x1, y1, txt))

    rows: List[Dict] = []

    for x0, y0, x1, y1, ltxt in left_blocks:
        ltxt = ltxt.strip()
        first_line = ltxt.splitlines()[0].strip() if ltxt else ""
        if not DATE_RE.match(first_line):
            continue

        # ищем правый блок с близким y
        candidates = []
        for rx0, ry0, rx1, ry1, rtxt in right_blocks:
            if abs(ry0 - y0) <= 2.5:  # допуск по y чуть шире
                candidates.append((rx0, ry0, rx1, ry1, rtxt))
        if not candidates:
            continue

        # берём самый широкий правый блок
        rx0, ry0, rx1, ry1, rtxt = max(
            candidates, key=lambda bb: bb[2] - bb[0]
        )

        l_lines = [ln.strip() for ln in ltxt.splitlines() if ln.strip()]
        if len(l_lines) < 3:
            continue

        date_posted = l_lines[0]
        date_processed = l_lines[1]
        descr = " ".join(l_lines[2:])
        descr = _normalize_text_block(descr)

        r_lines = [ln.strip() for ln in rtxt.splitlines() if ln.strip()]
        r_lines = [_normalize_text_block(ln) for ln in r_lines]

        # ожидаем минимум: сумма, валюта, приход, расход, комиссия, счёт
        if len(r_lines) < 6:
            continue

        amount_op = r_lines[0]
        currency = r_lines[1]
        credit = r_lines[2]
        debit = r_lines[3]
        fee = r_lines[4]
        account = r_lines[5]

        rows.append(
            {
                "Дата проведения операции": date_posted,
                "Дата обработки операции": date_processed,
                "Описание операции": descr,
                "Сумма операции": amount_op,
                "Валюта операции": currency,
                "Приход в валюте счета": credit,
                "Расход в валюте счета": debit,
                "Комиссия": fee,
                "Счет": account,
            }
        )

    return rows


def extract_halyk_ind_tx_from_pdf(pdf_path: str) -> pd.DataFrame:
    """
    Читает PDF и возвращает DataFrame только с колонками транзакций,
    НЕ преобразуя числа в float и НЕ трогая формат дат.
    """
    doc = fitz.open(pdf_path)
    all_rows: List[Dict] = []
    for page in doc:
        all_rows.extend(_parse_page_transactions(page))

    df = pd.DataFrame(all_rows)
    return df
