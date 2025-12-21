#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch-парсер для Kaspi Pay / Kaspi Gold выписок.

Теперь умеет:
  1) Работать как по JSONL (*_pages.jsonl), так и напрямую по PDF:
     - при PDF сам вызывает dump_pdf_pages() из convert_pdf_json_pages.py,
       чтобы сделать JSONL.
  2) Создавать простой meta-JSON по PDF (creator / creation / mod и т.п.),
     если он ещё не создан.
  3) Обрабатывать как один файл, так и папку.
  4) После парсинга:
       - делает числовую проверку баланса (opening + ΣКредит − ΣДебет = closing);
       - валидирует PDF-метаданные (CreationDate / ModDate / Creator / Producer),
         используя utils.statement_validation.validate_pdf_metadata_from_json().

На вход:
  - путь к файлу (PDF или *_pages.jsonl) ИЛИ директории.

На выход по каждому стейтменту (stem = имя файла без суффикса):
  <stem>_header.csv
  <stem>_tx.csv
  <stem>_footer.csv             (если футер есть)
  <stem>_tx_ip.csv
  <stem>_ip_income_monthly.csv
  (опционально) meta JSON:
  <stem>_pdf_meta.json          (в папке --pdf-meta-dir)
"""

import argparse
import sys
import json
from pathlib import Path
from typing import Sequence, Dict, Any, Tuple, List, Optional

import pandas as pd
from src.config import DATA_DIR

from src.kaspi_pay.parser import parse_kaspi_pay_statement
from src.utils.income_calc import compute_ip_income
from src.utils.convert_pdf_json_pages import dump_pdf_pages
from src.utils.statement_validation import validate_pdf_metadata_from_json
from src.kaspi_pay.header import _normalize_amount_to_float
from src.utils.statement_validation import BANK_SCHEMAS, validate_statement_generic, validate_pdf_metadata_from_json



# ---------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------
def _pick_first_existing(cols: Sequence[str], candidates, fallback=None):
    """Вернуть первый столбец из candidates, который реально есть в DataFrame."""
    for c in candidates:
        if c in cols:
            return c
    return fallback


def _parse_number_ru(val: Any) -> float:
    """'5 576 876,37' / '0,00' / 5.0 / None → float (для валидации)."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        try:
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    s = str(val)
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    s = s.replace(" ", "").replace(",", ".")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _ensure_jsonl_for_pdf(pdf_path: Path, jsonl_dir: Path) -> Path:
    """Для данного PDF вернуть путь к *_pages.jsonl, при отсутствии – создать."""
    jsonl_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = jsonl_dir / f"{pdf_path.stem}_pages.jsonl"
    if jsonl_path.exists():
        return jsonl_path

    print(f"   ▶ Генерируем JSONL через dump_pdf_pages() → {jsonl_path}")
    # dump_pdf_pages сам придумает имя, если out_path=None; но здесь мы явно задаём.
    written = dump_pdf_pages(pdf_path=pdf_path, out_path=jsonl_path)
    # written может быть либо Path, либо список путей – защищаемся:
    if isinstance(written, (list, tuple)):
        # ищем первый *_pages.jsonl среди возвращённых
        for p in written:
            p = Path(p)
            if p.name.endswith("_pages.jsonl") and p.exists():
                return p
    elif isinstance(written, (str, Path)):
        p = Path(written)
        if p.exists():
            return p

    # Fallback: просто верим нашему target-пути
    return jsonl_path


def _extract_pdf_meta_from_jsonl(jsonl_path: Path) -> Dict[str, Any]:
    """
    Достаём pdf.metadata.

    1) Сначала пробуем из первой строки JSONL (если dump_pdf_pages туда пишет).
    2) Если пусто – пробуем взять из большого каталожного JSON
       DATA_DIR/converted_jsons/<pdf_stem>.json,
       который делает convert_pdf_json_page.py.
    """
    meta: Dict[str, Any] = {}

    # --- 1. Попытка прочитать из JSONL ---
    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
        if first_line:
            first_page = json.loads(first_line)
            meta = first_page.get("metadata") or {}
    except Exception as e:
        print(f"   ⚠️ Не удалось прочитать первую строку JSONL для meta: {e}")

    if meta:
        # уже есть нормальная мета внутри JSONL
        return {"metadata": meta}

    # --- 2. Фолбэк: большой JSON от convert_pdf_json_page.py ---
    try:
        pdf_stem = jsonl_path.stem
        # наши jsonl обычно <stem>_pages.jsonl → отрежем хвост "_pages"
        if pdf_stem.endswith("_pages"):
            pdf_stem = pdf_stem[:-6]

        catalog_json = Path(DATA_DIR) / "converted_jsons" / f"{pdf_stem}.json"
        if catalog_json.exists():
            with open(catalog_json, "r", encoding="utf-8") as f:
                big = json.load(f)
            fallback_meta = big.get("metadata") or {}
            if fallback_meta:
                print(f"   ⓘ metadata взяли из {catalog_json.name}")
                return {"metadata": fallback_meta}
    except Exception as e:
        print(f"   ⚠️ Фолбэк чтения meta из каталожного JSON не удался: {e}")

    # ничего не нашли
    return {}


def _save_meta_if_missing(meta_json: Dict[str, Any], meta_path: Path) -> None:
    """
    Сохранить meta_json в meta_path.

    Если файл уже есть и в нём есть непустой metadata – не трогаем.
    Если файла нет ИЛИ в нём metadata пустой, а новый meta_json содержит что-то –
    перезаписываем.
    """
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    # если файл уже есть – посмотрим, что в нём
    if meta_path.exists():
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            existing_meta = existing.get("metadata") or {}
            new_meta = meta_json.get("metadata") or {}

            # если старый уже с нормальной метой или новая тоже пустая – ничего не делаем
            if existing_meta and not new_meta:
                return
            if existing_meta and new_meta:
                return
        except Exception:
            # если файл битый – просто перезапишем ниже
            pass

    # сюда попадаем, если файла нет или мета была пустая
    try:
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_json, f, ensure_ascii=False, indent=2)
        print(f"   ✅ Meta JSON   → {meta_path}")
    except Exception as e:
        print(f"   ⚠️ Не удалось записать meta JSON {meta_path}: {e}")

def _run_numeric_validation_kaspi(
    header_df: pd.DataFrame,
    tx_df: pd.DataFrame,
    footer_df: Optional[pd.DataFrame],
    tol: float = 0.01,
) -> Tuple[List[str], Dict[str, Any]]:
    """Простая числовая проверка для Kaspi Pay / Gold.

    Проверяем:
      - opening + ΣКредит − ΣДебет ≈ closing
      - (если есть футер) total_credit_turnover / total_debit_turnover vs Σ по tx
    """
    flags: List[str] = []
    debug: Dict[str, Any] = {}

    if header_df.empty or tx_df.empty:
        flags.append("empty_header_or_tx")
        return flags, debug

    row = header_df.iloc[0]

    opening_raw = row.get("Входящий остаток")
    closing_raw = row.get("Исходящий остаток")

    opening_val, opening_ccy = _normalize_amount_to_float(opening_raw) if isinstance(opening_raw, str) else (None, None)
    closing_val, closing_ccy = _normalize_amount_to_float(closing_raw) if isinstance(closing_raw, str) else (None, None)

    debug.update(
        opening_raw=opening_raw,
        closing_raw=closing_raw,
        opening_val=opening_val,
        closing_val=closing_val,
        opening_ccy=opening_ccy,
        closing_ccy=closing_ccy,
    )

    if opening_val is None or closing_val is None:
        flags.append("cannot_parse_opening_or_closing_balance")
        return flags, debug

    total_credit = pd.to_numeric(tx_df.get("Кредит"), errors="coerce").fillna(0.0).sum()
    total_debit = pd.to_numeric(tx_df.get("Дебет"), errors="coerce").fillna(0.0).sum()

    closing_calc = opening_val + total_credit - total_debit

    debug.update(
        total_credit=total_credit,
        total_debit=total_debit,
        closing_calc=closing_calc,
        tolerance=tol,
    )

    if abs(closing_calc - closing_val) > tol:
        flags.append("closing_balance_mismatch")

    # --- футер: total_debit_turnover / total_credit_turnover ---
    if footer_df is not None and not footer_df.empty:
        fr = footer_df.iloc[0]
        credit_footer = fr.get("total_credit_turnover")
        debit_footer = fr.get("total_debit_turnover")

        credit_footer_val = _parse_number_ru(credit_footer) if credit_footer is not None else None
        debit_footer_val = _parse_number_ru(debit_footer) if debit_footer is not None else None

        debug.update(
            footer_total_credit_turnover=credit_footer_val,
            footer_total_debit_turnover=debit_footer_val,
        )

        if credit_footer_val is not None and abs(credit_footer_val - total_credit) > tol:
            flags.append("footer_credit_turnover_mismatch")
        if debit_footer_val is not None and abs(debit_footer_val - total_debit) > tol:
            flags.append("footer_debit_turnover_mismatch")

    return flags, debug


def _process_one(
    jsonl_path: Path,
    out_dir: Path,
    pdf_meta_dir: Optional[Path] = None,
) -> None:
    print(f"{jsonl_path.name}")

    # --- PDF meta из JSONL ---
    pdf_meta_json: Optional[Dict[str, Any]] = None
    meta_path: Optional[Path] = None
    if pdf_meta_dir is not None:
        pdf_meta_json = _extract_pdf_meta_from_jsonl(jsonl_path)
        if pdf_meta_json:
            meta_path = pdf_meta_dir / f"{jsonl_path.stem}_pdf_meta.json"
            _save_meta_if_missing(pdf_meta_json, meta_path)

    # --- парсинг стейтмента ---
    header_df, tx_df, footer_df = parse_kaspi_pay_statement(str(jsonl_path))

    stem = jsonl_path.stem
    out_header         = out_dir / f"{stem}_header.csv"
    out_tx             = out_dir / f"{stem}_tx.csv"
    out_footer         = out_dir / f"{stem}_footer.csv"
    out_tx_ip          = out_dir / f"{stem}_tx_ip.csv"
    out_ip_monthly     = out_dir / f"{stem}_ip_income_monthly.csv"
    out_income_summary = out_dir / f"{stem}_income_summary.csv"

    # --- базовые CSV ---
    header_df.to_csv(out_header, index=False, encoding="utf-8-sig")
    tx_df.to_csv(out_tx, index=False, encoding="utf-8-sig")

    # --- нормализуем footer ---
    if footer_df is None:
        df_footer = None
    elif isinstance(footer_df, pd.DataFrame):
        df_footer = footer_df
    elif isinstance(footer_df, list):
        df_footer = pd.DataFrame(footer_df)
    elif isinstance(footer_df, dict):
        df_footer = pd.DataFrame([footer_df])
    else:
        df_footer = pd.DataFrame()

    if df_footer is not None and not df_footer.empty:
        df_footer.to_csv(out_footer, index=False, encoding="utf-8-sig")
        print(f"   ✅ Footer      → {out_footer}")
    else:
        print("   ⚠️ Footer      → пустой (не записан)")

    # === расчёт дохода ИП по Kaspi Pay ===
    cols = list(tx_df.columns)

    col_op_date = _pick_first_existing(cols, ["Дата операции", "Дата"], fallback=cols[1])
    col_credit = _pick_first_existing(cols, ["Кредит"], fallback=cols[3])
    col_knp = _pick_first_existing(cols, ["КНП"], fallback=None)
    col_purpose = _pick_first_existing(cols, ["Назначение платежа"], fallback=cols[-1])
    col_counterparty = _pick_first_existing(
        cols,
        [
            "Наименование получателя",
            "Наименование получателя (бенеф)",
            "Наименование получателя (отправителя денег)",
        ],
        fallback=cols[4] if len(cols) > 4 else cols[-1],
    )

    if col_knp is None:
        tx_df["КНП"] = ""
        col_knp = "КНП"

    # 🔁 теперь compute_ip_income возвращает income_summary (dict)
    enriched_tx, monthly_income, income_summary = compute_ip_income(
        tx_df,
        col_op_date=col_op_date,
        col_credit=col_credit,
        col_knp=col_knp,
        col_purpose=col_purpose,
        col_counterparty=col_counterparty,
        months_back=12,
        op_date_pattern=r"(\d{2}\.\d{2}\.\d{4})",  # dd.mm.yyyy
        op_date_format="%d.%m.%Y",
        verbose=True,
        max_examples=5,
    )

    # dict → DataFrame и сохраняем
    income_summary_df = pd.DataFrame([income_summary])

    enriched_tx.to_csv(out_tx_ip, index=False, encoding="utf-8-sig")
    monthly_income.to_csv(out_ip_monthly, index=False, encoding="utf-8-sig")
    income_summary_df.to_csv(out_income_summary, index=False, encoding="utf-8-sig")

    print(f"   ✅ Header      → {out_header}")
    print(f"   ✅ Tx          → {out_tx}")
    if df_footer is not None and not df_footer.empty:
        print(f"   ✅ Tx+IP       → {out_tx_ip}")
    else:
        print(f"   ✅ Tx+IP       → {out_tx_ip}")
    print(f"   ✅ IP monthly  → {out_ip_monthly}")
    print(f"   ✅ Income summary → {out_income_summary}")

    adj = income_summary.get("total_income_adjusted")
    if adj is not None:
        print(f"   ✅ Adjusted income: {adj:,.2f}")
    else:
        print("   ✅ Adjusted income: N/A")

    # === ЧИСЛОВАЯ ВАЛИДАЦИЯ ===
    num_flags, num_debug = _run_numeric_validation_kaspi(header_df, tx_df, df_footer)
    if num_flags:
        print(f"   ⚠️ Numeric validation flags: {num_flags}")
    else:
        print("   ✅ Numeric validation: OK")

    # === PDF META ВАЛИДАЦИЯ ===
    if pdf_meta_json:
        period_start = str(header_df.iloc[0].get("Период (начало)") or "")
        period_end = str(header_df.iloc[0].get("Период (конец)") or "")

        pdf_flags, pdf_debug = validate_pdf_metadata_from_json(
            pdf_meta_json,
            bank="KASPI_PAY",
            period_start=period_start or None,
            period_end=period_end or None,
            period_date_format="%d.%m.%Y",
            max_days_after_period_end=7,
            allowed_creators=None,
            allowed_producers=None,
        )

        if pdf_flags:
            print(f"   ⚠️ PDF metadata flags: {pdf_flags}")
        else:
            print("   ✅ PDF metadata: OK")

        print(
            "   ⓘ PDF meta: Creator={creator}, Producer={producer}, Creation={creation}, Mod={mod}".format(
                creator=pdf_debug.get("pdf_creator"),
                producer=pdf_debug.get("pdf_producer"),
                creation=pdf_debug.get("pdf_creation_dt"),
                mod=pdf_debug.get("pdf_mod_dt"),
            )
        )
    else:
        print("   ⚠️ PDF meta: не найдено в JSONL (metadata отсутствует)")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Batch: Kaspi Pay / Kaspi Gold → header/tx/footer + IP income + validation"
    )
    ap.add_argument(
        "root",
        help="Файл (PDF или *_pages.jsonl) ИЛИ директория с такими файлами",
    )
    ap.add_argument(
        "--pattern",
        default="*_pages.jsonl",
        help="Глоб-паттерн для входных файлов. Для JSONL по умолчанию '*_pages.jsonl', для PDF – '*.pdf'.",
    )
    ap.add_argument(
        "--out-dir",
        required=True,
        help="Куда писать CSV (будет создана, если не существует)",
    )
    ap.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Максимум файлов для обработки (для теста)",
    )
    ap.add_argument(
        "--input-type",
        choices=["jsonl", "pdf"],
        default="jsonl",
        help="Тип входа: jsonl (старое поведение) или pdf (авто-создание JSONL)",
    )
    ap.add_argument(
        "--jsonl-dir",
        default=None,
        help="Куда складывать/где искать *_pages.jsonl при input-type=pdf. По умолчанию: <out-dir>/converted_jsons",
    )
    ap.add_argument(
        "--pdf-meta-dir",
        default=None,
        help="Куда писать pdf_meta.json. По умолчанию: <out-dir>/pdf_meta",
    )

    args = ap.parse_args()

    root = Path(args.root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- директории для PDF-режима ---
    jsonl_dir = None
    pdf_meta_dir = None
    if args.input_type == "pdf":
        jsonl_dir = Path(args.jsonl_dir) if args.jsonl_dir else (out_dir / "converted_jsons")
        jsonl_dir.mkdir(parents=True, exist_ok=True)
        pdf_meta_dir = Path(args.pdf_meta_dir) if args.pdf_meta_dir else (out_dir / "pdf_meta")
        pdf_meta_dir.mkdir(parents=True, exist_ok=True)
    else:
        # jsonl-режим: pdf_meta_dir можем тоже создать, если указан
        if args.pdf_meta_dir:
            pdf_meta_dir = Path(args.pdf_meta_dir)
            pdf_meta_dir.mkdir(parents=True, exist_ok=True)

    # --- режим: единичный файл ---
    if root.is_file():
        if args.input_type == "jsonl":
            jsonl_path = root
            _process_one(jsonl_path, out_dir, pdf_meta_dir)
        else:  # pdf
            if root.suffix.lower() != ".pdf":
                raise SystemExit(f"Ожидается PDF, а получено: {root}")
            assert jsonl_dir is not None
            jsonl_path = _ensure_jsonl_for_pdf(root, jsonl_dir)
            _process_one(jsonl_path, out_dir, pdf_meta_dir)
        return

    # --- режим: папка ---
    if not root.is_dir():
        raise SystemExit(f"Root is neither file nor directory: {root}")

    if args.input_type == "jsonl":
        in_paths = sorted(root.rglob(args.pattern))
        if not in_paths:
            print(f"⚠️ JSONL не найдены в {root} (pattern={args.pattern})")
            return
    else:
        # PDF-режим
        pattern = args.pattern if args.pattern != "*_pages.jsonl" else "*.pdf"
        in_paths = sorted(root.rglob(pattern))
        if not in_paths:
            print(f"⚠️ PDF не найдены в {root} (pattern={pattern})")
            return

    if args.max_files is not None:
        in_paths = in_paths[: args.max_files]

    print(f"Нашёл {len(in_paths)} файл(ов) под {root}")
    print(f"Результаты будут писаться в: {out_dir}")

    for i, path in enumerate(in_paths, start=1):
        print(f"\n[{i}/{len(in_paths)}] {path}")
        try:
            if args.input_type == "jsonl":
                jsonl_path = path
            else:
                assert jsonl_dir is not None
                jsonl_path = _ensure_jsonl_for_pdf(path, jsonl_dir)
            _process_one(jsonl_path, out_dir, pdf_meta_dir)
        except Exception as e:
            print(f"   ❌ Ошибка на {path}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
