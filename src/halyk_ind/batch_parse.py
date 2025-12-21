#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import traceback
from pathlib import Path
import pandas as pd
import tempfile

from src.halyk_ind.parser import parse_halyk_b_statement
from src.ui.ui_analysis_report_generator import get_ui_analysis_tables
from src.utils.convert_pdf_json_pages import dump_pdf_pages


def process_halyk_individual_pdf(pdf_path: Path, out_dir: Path) -> None:
    """
    Обрабатывает один PDF Halyk Individual:
      - Конвертирует PDF в JSONL
      - Парсит данные (header, tx, footer)
      - Очищает описания операций (извлекает чистые имена ИП)
      - Генерирует 3 файла аналитики: Топ-9 Дебет, Топ-9 Кредит, Related Parties
    """
    print(f"[Halyk Ind] → {pdf_path.name}")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / f"{pdf_path.stem}.jsonl"
            # Генерируем JSONL структуру страниц
            dump_pdf_pages(pdf_path=pdf_path, out_path=jsonl_path)

            # Парсим
            header_df, tx_df, footer_data = parse_halyk_b_statement(
                str(jsonl_path),
                pdf_path=str(pdf_path)
            )
    except Exception as e:
        print(f"   ❌ Ошибка парсинга {pdf_path.name}: {e}")
        return

    if tx_df is None or tx_df.empty:
        print(f"   ⚠️ Транзакции не найдены в {pdf_path.name}")
        return

    stem = pdf_path.stem
    df_analysis = tx_df.copy()

    # --- НОРМАЛИЗАЦИЯ ДАННЫХ ДЛЯ ГЕНЕРАЦИИ CSV ---

    # 1. Поиск колонки с описанием
    desc_candidates = ['Описание операции', 'details', 'Назначение платежа', 'operation']
    actual_desc_col = next((c for c in desc_candidates if c in df_analysis.columns), None)

    # 2. Обработка суммы (создаем amount)
    if 'amount' not in df_analysis.columns:
        if 'Сумма операции' in df_analysis.columns:
            df_analysis['amount'] = df_analysis['Сумма операции']
        elif 'Сумма в KZT' in df_analysis.columns:
            df_analysis['amount'] = df_analysis['Сумма в KZT']
        elif 'Доход' in df_analysis.columns and 'Расход' in df_analysis.columns:
            df_analysis['amount'] = df_analysis['Доход'].fillna(0) - df_analysis['Расход'].fillna(0).abs()

    # 3. Очистка имен контрагентов согласно требованию
    if actual_desc_col:
        def extract_halyk_name(text):
            if not isinstance(text, str): return "N/A"
            text = text.strip()
            prefixes = [
                "Операция оплаты у коммерсанта ",
                "Поступление перевода ",
                "Перевод на другую карту "
            ]
            for p in prefixes:
                if text.startswith(p):
                    return text[len(p):].strip()
            return text

        df_analysis['counterparty_name'] = df_analysis[actual_desc_col].apply(extract_halyk_name)
        df_analysis['counterparty_id'] = df_analysis['counterparty_name']
        df_analysis['details'] = df_analysis[actual_desc_col]

    # Приведение к числам
    df_analysis['amount'] = pd.to_numeric(df_analysis['amount'], errors='coerce').fillna(0.0)

    # --- 4. ГЕНЕРАЦИЯ ФАЙЛОВ АНАЛИТИКИ ---
    try:
        analysis_results = get_ui_analysis_tables(df_analysis)

        out_debit = out_dir / f"{stem}_ui_debit_top_9.csv"
        out_credit = out_dir / f"{stem}_ui_credit_top_9.csv"
        out_related = out_dir / f"{stem}_ui_related_parties_net.csv"

        pd.DataFrame(analysis_results["debit_top"]).to_csv(out_debit, index=False, encoding="utf-8-sig")
        pd.DataFrame(analysis_results["credit_top"]).to_csv(out_credit, index=False, encoding="utf-8-sig")
        pd.DataFrame(analysis_results["related_parties"]).to_csv(out_related, index=False, encoding="utf-8-sig")

        print(f"   ✅ Файлы аналитики сгенерированы")
    except Exception as e:
        print(f"   ⚠️ Ошибка генерации файлов аналитики: {e}")

    # 5. Сохранение базовых таблиц
    if header_df is not None and not header_df.empty:
        header_df.to_csv(out_dir / f"{stem}_header.csv", index=False, encoding="utf-8-sig")

    tx_df.to_csv(out_dir / f"{stem}_tx.csv", index=False, encoding="utf-8-sig")

    # Исправлено: footer_data может быть списком или DataFrame
    if footer_data is not None:
        if isinstance(footer_data, list) and len(footer_data) > 0:
            pd.DataFrame(footer_data).to_csv(out_dir / f"{stem}_footer.csv", index=False, encoding="utf-8-sig")
        elif isinstance(footer_data, pd.DataFrame) and not footer_data.empty:
            footer_data.to_csv(out_dir / f"{stem}_footer.csv", index=False, encoding="utf-8-sig")


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch parse Halyk Individual statements.")
    ap.add_argument("input_path", help="Path to folder or PDF file")
    ap.add_argument("--out-dir", help="Output directory")

    args = ap.parse_args()
    in_path = Path(args.input_path).resolve()

    if in_path.is_file():
        files = [in_path]
        base_out = args.out_dir if args.out_dir else in_path.parent / "halyk_parsed"
    else:
        if not in_path.is_dir():
            raise SystemExit(f"Error: path {in_path} not found.")
        files = sorted(in_path.glob("*.pdf"))
        base_out = args.out_dir if args.out_dir else in_path / "out"

    out_dir = Path(base_out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(files)} files. Output directory: {out_dir}")

    for f in files:
        try:
            process_halyk_individual_pdf(f, out_dir)
        except Exception:
            print(f"❌ Critical failure on {f.name}:")
            traceback.print_exc()


if __name__ == "__main__":
    main()