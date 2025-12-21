#!/usr/bin/env python3
# src/kaspi_pay/batch_parse.py
import argparse
from pathlib import Path
import traceback
import tempfile
import pandas as pd

from src.kaspi_pay.parser import parse_kaspi_pay_statement
from src.utils.convert_pdf_json_pages import dump_pdf_pages
from src.utils.income_calc import compute_ip_income
from src.ui.ui_analysis_report_generator import get_ui_analysis_tables


def process_kaspi_pay_pdf(pdf_path: Path, out_dir: Path) -> None:
    """
    Обрабатывает один Kaspi Pay PDF:
      - конвертирует PDF в JSONL во временной папке
      - парсит данные (header, tx, footer)
      - нормализует колонки для UI Аналитики
      - генерирует CSV для Топ-9 и Related Parties
      - считает доход ИП
    """
    print(f"[Kaspi Pay] → {pdf_path.name}")

    # 1. Конвертация во временный JSONL и парсинг
    # Мы используем tempfile, чтобы не оставлять за собой мусорные .jsonl файлы
    with tempfile.TemporaryDirectory() as tmpdir:
        jsonl_path = Path(tmpdir) / f"{pdf_path.stem}.jsonl"
        try:
            dump_pdf_pages(pdf_path=pdf_path, out_path=jsonl_path)
            header_df, tx_df, footer_df = parse_kaspi_pay_statement(str(jsonl_path))
        except Exception as e:
            print(f"   ❌ Ошибка при конвертации/парсинге {pdf_path.name}: {e}")
            return

    stem = pdf_path.stem
    out_tx = out_dir / f"{stem}_tx.csv"
    out_header = out_dir / f"{stem}_header.csv"
    out_income_summary = out_dir / f"{stem}_income_summary.csv"

    # Файлы для UI таблиц
    out_debit_top_9 = out_dir / f"{stem}_ui_debit_top_9.csv"
    out_credit_top_9 = out_dir / f"{stem}_ui_credit_top_9.csv"
    out_related_ui = out_dir / f"{stem}_ui_related_parties_net.csv"

    if tx_df.empty:
        print(f"   ⚠️ Транзакции не найдены в {pdf_path.name}")
        return

    # 2. Сохранение базовых данных
    header_df.to_csv(out_header, index=False, encoding="utf-8-sig")
    tx_df.to_csv(out_tx, index=False, encoding="utf-8-sig")

    # 3. НОРМАЛИЗАЦИЯ ДАННЫХ ДЛЯ ГЕНЕРАТОРА ТАБЛИЦ
    ui_df = tx_df.copy()

    # Сводим Кредит и Дебет в одну колонку amount
    if 'Кредит' in ui_df.columns and 'Дебет' in ui_df.columns:
        ui_df['amount'] = pd.to_numeric(ui_df['Кредит'], errors='coerce').fillna(0.0) - \
                          pd.to_numeric(ui_df['Дебет'], errors='coerce').fillna(0.0)

    # Определяем контрагента
    cp_col = "Наименование получателя"
    if cp_col in ui_df.columns:
        ui_df['counterparty_name'] = ui_df[cp_col].fillna('N/A')
        ui_df['counterparty_id'] = ui_df[cp_col].fillna('N/A')

    # Гарантируем наличие колонки details
    if 'details' not in ui_df.columns and 'Назначение платежа' in ui_df.columns:
        ui_df['details'] = ui_df['Назначение платежа']

    # 4. ГЕНЕРАЦИЯ ТАБЛИЦ (Топ-9 + Related Parties)
    try:
        ui_tables = get_ui_analysis_tables(ui_df)
        pd.DataFrame(ui_tables["debit_top"]).to_csv(out_debit_top_9, index=False, encoding="utf-8-sig")
        pd.DataFrame(ui_tables["credit_top"]).to_csv(out_credit_top_9, index=False, encoding="utf-8-sig")
        pd.DataFrame(ui_tables["related_parties"]).to_csv(out_related_ui, index=False, encoding="utf-8-sig")
        print(f"   ✅ UI Tables Generated")
    except Exception as e:
        print(f"   ⚠️ Ошибка генерации UI таблиц: {e}")

    # 5. РАСЧЕТ ДОХОДА ИП
    # Подготовка текста для поиска ключевых слов
    tx_df["ip_text"] = (
            tx_df.get("Назначение платежа", "").astype(str).fillna("") + " " +
            tx_df.get("Наименование получателя", "").astype(str).fillna("")
    ).str.strip()

    try:
        enriched_tx, monthly_income, income_summary = compute_ip_income(
            tx_df,
            col_op_date="Дата операции",
            col_credit="Кредит",
            col_knp="КНП",
            col_purpose="ip_text",
            col_counterparty="ip_text",
            months_back=12,
            op_date_format="%d.%m.%Y",
            verbose=False
        )
        pd.DataFrame([income_summary]).to_csv(out_income_summary, index=False, encoding="utf-8-sig")
        print(f"   ✅ Income calculation saved to {out_income_summary.name}")
    except Exception as e:
        print(f"   ⚠️ Ошибка расчета дохода ИП: {e}")

    print(f"   ✨ Готово\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch-parse Kaspi Pay PDF statements.")
    ap.add_argument("input_dir", help="Папка с PDF файлами Kaspi Pay")
    ap.add_argument("--out-dir", default=None, help="Куда писать результаты")

    args = ap.parse_args()
    input_dir = Path(args.input_dir).resolve()
    if not input_dir.is_dir():
        raise SystemExit(f"Not a directory: {input_dir}")

    out_dir = Path(args.out_dir).resolve() if args.out_dir else input_dir.parent / f"{input_dir.name}_out"
    out_dir.mkdir(parents=True, exist_ok=True)

    pdf_files = sorted(input_dir.rglob("*.pdf"))
    print(f"Найдено {len(pdf_files)} PDF в {input_dir}\n")

    ok, failed = 0, 0
    for pdf in pdf_files:
        try:
            process_kaspi_pay_pdf(pdf, out_dir)
            ok += 1
        except Exception:
            failed += 1
            print(f"❌ Критическая ошибка при обработке {pdf.name}:")
            traceback.print_exc()

    print(f"\n==== SUMMARY Kaspi Pay: OK: {ok}, Failed: {failed} ====")


if __name__ == "__main__":
    main()