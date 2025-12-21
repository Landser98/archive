#!/usr/bin/env python3
# src/kaspi_gold/batch_parse.py
import argparse
from pathlib import Path
import traceback

import pandas as pd

from src.kaspi_gold.parser import parse_kaspi_statement_v6b
from src.utils.income_calc import compute_ip_income
from src.utils.kaspi_gold_related_parties import (
    summarize_kaspi_gold_persons,
    _extract_person_name_from_details,
)
from src.ui.ui_analysis_report_generator import get_ui_analysis_tables


def process_kaspi_pdf(pdf_path: Path, out_dir: Path) -> None:
    """
    Обрабатывает один Kaspi Gold PDF:
      - парсит header / tx / meta
      - считает доход ИП
      - Генерирует UI таблицы (Топ-9 и Related Parties)
      - пишет CSV файлы в out_dir
    """
    print(f"[Kaspi] → {pdf_path.name}")

    header_df, tx_df, meta_df = parse_kaspi_statement_v6b(str(pdf_path))

    stem = pdf_path.stem

    out_header = out_dir / f"{stem}_header.csv"
    out_tx = out_dir / f"{stem}_tx.csv"
    out_meta = out_dir / f"{stem}_meta.csv"
    out_tx_ip = out_dir / f"{stem}_tx_ip.csv"
    out_ip_monthly = out_dir / f"{stem}_ip_income_monthly.csv"
    out_income_summary = out_dir / f"{stem}_income_summary.csv"
    out_related = out_dir / f"{stem}_related_parties.csv"

    # Новые файлы для UI таблиц
    out_debit_top_9 = out_dir / f"{stem}_ui_debit_top_9.csv"
    out_credit_top_9 = out_dir / f"{stem}_ui_credit_top_9.csv"
    out_related_ui = out_dir / f"{stem}_ui_related_parties_net.csv"

    # --- Header/meta сразу пишем ---
    header_df.to_csv(out_header, index=False, encoding="utf-8-sig")
    meta_df.to_csv(out_meta, index=False, encoding="utf-8-sig")

    # === RELATED PARTIES (Legacy) ===
    tx_df = tx_df.copy()
    if "txn_date" not in tx_df.columns:
        tx_df["txn_date"] = pd.to_datetime(
            tx_df["date"],
            format="%d.%m.%y",
            errors="coerce",
        )

    related_parties_df = summarize_kaspi_gold_persons(
        tx_df,
        details_col="details",
        amount_col="amount",
        date_col="txn_date",
        fallback_date_col="date",
        fallback_date_format="%d.%m.%y",
    )
    related_parties_df.to_csv(out_related, index=False, encoding="utf-8-sig")

    # === UI ANALYSIS TABLES (Топ-9 + Related Parties Net) ===
    # Подготовка колонок для генератора
    ui_input_df = tx_df.copy()
    if 'counterparty_id' not in ui_input_df.columns:
        ui_input_df['counterparty_id'] = ui_input_df['details'].fillna('N/A')
    if 'counterparty_name' not in ui_input_df.columns:
        ui_input_df['counterparty_name'] = ui_input_df['details'].fillna('N/A')

    ui_tables = get_ui_analysis_tables(ui_input_df)

    pd.DataFrame(ui_tables["debit_top"]).to_csv(out_debit_top_9, index=False, encoding="utf-8-sig")
    pd.DataFrame(ui_tables["credit_top"]).to_csv(out_credit_top_9, index=False, encoding="utf-8-sig")
    pd.DataFrame(ui_tables["related_parties"]).to_csv(out_related_ui, index=False, encoding="utf-8-sig")

    # ===== Annotate each transaction with related-party info =====
    if not related_parties_df.empty:
        share_map = related_parties_df.set_index("person_name")["outgoing_share_pct"].to_dict()
        excl_map = related_parties_df.set_index("person_name")["exclude_from_income"].to_dict()
    else:
        share_map, excl_map = {}, {}

    tx_df["kp_person_name"] = tx_df["details"].apply(_extract_person_name_from_details)
    tx_df["kp_is_related_party"] = tx_df["kp_person_name"].notna()
    tx_df["kp_outgoing_share_pct"] = tx_df["kp_person_name"].map(share_map)
    tx_df["kp_exclude_from_income"] = tx_df["kp_person_name"].map(excl_map).fillna(False)
    tx_df["valid_for_ip_income"] = ~tx_df["kp_exclude_from_income"]

    tx_df.to_csv(out_tx, index=False, encoding="utf-8-sig")

    # --- IP Income Calculation ---
    if "КНП" not in tx_df.columns:
        tx_df["КНП"] = ""

    tx_df["ip_text"] = (tx_df["operation"].fillna("") + " " + tx_df["details"].fillna("")).str.strip()

    enriched_tx, monthly_income, income_summary = compute_ip_income(
        tx_df,
        col_op_date="date",
        col_credit="amount",
        col_knp="КНП",
        col_purpose="ip_text",
        col_counterparty="ip_text",
        months_back=12,
        op_date_pattern=r"(\d{2}\.\d{2}\.\d{2})",
        op_date_format="%d.%m.%y",
        verbose=True,
        max_examples=5,
        extra_candidate_mask=tx_df["valid_for_ip_income"],
    )

    pd.DataFrame([income_summary]).to_csv(out_income_summary, index=False, encoding="utf-8-sig")
    enriched_tx.to_csv(out_tx_ip, index=False, encoding="utf-8-sig")
    monthly_income.to_csv(out_ip_monthly, index=False, encoding="utf-8-sig")

    print(f"   ✅ UI Tables   → Top-9 & Related Net CSVs generated")
    print(f"   ✅ Adjusted income: {income_summary.get('total_income_adjusted', 0):,.2f}\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch-parse Kaspi Gold PDF statements.")
    ap.add_argument("input_dir", help="Папка с PDF файлами")
    ap.add_argument("--out-dir", default=None, help="Куда писать результаты")
    ap.add_argument("--max-files", type=int, default=0, help="Лимит файлов")

    args = ap.parse_args()
    input_dir = Path(args.input_dir).resolve()
    if not input_dir.is_dir(): raise SystemExit(f"Not a directory: {input_dir}")

    out_dir = Path(args.out_dir).resolve() if args.out_dir else input_dir.parent / f"{input_dir.name}_out"
    out_dir.mkdir(parents=True, exist_ok=True)

    pdf_files = sorted(input_dir.rglob("*.pdf"))
    if args.max_files > 0: pdf_files = pdf_files[:args.max_files]

    print(f"Найдено {len(pdf_files)} PDF в {input_dir}\n")

    ok, failed = 0, 0
    for i, pdf in enumerate(pdf_files, start=1):
        try:
            process_kaspi_pdf(pdf, out_dir)
            ok += 1
        except Exception:
            failed += 1
            traceback.print_exc()

    print(f"\n==== SUMMARY: OK: {ok}, Failed: {failed} ====")


if __name__ == "__main__":
    main()