# src/kaspi_gold/main.py
import argparse
from pathlib import Path

import pandas as pd

from src.kaspi_gold.parser import parse_kaspi_statement_v6b
from src.utils.income_calc import compute_ip_income

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse Kaspi Gold PDF statement and validate it."
    )
    parser.add_argument("pdf", help="Path to Kaspi Gold statement PDF")
    parser.add_argument(
        "--out-header",
        default="kaspi_header.csv",
        help="Output CSV for header (default: kaspi_header.csv)",
    )
    parser.add_argument(
        "--out-tx",
        default="kaspi_transactions.csv",
        help="Output CSV for transactions (default: kaspi_transactions.csv)",
    )
    parser.add_argument(
        "--out-meta",
        default="kaspi_meta.csv",
        help="Output CSV for meta/validation (default: kaspi_meta.csv)",
    )
    parser.add_argument(
        "--out-tx-ip",
        default="kaspi_tx_ip.csv",
        help="Output CSV for transactions with IP flags (default: kaspi_tx_ip.csv)",
    )
    parser.add_argument(
        "--out-ip-monthly",
        default="kaspi_ip_income_monthly.csv",
        help="Output CSV for monthly IP income (default: kaspi_ip_income_monthly.csv)",
    )

    args = parser.parse_args()

    pdf_path = Path(args.pdf)
    if not pdf_path.exists():
        raise SystemExit(f"PDF not found: {pdf_path}")

    # --- 1. Parse + validate ---
    header_df, tx_df, meta_df = parse_kaspi_statement_v6b(str(pdf_path))

    # --- 2. Базовые CSV ---
    header_df.to_csv(args.out_header, index=False, encoding="utf-8-sig")
    tx_df.to_csv(args.out_tx, index=False, encoding="utf-8-sig")
    meta_df.to_csv(args.out_meta, index=False, encoding="utf-8-sig")

    # --- 3. Расчёт дохода ИП ---
    # КНП в Kaspi Gold нет → делаем пустую колонку
    if "КНП" not in tx_df.columns:
        tx_df["КНП"] = ""

    # склеиваем operation + details в единый текст для поиска ключевых слов
    tx_df["ip_text"] = (
        tx_df.get("operation", "").astype(str).fillna("")
        + " "
        + tx_df.get("details", "").astype(str).fillna("")
    )

    enriched_tx, monthly_income, avg_income = compute_ip_income(
        tx_df,
        col_op_date="date",        # колонка с датой
        col_credit="amount",       # положительные = доход, отрицательные = расход
        col_knp="КНП",
        col_purpose="ip_text",
        col_counterparty="ip_text",
        months_back=12,
        op_date_pattern=r"(\d{2}\.\d{2}\.\d{2})",  # формат 29.09.25
        op_date_format="%d.%m.%y",
        verbose=True,
        max_examples=5,
    )

    enriched_tx.to_csv(args.out_tx_ip, index=False, encoding="utf-8-sig")
    monthly_income.to_csv(args.out_ip_monthly, index=False, encoding="utf-8-sig")

    # --- 4. Show validation result in console ---
    flags = meta_df.loc[0, "flags"]
    score = meta_df.loc[0, "score"]

    print(f"\nParsed: {pdf_path}")
    print(f"Header CSV      -> {args.out_header}")
    print(f"Transactions CSV-> {args.out_tx}")
    print(f"Meta CSV        -> {args.out_meta}")
    print(f"Tx+IP CSV       -> {args.out_tx_ip}")
    print(f"IP monthly CSV  -> {args.out_ip_monthly}")

    print("\n=== VALIDATION ===")
    print("Score:", score)
    print("Flags:", flags or "(no flags)")

    if "rollforward_sum_tx" in meta_df.columns:
        print("Rollforward sum of tx:", meta_df.loc[0, "rollforward_sum_tx"])
    if "opening_balance" in meta_df.columns and "closing_balance" in meta_df.columns:
        print(
            "Opening -> Closing:",
            meta_df.loc[0, "opening_balance"],
            "→",
            meta_df.loc[0, "closing_balance"],
        )

    print("\n=== IP INCOME ===")
    print(f"Business income rows: {int(enriched_tx['ip_is_business_income'].sum())}")
    print(f"Avg monthly IP income: {avg_income:,.2f}")


if __name__ == "__main__":
    main()


# python -m src.kaspi_gold.main_script \
#   "data/kaspi_gold/gold_statement (3).pdf" \
#   --out-header data/kaspi_header.csv \
#   --out-tx     data/kaspi_tx.csv \
#   --out-meta   data/kaspi_meta.csv \
#   --out-tx-ip  data/kaspi_tx_ip.csv \
#   --out-ip-monthly data/kaspi_ip_income_monthly.csv
