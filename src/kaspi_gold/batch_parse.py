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
    _extract_person_name_from_details,   # you already have this helper there
)

def process_kaspi_pdf(pdf_path: Path, out_dir: Path) -> None:
    """
    Обрабатывает один Kaspi Gold PDF:
      - парсит header / tx / meta
      - считает доход ИП
      - пишет CSV рядом в out_dir
    """
    print(f"[Kaspi] → {pdf_path.name}")

    header_df, tx_df, meta_df = parse_kaspi_statement_v6b(str(pdf_path))

    stem = pdf_path.stem

    out_header         = out_dir / f"{stem}_header.csv"
    out_tx             = out_dir / f"{stem}_tx.csv"
    out_meta           = out_dir / f"{stem}_meta.csv"
    out_tx_ip          = out_dir / f"{stem}_tx_ip.csv"
    out_ip_monthly     = out_dir / f"{stem}_ip_income_monthly.csv"
    out_income_summary = out_dir / f"{stem}_income_summary.csv"
    out_related        = out_dir / f"{stem}_related_parties.csv"

    # --- Header/meta сразу пишем ---
    header_df.to_csv(out_header, index=False, encoding="utf-8-sig")
    meta_df.to_csv(out_meta, index=False, encoding="utf-8-sig")

    # === RELATED PARTIES (Kaspi Gold) ===
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
    print(f"   ✅ Related parties → {out_related} (rows={len(related_parties_df)})")

    # ===== Annotate each transaction with related-party info =====

    # 1) build maps from person_name → share %, exclusion flag
    if not related_parties_df.empty:
        share_map = related_parties_df.set_index("person_name")["outgoing_share_pct"].to_dict()
        excl_map = related_parties_df.set_index("person_name")["exclude_from_income"].to_dict()
    else:
        share_map = {}
        excl_map = {}

    # 2) extract person_name from each tx
    tx_df["kp_person_name"] = tx_df["details"].apply(_extract_person_name_from_details)

    # 3) flag: is this tx related-party at all?
    tx_df["kp_is_related_party"] = tx_df["kp_person_name"].notna()

    # 4) per-tx turnover share (only for related ones, else NaN)
    tx_df["kp_outgoing_share_pct"] = tx_df["kp_person_name"].map(share_map)

    # 5) flag: this tx belongs to a related party that must be excluded from income
    tx_df["kp_exclude_from_income"] = tx_df["kp_person_name"].map(excl_map).fillna(False)

    # 6) convenience flag: valid for income calc
    tx_df["valid_for_ip_income"] = ~tx_df["kp_exclude_from_income"]

    # Теперь сохраняем TX уже с флагами
    tx_df.to_csv(out_tx, index=False, encoding="utf-8-sig")

    # --- подготовка к compute_ip_income ---
    if "КНП" not in tx_df.columns:
        tx_df["КНП"] = ""

    n = len(tx_df)
    op = tx_df["operation"].astype(str) if "operation" in tx_df.columns else pd.Series([""] * n)
    det = tx_df["details"].astype(str) if "details" in tx_df.columns else pd.Series([""] * n)
    tx_df["ip_text"] = (op.fillna("") + " " + det.fillna("")).str.strip()

    # --- считаем доход ИП (общая функция, но с extra_candidate_mask) ---
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

    # dict -> DataFrame
    income_summary_df = pd.DataFrame([income_summary])

    # --- пишем CSV ---
    enriched_tx.to_csv(out_tx_ip, index=False, encoding="utf-8-sig")
    monthly_income.to_csv(out_ip_monthly, index=False, encoding="utf-8-sig")
    income_summary_df.to_csv(out_income_summary, index=False, encoding="utf-8-sig")

    flags = meta_df.loc[0, "flags"] if "flags" in meta_df.columns else None
    score = meta_df.loc[0, "score"] if "score" in meta_df.columns else None

    print(f"   ✅ Header      → {out_header}")
    print(f"   ✅ Tx          → {out_tx} (rows={len(tx_df)})")
    print(f"   ✅ Meta        → {out_meta}")
    print(f"   ✅ Tx+IP       → {out_tx_ip} (rows={len(enriched_tx)})")
    print(f"   ✅ IP monthly  → {out_ip_monthly} (rows={len(monthly_income)})")
    print(f"   ✅ Income summary → {out_income_summary}")
    if score is not None:
        print(f"   Score: {score}")
    if flags is not None:
        print(f"   Flags: {flags or '(no flags)'}")

    adj = income_summary.get("total_income_adjusted")
    if adj is not None:
        print(f"   Adjusted income: {adj:,.2f}\n")
    else:
        print("   Adjusted income: N/A\n")

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Batch-parse Kaspi Gold PDF statements and compute IP income."
    )
    ap.add_argument(
        "input_dir",
        help="Папка, где лежат Kaspi Gold PDF (будет обход *.pdf рекурсивно)",
    )
    ap.add_argument(
        "--out-dir",
        default=None,
        help="Куда писать CSV (по умолчанию: <input_dir>/kaspi_parsed)",
    )
    ap.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Ограничить количество обрабатываемых PDF (0 = без лимита)",
    )

    args = ap.parse_args()
    input_dir = Path(args.input_dir).resolve()
    if not input_dir.is_dir():
        raise SystemExit(f"Not a directory: {input_dir}")

    base_name = input_dir.name
    default_out_dir = input_dir.parent / f"{base_name}_out"
    out_dir = Path(args.out_dir).resolve() if args.out_dir else default_out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    pdf_files = sorted(input_dir.rglob("*.pdf"))
    if not pdf_files:
        raise SystemExit(f"No PDF files found under {input_dir}")

    if args.max_files and args.max_files > 0:
        pdf_files = pdf_files[: args.max_files]

    print(f"Нашёл {len(pdf_files)} PDF-файл(ов) под {input_dir}")
    print(f"Результаты будут писаться в: {out_dir}\n")

    ok, failed = 0, 0
    for i, pdf in enumerate(pdf_files, start=1):
        print(f"[{i}/{len(pdf_files)}] {pdf}")
        try:
            process_kaspi_pdf(pdf, out_dir)
            ok += 1
        except Exception:
            failed += 1
            print(f"   ❌ Ошибка при обработке {pdf.name}:")
            traceback.print_exc()
            print()

    print("==== SUMMARY (Kaspi Gold batch) ====")
    print(f"OK:     {ok}")
    print(f"Failed: {failed}")


if __name__ == "__main__":
    main()
