# src/kaspi_parser/parser.py
from typing import Tuple, Dict, Any, List, Optional
import pandas as pd
import fitz



from src.kaspi_gold.layout import (
    get_header_spans,
    derive_column_limits,
    collect_table_spans,
    detect_icon_bands,
    assign_cols,
    build_row_bands,
    find_clock_rows,
    rebuild_transactions_from_page,
    collect_span_info,
    define_regions, cluster_rows_by_y
)
from src.kaspi_gold.extractors import (
    find_period,
    find_iban,
    find_currency,
    find_cardlast4,
    extract_summary_reported_from_page,
    extract_balances_from_page,
)
from src.kaspi_gold.checks_meta import (
    extract_pdf_meta,
    check_unprotected_statement,
    check_inconsistent_page_size,
    check_odd_page_aspect,
    check_footer_markers_per_page
)
from src.kaspi_gold.checks_visual import (
    check_region_suspicious_fonts,
    check_region_font_size_inconsistency,
)
from src.kaspi_gold.checks_consistency import (
    check_summary_mismatch_simple,
    check_balance_rollforward,
    check_tx_date_sorting,
    check_summary_sign_rules
)
import re
from src.kaspi_gold.utils import RowBand, cluster_rows_by_y, parse_amount, AMOUNT_ROW_REGEX

def _extract_name_card_account(full_text: str) -> dict[str, str]:
    """
    Из полного текста первой страницы вытаскиваем:
      - client_name (строки между 'по Kaspi Gold ...' и блоком с номерами,
                    а также строки сразу после номера карты до 'Номер счета' / 'Доступно на' / 'Валюта счета')
      - card_mask  (Номер карты: *XXXX)
      - account_number (Номер счета: KZ...)
    """
    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]

    client_name_parts: List[str] = []
    card_mask = ""
    account_number = ""

    period_idx: Optional[int] = None
    card_idx: Optional[int] = None

    for i, ln in enumerate(lines):
        if "Kaspi Gold" in ln and period_idx is None:
            period_idx = i
        if ln.startswith("Номер карты:") and card_idx is None:
            card_idx = i

    # 1) Имя между строкой с Kaspi Gold и "Номер карты:"
    if period_idx is not None and card_idx is not None and card_idx > period_idx + 1:
        client_name_parts.extend(lines[period_idx + 1:card_idx])

    # 2) Номер карты + возможное продолжение имени под ним
    if card_idx is not None:
        ln = lines[card_idx]
        m_card = re.search(r"Номер карты:\s*([^\s]+)", ln)
        j = card_idx + 1

        if m_card:
            card_mask = m_card.group(1)
        else:
            # вариант, когда маска на следующей строке
            if j < len(lines) and lines[j].startswith("*"):
                card_mask = lines[j].split()[0]
                j += 1

        stop_prefixes = ("Номер счета:", "Доступно на", "Валюта счета:")
        while j < len(lines):
            ln2 = lines[j]
            if ln2.startswith(stop_prefixes):
                break
            client_name_parts.append(ln2)
            j += 1

    # 3) Номер счёта
    for i, ln in enumerate(lines):
        if ln.startswith("Номер счета:"):
            tail = ln.split(":", 1)[1].strip() if ":" in ln else ""
            if tail:
                account_number = tail.split()[0]
            elif i + 1 < len(lines):
                account_number = lines[i + 1].split()[0]
            break

    # Склеиваем части имени, убирая дубли
    client_name_parts_clean: List[str] = []
    for part in client_name_parts:
        if part not in client_name_parts_clean:
            client_name_parts_clean.append(part)
    client_name = " ".join(client_name_parts_clean).strip()

    # fallback: если вообще ничего не нашли
    if not client_name and period_idx is not None and period_idx + 1 < len(lines):
        client_name = lines[period_idx + 1]

    # fallback по regex, если не нашли карту / счёт
    if not card_mask:
        m_card = re.search(r"Номер карты:\s*([^\s]+)", full_text)
        if m_card:
            card_mask = m_card.group(1)
    if not account_number:
        m_acc = re.search(r"Номер счета:\s*([A-Z0-9]+)", full_text)
        if m_acc:
            account_number = m_acc.group(1)

    return {
        "client_name": client_name,
        "card_mask": card_mask,
        "account_number": account_number,
    }

def _extract_available_and_currency(
    full_text: str,
    spans_df: pd.DataFrame,
    period_end: Optional[str] = None,
) -> tuple[str, float | None, str]:

    available_date = ""
    available_balance: float | None = None
    account_currency_name = ""

    # -------------------- STEP 1: DATE --------------------
    # Try clean text first
    m = re.search(r"Доступно\s*на\s*(\d{2}\.\d{2}\.\d{2})", full_text)
    if m:
        available_date = m.group(1)

    # Fallback: sometimes text is broken, so check spans too
    if not available_date and not spans_df.empty:
        for _, row in spans_df.iterrows():
            txt = str(row.get("text", ""))
            if "Доступно" in txt:
                m = re.search(r"(\d{2}\.\d{2}\.\d{2})", txt)
                if m:
                    available_date = m.group(1)
                    break

    # -------------------- STEP 2: AMOUNT (NEW: from header text) --------------------
    amt_pattern = re.compile(
        r"Доступно\s*на\s*(\d{2}\.\d{2}\.\d{2}):\s*([+\-−]?\s*\d[\d \u00A0\u202F]*(?:[.,]\d{2}))",
        flags=re.S,
    )
    m_amt = amt_pattern.search(full_text)
    if m_amt:
        # if date wasn't found above, use the one from this match
        if not available_date:
            available_date = m_amt.group(1)

        raw_amt = m_amt.group(2)
        s = raw_amt.replace("\xa0", " ").replace("\u202f", " ")
        m_num = re.search(r"[+\-−]?\s*\d[\d ]*(?:[.,]\d{2})", s)
        if m_num:
            num = m_num.group(0).replace("−", "-")
            num = re.sub(r"\s+", "", num).replace(",", ".")
            try:
                available_balance = float(num)
            except ValueError:
                pass

    candidate_amounts: list[float] = []

    # -------------------- STEP 3: old span-based search (fallback) --------------------
    if available_balance is None and not spans_df.empty:
        access_rows = spans_df[
            spans_df["text"].astype(str).str.contains("Доступно", case=False, na=False)
        ]

        for _, r in access_rows.iterrows():
            y = r["y0"]

            zone = spans_df[
                (spans_df["y0"] > y - 40) &
                (spans_df["y0"] < y + 120)
            ]

            for _, z in zone.iterrows():
                txt = str(z["text"])
                try:
                    val, _ = parse_amount(txt)
                    candidate_amounts.append(val)
                except Exception:
                    pass

    # -------------------- STEP 4: harsh text fallback --------------------
    if available_balance is None and not candidate_amounts:
        lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]
        for i, ln in enumerate(lines):
            if "Доступно на" in ln:
                for j in range(0, 6):
                    if i + j < len(lines):
                        try:
                            val, _ = parse_amount(lines[i + j])
                            candidate_amounts.append(val)
                        except Exception:
                            pass

    # -------------------- STEP 5: choose value --------------------
    if available_balance is None and candidate_amounts:
        # take the largest → almost always correct for Kaspi Gold
        available_balance = max(candidate_amounts)

    # -------------------- STEP 6: currency --------------------
    m_curr = re.search(r"Валюта\s+счета\s*:\s*([^\n]+)", full_text)
    if m_curr:
        account_currency_name = m_curr.group(1).strip()

    return available_date, available_balance, account_currency_name


def parse_kaspi_statement_v6b(pdf_path: str,
                              max_pages: int = 0
                              ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    doc = fitz.open(pdf_path)

    debug_info: Dict[str, Any] = {}

    # layout spans / regions for visual checks
    span_df = collect_span_info(doc)
    if len(doc) > 0:
        regions = define_regions(doc[0])
    else:
        regions = {"header": (0, 1e9), "summary": (0, 1e9), "tx_table": (0, 1e9)}

    # metadata (initial flags + score)
    meta_info = extract_pdf_meta(doc)
    meta_df = pd.DataFrame([meta_info])

    header = {
        "period_start": "",
        "period_end": "",
        "iban": "",
        "currency": "",
        "card_last4": "",
        # NEW:
        "client_name": "",
        "card_mask": "",
        "account_number": "",
        "available_date": "",
        "available_balance": None,
        "account_currency_name": "",
    }


    tx_rows: List[Dict] = []
    summary_reported: Dict[str, float] = {}
    opening_balance: Optional[float] = None
    closing_balance: Optional[float] = None

    pages = range(len(doc)) if max_pages in (0, None) else range(min(max_pages, len(doc)))
    carry_splits: Optional[List[float]] = None

    for p in pages:
        page = doc[p]

        if p == 0:
            full_text = page.get_text("text")
            header["period_start"], header["period_end"] = find_period(full_text)
            header["iban"] = find_iban(full_text)
            header["currency"] = find_currency(full_text)
            header["card_last4"] = find_cardlast4(full_text)

            # NEW: name / card / account
            name_card_acc = _extract_name_card_account(full_text)
            header["client_name"] = name_card_acc["client_name"]
            header["card_mask"] = name_card_acc["card_mask"]
            header["account_number"] = name_card_acc["account_number"]

            # NEW: available balance + currency name (чисто по тексту)
            avail_date, avail_balance, acc_curr_name = _extract_available_and_currency(
                full_text,
                span_df,
                period_end=header.get("period_end")
            )

            header["available_date"] = avail_date
            header["available_balance"] = avail_balance
            header["account_currency_name"] = acc_curr_name

            summary_reported = extract_summary_reported_from_page(page)

            balances = extract_balances_from_page(page)
            opening_balance = balances.get("opening_balance")
            closing_balance = balances.get("closing_balance")

        # derive splits
        hdr_rects = get_header_spans(page)
        if hdr_rects:
            splits = derive_column_limits(page, hdr_rects)
            carry_splits = splits
        else:
            W = float(page.rect.width)
            splits = carry_splits or [W * 0.25, W * 0.45, W * 0.65, 1e6]

        icon_bands = detect_icon_bands(page)
        table_spans = collect_table_spans(page)
        df = pd.DataFrame(table_spans).sort_values(["y0", "x0"]).reset_index(drop=True)
        df["row_id"] = cluster_rows_by_y(df["y0"].values, tol=3.0)
        df = assign_cols(df, splits)
        row_bands = build_row_bands(df)
        clock_rows = find_clock_rows(row_bands, icon_bands)
        rows_from_page = rebuild_transactions_from_page(df, p, clock_rows)
        tx_rows.extend(rows_from_page)

    header_df = pd.DataFrame([header])
    tx_df = pd.DataFrame(tx_rows)
    if not tx_df.empty:
        tx_df["operation"] = tx_df["operation"].fillna("")
        tx_df["details"] = tx_df["details"].fillna("")
        tx_df["amount_text"] = tx_df["amount_text"].fillna("")
        tx_df["amount"] = tx_df["amount"].fillna(0.0)

    # -------- CHECKS --------

    # 1. summary mismatch
    summary_flags = []
    summary_diffs = {}
    summary_sign_flags = []
    summary_sign_debug = {}

    if summary_reported:
        # mismatch between header "Покупки/..." and actual tx rollups
        summary_flags, summary_diffs = check_summary_mismatch_simple(
            summary_reported,
            tx_df,
            tol=1.0
        )

        # NEW: sign rules check for Пополнения / Покупки / Снятия
        summary_sign_flags, summary_sign_debug = check_summary_sign_rules(
            summary_reported,
            tx_df
        )

        debug_info["summary_reported"] = summary_reported
        debug_info["summary_diffs"] = summary_diffs
        debug_info["summary_sign_debug"] = summary_sign_debug
    # 2. rollforward math
    roll_flag = check_balance_rollforward(opening_balance, closing_balance, tx_df, tol=1.0)

    # 3. structural / visual checks
    extra_flags = []

    f = check_unprotected_statement(doc)
    if f:
        extra_flags.append(f)
        debug_info[f] = {"is_encrypted": doc.is_encrypted}

    f = check_inconsistent_page_size(doc)
    if f:
        extra_flags.append(f)
        # capture unique sizes
        sizes = []
        for pg in doc:
            sizes.append((round(pg.rect.width), round(pg.rect.height)))
        debug_info[f] = {"page_sizes": list(dict.fromkeys(sizes))}

    odd_flag, odd_debug = check_odd_page_aspect(doc)
    if odd_flag:
        extra_flags.append(odd_flag)
        debug_info[odd_flag] = {"weird_pages": odd_debug}

    footer_flag, footer_debug = check_footer_markers_per_page(doc)
    if footer_flag:
        extra_flags.append(footer_flag)
        debug_info[footer_flag] = footer_debug

    region_font_flags, region_font_debug = check_region_suspicious_fonts(span_df, regions)
    extra_flags.extend(region_font_flags)
    debug_info.update(region_font_debug)

    region_size_flags, region_size_debug = check_region_font_size_inconsistency(span_df, regions)
    extra_flags.extend(region_size_flags)
    debug_info.update(region_size_debug)

    sort_flag = check_tx_date_sorting(
        tx_df,
        header.get("period_start", ""),
        header.get("period_end", ""),
    )
    if sort_flag:
        extra_flags.append(sort_flag)

    # include rollforward mismatch if any
    if roll_flag:
        extra_flags.append(roll_flag)

    if roll_flag:
        extra_flags.append(roll_flag)

    # -------- MERGE FLAGS / SCORE --------
    base_flags = meta_df.loc[0, "flags"]
    if isinstance(base_flags, list):
        existing_flags_list = base_flags
    elif isinstance(base_flags, str) and base_flags.strip():
        existing_flags_list = base_flags.split(";")
    else:
        existing_flags_list = []

    all_flags = (
            list(existing_flags_list)
            + summary_flags  # from mismatch totals
            + summary_sign_flags  # <-- NEW
            + extra_flags
    )
    deduped_flags = list(dict.fromkeys(all_flags))

    meta_df.loc[0, "flags"] = ";".join(deduped_flags)
    meta_df.loc[0, "score"] = 100 - 5 * len(deduped_flags)


    meta_df.loc[0, "debug_info"] = str(debug_info)
    meta_df.loc[0, "summary_reported"] = str(summary_reported)
    meta_df.loc[0, "summary_diffs"] = str(summary_diffs)
    meta_df.loc[0, "opening_balance"] = opening_balance if opening_balance is not None else ""
    meta_df.loc[0, "closing_balance"] = closing_balance if closing_balance is not None else ""
    meta_df.loc[0, "rollforward_sum_tx"] = float(tx_df["amount"].sum()) if not tx_df.empty else 0.0
    meta_df[["flags", "score", "debug_info"]].to_string(index=False)

    return header_df, tx_df, meta_df
