import re
import pandas as pd
import camelot
import numpy as np

DATE_RE = re.compile(r"\d{2}\.\d{2}\.\d{4}")

def extract_entity_info(text: str) -> dict:
    info = {
        "company_name": None,
        "BIN": None,
        "IIK": None,
        "BIK": None,
    }
    if not isinstance(text, str):
        return info

    # very rough name grab
    name_m = re.search(
        r"(?:ИП|ТОО|АО|Индивидуальный предприниматель|ООО|Private Company|Частная компания)?\s*"
        r"([A-Za-zА-Яа-яЁё0-9\"'\.\s]+)",
        text,
    )
    if name_m:
        info["company_name"] = name_m.group(0).strip()

    bin_match = re.search(r"БИН[:\s]*([0-9]{8,12})", text)
    iik_match = re.search(r"ИИК[:\s]*(KZ[0-9A-Z]{16,20})", text)
    bik_match = re.search(r"БИК[:\s]*([A-Z0-9]{8,11})", text)

    if bin_match:
        info["BIN"] = bin_match.group(1)
    if iik_match:
        info["IIK"] = iik_match.group(1)
    if bik_match:
        info["BIK"] = bik_match.group(1)

    return info


def clean_whitespace(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return re.sub(r"\s+", " ", s.strip())


def _is_rate_like(val: str) -> bool:
    """Return True if val looks like numeric rate (1, 1.00, 450,25 etc.)."""
    if not isinstance(val, str):
        return False
    s = val.strip()
    if not s:
        return False
    if not re.fullmatch(r"[-+]?\d[\d\s.,]*", s):
        return False
    t = s.replace(" ", "").replace("\xa0", "").replace("\u202f", "")
    if "," in t and "." not in t:
        t = t.replace(",", ".")
    try:
        float(t)
        return True
    except ValueError:
        return False

def fix_forte_tx(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # ---------- 1. Убираем футер и строки без даты ----------
    if "Күні/Дата" in df.columns:
        date_str = df["Күні/Дата"].astype(str)

        # реальная дата
        is_date = date_str.str.contains(DATE_RE, na=False)

        # строка "Айналымдар / Обороты" и подобные
        is_footer_label = date_str.str.contains("Айналымдар / Обороты", na=False)

        df = df[is_date & ~is_footer_label].copy()

    # ---------- 2. Чиним «Курс» и «Назначение платежа» ----------
    kurs_col = "Бағам/Курс"
    purpose_col = "Назначение платежа"

    if kurs_col in df.columns and purpose_col in df.columns:
        kurs = df[kurs_col].astype(str)

        # если в "Курс" есть буквы → это на самом деле кусок текста
        is_text = kurs.str.contains(r"[A-Za-zА-Яа-яЁё#@]", regex=True, na=False)

        # приклеиваем этот кусок к Назначению платежа
        df.loc[is_text, purpose_col] = (
            df.loc[is_text, purpose_col].fillna("").astype(str).str.rstrip()
            + " "
            + kurs[is_text].str.lstrip()
        ).str.strip()

        # очищаем колонку курса в таких строках
        df.loc[is_text, kurs_col] = np.nan

        # ---------- 3. Там, где есть деньги, но курса нет — ставим 1.0 ----------
        money_mask = (
            df.get("Дебет").notna().fillna(False) |
            df.get("Кредит").notna().fillna(False)
        )

        kurs_num = pd.to_numeric(
            df[kurs_col].astype(str).str.replace(",", "."),
            errors="coerce",
        )
        missing_kurs = money_mask & kurs_num.isna()

        df.loc[missing_kurs, kurs_col] = 1.0

    return df.reset_index(drop=True)

def parse_fortebank_pdf(pdf_path: str) -> pd.DataFrame:
    # 1. Camelot
    tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")
    if len(tables) == 0:
        tables = camelot.read_pdf(pdf_path, pages="all", flavor="stream", edge_tol=150)

    if not tables:
        # your choice: raise or return empty df
        return pd.DataFrame()

    df = pd.concat([t.df for t in tables], ignore_index=True)

    # header row -> column names
    df.columns = [clean_whitespace(c) for c in df.iloc[0]]
    df = df.drop(index=0).reset_index(drop=True)

    # column mapping
    colmap = {}
    for c in df.columns:
        c_l = c.lower()
        if "№" in c_l or "no" in c_l:
            colmap[c] = "№"
        elif "күні" in c_l or "дата" in c_l:
            colmap[c] = "Күні/Дата"
        elif "құжат" in c_l or "документ" in c_l:
            colmap[c] = "Құжат Нөмірі/Номер документа"
        elif "жіберуш" in c_l or "отправ" in c_l:
            colmap[c] = "Жіберуші/Отправитель"
        elif "алушы" in c_l or "получ" in c_l:
            colmap[c] = "Алушы/Получатель"
        elif "дебет" in c_l:
            colmap[c] = "Дебет"
        elif "кредит" in c_l:
            colmap[c] = "Кредит"
        elif "тағай" in c_l or "назнач" in c_l:
            colmap[c] = "Назначение платежа"
        elif "курс" in c_l or "бағам" in c_l:
            colmap[c] = "Бағам/Курс"

    df = df.rename(columns=colmap)

    keep_cols = list(dict.fromkeys(colmap.values()))  # preserve order, dedupe
    df = df[keep_cols]

    # clean whitespace
    df = df.applymap(clean_whitespace)
    mask_tx = df["Күні/Дата"].astype(str).str.match(DATE_RE)
    df = df[mask_tx].reset_index(drop=True)
    # 2. sender / receiver structured fields
    if "Жіберуші/Отправитель" in df.columns:
        senders_series = df["Жіберуші/Отправитель"]
    else:
        # создаём пустую/NaN-серию нужной длины
        senders_series = pd.Series([None] * len(df))

    if "Алушы/Получатель" in df.columns:
        receivers_series = df["Алушы/Получатель"]
    else:
        receivers_series = pd.Series([None] * len(df))

    senders = senders_series.apply(extract_entity_info)
    receivers = receivers_series.apply(extract_entity_info)

    senders_df = pd.DataFrame(list(senders)).add_prefix("sender_")
    receivers_df = pd.DataFrame(list(receivers)).add_prefix("receiver_")


    df_final = pd.concat([df, senders_df, receivers_df], axis=1)

    # # 3. Merge multiline doc numbers and purposes
    # if "Құжат Нөмірі/Номер документа" in df_final.columns:
    #     df_final["Құжат Нөмірі/Номер документа"] = (
    #         df_final["Құжат Нөмірі/Номер документа"]
    #         .replace("", pd.NA)
    #         .ffill()
    #     )
    # if "Назначение платежа" in df_final.columns:
    #     df_final["Назначение платежа"] = (
    #         df_final["Назначение платежа"]
    #         .replace("", pd.NA)
    #         .ffill()
    #     )
    #
    # # 4. FIX: move text that leaked into Бағам/Курс back to Назначение платежа
    # if "Бағам/Курс" in df_final.columns and "Назначение платежа" in df_final.columns:
    #     kurs = df_final["Бағам/Курс"].astype(str)
    #     mask_bad = kurs.str.strip().ne("") & ~kurs.apply(_is_rate_like)
    #
    #     # append to purpose
    #     df_final.loc[mask_bad, "Назначение платежа"] = (
    #         df_final.loc[mask_bad, "Назначение платежа"].fillna("").str.rstrip()
    #         + " "
    #         + df_final.loc[mask_bad, "Бағам/Курс"].fillna("").str.lstrip()
    #     ).str.strip()
    #
    #     # clear fake rate
    #     df_final.loc[mask_bad, "Бағам/Курс"] = ""
    df_final = fix_forte_tx(df_final)

    return df_final

