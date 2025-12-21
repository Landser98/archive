from __future__ import annotations
import pandas as pd
import math
from datetime import date
from typing import Optional
from .models import Statement

def _first_not_nan(*values):
    for v in values:
        if v is None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None

# Which columns to use for IP-income calc for each bank
IP_INCOME_CONFIG = {
    # ---------- Kaspi ----------
    "Kaspi Gold": dict(
        # tx_df columns: page, date, amount, operation, details, clock_icon, amount_text
        col_op_date="date",
        col_credit="amount",              # положительная сумма = приход
        # KNP нет → используем operation как «псевдо-КНП», просто чтобы колонка была
        col_knp="operation",
        # текст операции
        col_purpose="details",
        # отдельного контрагента нет → берём operation (название операции)
        col_counterparty="operation",
    ),

    "Kaspi Pay": dict(
        # tx_df: Номер документа, Дата операции, Дебет, Кредит, Наименование получателя,
        #        ИИК бенеф/отправителя, БИК банка, КНП, Назначение платежа
        col_op_date="Дата операции",
        col_credit="Кредит",
        col_knp="КНП",
        col_purpose="Назначение платежа",
        col_counterparty="Наименование получателя",
    ),

    # ---------- Halyk ----------
    "Halyk Business": dict(
        # tx_df: Дата, Номер документа, Дебет, Кредит, Контрагент (имя),
        #        Контрагент ИИН/БИН, Детали платежа, КНП
        col_op_date="Дата",
        col_credit="Кредит",
        col_knp="КНП",
        col_purpose="Детали платежа",
        col_counterparty="Контрагент (имя)",
    ),

    "Halyk Individual": dict(
        # tx_df: Дата проведения операции, Дата обработки операции, Описание операции,
        #        Сумма операции, Валюта операции,
        #        Приход в валюте счета, Расход в валюте счета, Комиссия,
        #        № карточки/счета, КНП
        col_op_date="Дата проведения операции",
        col_credit="Приход в валюте счета",
        col_knp="КНП",
        col_purpose="Описание операции",
        # нет явного контрагента → используем описание
        col_counterparty="Описание операции",
    ),

    # ---------- BCC ----------
    "BCC": dict(
        # tx_df: ... Күні / Дата, ... Корреспондент / Корреспондент,
        #        Дебет / Дебет, Кредит / Кредит, ТМК /КНП,
        #        Төлемнің мақсаты / Назначение платежа
        col_op_date="Күні / Дата",
        col_credit="Кредит / Кредит",
        col_knp="ТМК /КНП",
        col_purpose="Төлемнің мақсаты / Назначение платежа",
        col_counterparty="Корреспондент / Корреспондент",
    ),

    # ---------- Alatau City Bank ----------
    "Alatau City Bank": dict(
        # tx_df: Дата операции, Дата отражения по счету, № док,
        #        Дебет, Кредит, ..., КНП, Назначение платежа, Корреспондент, ...
        col_op_date="Дата операции",
        col_credit="Кредит",
        col_knp="КНП",
        col_purpose="Назначение платежа",
        col_counterparty="Корреспондент",
    ),

    # ---------- Eurasian Bank ----------
    "Eurasian Bank": dict(
        # tx_df: Дата проводки, Вид операции, Номер документа клиента,
        #        Наименование Бенефициара/Отправителя, ...,
        #        Назначение платежа, Дебет, Кредит, КНП
        col_op_date="Дата проводки",
        col_credit="Кредит",
        col_knp="КНП",
        col_purpose="Назначение платежа",
        col_counterparty="Наименование Бенефициара/Отправителя",
    ),

    # ---------- ForteBank ----------
    "ForteBank": dict(
        # tx_df: Күні/Дата, ..., Жіберуші/Отправитель, Алушы/Получатель,
        #        Дебет, Кредит, Назначение платежа, ..., КНП
        col_op_date="Күні/Дата",
        col_credit="Кредит",
        col_knp="КНП",
        col_purpose="Назначение платежа",
        col_counterparty="Жіберуші/Отправитель",
    ),

    # ---------- Freedom Bank ----------
    "Freedom Bank": dict(
        # tx_df: Дата, ..., Корреспондент, ..., Дебет, Кредит,
        #        Назначение платежа, КНП
        col_op_date="Дата",
        col_credit="Кредит",
        col_knp="КНП",
        col_purpose="Назначение платежа",
        col_counterparty="Корреспондент",
    ),
}

def compute_ip_income_for_statement(
    stmnt: Statement,
    window_start: date,
    window_end: date,
):
    """
    1) Фильтрует tx_df стейтмента по 12-месячному окну.
    2) Добавляет ip_* флаги через compute_ip_income.
    3) Возвращает (enriched_tx, summary_dict) или (None, None), если банк не поддержан.
    """
    cfg = IP_INCOME_CONFIG.get(stmnt.bank)
    if cfg is None:
        # Для этого банка IP доход пока не настроен
        return None, None

    df = stmnt.tx_df.copy()

    if "txn_date" not in df.columns:
        raise ValueError(
            f"Statement from {stmnt.bank} / {stmnt.pdf_name} "
            f"does not have 'txn_date' column."
        )

    # фильтр по окну
    mask = (
        (df["txn_date"] >= pd.Timestamp(window_start))
        & (df["txn_date"] <= pd.Timestamp(window_end))
    )
    df_win = df.loc[mask].copy()
    if df_win.empty:
        return None, None

    enriched, monthly_income, summary = compute_ip_income(
        df_win,
        col_op_date=cfg["col_op_date"],
        col_credit=cfg["col_credit"],
        col_knp=cfg["col_knp"],
        col_purpose=cfg["col_purpose"],
        col_counterparty=cfg["col_counterparty"],
        months_back=None,                 # окно уже вручную ограничили
        statement_generation_date=None,   # здесь не нужен
        verbose=False,
    )

    # добавим мета-инфо
    enriched["bank"] = stmnt.bank
    enriched["account_number"] = stmnt.account_number
    enriched["source_pdf"] = stmnt.pdf_name

    # summary дополним идентификаторами
    if summary is not None:
        summary = {
            "bank": stmnt.bank,
            "account_number": stmnt.account_number,
            "source_pdf": stmnt.pdf_name,
            **summary,
        }

    return enriched, summary


# ========== Parsing adapter ==========
# This is the ONLY place you need to integrate your existing parsers.

def parse_statement(bank_key: str, pdf_name: str, pdf_bytes: bytes) -> Statement:
    """
    Adapter around your existing parsers.

    Kaspi Gold implementation.
    """
    if bank_key == "kaspi_gold":
        return parse_kaspi_gold_statement(pdf_name, pdf_bytes)
    elif bank_key == "kaspi_pay":
        return parse_kaspi_pay_statement(pdf_name, pdf_bytes)
    elif bank_key == "halyk_business":
        return parse_halyk_business_statement(pdf_name, pdf_bytes)
    elif bank_key == "halyk_individual":
        return parse_halyk_individual_statement(pdf_name, pdf_bytes)
    elif bank_key == "freedom_bank":
        return parse_freedom_bank_statement(pdf_name, pdf_bytes)
    if bank_key == "forte_bank":
        return parse_forte_bank_statement(pdf_name, pdf_bytes)
    if bank_key == "eurasian_bank":
        return parse_eurasian_bank_statement(pdf_name, pdf_bytes)
    if bank_key == "bcc_bank":
        return parse_bcc_bank_statement(pdf_name, pdf_bytes)
    elif bank_key == "alatau_city_bank":                     # ← НОВОЕ
        return parse_alatau_city_bank_statement(pdf_name, pdf_bytes)


    # other banks later
    raise NotImplementedError(f"parse_statement() not implemented for bank_key={bank_key}")

def parse_kaspi_gold_statement(pdf_name: str, pdf_bytes: bytes) -> Statement:
    """
    Kaspi Gold: парсер через parse_kaspi_statement_v6b(path).
    """
    from src.kaspi_gold.parser import parse_kaspi_statement_v6b
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / pdf_name
        tmp_path.write_bytes(pdf_bytes)

        header_df, tx_df, meta_df = parse_kaspi_statement_v6b(str(tmp_path))

    header_row = header_df.iloc[0]

    # --- metadata from header_df ---
    account_holder_name = header_row.get("client_name")

    account_number = header_row.get("account_number")
    card_mask = header_row.get("card_mask")

    # для Kaspi Gold пока используем account_number/card_mask как "iid_bin"
    iin_bin = account_number or card_mask or ""

    period_start_raw = header_row.get("period_start")
    period_end_raw = header_row.get("period_end")

    period_from = (
        pd.to_datetime(period_start_raw, dayfirst=True, errors="coerce").date()
        if period_start_raw
        else None
    )
    period_to = (
        pd.to_datetime(period_end_raw, dayfirst=True, errors="coerce").date()
        if period_end_raw
        else None
    )

    available_date_raw = header_row.get("available_date")
    statement_generation_date = (
        pd.to_datetime(available_date_raw, dayfirst=True, errors="coerce").date()
        if available_date_raw
        else None
    )

    # --- tx_df: txn_date из 'date' (%d.%m.%y) ---
    if "date" not in tx_df.columns:
        raise ValueError(
            "Kaspi Gold tx_df has no 'date' column, "
            "but compute_ip_income expects it. Check parser."
        )

    tx_df = tx_df.copy()
    tx_df["txn_date"] = pd.to_datetime(
        tx_df["date"], format="%d.%m.%y", errors="coerce"
    )



    return Statement(
        bank="Kaspi Gold",
        pdf_name=pdf_name,
        account_holder_name=str(account_holder_name) if account_holder_name is not None else "",
        iin_bin=str(iin_bin),
        account_number=str(account_number) if account_number is not None else None,
        period_from=period_from,
        period_to=period_to,
        statement_generation_date=statement_generation_date,
        tx_df=tx_df,
        header_df=header_df,

    )

def parse_kaspi_gold_statement(pdf_name: str, pdf_bytes: bytes) -> Statement:
    """
    Kaspi Gold: парсер через parse_kaspi_statement_v6b(path).
    """
    from src.kaspi_gold.parser import parse_kaspi_statement_v6b
    import tempfile
    from pathlib import Path
    import re

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / pdf_name
        tmp_path.write_bytes(pdf_bytes)

        header_df, tx_df, meta_df = parse_kaspi_statement_v6b(str(tmp_path))

    header_row = header_df.iloc[0]

    # --- metadata from header_df ---
    account_holder_name = header_row.get("client_name")

    account_number = header_row.get("account_number")
    card_mask = header_row.get("card_mask")

    # для Kaspi Gold пока используем account_number/card_mask как "iin_bin"
    iin_bin = account_number or card_mask or ""

    period_start_raw = header_row.get("period_start")
    period_end_raw = header_row.get("period_end")

    period_from = (
        pd.to_datetime(period_start_raw, dayfirst=True, errors="coerce").date()
        if period_start_raw
        else None
    )
    period_to = (
        pd.to_datetime(period_end_raw, dayfirst=True, errors="coerce").date()
        if period_end_raw
        else None
    )

    available_date_raw = header_row.get("available_date")
    statement_generation_date = (
        pd.to_datetime(available_date_raw, dayfirst=True, errors="coerce").date()
        if available_date_raw
        else None
    )

    # --- tx_df: гарантируем наличие txn_date ---
    tx_df = tx_df.copy()

    # 1) если парсер уже добавил txn_date — просто приводим к datetime
    if "txn_date" in tx_df.columns:
        tx_df["txn_date"] = pd.to_datetime(
            tx_df["txn_date"],
            dayfirst=True,
            errors="coerce",
        )
    else:
        # 2) нормализуем имена колонок (убираем пробелы, неразрывные пробелы, регистр)
        def _norm(col: object) -> str:
            s = "" if col is None else str(col)
            s = s.replace("\xa0", " ").replace("\u202f", " ")
            s = re.sub(r"\s+", " ", s.strip())
            return s.lower()

        normalized_to_real = { _norm(c): c for c in tx_df.columns }

        # наши кандидаты (по нормализованному имени)
        candidates = [
            "date",                      # ожидаемое имя
            "дата операции",
            "дата проведения операции",
            "operation_date",
            "date_posted",
            "date_processed",
        ]

        real_date_col = None
        for key in candidates:
            if key in normalized_to_real:
                real_date_col = normalized_to_real[key]
                break

        if real_date_col is None:
            # если вообще ни одна колонка не подошла — даём честный список колонок
            raise ValueError(
                "Kaspi Gold: cannot find date column. "
                f"Tried normalized names {candidates}, "
                f"actual columns: {list(tx_df.columns)}"
            )

        tx_df["txn_date"] = pd.to_datetime(
            tx_df[real_date_col],
            dayfirst=True,
            errors="coerce",
        )

    return Statement(
        bank="Kaspi Gold",
        pdf_name=pdf_name,
        account_holder_name=str(account_holder_name) if account_holder_name is not None else "",
        iin_bin=str(iin_bin),
        account_number=str(account_number) if account_number is not None else None,
        period_from=period_from,
        period_to=period_to,
        statement_generation_date=statement_generation_date,
        tx_df=tx_df,
        header_df=header_df,
    )

def parse_kaspi_pay_statement(pdf_name: str, pdf_bytes: bytes) -> Statement:
    """
    Kaspi Pay:

    1) Save PDF into a temp file
    2) Create *_pages.jsonl via dump_pdf_pages
    3) Parse header / tx / footer via src.kaspi_pay.parser.parse_kaspi_pay_statement
    4) Extract metadata and build txn_date.
    """
    import tempfile
    from pathlib import Path

    from src.utils.convert_pdf_json_pages import dump_pdf_pages
    from src.kaspi_pay.parser import parse_kaspi_pay_statement as _parse_kaspi_pay_jsonl

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # 1) save PDF
        pdf_path = tmpdir_path / pdf_name
        pdf_path.write_bytes(pdf_bytes)

        # 2) make JSONL of pages (same style as other banks)
        jsonl_path = tmpdir_path / f"{pdf_path.stem}_pages.jsonl"
        dump_pdf_pages(
            pdf_path=pdf_path,
            out_path=jsonl_path,
            # defaults are fine; you can mirror your other calls if needed
        )

        # 3) parse via kaspi_pay.parser
        header_df, tx_df, footer_df = _parse_kaspi_pay_jsonl(str(jsonl_path))

    if header_df.empty:
        raise ValueError("Kaspi Pay: header_df is empty – parser failed.")

    header_row = header_df.iloc[0]

    # ---------- METADATA ----------

    # Name of the client
    account_holder_name = _first_not_nan(
        header_row.get("Наименование клиента"),
        header_row.get("client_name"),
    )

    # IIN/BIN
    iin_bin = _first_not_nan(
        header_row.get("ИИН/БИН"),
        header_row.get("iin_bin"),
    )

    # Account number
    account_number = _first_not_nan(
        header_row.get("Лицевой счет"),
        header_row.get("account_number"),
    )

    def _parse_date(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        d = pd.to_datetime(val, dayfirst=True, errors="coerce")
        return d.date() if pd.notna(d) else None

    period_from = _parse_date(header_row.get("Период (начало)"))
    period_to   = _parse_date(header_row.get("Период (конец)"))

    # As a proxy for "statement generation date" we can use "Дата последнего движения"
    statement_generation_date = _parse_date(
        header_row.get("Дата последнего движения")
    )

    # ---------- TRANSACTIONS ----------

    tx_df = tx_df.copy()

    # minimal set needed for IP income (matches your IP_INCOME_CONFIG["Kaspi Pay"])
    required_cols = [
        "Дата операции",
        "Кредит",
        "КНП",
        "Назначение платежа",
        "Наименование получателя",
    ]
    missing = [c for c in required_cols if c not in tx_df.columns]
    if missing:
        raise ValueError(f"Kaspi Pay: missing required columns in tx_df: {missing}")

    # txn_date used throughout the app as normalized datetime
    tx_df["txn_date"] = pd.to_datetime(
        tx_df["Дата операции"],
        dayfirst=True,
        errors="coerce",
    )

    return Statement(
        bank="Kaspi Pay",
        pdf_name=pdf_name,
        account_holder_name=str(account_holder_name or ""),
        iin_bin=str(iin_bin or ""),
        account_number=str(account_number or "") if account_number is not None else None,
        period_from=period_from,
        period_to=period_to,
        statement_generation_date=statement_generation_date,
        tx_df=tx_df,
        header_df=header_df,
    )

def parse_halyk_business_statement(pdf_name: str, pdf_bytes: bytes) -> Statement:
    """
    Halyk Bank (business, type A):

    1) сохраняем PDF во временный файл
    2) делаем *_pages.jsonl через dump_pdf_pages
    3) парсим JSONL через parse_halyk_statement
    4) вытаскиваем метаданные и создаём колонку txn_date.
    """
    import tempfile
    from pathlib import Path

    from src.utils.convert_pdf_json_pages import dump_pdf_pages
    from src.halyk_business.parser import parse_halyk_statement

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # 1) сохраняем PDF
        pdf_path = tmpdir_path / pdf_name
        pdf_path.write_bytes(pdf_bytes)

        # 2) создаём JSONL (аналог ensure_jsonl_for_pdf из batch_parse)
        jsonl_path = tmpdir_path / f"{pdf_path.stem}_pages.jsonl"
        dump_pdf_pages(
            pdf_path=pdf_path,
            out_path=jsonl_path,
            stream_preview_len=4000,
            include_full_stream=False,
        )

        # 3) парсим из JSONL
        header_df, tx_df, footer_df = parse_halyk_statement(str(jsonl_path))

    if header_df.empty:
        raise ValueError("Halyk business: header_df is empty")
    header_row = header_df.iloc[0]

    # ---------- МЕТАДАННЫЕ ----------

    # Имя клиента / компании
    account_holder_name = (
        header_row.get("Наименование клиента")
        or header_row.get("client_name")
        or header_row.get("account_name")
        or header_row.get("Наименование клиента (полное)")
    )

    # ИИН/БИН
    iin_bin = (
        header_row.get("ИИН/БИН")
        or header_row.get("БИН")
        or header_row.get("ИИН")
        or header_row.get("iin_bin")
    )

    # Номер счёта / IBAN
    account_number = (
        header_row.get("Номер счета")
        or header_row.get("IBAN")
        or header_row.get("iban")
        or header_row.get("account_number")
    )

    # Период
    period_start_raw = (
        header_row.get("period_start")
        or header_row.get("Период (начало)")
        or header_row.get("Период с")
        or header_row.get("period_from")
    )
    period_end_raw = (
        header_row.get("period_end")
        or header_row.get("Период (конец)")
        or header_row.get("Период по")
        or header_row.get("period_to")
    )

    period_from = (
        pd.to_datetime(period_start_raw, dayfirst=True, errors="coerce").date()
        if period_start_raw
        else None
    )
    period_to = (
        pd.to_datetime(period_end_raw, dayfirst=True, errors="coerce").date()
        if period_end_raw
        else None
    )

    # Дата формирования выписки (если где-то есть)
    statement_generation_date_raw = (
        header_row.get("Дата выписки")
        or header_row.get("Дата формирования")
        or header_row.get("statement_generation_date")
    )
    statement_generation_date = (
        pd.to_datetime(statement_generation_date_raw, dayfirst=True, errors="coerce").date()
        if statement_generation_date_raw
        else None
    )

    # ---------- ТРАНЗАКЦИИ ----------

    # В batch_parse для IP-дохода используются:
    #   "Дата", "Дебет", "Кредит", "Детали платежа", "Контрагент (имя)"
    required_cols = [
        "Дата",
        "Дебет",
        "Кредит",
        "Детали платежа",
        "Контрагент (имя)",
    ]
    missing = [c for c in required_cols if c not in tx_df.columns]
    if missing:
        # В UI лучше честно упасть понятным сообщением
        raise ValueError(f"Halyk business: missing columns in tx_df: {missing}")

    tx_df = tx_df.copy()
    tx_df["txn_date"] = pd.to_datetime(
        tx_df["Дата"],
        dayfirst=True,
        errors="coerce",
    )

    return Statement(
        bank="Halyk Business",
        pdf_name=pdf_name,
        account_holder_name=str(account_holder_name) if account_holder_name is not None else "",
        iin_bin=str(iin_bin) if iin_bin is not None else "",
        account_number=str(account_number) if account_number is not None else None,
        period_from=period_from,
        period_to=period_to,
        statement_generation_date=statement_generation_date,
        tx_df=tx_df,
    )

def parse_halyk_individual_statement(pdf_name: str, pdf_bytes: bytes) -> Statement:
    """
    Halyk Bank (individual, type B / halyk_ind):

    1) Сохраняем PDF во временный файл
    2) Делаем *_pages.jsonl через dump_pdf_pages
    3) Парсим JSONL через parse_halyk_b_statement
    4) Вытаскиваем метаданные и создаём колонку txn_date.
    """
    import tempfile
    from pathlib import Path

    from src.utils.convert_pdf_json_pages import dump_pdf_pages
    from src.halyk_ind.parser import parse_halyk_b_statement

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # 1) Сохраняем PDF
        pdf_path = tmpdir_path / pdf_name
        pdf_path.write_bytes(pdf_bytes)

        # 2) Делаем JSONL, как в process_one_pdf
        jsonl_path = tmpdir_path / f"{pdf_path.stem}_pages.jsonl"
        dump_pdf_pages(pdf_path=pdf_path, out_path=jsonl_path)

        # 3) Парсим JSONL + сам PDF (для обогащения описаний)
        header_df, tx_df, footer_df = parse_halyk_b_statement(
            str(jsonl_path),
            pdf_path=str(pdf_path),
        )

    if header_df.empty:
        raise ValueError("Halyk individual: header_df is empty")

    header_row = header_df.iloc[0]

    # ---------- МЕТАДАННЫЕ ----------

    # Имя клиента (ФЛ)
    account_holder_name = (
        header_row.get("ФИО")
        or header_row.get("Наименование клиента")
        or header_row.get("client_name")
    )

    # ИИН
    iin_bin = (
        header_row.get("ИИН")
        or header_row.get("ИИН/БИН")
        or header_row.get("iin_bin")
    )

    # Номер счёта / карты
    account_number = (
        header_row.get("Номер счета")
        or header_row.get("Номер карты")
        or header_row.get("IBAN")
        or header_row.get("iban")
        or header_row.get("account_number")
    )

    # Период выписки
    def _get_date_from_header(keys):
        for k in keys:
            if k in header_row.index:
                val = header_row.get(k)
                if pd.isna(val):
                    continue
                d = pd.to_datetime(val, dayfirst=True, errors="coerce")
                if pd.notna(d):
                    return d.date()
        return None

    period_from = _get_date_from_header(
        ["Период с", "Период (начало)", "period_start", "period_from"]
    )
    period_to = _get_date_from_header(
        ["Период по", "Период (конец)", "period_end", "period_to"]
    )

    statement_generation_date = _get_date_from_header(
        ["Дата выписки", "Дата формирования", "statement_generation_date"]
    )

    # ---------- ТРАНЗАКЦИИ ----------

    tx_df = tx_df.copy()

    # В batch для compute_ip_income используется "Дата проведения операции"
    if "Дата проведения операции" in tx_df.columns:
        tx_df["txn_date"] = pd.to_datetime(
            tx_df["Дата проведения операции"],
            dayfirst=True,
            errors="coerce",
        )
    else:
        # Фолбэк, если вдруг формат поменяется
        date_col = None
        for candidate in ["Дата операции", "Дата", "date_posted", "date_processed"]:
            if candidate in tx_df.columns:
                date_col = candidate
                break
        if date_col is None:
            raise ValueError(
                "Halyk individual: cannot find date column "
                "('Дата проведения операции', 'Дата операции', 'Дата', ...)"
            )
        tx_df["txn_date"] = pd.to_datetime(
            tx_df[date_col],
            dayfirst=True,
            errors="coerce",
        )

    return Statement(
        bank="Halyk Individual",
        pdf_name=pdf_name,
        account_holder_name=str(account_holder_name) if account_holder_name is not None else "",
        iin_bin=str(iin_bin) if iin_bin is not None else "",
        account_number=str(account_number) if account_number is not None else None,
        period_from=period_from,
        period_to=period_to,
        statement_generation_date=statement_generation_date,
        tx_df=tx_df,
    )

def parse_freedom_bank_statement(pdf_name: str, pdf_bytes: bytes) -> Statement:
    """
    Freedom Bank:

    1) Сохраняем PDF во временный файл
    2) Парсим header/footer и tx через freedom_bank.parser
    3) Вытаскиваем мету и создаём txn_date.
    """
    import tempfile
    from pathlib import Path

    from src.freedom_bank.parser import extract_header_footer, extract_transactions

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # 1) сохранить PDF
        pdf_path = tmpdir_path / pdf_name
        pdf_path.write_bytes(pdf_bytes)

        # 2) парсинг (как в process_one_freedom)
        header_df, footer_df = extract_header_footer(str(pdf_path))
        tx_df = extract_transactions(str(pdf_path))

    if header_df.empty:
        raise ValueError("Freedom Bank: header_df is empty")

    header_row = header_df.iloc[0]

    # ---------- МЕТАДАННЫЕ ----------

    account_holder_name = (
        header_row.get("Наименование клиента")
        or header_row.get("client_name")
        or header_row.get("ФИО")
        or header_row.get("account_name")
    )

    iin_bin = (
        header_row.get("ИИН/БИН")
        or header_row.get("ИИН")
        or header_row.get("БИН")
        or header_row.get("iin_bin")
    )

    account_number = (
        header_row.get("Номер счета")
        or header_row.get("IBAN")
        or header_row.get("iban")
        or header_row.get("account_number")
    )

    # period_start не явно в batch, но есть period_end → предполагаем пару
    period_start_raw = (
        header_row.get("period_start")
        or header_row.get("Период с")
        or header_row.get("Период (начало)")
    )
    period_end_raw = (
        header_row.get("period_end")
        or header_row.get("Период по")
        or header_row.get("Период (конец)")
    )

    period_from = (
        pd.to_datetime(period_start_raw, dayfirst=True, errors="coerce").date()
        if period_start_raw
        else None
    )
    period_to = (
        pd.to_datetime(period_end_raw, dayfirst=True, errors="coerce").date()
        if period_end_raw
        else None
    )

    statement_generation_date_raw = (
        header_row.get("Дата выписки")
        or header_row.get("Дата формирования")
        or header_row.get("statement_generation_date")
    )
    statement_generation_date = (
        pd.to_datetime(statement_generation_date_raw, dayfirst=True, errors="coerce").date()
        if statement_generation_date_raw
        else None
    )

    # ---------- ТРАНЗАКЦИИ ----------

    # batch-парсер требует минимум: "Дата", "Кредит", "Назначение платежа", "Корреспондент"
    required_cols = ["Дата", "Кредит", "Назначение платежа", "Корреспондент"]
    missing = [c for c in required_cols if c not in tx_df.columns]
    if missing:
        raise ValueError(f"Freedom Bank: missing columns in tx_df: {missing}")

    tx_df = tx_df.copy()
    # В batch compute_ip_income для Freedom дата формата dd.mm.yy → "%d.%m.%y"
    tx_df["txn_date"] = pd.to_datetime(
        tx_df["Дата"],
        format="%d.%m.%y",
        errors="coerce",
    )

    return Statement(
        bank="Freedom Bank",
        pdf_name=pdf_name,
        account_holder_name=str(account_holder_name) if account_holder_name is not None else "",
        iin_bin=str(iin_bin) if iin_bin is not None else "",
        account_number=str(account_number) if account_number is not None else None,
        period_from=period_from,
        period_to=period_to,
        statement_generation_date=statement_generation_date,
        tx_df=tx_df,
    )

def parse_forte_bank_statement(pdf_name: str, pdf_bytes: bytes) -> Statement:
    """
    ForteBank:

    1) сохраняем PDF во временный файл
    2) делаем *_pages.jsonl через dump_pdf_pages
    3) парсим header/tx/footer через parse_forte_statement
    4) вытаскиваем метаданные и создаём txn_date.
    """
    import tempfile
    from pathlib import Path

    from src.utils.convert_pdf_json_pages import dump_pdf_pages
    from src.forte_bank.parser import parse_forte_statement

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # 1) сохранить PDF
        pdf_path = tmpdir_path / pdf_name
        pdf_path.write_bytes(pdf_bytes)

        # 2) JSONL страниц (как ensure_jsonl_for_pdf в batch_parse)
        jsonl_path = tmpdir_path / f"{pdf_path.stem}_pages.jsonl"
        dump_pdf_pages(
            pdf_path=pdf_path,
            out_path=jsonl_path,
            stream_preview_len=4000,
            include_full_stream=False,
        )

        # 3) парсим стейтмент
        header_df, tx_df, footer_df = parse_forte_statement(
            str(pdf_path),
            str(jsonl_path),
        )

    if header_df.empty:
        raise ValueError("ForteBank: header_df is empty")

    header_row = header_df.iloc[0]

    # ---------- МЕТАДАННЫЕ ----------

    # Имя клиента / компании
    account_holder_name = (
        header_row.get("account_holder_name")  # из header.py
        or header_row.get("Клиент")
        or header_row.get("Наименование клиента")
        or header_row.get("ФИО")
        or header_row.get("client_name")
    )
    # ИИН/БИН
    iin_bin = (
        header_row.get("ИИН/БИН")
        or header_row.get("ИИН")
        or header_row.get("ИИН/БИН")
        or header_row.get("БИН")
        or header_row.get("iin_bin")
    )

    # Номер счёта / IBAN
    account_number = (
        header_row.get("Номер счета")
        or header_row.get("IBAN")
        or header_row.get("iban")
        or header_row.get("account_number")
    )

    # Период
    period_start_raw = (
        header_row.get("period_start")
        or header_row.get("Период с")
        or header_row.get("Период (начало)")
    )
    period_end_raw = (
        header_row.get("period_end")
        or header_row.get("Период по")
        or header_row.get("Период (конец)")
    )

    period_from = (
        pd.to_datetime(period_start_raw, dayfirst=True, errors="coerce").date()
        if period_start_raw
        else None
    )
    period_to = (
        pd.to_datetime(period_end_raw, dayfirst=True, errors="coerce").date()
        if period_end_raw
        else None
    )

    # Дата формирования выписки (если есть)
    statement_generation_date_raw = (
        header_row.get("Дата выписки")
        or header_row.get("Дата формирования")
        or header_row.get("statement_generation_date")
    )
    statement_generation_date = (
        pd.to_datetime(statement_generation_date_raw, dayfirst=True, errors="coerce").date()
        if statement_generation_date_raw
        else None
    )

    # ---------- ТРАНЗАКЦИИ ----------

    # В batch-парсере ты проверяешь, что есть:
    #   "Күні/Дата", "Кредит", "Назначение платежа", "Жіберуші/Отправитель"
    required_cols = [
        "Күні/Дата",
        "Кредит",
        "Назначение платежа",
        "Жіберуші/Отправитель",
    ]
    missing = [c for c in required_cols if c not in tx_df.columns]
    if missing:
        raise ValueError(f"ForteBank: missing columns in tx_df: {missing}")

    tx_df = tx_df.copy()
    tx_df["txn_date"] = pd.to_datetime(
        tx_df["Күні/Дата"],
        dayfirst=True,
        errors="coerce",
    )

    return Statement(
        bank="ForteBank",
        pdf_name=pdf_name,
        account_holder_name=str(account_holder_name) if account_holder_name is not None else "",
        iin_bin=str(iin_bin) if iin_bin is not None else "",
        account_number=str(account_number) if account_number is not None else None,
        period_from=period_from,
        period_to=period_to,
        statement_generation_date=statement_generation_date,
        tx_df=tx_df,
    )

def parse_eurasian_bank_statement(pdf_name: str, pdf_bytes: bytes) -> Statement:
    """
    Eurasian Bank:

    1) сохраняем PDF во временный файл
    2) парсим header/tx/footer через parse_eurasian_statement
    3) вытаскиваем метаданные и создаём txn_date.
    """
    import tempfile
    from pathlib import Path

    from src.eurasian_bank.parser import parse_eurasian_statement

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # 1) сохранить PDF
        pdf_path = tmpdir_path / pdf_name
        pdf_path.write_bytes(pdf_bytes)

        # 2) парсим PDF (как в process_one_pdf, default pages/flavor)
        header_df, tx_df, footer_df = parse_eurasian_statement(
            str(pdf_path),
            pages="1-end",
            flavor="lattice",
        )

    if header_df.empty:
        raise ValueError("Eurasian Bank: header_df is empty")

    header_row = header_df.iloc[0]

    # ---------- МЕТАДАННЫЕ ----------

    # Имя клиента / компании
    account_holder_name = (
        header_row.get("Наименование клиента")
        or header_row.get("client_name")
        or header_row.get("ФИО")
        or header_row.get("account_name")
    )

    # ИИН/БИН
    iin_bin = (
        header_row.get("ИИН/БИН")
        or header_row.get("ИИН")
        or header_row.get("БИН")
        or header_row.get("iin_bin")
    )

    # Номер счёта / IBAN
    account_number = (
        header_row.get("Номер счета")
        or header_row.get("IBAN")
        or header_row.get("iban")
        or header_row.get("account_number")
    )

    # Период (в batch-парсере точно есть period_end, часто и period_start)
    def _get_date_from_header(keys):
        for k in keys:
            if k in header_row.index:
                val = header_row.get(k)
                if pd.isna(val):
                    continue
                d = pd.to_datetime(val, dayfirst=True, errors="coerce")
                if pd.notna(d):
                    return d.date()
        return None

    period_from = _get_date_from_header(
        ["period_start", "Период с", "Период (начало)", "period_from"]
    )
    period_to = _get_date_from_header(
        ["period_end", "Период по", "Период (конец)", "period_to"]
    )

    statement_generation_date = _get_date_from_header(
        ["Дата выписки", "Дата формирования", "statement_generation_date"]
    )

    # ---------- ТРАНЗАКЦИИ ----------

    tx_df = tx_df.copy()

    # В batch для compute_ip_income используется "Дата проводки"
    if "Дата проводки" in tx_df.columns:
        tx_df["txn_date"] = pd.to_datetime(
            tx_df["Дата проводки"],
            dayfirst=True,
            errors="coerce",
        )
    else:
        # фолбэк, если вдруг другая колонка
        date_col = None
        for candidate in ["Дата операции", "Дата", "date", "operation_date"]:
            if candidate in tx_df.columns:
                date_col = candidate
                break
        if date_col is None:
            raise ValueError(
                "Eurasian Bank: cannot find date column "
                "('Дата проводки', 'Дата операции', 'Дата', ...)"
            )
        tx_df["txn_date"] = pd.to_datetime(
            tx_df[date_col],
            dayfirst=True,
            errors="coerce",
        )

    return Statement(
        bank="Eurasian Bank",
        pdf_name=pdf_name,
        account_holder_name=str(account_holder_name) if account_holder_name is not None else "",
        iin_bin=str(iin_bin) if iin_bin is not None else "",
        account_number=str(account_number) if account_number is not None else None,
        period_from=period_from,
        period_to=period_to,
        statement_generation_date=statement_generation_date,
        tx_df=tx_df,
    )

def parse_bcc_bank_statement(pdf_name: str, pdf_bytes: bytes) -> Statement:
    """
    Parse BCC PDF exactly like process_one_bcc does, but simplified for UI:
      - write temp PDF
      - generate *_pages.jsonl via dump_pdf_pages
      - call parse_bcc_statement(pdf_path, jsonl_path)
      - extract metadata (iin, name, period)
      - build txn_date column
    """
    import tempfile
    from pathlib import Path
    from src.bcc.parser import parse_bcc_statement
    from src.utils.convert_pdf_json_pages import dump_pdf_pages

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        pdf_path = tmpdir / pdf_name
        pdf_path.write_bytes(pdf_bytes)

        jsonl_path = tmpdir / f"{pdf_path.stem}_pages.jsonl"

        dump_pdf_pages(
            pdf_path=pdf_path,
            out_path=jsonl_path,
            stream_preview_len=4000,
            include_full_stream=False,
        )

        header_df, tx_df, footer_df = parse_bcc_statement(str(pdf_path), str(jsonl_path))

    if header_df.empty:
        raise ValueError("BCC: empty header_df — parser failed")

    header_row = header_df.iloc[0]

    # ----------------- METADATA -----------------

    account_holder_name = _first_not_nan(
        header_row.get("account_holder_name"),  # из header.py
        header_row.get("Клиент"),
        header_row.get("Наименование клиента"),
        header_row.get("ФИО"),
        header_row.get("client_name"),
    )

    iin_bin = _first_not_nan(
        header_row.get("iin_bin"),  # из header.py
        header_row.get("ИИН/БИН"),
        header_row.get("ИИН"),
        header_row.get("БИН"),
    )

    account_number = _first_not_nan(
        header_row.get("account_number"),  # из header.py
        header_row.get("Номер счета"),
        header_row.get("ИИК/IBAN"),
        header_row.get("IBAN"),
        header_row.get("iban"),
    )

    # Period extraction
    def parse_date(value):
        if value is None:
            return None
        d = pd.to_datetime(value, dayfirst=True, errors="coerce")
        return d.date() if pd.notna(d) else None

    period_from = parse_date(
        header_row.get("Период (начало)") or header_row.get("period_start")
    )
    period_to = parse_date(
        header_row.get("Период (конец)") or header_row.get("period_end")
    )

    # Statement generation date
    statement_generation_date = parse_date(
        header_row.get("Дата выписки") or header_row.get("Дата формирования")
    )

    # ----------------- TRANSACTIONS -----------------

    required_cols = [
        "Күні / Дата",
        "Кредит / Кредит",
        "ТМК /КНП",
        "Төлемнің мақсаты / Назначение платежа",
        "Корреспондент / Корреспондент",
    ]
    missing = [c for c in required_cols if c not in tx_df.columns]
    if missing:
        raise ValueError(f"BCC: missing required columns: {missing}")

    tx_df = tx_df.copy()

    tx_df["txn_date"] = pd.to_datetime(
        tx_df["Күні / Дата"],
        dayfirst=True,
        errors="coerce",
    )

    return Statement(
        bank="BCC",
        pdf_name=pdf_name,
        account_holder_name=str(account_holder_name or ""),
        iin_bin=str(iin_bin or ""),
        account_number=str(account_number or ""),
        period_from=period_from,
        period_to=period_to,
        statement_generation_date=statement_generation_date,
        tx_df=tx_df,
        header_df=header_df,  # ← добавили
    )

def parse_alatau_city_bank_statement(pdf_name: str, pdf_bytes: bytes) -> Statement:
    """
    Alatau City Bank:

    - write PDF to a temp file
    - call parse_acb_pdf_with_camelot(pdf_path)
    - extract metadata + build txn_date
    """
    import tempfile
    from pathlib import Path

    from src.alatau_city_bank.parser import parse_acb_pdf_with_camelot

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # 1) save PDF
        pdf_path = tmpdir_path / pdf_name
        pdf_path.write_bytes(pdf_bytes)

        # 2) parse header / tx / footer
        header_df, tx_df, footer_df = parse_acb_pdf_with_camelot(str(pdf_path))

    if header_df.empty:
        raise ValueError("Alatau City Bank: header_df is empty")

    header_row = header_df.iloc[0]

    # ---------- METADATA ----------

    # from parser: account, currency, client, iin_bin, ...
    account_holder_name = (
        header_row.get("client")
        or header_row.get("account_holder_name")
    )

    iin_bin = header_row.get("iin_bin")

    account_number = (
        header_row.get("account")
        or header_row.get("account_number")
    )

    def _parse_date(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        d = pd.to_datetime(val, dayfirst=True, errors="coerce")
        return d.date() if pd.notna(d) else None

    # we treat opening/closing balance dates as period start/end
    period_from = _parse_date(header_row.get("opening_balance_date"))
    period_to = _parse_date(header_row.get("closing_balance_date"))

    # no explicit "Дата выписки" -> can reuse closing_balance_date
    statement_generation_date = _parse_date(
        header_row.get("closing_balance_date")
    )

    # ---------- TRANSACTIONS ----------

    tx_df = tx_df.copy()

    if "Дата операции" not in tx_df.columns:
        raise ValueError(
            "Alatau City Bank: tx_df has no 'Дата операции' column"
        )

    tx_df["txn_date"] = pd.to_datetime(
        tx_df["Дата операции"],
        dayfirst=True,
        errors="coerce",
    )

    return Statement(
        bank="Alatau City Bank",
        pdf_name=pdf_name,
        account_holder_name=str(account_holder_name or ""),
        iin_bin=str(iin_bin or ""),
        account_number=str(account_number or ""),
        period_from=period_from,
        period_to=period_to,
        statement_generation_date=statement_generation_date,
        tx_df=tx_df,
        header_df=header_df,   # so we can show extra header fields later
    )
