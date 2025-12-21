# -*- coding: utf-8 -*-
"""
Kaspi Gold — Transactions Table Parser
Inputs:
    pages: List[Dict], each with "words": [{"text","x0","x1","top","doctop","bottom","upright",...}, ...]
Output:
    Pandas DataFrame with columns:
        [
          "Номер документа", "Дата операции", "Дебет", "Кредит",
          "Наименование получателя", "ИИК бенеф/отправителя",
          "БИК банка", "КНП", "Назначение платежа"
        ]
Notes:
  - Uses fixed Kaspi column bands inferred from coordinates; tweak BANDS_X if your file shifts.
  - Enforces validation rules you specified (see validate_transactions()).
"""

import re
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import math

# =========================
# Column names (final table)
# =========================
COLS = [
    "Номер документа",
    "Дата операции",
    "Дебет",
    "Кредит",
    "Наименование получателя",
    "ИИК бенеф/отправителя",
    "БИК банка",
    "КНП",
    "Назначение платежа",
]

# =========================
# Heuristics / Layout
# =========================
# Large vertical gap to keep pages separated when sorting by Y
PAGE_Y_OFFSET = 100_000.0
# Same-line tolerance (in "top" units)
LINE_Y_EPS = 0.9
# Minimum doc number length (after removing dashes)
DOCNO_MINLEN = 6

# Kaspi-like horizontal bands (x0-based), with a bit of margin.
# [0] DocNo, [1] Date/Time, [2] Debit, [3] Credit, [4] Name(L), [5] IIK(Name tail + IIK),
# [6] BIC, [7] KNP(+ early purpose), [8] Purpose tail
BANDS_X = [
    (0.0,    95.0),   # 0: Номер документа
    (95.0,   200.0),  # 1: Дата/время (split may flow to next line)
    (200.0,  240.0),  # 2: Дебет
    (240.0,  310.0),  # 3: Кредит
    (310.0,  450.0),  # 4: Имя (первая часть)
    (450.0,  560.0),  # 5: Имя (хвост) / ИИК
    (560.0,  620.0),  # 6: БИК
    (620.0,  650.0),  # 7: КНП (+ обрезок назначения)
    (650.0,  2000.0), # 8: Назначение платежа (основное поле)
]

# =========================
# Regexes
# =========================
SPACES_RE   = re.compile(r"\s+")
DOCNO_LIKE  = re.compile(r"^[A-Za-zА-Яа-я0-9\-]+$")
DATE_TOKEN  = re.compile(r"\d{2}\.\d{2}\.\d{4}")
TIME_TOKEN  = re.compile(r"\d{1,2}:\d{2}:\d{2}")
IIK_RE      = re.compile(r"^[A-Z]{2}[A-Za-z0-9]{18}$")     # 2 letters + 18 alnum = 20 chars
BIC_RE      = re.compile(r"^[A-Z0-9]{8}$")
KNP_RE      = re.compile(r"^\d{1,5}$")
AMOUNT_ANY  = re.compile(r"^[\d\s.,]+$")

# =========================
# Helpers
# =========================
def _norm_spaces(s: str) -> str:
    return SPACES_RE.sub(" ", s).strip()

def _to_float_or_none(txt: Optional[str]) -> Optional[float]:
    if not txt:
        return None
    t = txt.replace(" ", "").replace("\xa0", "").replace(",", ".")
    try:
        # avoid "" or "."
        if re.fullmatch(r"[+-]?(\d+(\.\d+)?|\.\d+)", t):
            return float(t)
    except Exception:
        pass
    return None

def _join_amount(tokens: List[str]) -> Optional[float]:
    # Join tokens that belong to an amount (e.g., "30", "000" -> "30000")
    if not tokens:
        return None
    s = _norm_spaces("".join(tokens))
    s = s.replace(" ", "")
    # normalize comma as decimal
    s = s.replace(",", ".")
    # guard against trailing punctuation
    s = re.sub(r"[^\d.]", "", s)
    if not s:
        return None
    # handle thousand separators that slipped as many dots: keep last dot as decimal
    if s.count(".") > 1:
        head, _, tail = s.rpartition(".")
        head = head.replace(".", "")
        s = head + "." + tail
    try:
        return float(s)
    except Exception:
        return None

def _make_empty_row() -> Dict[str, Optional[str]]:
    return {c: None for c in COLS}

def _flatten_and_sort(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    words = []
    for pi, page in enumerate(pages):
        for w in page.get("words", []):
            w = dict(w)
            top    = float(w.get("top", 0.0))
            doctop = float(w.get("doctop", top))
            w["_doctop"] = doctop + pi * PAGE_Y_OFFSET
            w["_top"]    = top    + pi * PAGE_Y_OFFSET
            w["_x0"]     = float(w.get("x0", 0.0))
            w["_x1"]     = float(w.get("x1", 0.0))
            w["_pi"]     = pi
            words.append(w)
    words.sort(key=lambda z: (z["_doctop"], z["_top"], z["_x0"]))
    return words

def _cluster_lines(all_words: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    lines = []
    cur, cur_top = [], None
    for w in all_words:
        t = w["_top"]
        if cur_top is None:
            cur = [w]
            cur_top = t
        elif abs(t - cur_top) <= LINE_Y_EPS:
            cur.append(w)
        else:
            if cur:
                cur.sort(key=lambda z: z["_x0"])
                lines.append(cur)
            cur = [w]
            cur_top = t
    if cur:
        cur.sort(key=lambda z: z["_x0"])
        lines.append(cur)
    return lines

def _bucket_line(line: List[Dict[str, Any]], bands: List[Tuple[float, float]]) -> List[List[Dict[str, Any]]]:
    buckets = [[] for _ in bands]
    for w in line:
        xmid = 0.5 * (w["_x0"] + w["_x1"])
        for bi, (lo, hi) in enumerate(bands):
            if lo <= xmid < hi:
                buckets[bi].append(w)
                break
    return buckets

DOCNO_RE = re.compile(r"[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-_/]{0,19}")  # len 1..20
DATE_HINT_RE = re.compile(r"\d{2}\.\d{2}\.\d{4}")  # “30.09.2024”

def _looks_like_row_start(buckets):
    # first band must have some non-space text and look like a doc id (short allowed)
    left_txt = " ".join(w["text"] for w in buckets[0]).strip()
    if not left_txt:
        return False
    if not DOCNO_RE.fullmatch(left_txt):
        return False
    # band 1 should contain a date/time hint (date often on one line, time on next)
    band1_txt = " ".join(w["text"] for w in buckets[1]).strip()
    if not (DATE_HINT_RE.search(band1_txt) or band1_txt.count(":") >= 1):
        return False
    return True

def _is_numbering_row(buckets: List[List[Dict[str, Any]]]) -> bool:
    # Skip obvious header/numbering: "Номер", "Лицевой счет", or a run like "0 1 2 3"
    b0 = _norm_spaces(" ".join(w["text"] for w in buckets[0])) if buckets[0] else ""
    header_hits = ("Номер" in b0) or ("Лицевой" in b0) or ("счет" in b0)
    # crude numbering pattern: small sequence of integers only
    only_small_ints = all(re.fullmatch(r"\d{1,2}", w["text"]) for w in buckets[0]) and len(buckets[0]) >= 3
    return header_hits or only_small_ints

# =========================
# Core Parser
# =========================
def parse_transactions_from_pages(pages: List[Dict[str, Any]]) -> pd.DataFrame:
    if not pages:
        return pd.DataFrame(columns=COLS)

    all_words = _flatten_and_sort(pages)
    lines = _cluster_lines(all_words)
    bands = BANDS_X

    out_rows: List[Dict[str, Optional[str]]] = []
    cur_row: Optional[Dict[str, Optional[str]]] = None

    debit_tokens: List[str] = []
    credit_tokens: List[str] = []
    name_parts: List[str] = []
    iik_value: Optional[str] = None
    purpose_parts: List[str] = []
    knp_value: Optional[str] = None

    def reset_row_state():
        nonlocal debit_tokens, credit_tokens, name_parts, iik_value, purpose_parts, knp_value
        debit_tokens, credit_tokens = [], []
        name_parts, purpose_parts   = [], []
        iik_value = None
        knp_value = None

    def flush_row():
        nonlocal cur_row
        if cur_row is None:
            return

        # Assemble name
        name_txt = _norm_spaces(" ".join(name_parts)) if name_parts else None
        if name_txt:
            cur_row["Наименование получателя"] = name_txt

        if iik_value:
            cur_row["ИИК бенеф/отправителя"] = iik_value

        d = _join_amount(debit_tokens)
        c = _join_amount(credit_tokens)

        if d is not None and c is not None:
            # Попробуем понять, это два разных числа или одно,
            # которое разъехалось между Band 2 и Band 3
            all_tokens = debit_tokens + credit_tokens
            merged = _join_amount(all_tokens)

            dlen = len("".join(debit_tokens))
            clen = len("".join(credit_tokens))
            totlen = len("".join(all_tokens))

            if merged is not None and totlen > max(dlen, clen):
                # Похоже, это одно число, размазанное по двум колонкам
                if dlen >= clen:
                    d, c = merged, None
                else:
                    d, c = None, merged
            else:
                # Фолбэк: оставляем только ту сторону, где строка длиннее
                if clen >= dlen:
                    d = None
                else:
                    c = None

        cur_row["Дебет"]  = d
        cur_row["Кредит"] = c


        if knp_value:
            cur_row["КНП"] = knp_value

        pur = _norm_spaces(" ".join(purpose_parts)) if purpose_parts else None
        if pur:
            cur_row["Назначение платежа"] = pur

        # Required
        # Required: doc number and date must exist
        if not cur_row.get("Номер документа") or not cur_row.get("Дата операции"):
            cur_row = None
            return

        # Extra guard: Дата операции должна содержать реальную дату вида dd.mm.yyyy
        dt_txt = str(cur_row.get("Дата операции") or "")
        if not DATE_HINT_RE.search(dt_txt):
            # это всякие "остаток остаток Дата операции Дебет" и прочий мусор
            cur_row = None
            return

        out_rows.append(cur_row)
        cur_row = None

    # --- NEW: footer/summary detection helpers ---
    FOOTER_RE = re.compile(
        r"(?i)\bитого\b|итого обороты|итог[а-я]* операций|отчет сформирован|наименование и бик|бик[:\s]*caspkzka|бик\s+caspkzka|бик\s*:",
    )

    def _is_summary_or_footer(buckets: List[List[Dict[str, Any]]]) -> bool:
        # Combine all visible text in the line
        line_txt = " ".join(w["text"] for b in buckets for w in b).strip()
        if not line_txt:
            return False
        if FOOTER_RE.search(line_txt):
            return True

        # Extra safety: lines that begin with "Итого" in any bucket
        head = line_txt.lower().lstrip()
        if head.startswith("итого"):
            return True

        # Heuristic: no doc number/date/amounts but long text in right bands ⇒ very likely footer
        has_doc = any(w["text"].strip() for w in (buckets[0] if len(buckets) > 0 else []))
        has_dt = any(w["text"].strip() for w in (buckets[1] if len(buckets) > 1 else []))
        has_amt = any(b for b in (buckets[2:4] if len(buckets) > 4 else []))
        long_right = len(" ".join(w["text"] for b in (buckets[6:9] if len(buckets) > 8 else []) for w in b)) > 20
        if not has_doc and not has_dt and not has_amt and long_right:
            return True

        return False

    # If we hit a summary/footer on a page, skip the rest of THAT page
    skip_page_idx: Optional[int] = None

    for line in lines:
        if not line:
            continue
        line_page = line[0]["_pi"]
        if skip_page_idx is not None and line_page != skip_page_idx:
            skip_page_idx = None


        buckets = _bucket_line(line, bands)

        def _is_pure_numbering(buckets):
            txt = " ".join(w["text"] for b in buckets for w in b).strip()
            # only digits separated by spaces, at least 5 tokens, no punctuation
            if not re.fullmatch(r"(?:\d+\s+){4,}\d+", txt):
                return False
            # and no token longer than 2 chars (to avoid “200840000951” etc)
            return all(len(tok) <= 2 for tok in txt.split())

        # inside the lines loop, after `buckets = _bucket_line(line, bands)`:
        if _is_pure_numbering(buckets):
            continue

        # NEW: hard-stop on footer/summary for this page
        if _is_summary_or_footer(buckets):
            flush_row()  # finish the ongoing row so its purpose/name won’t swallow footer
            skip_page_idx = line[0]["_pi"]  # ignore the rest of this page
            continue


        # Start of a new row?
        if _looks_like_row_start(buckets):
            flush_row()
            cur_row = _make_empty_row()
            reset_row_state()
            cur_row["Номер документа"] = _norm_spaces(" ".join(w["text"] for w in buckets[0]))

        if cur_row is None:
            continue

        # Band 1: Date/time fragments
        if buckets[1]:
            add = _norm_spaces(" ".join(w["text"] for w in buckets[1]))
            if add:
                prev = cur_row.get("Дата операции")
                cur_row["Дата операции"] = _norm_spaces(" ".join(x for x in [prev, add] if x))

        # Band 2/3: amounts
        if buckets[2]:
            debit_tokens.extend([w["text"] for w in buckets[2] if AMOUNT_ANY.match(w["text"])])
        if buckets[3]:
            credit_tokens.extend([w["text"] for w in buckets[3] if AMOUNT_ANY.match(w["text"])])

        # Band 4: name (left)
        if buckets[4]:
            frag = _norm_spaces(" ".join(w["text"] for w in buckets[4]))
            if frag:
                name_parts.append(frag)

        # Band 5: name tail + IIK
        if buckets[5]:
            for w in buckets[5]:
                t = w["text"].strip()
                if iik_value is None and IIK_RE.fullmatch(t):
                    iik_value = t
                elif iik_value is None:
                    if t:
                        name_parts.append(t)

        # Band 6: BIC
        if buckets[6]:
            for w in buckets[6]:
                t = w["text"].strip()
                if BIC_RE.fullmatch(t):
                    cur_row["БИК банка"] = t
                    break

        # Band 7: KNP + early purpose
        if buckets[7]:
            text7 = _norm_spaces(" ".join(w["text"] for w in buckets[7]))
            if text7:
                parts = text7.split()
                residue_parts = []
                found_knp = False
                if knp_value is None:
                    for p in parts:
                        if KNP_RE.fullmatch(p) and not found_knp:
                            knp_value = p
                            found_knp = True
                        else:
                            residue_parts.append(p)
                else:
                    residue_parts = parts
                residue = _norm_spaces(" ".join(residue_parts))
                if residue:
                    purpose_parts.append(residue)

        # Band 8: purpose continuation
        if buckets[8]:
            add = _norm_spaces(" ".join(w["text"] for w in buckets[8]))
            if add:
                purpose_parts.append(add)

    # flush last row
    flush_row()

    # Post-filter bogus rows (e.g., “Лицевой счет”)
    # --- replace this whole cleaner block ---
    cleaned = []
    for r in out_rows:
        doc = (r.get("Номер документа") or "").strip()
        if not doc:
            continue
        if "Лицевой" in doc and "счет" in doc:
            continue
        # REMOVE the min-length check that was dropping '47'
        # if len(doc.replace("-", "")) < DOCNO_MINLEN:
        #     continue
        cleaned.append(r)

    df = pd.DataFrame(cleaned, columns=COLS)
    if not df.empty:
        df["Дебет"]  = df["Дебет"].apply(lambda v: float(v) if isinstance(v, (int, float)) else (None if v is None else _to_float_or_none(str(v))))
        df["Кредит"] = df["Кредит"].apply(lambda v: float(v) if isinstance(v, (int, float)) else (None if v is None else _to_float_or_none(str(v))))
    return df.reset_index(drop=True)

# =========================
# Validation per your rules
# =========================
def validate_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a DataFrame with validation flags for each row (True = OK).
    Rules:
      - Номер документа: non-empty str
      - Дата операции: non-empty, must contain date and time fragment
      - Дебет/Kредит: numbers or NaN; exactly one side must be non-null
      - Наименование получателя: non-empty str
      - ИИК: 20 chars, first two letters; non-empty
      - БИК: optional, when present 8 chars (A-Z0-9)
      - КНП: non-empty, 1-5 digits
      - Назначение платежа: non-empty
    """
    def _is_nonempty_str(x): return isinstance(x, str) and _norm_spaces(x) != ""
    def _ok_docno(x): return _is_nonempty_str(x)
    def _ok_datetime(x):
        if not _is_nonempty_str(x): return False
        return bool(DATE_TOKEN.search(x) and TIME_TOKEN.search(x))

    def _ok_amount_or_nan(x):
        return (x is None) or (isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x)))

    def _ok_name(x): return _is_nonempty_str(x)

    def _ok_iik(x): return isinstance(x, str) and bool(IIK_RE.fullmatch(x))

    def _ok_bic_optional(x):
        if x in (None, "", float("nan")): return True
        return isinstance(x, str) and bool(BIC_RE.fullmatch(x))

    def _ok_knp(x): return isinstance(x, str) and bool(KNP_RE.fullmatch(x))

    def _ok_purpose(x): return _is_nonempty_str(x)

    # apply checks
    checks = pd.DataFrame({
        "ok_docno":   df["Номер документа"].apply(_ok_docno),
        "ok_dt":      df["Дата операции"].apply(_ok_datetime),
        "ok_debit":   df["Дебет"].apply(_ok_amount_or_nan),
        "ok_credit":  df["Кредит"].apply(_ok_amount_or_nan),
        "ok_name":    df["Наименование получателя"].apply(_ok_name),
        "ok_iik":     df["ИИК бенеф/отправителя"].apply(_ok_iik),
        "ok_bic":     df["БИК банка"].apply(_ok_bic_optional),
        "ok_knp":     df["КНП"].apply(_ok_knp),
        "ok_purpose": df["Назначение платежа"].apply(_ok_purpose),
    })

    # exactly one of debit/credit must be set
    one_side = (df["Дебет"].notna()) ^ (df["Кредит"].notna())
    checks["ok_one_side"] = one_side

    checks["all_ok"] = checks.all(axis=1)
    return checks




