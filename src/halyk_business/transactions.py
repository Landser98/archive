# halyk_transactions_parser.py
import re
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

# ===== Output schema =====
COLS = [
    "Дата",                # dd.mm.yyyy
    "Номер документа",     # str
    "Дебет",               # float or NaN
    "Кредит",              # float or NaN
    "Контрагент (имя)",    # str
    "Контрагент ИИН/БИН",  # 12 digits
    "Детали платежа",      # long str
]

# ===== Layout heuristics (from your snippet) =====
PAGE_Y_OFFSET = 100_000.0
LINE_Y_EPS    = 0.9

# X bands (midpoint) tuned to the sample:
# [0] Date, [1] DocNo, [2] Debit, [3] Credit, [4] Counterparty (name + BIN/IIN words),
# [5] Payment details (long text)
BANDS_X = [
    ( 20.0,  85.0),   # 0: Дата
    ( 85.0, 170.0),   # 1: Номер документа
    (170.0, 235.0),   # 2: Дебет
    (235.0, 320.0),   # 3: Кредит
    (320.0, 410.0),   # 4: Контрагент (имя + куски БИН/ИИН)
    (410.0, 2000.0),  # 5: Детали платежа
]

# ===== Regex =====
SPACES_RE  = re.compile(r"\s+")
DATE_RE    = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")
DOCNO_RE   = re.compile(r"[A-Za-zА-Яа-я0-9][\w\-/]{0,30}$")
BIN_IIN_RE = re.compile(r"(?<!\d)(\d{12})(?!\d)")
AMOUNT_TOK = re.compile(r"^[\d\s.,]+$")
# стало
FOOTER_RE = re.compile(
    r"(?i)\b(?:обороты|исходящий остаток|дата остатка)\b"
)
DATE_RE = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")   # 01.02.2024

# ===== Small helpers =====

def _is_footer_or_summary(buckets) -> bool:
    line_txt = _norm_spaces(" ".join(w["text"] for bb in buckets for w in bb))
    if not line_txt:
        return False
    if FOOTER_RE.search(line_txt):
        return True
    # Heuristic: if there’s no date+doc+amount but footer-ish keywords present at right bands
    has_date = bool(_norm_spaces(" ".join(w["text"] for w in buckets[0])))
    has_doc  = bool(_norm_spaces(" ".join(w["text"] for w in buckets[1])))
    has_amt  = any(_norm_spaces(" ".join(w["text"] for w in b)) for b in (buckets[2], buckets[3]))
    if not has_date and not has_doc and not has_amt:
        if any(k in line_txt.lower() for k in ("обороты", "исходящий", "остаток", "за период", "дата остатка")):
            return True
    return False


def _norm_spaces(s: str) -> str:
    return SPACES_RE.sub(" ", (s or "")).strip()

def _amount_to_float(txt: Optional[str]) -> Optional[float]:
    if not txt:
        return None
    t = txt.replace(" ", "").replace("\xa0", "")
    # Mixed formatting guard:
    if "," in t and "." in t:
        # assume comma = thousands, dot = decimal  -> drop commas
        t = t.replace(",", "")
    elif "," in t and "." not in t:
        # comma as decimal
        t = t.replace(",", ".")
    # keep only digits and at most one dot
    t = re.sub(r"[^0-9.]", "", t)
    if t.count(".") > 1:
        head, _, tail = t.rpartition(".")
        head = head.replace(".", "")
        t = head + "." + tail
    try:
        return float(t) if t else None
    except Exception:
        return None

def _flatten_and_sort(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    words = []
    for pi, page in enumerate(pages):
        for w in page.get("words", []):
            ww = dict(w)
            top    = float(ww.get("top", 0.0))
            doctop = float(ww.get("doctop", top))
            ww["_pi"]     = pi
            ww["_top"]    = top    + pi * PAGE_Y_OFFSET
            ww["_doctop"] = doctop + pi * PAGE_Y_OFFSET
            ww["_x0"]     = float(ww.get("x0", 0.0))
            ww["_x1"]     = float(ww.get("x1", 0.0))
            words.append(ww)
    words.sort(key=lambda z: (z["_doctop"], z["_top"], z["_x0"]))
    return words

def _cluster_lines(words: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    lines, cur = [], []
    cur_top = None
    for w in words:
        t = w["_top"]
        if cur_top is None or abs(t - cur_top) <= LINE_Y_EPS:
            cur.append(w)
            cur_top = t if cur_top is None else cur_top
        else:
            cur.sort(key=lambda z: z["_x0"])
            lines.append(cur)
            cur = [w]
            cur_top = t
    if cur:
        cur.sort(key=lambda z: z["_x0"])
        lines.append(cur)
    return lines

def _bucket_line(line: List[Dict[str, Any]], bands: List[Tuple[float, float]]):
    buckets = [[] for _ in bands]
    for w in line:
        xmid = 0.5 * (w["_x0"] + w["_x1"])
        for bi, (lo, hi) in enumerate(bands):
            if lo <= xmid < hi:
                buckets[bi].append(w)
                break
    return buckets

def _line_text(b) -> str:
    return _norm_spaces(" ".join(w["text"] for w in b))

def _is_header_or_ruler(buckets) -> bool:
    rowtxt = _norm_spaces(" ".join(w["text"] for bb in buckets for w in bb))
    if not rowtxt:
        return True
    # table header row with column names
    if "Дата" in rowtxt and "Номер" in rowtxt and "Дебет" in rowtxt and "Кредит" in rowtxt:
        return True
    # pure numbering like "0 1 2 3 ..." (sometimes present)
    tokens = rowtxt.split()
    if len(tokens) >= 4 and all(tok.isdigit() and len(tok) <= 2 for tok in tokens):
        return True
    return False

def _looks_like_row_start(buckets) -> bool:
    # band0 must be a date; band1 should have some doc ref
    b0 = _line_text(buckets[0])
    if not DATE_RE.fullmatch(b0):
        return False
    b1 = _line_text(buckets[1])
    return bool(b1 and DOCNO_RE.match(b1))

def _extract_bin_iin(text: str) -> Optional[str]:
    m = BIN_IIN_RE.search(text)
    return m.group(1) if m else None

# ===== Main parser =====
def parse_halyk_transactions_from_pages(pages: List[Dict[str, Any]]) -> pd.DataFrame:
    if not pages:
        return pd.DataFrame(columns=COLS)

    words = _flatten_and_sort(pages)
    lines = _cluster_lines(words)

    out: List[Dict[str, Any]] = []
    cur: Optional[Dict[str, Any]] = None

    name_parts: List[str] = []
    details_parts: List[str] = []
    bin_iin: Optional[str] = None
    debit_tokens: List[str] = []
    credit_tokens: List[str] = []

    def _reset_state():
        nonlocal name_parts, details_parts, bin_iin, debit_tokens, credit_tokens
        name_parts, details_parts = [], []
        bin_iin = None
        debit_tokens, credit_tokens = [], []

    def _flush():
        nonlocal cur
        if cur is None:
            return
        name = _norm_spaces(" ".join(name_parts)) or None
        if name:
            name = re.sub(r"\b(БИН|ИИН)\b\s*\d{12}", "", name).strip()
        cur["Контрагент (имя)"] = name
        cur["Контрагент ИИН/БИН"] = bin_iin

        d = _amount_to_float(_norm_spaces("".join(debit_tokens)))
        c = _amount_to_float(_norm_spaces("".join(credit_tokens)))
        if d is not None and c is not None:
            if len("".join(debit_tokens)) >= len("".join(credit_tokens)):
                c = None
            else:
                d = None
        cur["Дебет"]  = d
        cur["Кредит"] = c

        cur["Детали платежа"] = _norm_spaces(" ".join(details_parts)) or None

        if cur.get("Дата") and cur.get("Номер документа"):
            out.append(cur)
        cur = None

    # NEW: once the footer is seen on a page, ignore all remaining lines of that page
    # skip_page_idx: Optional[int] = None

    for line in lines:
        page_idx = line[0]["_pi"]

        buckets = _bucket_line(line, BANDS_X)

        if _is_header_or_ruler(buckets):
            continue

        buckets = _bucket_line(line, BANDS_X)

        if _is_header_or_ruler(buckets):
            continue

        # NEW: hard-stop when footer appears
        if _is_footer_or_summary(buckets):
            _flush()
            # skip_page_idx = page_idx
            continue

        if _looks_like_row_start(buckets):
            _flush()
            _reset_state()
            cur = {c: None for c in COLS}
            cur["Дата"] = _line_text(buckets[0])
            cur["Номер документа"] = _line_text(buckets[1])

        if cur is None:
            continue

        # amounts
        if buckets[2]:
            debit_tokens += [w["text"] for w in buckets[2] if AMOUNT_TOK.match(w["text"])]
        if buckets[3]:
            credit_tokens += [w["text"] for w in buckets[3] if AMOUNT_TOK.match(w["text"])]

        # counterparty (name + possible BIN/IIN)
        if buckets[4]:
            frag = _line_text(buckets[4])
            if frag:
                name_parts.append(frag)
                bi = _extract_bin_iin(frag)
                if bi and not bin_iin:
                    bin_iin = bi

        # details (also sometimes includes BIN/IIN on following lines)
        if buckets[5]:
            det = _line_text(buckets[5])
            if det:
                details_parts.append(det)
                bi = _extract_bin_iin(det)
                if bi and not bin_iin:
                    bin_iin = bi

    _flush()

    df = pd.DataFrame(out, columns=COLS)
    if not df.empty:
        for col in ("Дебет", "Кредит"):
            df[col] = df[col].astype(float)
        both = df["Дебет"].notna() & df["Кредит"].notna()
        df.loc[both, "Кредит"] = pd.NA
    return df.reset_index(drop=True)

# ===== Optional validation helper =====
def validate_halyk(df: pd.DataFrame) -> pd.DataFrame:
    def _nonempty(x): return isinstance(x, str) and _norm_spaces(x) != ""
    def _date(x): return isinstance(x, str) and bool(DATE_RE.fullmatch(_norm_spaces(x)))
    def _doc(x):  return _nonempty(x)
    def _amt(x):  return pd.isna(x) or isinstance(x, (int, float))
    def _iin(x):  return (x is None) or bool(BIN_IIN_RE.fullmatch(str(x)))
    def _details(x): return _nonempty(x)

    checks = pd.DataFrame({
        "ok_date":   df["Дата"].apply(_date),
        "ok_doc":    df["Номер документа"].apply(_doc),
        "ok_debit":  df["Дебет"].apply(_amt),
        "ok_credit": df["Кредит"].apply(_amt),
        "ok_iin":    df["Контрагент ИИН/БИН"].apply(_iin),
        "ok_details":df["Детали платежа"].apply(_details),
    })
    # exactly one side
    checks["ok_one_side"] = (df["Дебет"].notna()) ^ (df["Кредит"].notna())
    checks["all_ok"] = checks.all(axis=1)
    return checks



def _is_valid_row(row) -> bool:
    # валидная дата
    if not isinstance(row["Дата"], str) or not DATE_RE.match(row["Дата"]):
        return False

    # номер документа не пустой
    if not isinstance(row["Номер документа"], str) or not row["Номер документа"].strip():
        return False

    # должна быть либо Дебет, либо Кредит (но не обе NaN и не обе заполнены)
    debit  = row["Дебет"]
    credit = row["Кредит"]

    has_debit  = pd.notna(debit)
    has_credit = pd.notna(credit)

    if has_debit == has_credit:  # либо обе NaN, либо обе не NaN
        return False

    # Детали платежа хоть какие-то
    if not isinstance(row["Детали платежа"], str) or not row["Детали платежа"].strip():
        return False

    return True

