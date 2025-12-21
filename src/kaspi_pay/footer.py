# footer_parser.py
import re
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd

# =========================
# Regexes & small utilities
# =========================
AMOUNT_RE = re.compile(
    r"(?:\d{1,3}(?:[ \u00A0]\d{3})+|\d+)(?:[.,]\d+)?"
)  # 28 815 232,9  | 2614060 | 2,200,000 (commas also handled after cleanup)
INT_RE = re.compile(r"\b\d+\b")
DATE_RE = re.compile(r"\b(\d{2}\.\d{2}\,\d{4}|\d{2}\.\d{2}\.\d{4})\b")
TIME_RE = re.compile(r"\b\d{2}:\d{2}(?::\d{2})?\b")
BIC_RE  = re.compile(r"\b[A-Z]{8}\b")
FOOTER_TRIG = re.compile(
    r"(?i)\bитого\b|итого обороты|итого операций|отчет сформирован|наименование и бик|бик[:\s]*[A-Z]{4,8}"
)

def _to_float(s: str) -> Optional[float]:
    if not s:
        return None
    # normalize 28 815 232,9  -> 28815232.9
    t = s.replace("\u00A0", " ").replace(" ", "").replace(",", ".")
    # sometimes commas used as 1,000 separators -> remove, leave last dot
    # (already removed spaces; here commas were converted to dots)
    try:
        return float(t)
    except Exception:
        # try removing all non-digit/dot except minus
        t2 = re.sub(r"[^0-9\.-]", "", t)
        try:
            return float(t2)
        except Exception:
            return None

def _to_int_amount(s: str) -> Optional[int]:
    """
    Склеивает "1 943" -> 1943, "429" -> 429.
    Использует _to_float и округляет до целого.
    """
    f = _to_float(s)
    if f is None:
        return None
    try:
        return int(round(f))
    except (TypeError, ValueError):
        return None



def _norm(txt: str) -> str:
    return re.sub(r"\s+", " ", txt).strip()

# =========================
# Geometric line clustering
# =========================
def _flatten_words(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for pi, p in enumerate(pages):
        words = p.get("words", p)  # support either {words: [...]} or already a word-list
        for w in words:
            if not w.get("text"):
                continue
            ww = dict(w)
            ww["_pi"] = pi
            out.append(ww)
    # sort by (page, top, x0)
    out.sort(key=lambda w: (w["_pi"], round(float(w["top"]), 3), round(float(w["x0"]), 3)))
    return out

def _cluster_lines(words: List[Dict[str, Any]], y_eps: float = 1.8) -> List[List[Dict[str, Any]]]:
    if not words:
        return []
    lines: List[List[Dict[str, Any]]] = []
    cur: List[Dict[str, Any]] = []
    cur_top: Optional[float] = None
    cur_page: Optional[int] = None

    for w in words:
        top = float(w["top"])
        pi  = int(w["_pi"])
        if cur and (pi != cur_page or abs(top - cur_top) > y_eps):
            # flush line
            cur.sort(key=lambda z: float(z["x0"]))
            lines.append(cur)
            cur = []
            cur_top = None
            cur_page = None
        cur.append(w)
        cur_top = top if cur_top is None else cur_top
        cur_page = pi if cur_page is None else cur_page

    if cur:
        cur.sort(key=lambda z: float(z["x0"]))
        lines.append(cur)
    return lines

# =========================
# Footer parsing
# =========================
def parse_footer_from_pages(pages: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Returns a 1-row DataFrame (meta_df) with:
      - total_debit_turnover
      - total_credit_turnover
      - total_operations_debit
      - total_operations_credit
      - report_generated_at
      - report_user
      - servicing_bank_name
      - servicing_bank_bic
      - flags
      - debug_info
    If nothing is found, returns an empty DataFrame with those columns.
    """
    cols = [
        "total_debit_turnover",
        "total_credit_turnover",
        "total_operations_debit",
        "total_operations_credit",
        "report_generated_at",
        "report_user",
        "servicing_bank_name",
        "servicing_bank_bic",
        "flags",
        "debug_info",
    ]
    if not pages:
        return pd.DataFrame(columns=cols)

    words = _flatten_words(pages)
    lines = _cluster_lines(words)

    meta: Dict[str, Any] = dict(
        total_debit_turnover=None,
        total_credit_turnover=None,
        total_operations_debit=None,
        total_operations_credit=None,
        report_generated_at=None,
        report_user=None,
        servicing_bank_name=None,
        servicing_bank_bic=None,
        flags=[],
        debug_info={},
    )

    matched_lines = []
    # We walk lines per page, but footers typically are at the end of a page.
    # We'll simply scan all lines and match by regex cues.
    for ln in lines:
        txt = _norm(" ".join(w["text"] for w in ln))
        if not txt:
            continue
        is_footerish = bool(FOOTER_TRIG.search(txt))
        # Also consider very bottom lines: big top value per page often means footer
        # (we keep it simple: if we already matched something footerish on the same page,
        # we treat the following few lines as footer continuation)
        if not is_footerish:
            continue

        matched_lines.append(txt)

        low = txt.lower()

        # --- Итого обороты в нац. валюте -> two amounts (debit, credit)
        if "итого обороты" in low:
            amts = [m.group(0) for m in AMOUNT_RE.finditer(txt)]
            if len(amts) >= 2:
                meta["total_debit_turnover"]  = _to_float(amts[0])
                meta["total_credit_turnover"] = _to_float(amts[1])
            elif len(amts) == 1:
                # keep at least debit total, flag credit missing
                meta["total_debit_turnover"] = _to_float(amts[0])
                meta["flags"].append("credit_total_missing_on_turnover_line")

        # --- Итого операций за период -> two integers (debit_count, credit_count)
        # --- Итого операций за период -> two integers (debit_count, credit_count)
        # --- Итого операций за период -> two integers (debit_count, credit_count)
        if "итого операций" in low:
            # пример текста: "Итого операций за период 1 943 429"
            ints = [int(m.group(0)) for m in INT_RE.finditer(txt)]

            debit_count: Optional[int] = None
            credit_count: Optional[int] = None

            if len(ints) == 2:
                # классический случай: "Итого операций за период 12 3"
                debit_count, credit_count = ints[0], ints[1]

            elif len(ints) == 3 and ints[0] < 10:
                # наш кейс: "1 943 429" -> 1 943 (debit), 429 (credit)
                debit_count = ints[0] * 1000 + ints[1]
                credit_count = ints[2]

            elif len(ints) >= 2:
                # запасной вариант: берём первые два числа как есть
                debit_count, credit_count = ints[0], ints[1]

            elif len(ints) == 1:
                debit_count = ints[0]

            if debit_count is not None:
                meta["total_operations_debit"] = debit_count
            if credit_count is not None:
                meta["total_operations_credit"] = credit_count
            if credit_count is None:
                meta["flags"].append("operations_credit_count_missing")


        # --- Отчет сформирован пользователем ... <date> <time>
        if "отчет сформирован" in low:
            # user: text between 'пользователем' and the first date
            user = None
            if "пользователем" in low:
                # crude slice to the first date
                pos = low.find("пользователем")
                tail = txt[pos + len("пользователем"):].strip(" :")
                dmatch = DATE_RE.search(tail)
                user = tail[: dmatch.start()].strip(" :-") if dmatch else tail
                user = _norm(user)
                if user:
                    meta["report_user"] = user

            dmatch = DATE_RE.search(txt)
            tmatch = TIME_RE.search(txt)
            if dmatch:
                date_s = dmatch.group(0).replace(",", ".")
                if tmatch:
                    meta["report_generated_at"] = f"{date_s} {tmatch.group(0)}"
                else:
                    meta["report_generated_at"] = date_s

        # --- Наименование и БИК обслуживающего банка ...
        if "наименование и бик" in low or ("бик" in low and "банк" in low):
            # bank name: the phrase after ':' and before 'Бик' (or end)
            bank_name = None
            bic = None

            # Try explicit BIC first
            bic_m = BIC_RE.search(txt)
            if bic_m:
                bic = bic_m.group(0)

            # Name heuristic:
            # look for "банк:" or first ':' then strip trailing 'Бик ...'
            name_candidate = None
            if ":" in txt:
                after_colon = txt.split(":", 1)[1].strip()
                # drop trailing 'Бик ...'
                name_candidate = re.split(r"\sБик[:\s]", after_colon, flags=re.IGNORECASE)[0].strip()
            # fallback: take chunk around the word 'Банк'
            if not name_candidate and "Банк" in txt:
                # take from first 'Банк' word backwards a bit
                name_candidate = txt[: txt.lower().find("бик")].strip()

            bank_name = _norm(name_candidate) if name_candidate else None

            if bank_name:
                meta["servicing_bank_name"] = bank_name
            if bic:
                meta["servicing_bank_bic"] = bic

    # Save debug lines
    meta["debug_info"] = {"matched_footer_lines": matched_lines}

    # Sanity flags
    if meta["total_debit_turnover"] is None and meta["total_credit_turnover"] is None:
        meta["flags"].append("turnover_totals_not_found")
    if meta["total_operations_debit"] is None and meta["total_operations_credit"] is None:
        meta["flags"].append("operations_totals_not_found")
    if not meta["report_generated_at"]:
        meta["flags"].append("report_generated_at_missing")
    if not meta["servicing_bank_bic"]:
        meta["flags"].append("servicing_bank_bic_missing")

    return pd.DataFrame([meta], columns=cols)

