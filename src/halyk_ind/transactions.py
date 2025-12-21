# halyk_hard_debug_parser.py

from __future__ import annotations
import re, math, sys, json
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd

DEBUG = False  # flip to True to enable verbose debug logs

def dprint(*a, **k):
    if DEBUG:
        print(*a, **k)

# -------------------- robust adapters --------------------
def _as_word_dict(obj: Any) -> Optional[Dict[str, Any]]:
    """
    Normalize different token shapes into dicts with:
      text, x0, x1, top, bottom
    Accepts: dict-like {text,x0,x1,top,bottom,...}
             tuple/list (text, x, y)    -> no x1
             tuple/list (text, x0, x1, top, bottom)
    Returns None if can't parse.
    """
    if isinstance(obj, dict) and "text" in obj:
        w = {
            "text": str(obj["text"]),
            "x0": float(obj.get("x0", obj.get("x", 0.0))),
            "x1": float(obj.get("x1", obj.get("x", 0.0))),
            "top": float(obj.get("top", obj.get("y", obj.get("y0", 0.0)))),
            "bottom": float(obj.get("bottom", obj.get("y1", obj.get("y", 0.0)))),
        }
        if w["x1"] <= w["x0"]:
            # estimate width ≈ 4.6 px per char
            w["x1"] = w["x0"] + max(6.0, 4.6 * max(1, len(w["text"])))
        if w["bottom"] <= w["top"]:
            w["bottom"] = w["top"] + 8.0
        return w

    if isinstance(obj, (list, tuple)):
        if len(obj) == 3 and isinstance(obj[0], str):
            t, x, y = obj
            x0 = float(x); x1 = x0 + max(6.0, 4.6 * max(1, len(t)))
            top = float(y); bottom = top + 8.0
            return {"text": t, "x0": x0, "x1": x1, "top": top, "bottom": bottom}
        if len(obj) >= 5 and isinstance(obj[0], str):
            t, x0, x1, top, bottom = obj[:5]
            x0 = float(x0); x1 = float(x1); top = float(top); bottom = float(bottom)
            if x1 <= x0: x1 = x0 + max(6.0, 4.6 * max(1, len(t)))
            if bottom <= top: bottom = top + 8.0
            return {"text": t, "x0": x0, "x1": x1, "top": top, "bottom": bottom}
    return None

def _normalize_words(raw: List[Any]) -> List[Dict[str, Any]]:
    words = []
    for r in raw:
        w = _as_word_dict(r)
        if w and w["text"]:
            words.append(w)
    return words

# -------------------- regexes & helpers --------------------
DATE_RE = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")
NUM_TOKEN = re.compile(r"^[+-]?\d{1,3}(?:[ \u00A0\u202F]?\d{3})*(?:,\d{2})?$")
CURRENCY_RE = re.compile(r"^[A-Z]{3}$")  # KZT etc.
CARD_RE = re.compile(r"^(?:\d{12,20}|[0-9]{6}\*{6}\d{4}|[0-9]{4}\*{6}\d{4}|KZ[0-9A-Z]{10,})$")

ANCHORS = {
    "date_posted":    r"\bдата\s+проведения\b",
    "date_processed": r"\bдата\s+обработки\b",
    "description":    r"\bописан[ие][ея]?\s+операц[иы]и\b|\bописание\b",
    "amount":         r"\bсумма(\s+операц[иы]и)?\b",
    "currency":       r"\bвалюта(\s+операц[иы]и)?\b",
    "credit":         r"\bприход\b",
    "debit":          r"\bрасход\b",
    "fee":            r"\bкомисси[яи]\b",
    "account":        r"\b№\s*карточки/счета\b|\bкарточки/счета\b",
}
RX = {k: re.compile(v, re.I) for k, v in ANCHORS.items()}

def clean_spaces(s: str) -> str:
    return re.sub(r"[ \t\r\n\u00A0\u202F]+", " ", s or "").strip()

def parse_kzt_amount(s: str | None) -> float:
    if not s or not s.strip():
        return 0.0
    t = s.replace("\u00A0"," ").replace("\u202F"," ").strip()
    t = t.replace(" ", "").replace(".", "").replace(",", ".")
    if not re.match(r"^[+-]?\d+(?:\.\d+)?$", t):
        raise ValueError(f"bad amount: {s!r}")
    return float(t)

def is_date(s: str | None) -> bool:
    return bool(s and DATE_RE.match(s))

def to_date_iso(s: str) -> str:
    return datetime.strptime(s, "%d.%m.%Y").date().isoformat()

def same_line(y1: float, y2: float, tol: float = 3.0) -> bool:
    return abs(y1 - y2) <= tol

def band_hit(x0: float, x1: float, band: Tuple[float, float]) -> bool:
    L, R = band
    return not (x1 < L + 0.5 or x0 > R - 0.5)

def _words_to_lines(words: List[Dict[str, Any]], y_tol: float = 3.0) -> List[List[Dict[str, Any]]]:
    ws = sorted(words, key=lambda w: (w["top"], w["x0"]))
    lines: List[List[Dict[str, Any]]] = []
    for w in ws:
        if not lines or not same_line(lines[-1][0]["top"], w["top"], y_tol):
            lines.append([w])
        else:
            lines[-1].append(w)
    return lines

def _line_text(line: List[Dict[str, Any]]) -> str:
    return clean_spaces(" ".join(w["text"] for w in sorted(line, key=lambda x: x["x0"])))

# -------------------- header detection (1–3 lines) --------------------
def _score_header_tokens(tokens: List[Dict[str, Any]]) -> Tuple[int, Dict[str, float]]:
    toks = sorted(tokens, key=lambda z: z["x0"])
    s = " ".join(t["text"] for t in toks)
    acc = 0
    spans = []
    for t in toks:
        start = acc
        acc += len(t["text"]) + 1
        spans.append((t["text"], t["x0"], start, acc - 1))
    hits, col_x = 0, {}
    for key, rx in RX.items():
        m = rx.search(s)
        if m:
            hits += 1
            st = m.start()
            for txt, x, ts, te in spans:
                if ts <= st < te:
                    col_x[key] = x
                    break
    return hits, col_x

def _merge(*lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for ln in lines: out.extend(ln)
    return sorted(out, key=lambda w: w["x0"])

def find_table_header_bands(words: List[Dict[str, Any]], y_tol: float = 3.0):
    lines = _words_to_lines(words, y_tol=y_tol)
    N = len(lines)
    candidates: List[Tuple[int, float, List[Dict[str, Any]]]] = []
    for i in range(N):
        hits, _ = _score_header_tokens(lines[i])
        if hits >= 3:
            candidates.append((hits, lines[i][0]["top"], lines[i]))
        if i + 1 < N:
            merged2 = _merge(lines[i], lines[i + 1])
            hits2, _ = _score_header_tokens(merged2)
            if hits2 >= 4:
                yref = 0.5 * (lines[i][0]["top"] + lines[i + 1][0]["top"])
                candidates.append((hits2, yref, merged2))
        if i + 2 < N:
            merged3 = _merge(lines[i], lines[i + 1], lines[i + 2])
            hits3, _ = _score_header_tokens(merged3)
            if hits3 >= 5:
                yref = (lines[i][0]["top"] + lines[i + 1][0]["top"] + lines[i + 2][0]["top"]) / 3.0
                candidates.append((hits3, yref, merged3))

    if not candidates:
        raise RuntimeError("Header bands not found: no line with enough anchors")

    candidates.sort(key=lambda t: (-t[0], t[1]))
    best_hits, y_ref, header_tokens = candidates[0]
    hits, col_x = _score_header_tokens(header_tokens)
    if "description" not in col_x:
        raise RuntimeError("Header anchors present but 'description' column x not resolved")

    xs_sorted = sorted(col_x.items(), key=lambda kv: kv[1])
    xs = [v for _, v in xs_sorted]
    col_bounds: Dict[str, Tuple[float, float]] = {}
    for key, x in col_x.items():
        i = min(range(len(xs)), key=lambda j: abs(xs[j] - x))
        left = xs[i - 1] if i > 0 else x - 1e6
        right = xs[i + 1] if i < len(xs) - 1 else x + 1e6
        col_bounds[key] = ((left + x) / 2.0, (x + right) / 2.0)

    dprint(f"[DEBUG] Header selected: hits={best_hits}, y≈{y_ref:.1f}, columns={list(col_x.keys())}")
    return {"y_band": (y_ref - y_tol, y_ref + y_tol), "header_line": {"y_ref": y_ref, "tokens": header_tokens}, "col_bounds": col_bounds}

# -------------------- totals ("Всего") --------------------
def parse_totals_subtable(words: List[Dict[str, Any]]) -> Optional[dict]:
    lines = _words_to_lines(words)
    for idx, line in enumerate(lines):
        if not _line_text(line).startswith("Всего"):
            continue

        def stitch(nums: List[Dict[str, Any]]) -> List[Tuple[float, str]]:
            nums = sorted(nums, key=lambda w: w["x0"])
            out, buf = [], []
            for w in nums:
                if not buf or w["x0"] - buf[-1]["x1"] < 4.5:
                    buf.append(w)
                else:
                    s = clean_spaces("".join(x["text"] for x in buf))
                    xmid = 0.5 * (buf[0]["x0"] + buf[-1]["x1"])
                    out.append((xmid, s)); buf = [w]
            if buf:
                s = clean_spaces("".join(x["text"] for x in buf))
                xmid = 0.5 * (buf[0]["x0"] + buf[-1]["x1"])
                out.append((xmid, s))
            return out

        amnts = [w for w in line if re.search(r"[\d,]", w["text"])]
        if len(amnts) < 3 and idx + 1 < len(lines):
            amnts += [w for w in lines[idx + 1] if re.search(r"[\d,]", w["text"])]
        stitched = [(x, s) for (x, s) in stitch(amnts) if re.search(r"\d", s)]
        if len(stitched) >= 3:
            stitched.sort(key=lambda z: z[0])
            try:
                tot = {
                    "total_income": parse_kzt_amount(stitched[0][1]),
                    "total_expense": parse_kzt_amount(stitched[1][1]),
                    "total_commission": parse_kzt_amount(stitched[2][1]),
                }
                dprint(f"[DEBUG] Totals parsed: {tot}")
                return tot
            except Exception as e:
                dprint("[DEBUG] Totals parse failed:", e)
    return None

# -------------------- page header skip --------------------
SKIP_PHRASES = (
    "Выписка по счету", "Клиент", "ФИО:", "ИИН:", "Период выписки:",
    "Тип счета:", "Номер счета:", "Валюта счета:", "Входящий остаток:",
    "Исходящий остаток:", "Номер карточки:", "Доступная сумма",
    "Дата формирования выписки:", "Дата открытия счета:", "Дата закрытия счета:",
    "Расшифровка заблокированных сумм:", "По операциям", "По требованиям третьих лиц",
    "БИК:", "Всего"
)
def looks_like_page_header(txt: str) -> bool:
    return any(k in txt for k in SKIP_PHRASES)

# -------------------- primary extractor (header bands) --------------------
def extract_transactions_from_page(words: List[Dict[str, Any]]) -> Tuple[List[dict], Optional[dict], Dict[str, Any]]:
    debug_info = {"mode": "header", "lines_considered": 0, "row_candidates": 0, "rows_kept": 0, "skips": []}
    bands_info = find_table_header_bands(words)
    col_bounds = bands_info["col_bounds"]
    totals = parse_totals_subtable(words)

    lines = _words_to_lines(words)
    rows: List[dict] = []

    def pick_text(tokens_on_line: List[Dict[str, Any]], band: Tuple[float, float]) -> Optional[str]:
        xL, xR = band
        parts = [t["text"] for t in sorted(tokens_on_line, key=lambda z: z["x0"]) if xL <= t["x0"] < xR]
        s = clean_spaces(" ".join(parts))
        return s if s else None

    for line in lines:
        debug_info["lines_considered"] += 1
        txt = _line_text(line)
        if not txt or looks_like_page_header(txt):
            continue

        buckets: Dict[str, List[Dict[str, Any]]] = {k: [] for k in col_bounds}
        for w in line:
            for col, band in col_bounds.items():
                if band_hit(w["x0"], w["x1"], band):
                    buckets[col].append(w)
                    break

        date_posted    = pick_text(buckets.get("date_posted", []),    col_bounds.get("date_posted", (-1e9,1e9)))
        date_processed = pick_text(buckets.get("date_processed", []), col_bounds.get("date_processed", (-1e9,1e9)))
        descr          = pick_text(buckets.get("description", []),    col_bounds["description"]) if "description" in col_bounds else None
        amount         = pick_text(buckets.get("amount", []),         col_bounds.get("amount", (-1e9,1e9)))
        currency       = pick_text(buckets.get("currency", []),       col_bounds.get("currency", (-1e9,1e9)))
        credit         = pick_text(buckets.get("credit", []),         col_bounds.get("credit", (-1e9,1e9)))
        debit          = pick_text(buckets.get("debit", []),          col_bounds.get("debit", (-1e9,1e9)))
        fee            = pick_text(buckets.get("fee", []),            col_bounds.get("fee", (-1e9,1e9)))
        account        = pick_text(buckets.get("account", []),        col_bounds.get("account", (-1e9,1e9)))

        # wrapped description
        if not (is_date(date_posted) and is_date(date_processed)):
            if descr and rows:
                rows[-1]["Описание операции"] = clean_spaces(rows[-1]["Описание операции"] + " " + descr)
            continue

        debug_info["row_candidates"] += 1
        if not descr or not currency or not account:
            debug_info["skips"].append({"y": round(line[0]["top"],1), "reason": f"missing fields descr={bool(descr)} curr={bool(currency)} acc={bool(account)}", "txt": txt[:160]})
            continue

        def maybe_num(s: Optional[str]) -> float:
            try: return parse_kzt_amount(s)
            except: return 0.0

        amount_v = maybe_num(amount)
        credit_v = maybe_num(credit)
        debit_v  = maybe_num(debit)
        fee_v    = maybe_num(fee)
        if math.isclose(amount_v, 0.0, abs_tol=1e-9):
            amount_v = credit_v - debit_v - fee_v

        row = {
            "Дата проведения операции": to_date_iso(date_posted),
            "Дата обработки операции": to_date_iso(date_processed),
            "Описание операции": descr,
            "Сумма операции": round(amount_v, 2),
            "Валюта операции": currency,
            "Приход в валюте счета": round(credit_v, 2),
            "Расход в валюте счета": round(debit_v, 2),
            "Комиссия": round(fee_v, 2),
            "№ карточки/счета": account,
        }
        rows.append(row); debug_info["rows_kept"] += 1

    return rows, totals, debug_info

# -------------------- fallback extractor (two-dates pattern) --------------------
def _stitch_numeric_tokens(tokens: List[Dict[str, Any]], gap: float = 5.2) -> List[Tuple[float,str]]:
    nums = [t for t in tokens if NUM_TOKEN.match(t["text"])]
    nums = sorted(nums, key=lambda w: w["x0"])
    out, buf = [], []
    for w in nums:
        if not buf or w["x0"] - buf[-1]["x1"] <= gap:
            buf.append(w)
        else:
            s = clean_spaces("".join(x["text"] for x in buf))
            xmid = 0.5 * (buf[0]["x0"] + buf[-1]["x1"])
            out.append((xmid, s)); buf = [w]
    if buf:
        s = clean_spaces("".join(x["text"] for x in buf))
        xmid = 0.5 * (buf[0]["x0"] + buf[-1]["x1"])
        out.append((xmid, s))
    return out

def fallback_extract_transactions_from_page(words: List[Dict[str, Any]]) -> Tuple[List[dict], Dict[str, Any]]:
    debug_info = {
        "mode": "fallback",
        "lines_considered": 0,
        "row_candidates": 0,
        "rows_kept": 0,
        "skips": [],
        "first_hits": [],
    }

    lines = _words_to_lines(words)
    rows: List[dict] = []
    pending_prefix = ""  # строки типа "Коммунальное гос. учреждение ..."

    for line in lines:
        debug_info["lines_considered"] += 1
        txt_line = _line_text(line)
        if not txt_line:
            continue

        toks = sorted(line, key=lambda z: z["x0"])
        texts = [t["text"] for t in toks]

        # --- строки без дат: либо префикс к след. операции, либо хвост к предыдущей ---
        date_ix = [i for i, t in enumerate(texts) if is_date(t)]
        if len(date_ix) < 2:
            # нет двух дат → это не транзакционная строка
            low = txt_line.lower()

            # хвост для предыдущей строки: банкомат / терминал и т.п.
            if ("банкомат" in low or "pos" in low or "терминал" in low) and rows:
                rows[-1]["Описание операции"] = clean_spaces(
                    rows[-1]["Описание операции"] + " " + txt_line
                )
                continue

            # префикс к следующей операции: имя контрагента
            if any(ch.isalpha() for ch in txt_line) and not CURRENCY_RE.search(txt_line):
                pending_prefix = clean_spaces((pending_prefix + " " + txt_line).strip())
                continue

            continue  # остальное игнорируем

        # --- тут уже реальная строка с двумя датами ---
        i1, i2 = date_ix[0], date_ix[1]
        date_posted, date_processed = texts[i1], texts[i2]
        if not (is_date(date_posted) and is_date(date_processed)):
            continue

        after = toks[i2 + 1:]
        if not after:
            continue

        # найти валюту и номер карты/счёта
        currency_idx = next((k for k, u in enumerate(after) if CURRENCY_RE.match(u["text"])), None)
        account_idx = None
        for k in range(len(after) - 1, -1, -1):
            if CARD_RE.match(after[k]["text"]):
                account_idx = k
                break

        if currency_idx is None or account_idx is None or account_idx <= currency_idx:
            debug_info["skips"].append({
                "y": round(line[0]["top"], 1),
                "reason": "no currency/account in right order",
                "txt": txt_line[:160],
            })
            continue

        stitched = _stitch_numeric_tokens(after)
        x_currency = after[currency_idx]["x0"]
        x_account = after[account_idx]["x0"]

        left_nums = [(x, s) for (x, s) in stitched if x < x_currency]
        mid_nums  = [(x, s) for (x, s) in stitched if x_currency < x < x_account]
        left_nums.sort(key=lambda z: z[0])
        mid_nums.sort(key=lambda z: z[0])

        amount_s = left_nums[-1][1] if left_nums else None
        credit_s = mid_nums[0][1] if len(mid_nums) >= 1 else None
        debit_s  = mid_nums[1][1] if len(mid_nums) >= 2 else None
        fee_s    = mid_nums[2][1] if len(mid_nums) >= 3 else None

        def maybe_num(s: Optional[str]) -> float:
            try:
                return parse_kzt_amount(s)
            except Exception:
                return 0.0

        amount_v = maybe_num(amount_s)
        credit_v = maybe_num(credit_s)
        debit_v  = maybe_num(debit_s)
        fee_v    = maybe_num(fee_s)
        if math.isclose(amount_v, 0.0, abs_tol=1e-9):
            amount_v = credit_v + debit_v + fee_v

        currency = after[currency_idx]["text"]
        account  = after[account_idx]["text"]

        # --- Описание операции: префикс (контрагент) + базовое описание ---
        cut_x = after[currency_idx]["x0"]
        descr_tokens = [
            t["text"]
            for t in after
            if t["x0"] < cut_x
            and not NUM_TOKEN.match(t["text"])
            and not CURRENCY_RE.match(t["text"])
        ]
        base_descr = clean_spaces(" ".join(descr_tokens))
        if pending_prefix:
            descr = clean_spaces(pending_prefix + " " + base_descr) if base_descr else pending_prefix
        else:
            descr = base_descr
        pending_prefix = ""

        row = {
            "Дата проведения операции": date_posted,
            "Дата обработки операции": date_processed,
            "Описание операции": descr,
            "Сумма операции": amount_v,
            "Валюта операции": currency,
            "Приход в валюте счета": credit_v,
            "Расход в валюте счета": debit_v,
            "Комиссия": fee_v,
            "№ карточки/счета": account,
        }
        rows.append(row)
        debug_info["rows_kept"] += 1
        debug_info["first_hits"].append({
            "y": round(line[0]["top"], 1),
            "date_posted": date_posted,
            "date_processed": date_processed,
            "descr": descr,
            "amount": amount_s,
            "credit": credit_s,
            "debit": debit_s,
            "fee": fee_s,
            "currency": currency,
            "account": account,
        })

    debug_info["row_candidates"] = len(rows)
    return rows, debug_info

# -------------------- high level --------------------
def parse_halyk_transactions(pages: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, Optional[dict]]:
    all_rows: List[dict] = []
    found_totals: Optional[dict] = None

    def page_words(pg: Dict[str, Any]) -> List[Dict[str, Any]]:
        # common keys
        for k in ("words", "page_words", "items", "tokens", "spans", "chars", "layout_words"):
            if k in pg and isinstance(pg[k], list):
                return _normalize_words(pg[k])
        for k in ("page", "data"):
            if k in pg and isinstance(pg[k], dict):
                for kk in ("words", "page_words", "items", "tokens", "spans", "chars"):
                    if kk in pg[k] and isinstance(pg[k][kk], list):
                        return _normalize_words(pg[k][kk])
        if isinstance(pg, list) and pg:
            return _normalize_words(pg)
        raise RuntimeError(f"No words list found in page (keys: {list(pg.keys())[:10]})")

    for pidx, pg in enumerate(pages):
        w = page_words(pg)
        if DEBUG:
            # dump a small CSV of tokens for visual inspection
            if pidx == 0:
                try:
                    tmp = pd.DataFrame(w)[["text","x0","x1","top","bottom"]]
                    tmp.head(200).to_csv("halyk_tokens_page0_head200.csv", index=False)
                    dprint("[DEBUG] wrote halyk_tokens_page0_head200.csv")
                except Exception as e:
                    dprint("[DEBUG] token CSV write failed:", e)

        # Try header-mode
        rows = []; totals = None; dbg = {}
        try:
            rows, totals, dbg = extract_transactions_from_page(w)
            dprint(f"[DEBUG] Page {pidx}: header-mode rows={len(rows)}; {dbg}")
        except Exception as e:
            dprint(f"[DEBUG] Page {pidx}: header-mode failed → {e}")

        # Fallback if needed
        if not rows:
            fr, fdbg = fallback_extract_transactions_from_page(w)
            rows = fr
            dprint(f"[DEBUG] Page {pidx}: fallback rows={len(rows)}; {fdbg}")

        if totals and not found_totals:
            found_totals = totals
        all_rows.extend(rows)

    if not all_rows:
        raise RuntimeError("No transactions parsed")

    df = pd.DataFrame(all_rows)[[
        "Дата проведения операции",
        "Дата обработки операции",
        "Описание операции",
        "Сумма операции",
        "Валюта операции",
        "Приход в валюте счета",
        "Расход в валюте счета",
        "Комиссия",
        "№ карточки/счета",
    ]]

    return df, found_totals




