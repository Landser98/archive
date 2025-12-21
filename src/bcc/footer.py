#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parse footer totals and closing balance from BCC (Bank CenterCredit) statement
exported to pdfplumber-style JSONL (one JSON per page with a "words" list).

Finds:
- Totals line:  "Жиынтығы / Итого" (numbers may be on same or the next line)
  -> total_debit (first number), total_credit (second number)
- Closing bal:  "Шығыс сальдо / Исходящее сальдо: <closing_balance>"

Usage:
  python footer.py /path/to/pages.jsonl
  python footer.py /path/to/pages.jsonl --out /path/to/footer.csv
"""

import argparse
import json
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

PAGE_Y_OFFSET = 100_000.0
LINE_Y_EPS    = 0.9

# Labels (Kazakh / Russian)
TOTALS_LABEL_RE   = re.compile(
    r"(Жиынтығы\s*/\s*Итого|Итого\s*/\s*Жиынтығы|Жиынтығы|Итого)",
    re.IGNORECASE
)
CLOSING_LABEL_RE  = re.compile(r"(Шығыс\s*сальдо|Исходящее\s*сальдо)", re.IGNORECASE)

SPACES_RE = re.compile(r"\s+")

def norm(s: Optional[str]) -> str:
    return SPACES_RE.sub(" ", (s or "")).strip()

def amount_to_float(txt: Optional[str]) -> Optional[float]:
    if not txt:
        return None
    t = str(txt).replace("\xa0", "").replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(",", "")
    elif "," in t and "." not in t:
        t = t.replace(",", ".")
    t = re.sub(r"[^0-9.\-]", "", t)
    if t.count(".") > 1:
        head, _, tail = t.rpartition(".")
        head = head.replace(".", "")
        t = head + "." + tail
    try:
        return float(t) if t else None
    except Exception:
        return None

def flatten_and_sort(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    words: List[Dict[str, Any]] = []
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
            ww["_xmid"]   = 0.5 * (ww["_x0"] + ww["_x1"])
            ww["text"]    = str(ww.get("text", ""))
            words.append(ww)
    words.sort(key=lambda z: (z["_doctop"], z["_top"], z["_x0"]))
    return words

def cluster_lines(words: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    lines: List[List[Dict[str, Any]]] = []
    cur: List[Dict[str, Any]] = []
    cur_top: Optional[float] = None
    for w in words:
        t = w["_top"]
        if cur_top is None or abs(t - cur_top) <= LINE_Y_EPS:
            cur.append(w)
            if cur_top is None:
                cur_top = t
        else:
            cur.sort(key=lambda z: z["_x0"])
            lines.append(cur)
            cur = [w]
            cur_top = t
    if cur:
        cur.sort(key=lambda z: z["_x0"])
        lines.append(cur)
    return lines

def line_text(line: List[Dict[str, Any]]) -> str:
    return norm(" ".join(w["text"] for w in line))

NUM_RE = re.compile(r"[-+]?\d[\d\s.,]*$")

def numeric_tokens_right_of(line: List[Dict[str, Any]], x_threshold: float) -> List[Tuple[float, float]]:
    """
    Return list of (xmid, value) for numeric tokens whose center is to the right of x_threshold.
    """
    out: List[Tuple[float, float]] = []
    for w in line:
        if w["_xmid"] > x_threshold and NUM_RE.fullmatch(w["text"]):
            val = amount_to_float(w["text"])
            if val is not None:
                out.append((w["_xmid"], val))
    # keep left->right order
    out.sort(key=lambda z: z[0])
    return out

def parse_footer_from_lines(lines: List[List[Dict[str, Any]]]) -> Dict[str, Optional[float]]:
    """
    Scan all lines; keep the LAST found totals and closing balance.
    Totals search:
      - find label line (Жиынтығы/Итого)
      - x_label = max x of label tokens (right edge of label block)
      - collect next two numeric tokens to the RIGHT of x_label
        on the SAME line or the NEXT up to 3 lines.
      - order = (debit first, credit second).
    """
    total_debit: Optional[float] = None
    total_credit: Optional[float] = None
    closing_balance: Optional[float] = None

    for i, ln in enumerate(lines):
        txt = line_text(ln)
        if not txt:
            continue

        # ---- CLOSING BALANCE (same as before, keep last) ----
        if CLOSING_LABEL_RE.search(txt):
            nums = re.findall(r"[-+]?\d[\d\s.,]*", txt)
            if nums:
                v = amount_to_float(nums[-1])
                if v is not None:
                    closing_balance = v

        # ---- TOTALS (new robust logic) ----
        if TOTALS_LABEL_RE.search(txt):
            # x position to the right of the label block
            label_tokens = [w for w in ln if TOTALS_LABEL_RE.search(w["text"])]
            if label_tokens:
                x_label = max(w["_xmid"] for w in label_tokens)
            else:
                # fallback: use the rightmost token on label line as threshold
                x_label = max((w["_xmid"] for w in ln), default=0.0) - 200.0

            found_vals: List[float] = []

            # 1) same line
            same_line_vals = numeric_tokens_right_of(ln, x_label)
            found_vals.extend([v for _, v in same_line_vals])

            # 2) next up to 3 lines (some PDFs move numbers to next row)
            if len(found_vals) < 2:
                for k in (i+1, i+2, i+3):
                    if k >= len(lines):
                        break
                    # stop if we hit another label line (closing or totals again)
                    next_txt = line_text(lines[k])
                    if CLOSING_LABEL_RE.search(next_txt) or TOTALS_LABEL_RE.search(next_txt):
                        break
                    vals = numeric_tokens_right_of(lines[k], x_label)
                    found_vals.extend([v for _, v in vals])
                    if len(found_vals) >= 2:
                        break

            if len(found_vals) >= 2:
                # ASSUMPTION: first is debit, second is credit
                total_debit  = found_vals[0]
                total_credit = found_vals[1]
                # keep scanning to allow later pages to overwrite (we want LAST)

    return {
        "total_debit": total_debit,
        "total_credit": total_credit,
        "closing_balance": closing_balance,
    }

def parse_bcc_footer(jsonl_path: str) -> Dict[str, Optional[float]]:
    pages: List[Dict[str, Any]] = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                pages.append(json.loads(line))
    words = flatten_and_sort(pages)
    lines = cluster_lines(words)
    return parse_footer_from_lines(lines)

def main():
    ap = argparse.ArgumentParser(description="Parse BCC footer totals and closing balance from JSONL.")
    ap.add_argument("jsonl", help="Path to pdfplumber-style pages.jsonl")
    ap.add_argument("--out", help="Optional CSV output file (total_debit,total_credit,closing_balance)")
    args = ap.parse_args()

    res = parse_bcc_footer(args.jsonl)

    print({
        "total_debit": res["total_debit"],
        "total_credit": res["total_credit"],
        "closing_balance": res["closing_balance"],
    })

    if args.out:
        pd.DataFrame([res]).to_csv(args.out, index=False, encoding="utf-8")
        print(f"Saved: {args.out}")
