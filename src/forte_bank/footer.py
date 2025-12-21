#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parse footer totals and closing balance from ForteBank statement exported to
pdfplumber-style JSONL (one JSON per page with a "words" list).

Finds:
- Totals label:   "Жиынтығы / Итого" or "Итого" (numbers may sit on the same
                  line or spill onto next lines). Extracts two numbers:
                  total_debit (first), total_credit (second).
- Closing balance: "Шығыс сальдо / Исходящее сальдо" (and common synonyms).

Usage:
  python footer.py /path/to/pages.jsonl
  python footer.py /path/to/pages.jsonl --out /path/to/footer.csv
"""

import argparse
import json
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

PAGE_Y_OFFSET = 100_000.0      # separate pages in Y so lines never merge
LINE_Y_EPS    = 0.9            # vertical tolerance for line clustering

# ---- Labels (Kazakh/Russian, with a few forgiving variants) ----
TOTALS_LABEL_RE = re.compile(
    r"(Жиынтығы\s*/\s*Итого|Итого\s*/\s*Жиынтығы|Жиынтығы|Итого|Всего)",
    re.IGNORECASE
)

CLOSING_LABEL_RE = re.compile(
    r"(Шығыс\s*сальдо|Исходящее\s*сальдо|Сальдо\s*на\s*конец(?:\s*периода)?|Сальдо\s*конечн\w+)",
    re.IGNORECASE
)

SPACES_RE = re.compile(r"\s+")
NUM_TOKEN_RE = re.compile(r"[-+]?\d[\d\s.,]*$")  # flexible numeric token at word-level

def norm(s: Optional[str]) -> str:
    return SPACES_RE.sub(" ", (s or "")).strip()

def amount_to_float(txt: Optional[str]) -> Optional[float]:
    """Parse '1 234 567,89' / '1,234,567.89' -> float. Returns None if empty/bad."""
    if not txt:
        return None
    t = str(txt).replace("\xa0", "").replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(",", "")              # assume comma = thousands
    elif "," in t and "." not in t:
        t = t.replace(",", ".")             # EU decimal comma
    t = re.sub(r"[^0-9.\-]", "", t)
    if t.count(".") > 1:
        head, _, tail = t.rpartition(".")
        head = head.replace(".", "")
        t = head + "." + tail
    try:
        return float(t) if t else None
    except Exception:
        return None

# ---------- Geometry helpers ----------
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

# ---------- Numeric harvesting to the right of a label X ----------
def numeric_tokens_right_of(line: List[Dict[str, Any]], x_threshold: float) -> List[Tuple[float, float]]:
    """
    Return list of (xmid, value) for numeric tokens whose center is to the right of x_threshold.
    Ordered left->right by xmid.
    """
    out: List[Tuple[float, float]] = []
    for w in line:
        if w["_xmid"] > x_threshold and NUM_TOKEN_RE.fullmatch(w["text"]):
            val = amount_to_float(w["text"])
            if val is not None:
                out.append((w["_xmid"], val))
    out.sort(key=lambda z: z[0])
    return out

# ---------- Main footer scan ----------
def parse_footer_from_lines(lines: List[List[Dict[str, Any]]]) -> Dict[str, Optional[float]]:
    """
    Keep the LAST totals/closing balance found (in case of multi-page statements).
    Totals detection:
      - Find a line with the totals label (Жиынтығы/Итого/Всего).
      - Measure x_label = rightmost center among label tokens on that line.
      - Collect the next two numeric tokens to the RIGHT of x_label:
        first -> total_debit, second -> total_credit.
      - If not enough on the same line, keep scanning the next up to 4 lines,
        but stop if another label line appears.
    Closing balance detection:
      - Find a line with CLOSING_LABEL_RE; take the last number on that line.
    """
    total_debit: Optional[float] = None
    total_credit: Optional[float] = None
    closing_balance: Optional[float] = None

    for i, ln in enumerate(lines):
        txt = line_text(ln)
        if not txt:
            continue

        # ---- Closing balance (keep last) ----
        if CLOSING_LABEL_RE.search(txt):
            nums = re.findall(r"[-+]?\d[\d\s.,]*", txt)
            if nums:
                v = amount_to_float(nums[-1])
                if v is not None:
                    closing_balance = v

        # ---- Totals (robust, cross-line) ----
        if TOTALS_LABEL_RE.search(txt):
            # right edge of the label block
            label_tokens = [w for w in ln if TOTALS_LABEL_RE.search(w["text"])]
            if label_tokens:
                x_label = max(w["_xmid"] for w in label_tokens)
            else:
                # fallback: somewhere near the middle-right of the line
                x_label = max((w["_xmid"] for w in ln), default=0.0) - 200.0

            found_vals: List[float] = []

            # 1) Same line
            same_line_vals = numeric_tokens_right_of(ln, x_label)
            found_vals.extend([v for _, v in same_line_vals])

            # 2) Next up to 4 lines (Forte sometimes breaks numbers to a new line)
            k = i + 1
            while len(found_vals) < 2 and k < len(lines) and k <= i + 4:
                next_txt = line_text(lines[k])
                # Stop if we hit another label line (don't cross into other sections)
                if TOTALS_LABEL_RE.search(next_txt) or CLOSING_LABEL_RE.search(next_txt):
                    break
                vals = numeric_tokens_right_of(lines[k], x_label)
                found_vals.extend([v for _, v in vals])
                k += 1

            # Assign if we got 2 numbers
            if len(found_vals) >= 2:
                total_debit  = found_vals[0]
                total_credit = found_vals[1]
                # do not return yet; keep last occurrence across pages

    return {
        "total_debit": total_debit,
        "total_credit": total_credit,
        "closing_balance": closing_balance,
    }

def parse_forte_footer(jsonl_path: str) -> Dict[str, Optional[float]]:
    # Load pages
    pages: List[Dict[str, Any]] = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                pages.append(json.loads(line))
    words = flatten_and_sort(pages)
    lines = cluster_lines(words)
    return parse_footer_from_lines(lines)
