# -*- coding: utf-8 -*-
"""
Halyk statement footer parser (robust).

API:
    foots = parse_footers(pages)
    ft = footer_triple(foots)

Returns:
  parse_footers -> list of dicts with:
    page_index, footer_found, stamp_place, stamp_text,
    statement_line, iban, page_no, validity_line, validity_text, raw_footer_lines

  footer_triple -> dict with the final triple (from last page, with backfill):
    stamp_text, statement_line, validity_text, iban, page_no,
    last_page_index, last_page_number
"""

from __future__ import annotations
import re
from typing import List, Dict, Any, Tuple

# ---------- helpers ----------

def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("\u00A0"," ").replace("\u202F"," ").strip())

def _words_to_lines(words: List[Dict[str, Any]], y_tol: float = 3.5) -> List[List[Dict[str, Any]]]:
    if not words:
        return []
    words_sorted = sorted(words, key=lambda w: (w["top"], w["x0"]))
    lines: List[List[Dict[str, Any]]] = [[words_sorted[0]]]
    for w in words_sorted[1:]:
        if abs(w["top"] - lines[-1][0]["top"]) <= y_tol:
            lines[-1].append(w)
        else:
            lines.append([w])
    for ln in lines:
        ln.sort(key=lambda x: x["x0"])
    return lines

def _line_text(line: List[Dict[str, Any]]) -> Tuple[str, Tuple[float,float,float,float]]:
    if not line:
        return "", (0,0,0,0)
    txt = _norm_spaces(" ".join(w["text"] for w in line))
    minx = min(w["x0"] for w in line)
    maxx = max(w["x1"] for w in line)
    top = min(w["top"] for w in line)
    bottom = max(w["bottom"] for w in line)
    return txt, (minx, maxx, top, bottom)

def _page_words(pg: Dict[str, Any]) -> List[Dict[str, Any]]:
    for k in ("words", "page_words", "items", "tokens", "spans", "chars", "layout_words"):
        if k in pg and isinstance(pg[k], list):
            return [w for w in pg[k] if isinstance(w, dict) and "text" in w]
    for k in ("page", "data"):
        if k in pg and isinstance(pg[k], dict):
            inner = pg[k]
            for kk in ("words","page_words","items","tokens","spans","chars"):
                if kk in inner and isinstance(inner[kk], list):
                    return [w for w in inner[kk] if isinstance(w, dict) and "text" in w]
    if isinstance(pg, list) and pg and isinstance(pg[0], dict) and "text" in pg[0]:
        return pg
    return []

# ---------- regexes (and fallbacks) ----------

STAMP_RX = re.compile(r"(?:^|\b)место\s+печати(?:\s+банка)?(?:\b|$)", re.I)
VALID_RX = re.compile(r"выписка\s+действительна\s+при\s+наличии\s+печати\s+банка", re.I)

# primary: colon optional; IBAN (KZ + 18–30 alnums); optional trailing page number
STMT_RX = re.compile(
    r"выписка\s+по\s+счету[:\s]*([Kk][Zz][0-9A-Za-z]{18,30})\s*(\d{1,4})?\s*$",
    re.I
)

# extra-lenient IBAN finder: allow spaces inside digits, we’ll strip them
IBAN_LOOSE_RX = re.compile(r"([Kk][Zz]\s*[0-9A-Za-z](?:\s*[0-9A-Za-z]){17,35})")

# ---------- main ----------

def parse_footers(
    pages: List[Dict[str, Any]],
    bottom_fraction: float = 0.30,  # check bottom 30%
    min_footer_lines: int = 1,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for i, pg in enumerate(pages):
        words = _page_words(pg)
        if not words:
            out.append({
                "page_index": i, "footer_found": False,
                "stamp_place": False, "stamp_text": None,
                "statement_line": None, "iban": None, "page_no": None,
                "validity_line": False, "validity_text": None,
                "raw_footer_lines": [],
            })
            continue

        page_top = min(w["top"] for w in words)
        page_bottom = max(w["bottom"] for w in words)
        height = max(1.0, page_bottom - page_top)
        cutoff_y = page_bottom - bottom_fraction * height

        footer_words = [w for w in words if w["top"] >= cutoff_y]
        lines_raw = _words_to_lines(footer_words)
        norm_lines: List[str] = []
        for ln in lines_raw:
            txt, _ = _line_text(ln)
            if txt:
                norm_lines.append(txt)
        raw_lines = norm_lines[:]

        if len(norm_lines) < min_footer_lines:
            cutoff_y2 = page_bottom - (bottom_fraction + 0.06) * height
            footer_words2 = [w for w in words if w["top"] >= cutoff_y2]
            lines_raw2 = _words_to_lines(footer_words2)
            norm_lines = []
            for ln in lines_raw2:
                txt, _ = _line_text(ln)
                if txt:
                    norm_lines.append(txt)
            raw_lines = norm_lines[:]

        stamp_place = False
        stamp_text = None
        statement_line = None
        iban = None
        page_no = None
        validity_line = False
        validity_text = None

        # 1) Single-line scans
        for txt in norm_lines:
            if not stamp_place and STAMP_RX.search(txt):
                stamp_place, stamp_text = True, txt
            if not validity_line and VALID_RX.search(txt):
                validity_line, validity_text = True, txt
            if statement_line is None:
                m = STMT_RX.search(txt)
                if m:
                    iban = (m.group(1) or "").upper()
                    if m.lastindex and m.group(2):
                        try:
                            page_no = int(m.group(2))
                        except ValueError:
                            page_no = None
                    statement_line = f"Выписка по счету: {iban}"

        # 2) Two-line windows (split cases)
        def _scan_windows(lines: List[str], win: int = 2) -> None:
            nonlocal stamp_place, stamp_text, statement_line, iban, page_no
            if len(lines) < 2:
                return
            for j in range(len(lines) - 1):
                joined = _norm_spaces(lines[j] + " " + lines[j + 1])
                if not stamp_place and STAMP_RX.search(joined):
                    stamp_place, stamp_text = True, joined
                if statement_line is None:
                    mm = STMT_RX.search(joined)
                    if mm:
                        iban = (mm.group(1) or "").upper()
                        if mm.lastindex and mm.group(2):
                            try:
                                page_no = int(mm.group(2))
                            except ValueError:
                                page_no = None
                        statement_line = f"Выписка по счету: {iban}"

        _scan_windows(norm_lines, 2)

        # 3) Whole-block scan (very split/noisy)
        if statement_line is None and norm_lines:
            whole = _norm_spaces(" ".join(norm_lines))
            m = STMT_RX.search(whole)
            if m:
                iban = (m.group(1) or "").upper()
                if m.lastindex and m.group(2):
                    try:
                        page_no = int(m.group(2))
                    except ValueError:
                        page_no = None
                statement_line = f"Выписка по счету: {iban}"

        # 4) Fallback: detect phrase + loose IBAN separately
        if statement_line is None and norm_lines:
            # find a line that mentions "Выписка по счету"
            idxs = [k for k, t in enumerate(norm_lines) if re.search(r"выписка\s+по\s+счету", t, re.I)]
            if idxs:
                # try same line first
                t = norm_lines[idxs[0]]
                m1 = IBAN_LOOSE_RX.search(t)
                if not m1 and idxs[0] + 1 < len(norm_lines):
                    # try next line
                    t = _norm_spaces(norm_lines[idxs[0]] + " " + norm_lines[idxs[0] + 1])
                    m1 = IBAN_LOOSE_RX.search(t)
                if not m1:
                    # try whole block as last resort
                    whole = _norm_spaces(" ".join(norm_lines))
                    m1 = IBAN_LOOSE_RX.search(whole)
                if m1:
                    raw = m1.group(1)
                    iban_clean = _norm_spaces(raw).replace(" ", "").upper()
                    # strip trailing page digits if attached
                    # e.g., KZ76...65363 26  → take digits until non-alnum
                    mtrim = re.match(r"([KZ0-9A-Z]+)", iban_clean)
                    if mtrim:
                        iban = mtrim.group(1)
                    else:
                        iban = iban_clean
                    # capture trailing page number separately if present in nearby text
                    tail = t[m1.end():].strip()
                    mpage = re.search(r"\b(\d{1,4})\b$", tail)
                    if mpage:
                        try:
                            page_no = int(mpage.group(1))
                        except ValueError:
                            page_no = None
                    statement_line = f"Выписка по счету: {iban}"

        footer_found = bool(raw_lines) and (stamp_place or statement_line or validity_line)

        out.append({
            "page_index": i,
            "footer_found": footer_found,
            "stamp_place": stamp_place,
            "stamp_text": stamp_text,
            "statement_line": statement_line,
            "iban": iban,
            "page_no": page_no,
            "validity_line": validity_line,
            "validity_text": validity_text,
            "raw_footer_lines": raw_lines,
        })

    return out


def footer_triple(foots: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not foots:
        return {
            "stamp_text": None, "statement_line": None, "validity_text": None,
            "iban": None, "page_no": None, "last_page_index": None, "last_page_number": None,
        }

    last = foots[-1]
    stamp_text = last.get("stamp_text")
    statement_line = last.get("statement_line")
    validity_text = last.get("validity_text")
    iban = last.get("iban")
    page_no = last.get("page_no")

    # backfill missing bits from earlier pages
    if stamp_text is None:
        for item in reversed(foots):
            if item.get("stamp_text"):
                stamp_text = item["stamp_text"]; break

    if statement_line is None or iban is None:
        for item in reversed(foots):
            if item.get("statement_line") and item.get("iban"):
                statement_line = item["statement_line"]
                iban = item["iban"]
                if page_no is None and item.get("page_no"):
                    page_no = item["page_no"]
                break

    if validity_text is None:
        for item in reversed(foots):
            if item.get("validity_text"):
                validity_text = item["validity_text"]; break

    if iban:
        statement_line = f"Выписка по счету: {iban}"

    last_idx = max((f["page_index"] for f in foots), default=None)
    last_pg_no = None
    for item in reversed(foots):
        if item.get("page_no"):
            last_pg_no = item["page_no"]
            break

    return {
        "stamp_text": stamp_text,
        "statement_line": statement_line,
        "validity_text": validity_text,
        "iban": iban,
        "page_no": page_no,
        "last_page_index": last_idx,
        "last_page_number": last_pg_no,
    }



