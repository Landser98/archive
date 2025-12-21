#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

# ---------------- Config ----------------
FINAL_COLS = [
    "Дата операции",
    "Дата отражения на счете",
    "Описание операции",
    "Сумма в валюте операции",
    "Сумма в KZT",
    "Комиссия, KZT",
    "Cashback, KZT",
]

# Numbers like "1 234 567,89", "−4 000,00", "+0,01"
AMT_RE  = re.compile(r"[+\-−]?\d{1,3}(?:[ \u00A0\u202F]?\d{3})*(?:[.,]\d{2})")
# Dates: dd.mm.yyyy or yyyy-mm-dd with optional time " HH:MM(:SS)?" or "T.."
DATE_RE = re.compile(r"^\s*(?:\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2})(?:[ T]\d{2}:\d{2}(?::\d{2})?)?")

# --------------- Helpers ----------------
def _norm_spaces_basic(s: str) -> str:
    if s is None:
        return ""
    return (str(s)
            .replace("\u00A0", " ")
            .replace("\u202F", " ")
            .replace("\r", " ")
            .replace("\t", " ")
            .strip())

def _norm_desc_keep_newlines(s: str) -> str:
    if s is None:
        return ""
    lines = str(s).split("\n")
    lines = [_norm_spaces_basic(x) for x in lines]
    out = []
    for ln in lines:
        if ln == "" and (not out or out[-1] == ""):
            continue
        out.append(ln)
    return "\n".join(out).strip()

def _norm_minus(s: str) -> str:
    return s.replace("−", "-") if isinstance(s, str) else s

def _to_float(val: Optional[str]):
    if val is None:
        return None
    s = _norm_minus(_norm_spaces_basic(val))
    m = AMT_RE.search(s)
    if not m:
        return None
    num = (m.group(0)
           .replace(" ", "")
           .replace("\u00A0", "")
           .replace("\u202F", "")
           .replace(",", "."))
    try:
        return float(num)
    except Exception:
        return None

def _is_date_like(s: str) -> bool:
    return bool(DATE_RE.match(str(s).strip())) if s else False

# ---------- Row shaping (from Camelot df) ----------
def _find_amount_indices(cells: List[str]) -> List[int]:
    """Pick up to 4 right-most amount-like cells (by regex)."""
    idxs = []
    for i in range(len(cells) - 1, -1, -1):
        c = _norm_minus(_norm_spaces_basic(cells[i]))
        if AMT_RE.search(c):
            idxs.append(i)
            if len(idxs) == 4:
                break
    return sorted(idxs)

def _coerce_final(df_any: pd.DataFrame) -> pd.DataFrame:
    """
    For each raw row from Camelot:
      - detect the 4 right-most amount-like cells,
      - description is everything between col2 and the first amount index,
      - first two columns (dates) are taken as-is.
    """
    ncols = df_any.shape[1]
    out_rows = []

    for _, r in df_any.iterrows():
        cells = ["" if pd.isna(r[i]) else str(r[i]) for i in range(ncols)]
        if ncols < 7:
            cells += [""] * (7 - ncols)

        amt_idxs = _find_amount_indices(cells)
        first_amt_idx = amt_idxs[0] if amt_idxs else ncols

        d1 = _norm_spaces_basic(cells[0])
        d2 = _norm_spaces_basic(cells[1])

        # Description = cells [2 : first_amt_idx)
        desc_cells = cells[2:first_amt_idx]
        desc_raw = " ".join(_norm_desc_keep_newlines(c) for c in desc_cells if _norm_spaces_basic(c))
        desc = _norm_desc_keep_newlines(desc_raw)

        amt_vals = [None, None, None, None]
        for j, idx in enumerate(amt_idxs[:4]):
            amt_vals[j] = _to_float(cells[idx])

        out_rows.append({
            "Дата операции": d1,
            "Дата отражения на счете": d2,
            "Описание операции": desc,
            "Сумма в валюте операции": amt_vals[0],
            "Сумма в KZT": amt_vals[1],
            "Комиссия, KZT": amt_vals[2],
            "Cashback, KZT": amt_vals[3],
        })

    out = pd.DataFrame(out_rows, columns=FINAL_COLS)

    # Drop header lines if Camelot captured them
    def _is_header_row(r):
        a = str(r["Дата операции"]).lower()
        b = str(r["Дата отражения на счете"]).lower()
        c = str(r["Описание операции"]).lower()
        return (a.startswith("дата операции") or b.startswith("дата отражения") or c.startswith("описание операции"))

    out = out.loc[~out.apply(_is_header_row, axis=1)].reset_index(drop=True)
    return out

def _merge_multiline_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Start a new logical record only if BOTH first columns look like dates.
    Otherwise treat as continuation: append description with '\n'.
    Backfill amounts if they appear on continuation lines.
    """
    rows = []
    current = None

    for _, r in df.iterrows():
        d1 = r["Дата операции"]
        d2 = r["Дата отражения на счете"]
        is_start = _is_date_like(d1) and _is_date_like(d2)

        if is_start:
            if current is not None:
                rows.append(current)
            current = r.copy()
        else:
            if current is None:
                current = r.copy()
            else:
                tail = str(r["Описание операции"]).strip()
                if tail:
                    if str(current["Описание операции"]).strip():
                        current["Описание операции"] = str(current["Описание операции"]).rstrip() + "\n" + tail
                    else:
                        current["Описание операции"] = tail
                for col in FINAL_COLS[3:]:
                    if pd.isna(current[col]) and not pd.isna(r[col]):
                        current[col] = r[col]

    if current is not None:
        rows.append(current)

    out = pd.DataFrame(rows, columns=FINAL_COLS)
    for col in FINAL_COLS[3:]:
        out[col] = out[col].map(_to_float)
    keep = ~(out["Описание операции"].eq("") & out[FINAL_COLS[3:]].isna().all(axis=1))
    return out.loc[keep].reset_index(drop=True)

# ---------- Calibration with pdfplumber ----------
def suggest_layout_with_pdfplumber(pdf_path: str, page_no: int = 1) -> Tuple[str, str]:
    """
    Heuristically suggest (table_areas, columns) for Camelot(stream) by scanning the header row.
    Returns (area_str, columns_str) ready for Camelot.
    """
    import pdfplumber

    header_keywords = [
        "Дата операции", "Дата отражения", "Описание",
        "Сумма в валюте", "Сумма в KZT", "Комиссия", "Cashback",
    ]

    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[page_no - 1]
        W, H = page.width, page.height
        words = page.extract_words(use_text_flow=True, keep_blank_chars=False)
        if not words:
            raise RuntimeError("No words found; page may be image-only (needs OCR).")

        # bin by y-center to find the header band
        buckets = {}
        for w in words:
            y = (w["top"] + w["bottom"]) / 2
            ykey = round(y / 5) * 5
            buckets.setdefault(ykey, []).append(w)

        def _hits(ws):
            text = " ".join(w["text"] for w in ws).lower()
            return sum(k.lower() in text for k in header_keywords)

        # choose the band with max header keyword hits
        best = max(buckets.values(), key=_hits)
        xs = sorted((w["x0"] + w["x1"]) / 2 for w in best)
        gaps = sorted(((xs[i+1] - xs[i], i) for i in range(len(xs)-1)), reverse=True)

        # 6 separators (for 7 columns) -> midpoints of the 6 biggest gaps
        sep_ix = sorted(i for _, i in gaps[:6])
        seps = [ (xs[i] + xs[i+1]) / 2 for i in sep_ix ]

        # fallback rough cut if too few words
        if len(seps) < 6:
            seps = [W*0.12, W*0.25, W*0.60, W*0.72, W*0.82, W*0.90]

        band_top = min(w["top"] for w in best)
        band_bottom = max(w["bottom"] for w in best)
        # crop from just above header to near bottom
        y_top_pl = band_top - 6
        y_bot_pl = H - 36

        # Convert to Camelot coords (origin bottom-left)
        y1_c = H - y_top_pl
        y2_c = H - y_bot_pl
        x1_c, x2_c = 36, W - 36

        area_str = f"{x1_c:.1f},{y1_c:.1f},{x2_c:.1f},{y2_c:.1f}"
        columns_str = ",".join(f"{x:.1f}" for x in seps)
        return area_str, columns_str

def calibrate_layout(pdf_path: str, calib_pages=(1,2,3)) -> Tuple[str, str]:
    last = None
    for p in calib_pages:
        try:
            return suggest_layout_with_pdfplumber(pdf_path, page_no=p)
        except Exception as e:
            last = e
            continue
    raise RuntimeError(f"Calibration failed on pages {calib_pages}. Last error: {last}")

# ---------- Camelot parsing ----------
def parse_with_camelot_fixed(pdf_path: str, pages: str, area: str, cols: str) -> pd.DataFrame:
    import camelot

    # Try lattice first (often best if Ghostscript available)
    try:
        tables = camelot.read_pdf(
            pdf_path,
            pages=pages,
            flavor="lattice",
            table_areas=[area],
            line_scale=50,
        )
        chunks = []
        for t in tables:
            df_raw = t.df.copy()
            df_norm = _coerce_final(df_raw)
            if not df_norm.empty:
                chunks.append(df_norm)
        if chunks:
            out = pd.concat(chunks, ignore_index=True).drop_duplicates().reset_index(drop=True)
            out = _merge_multiline_rows(out)
            return out
    except Exception:
        pass  # fall through to stream

    # Stream with locked columns
    tables = camelot.read_pdf(
        pdf_path,
        pages=pages,
        flavor="stream",
        table_areas=[area],
        columns=[cols],
        row_tol=12,
        column_tol=28,
        strip_text="",     # keep newlines if present
        layout_kwargs=dict(
            char_margin=2.0,
            word_margin=0.12,
            line_margin=0.2,
            boxes_flow=0.35,
        ),
    )
    chunks = []
    for t in tables:
        df_raw = t.df.copy()
        df_norm = _coerce_final(df_raw)
        if not df_norm.empty:
            chunks.append(df_norm)
    if not chunks:
        raise RuntimeError("Camelot(stream) returned no tables with fixed columns.")
    out = pd.concat(chunks, ignore_index=True).drop_duplicates().reset_index(drop=True)
    out = _merge_multiline_rows(out)
    return out

# ------------------- CLI -------------------
def main():
    ap = argparse.ArgumentParser(description="Parse BCC statement (Camelot) into CSV with 7 columns.")
    ap.add_argument("pdf", help="Path to PDF (e.g., 'Vypiska 2.pdf').")
    ap.add_argument("-p", "--pages", default="1-end", help="Pages range, default: 1-end.")
    ap.add_argument("-o", "--output", default="parsed.csv", help="Output CSV path.")
    ap.add_argument("--debug", action="store_true", help="Print calibration info.")
    ap.add_argument("--calib-pages", default="1,2,3", help="Pages to try for calibration (e.g. '1,2,3').")
    args = ap.parse_args()

    pdf = Path(args.pdf)
    if not pdf.exists():
        raise SystemExit(f"File not found: {pdf}")

    calib_pages = tuple(int(x) for x in args.calib_pages.split(",") if x.strip())

    area, cols = calibrate_layout(str(pdf), calib_pages=calib_pages)
    if args.debug:
        print(f"[calib] area={area}")
        print(f"[calib] cols={cols}")

    df = parse_with_camelot_fixed(str(pdf), pages=args.pages, area=area, cols=cols)

    # Optional sort by dates
    try:
        df["__d1"] = pd.to_datetime(df["Дата операции"], dayfirst=True, errors="coerce")
        df["__d2"] = pd.to_datetime(df["Дата отражения на счете"], dayfirst=True, errors="coerce")
        df = df.sort_values(["__d1", "__d2"], kind="mergesort").drop(columns=["__d1", "__d2"])
    except Exception:
        pass

    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"OK: {args.output} | rows={len(df)}")

    with pd.option_context("display.max_colwidth", None):
        print(df.head(3)[["Описание операции"]].to_string(index=False))

if __name__ == "__main__":
    main()
