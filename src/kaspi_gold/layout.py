# src/kaspi_parser/layout.py

from typing import Dict, List, Tuple, Any, Set
import numpy as np
import pandas as pd
import fitz
import re

from src.kaspi_gold.utils import RowBand, cluster_rows_by_y, parse_amount, AMOUNT_ROW_REGEX

TRUSTED_FONTS = {
    "arialmt",
    "arial-boldmt",
}


def get_header_spans(page: fitz.Page) -> Dict[str, fitz.Rect]:
    """
    Detect header column titles ('дата', 'сумма', 'операция', 'детали').
    Returns a dict of keyword -> bounding box Rect.
    If 'операция'+'детали' appear in a single span, we store it under 'оп_дет'.
    """
    found: Dict[str, fitz.Rect] = {}
    d = page.get_text("dict")

    for b in d.get("blocks", []):
        if b.get("type") != 0:
            continue
        for ln in b.get("lines", []):
            for sp in ln.get("spans", []):
                txt = (sp.get("text") or "").strip().lower()
                if not txt:
                    continue
                x0, y0, x1, y1 = sp["bbox"]
                r = fitz.Rect(x0-3, y0-3, x1+3, y1+3)

                if txt == "дата":
                    found["дата"] = r
                elif txt == "сумма":
                    found["сумма"] = r
                elif txt == "операция":
                    found["операция"] = r
                elif txt == "детали":
                    found["детали"] = r
                elif "операц" in txt and "детал" in txt:
                    # merged header "Операция     Детали"
                    found["оп_дет"] = r

    return found


def derive_column_limits(page: fitz.Page, header_rects: Dict[str, fitz.Rect]) -> List[float]:
    """
    Infer horizontal split boundaries (x positions) between logical table columns:
    [split_date_sum, split_sum_op, split_op_det, big_guard]

    Case A: We have 'операция' and 'детали' separately → easy.
    Case B: We only have merged 'оп_дет' → estimate boundary inside that block
            using actual body spans under that header area.
    """
    W = page.rect.width

    if not header_rects.get("дата") or not header_rects.get("сумма"):
        # fallback generic splits
        return [W * 0.25, W * 0.45, W * 0.65, 1e6]

    x_date = header_rects["дата"].x0
    x_sum = header_rects["сумма"].x0

    # Case A: we have both 'операция' and 'детали'
    if "операция" in header_rects and "детали" in header_rects:
        x_op = header_rects["операция"].x0
        x_det = header_rects["детали"].x0
        return [
            (x_date + x_sum) / 2.0,
            (x_sum + x_op) / 2.0,
            (x_op + x_det) / 2.0,
            1e6,
        ]

    # Case B: only merged block 'оп_дет'
    opdet = header_rects.get("оп_дет")
    if not opdet:
        return [W * 0.25, W * 0.45, W * 0.65, 1e6]

    s_date_sum = (x_date + x_sum) / 2.0
    s_sum_opdet = (x_sum + opdet.x0) / 2.0

    # We now want to guess internal split between 'operation' and 'details'.
    # Strategy:
    # - get all body spans whose x0 lies inside the opdet block region (under the header)
    # - collect unique x0 positions
    # - find the biggest gap >= 40px → assume that's the boundary between subcolumns
    d = page.get_text("dict")
    x_candidates = []
    header_y = opdet.y1

    for b in d.get("blocks", []):
        if b.get("type") != 0:
            continue
        for ln in b.get("lines", []):
            for sp in ln.get("spans", []):
                sz = float(sp.get("size", 0))
                if not (8.5 <= sz <= 11.5):
                    continue
                x0, y0, x1, y1 = sp["bbox"]

                # below header row only
                if y0 <= header_y:
                    continue

                # must start roughly inside that right-side block
                if x0 >= opdet.x0 - 5 and x0 <= opdet.x1 + 5:
                    x_candidates.append(x0)

    x_candidates = np.unique(np.round(x_candidates, 1))
    x_candidates.sort()

    if x_candidates.size >= 4:
        gaps = np.diff(x_candidates)
        mask = gaps >= 40.0  # ignore tiny gaps between words
        if mask.any():
            i = np.argmax(gaps * mask)
            s_op_det = (x_candidates[i] + x_candidates[i + 1]) / 2.0
        else:
            s_op_det = opdet.x0 + 0.6 * (opdet.x1 - opdet.x0)
    else:
        s_op_det = opdet.x0 + 0.6 * (opdet.x1 - opdet.x0)

    return [s_date_sum, s_sum_opdet, s_op_det, 1e6]


def collect_table_spans(page: fitz.Page) -> List[Dict[str, Any]]:
    """
    Collect "body" spans that look like transaction table rows.
    We include spans whose font size is in [8.5, 11.5] and non-empty text.

    Returns list of dicts:
      {
        "text": str,
        "x0": float, "y0": float,
        "x1": float, "y1": float,
      }
    """
    spans_out: List[Dict[str, Any]] = []

    d = page.get_text("dict")
    for b in d.get("blocks", []):
        if b.get("type") != 0:
            continue
        for ln in b.get("lines", []):
            for sp in ln.get("spans", []):
                sz = float(sp.get("size", 0))
                if not (8.5 <= sz <= 11.5):
                    continue

                raw_txt = sp.get("text") or ""
                txt = raw_txt.replace("\u00A0", " ").replace("\u202F", " ").strip()
                if not txt:
                    continue

                x0, y0, x1, y1 = sp["bbox"]
                spans_out.append({
                    "text": txt,
                    "x0": x0,
                    "y0": y0,
                    "x1": x1,
                    "y1": y1,
                })

    return spans_out


def detect_icon_bands(page: fitz.Page) -> List[Tuple[float, float]]:
    """
    Detect the vertical bands of small icons (clock, etc.).
    We look at rawdict blocks with type==1 and size <=35x35.
    Returns list of (y0, y1) bands that indicate "clock icon present here".
    """
    icon_bands: List[Tuple[float, float]] = []

    raw = page.get_text("rawdict")
    for blk in raw.get("blocks", []):
        if blk.get("type") == 1 and "bbox" in blk:
            x0, y0, x1, y1 = blk["bbox"]
            if (x1 - x0) <= 35 and (y1 - y0) <= 35:
                icon_bands.append((y0, y1))

    return icon_bands


def assign_cols(df: pd.DataFrame, splits: List[float]) -> pd.DataFrame:
    """
    Assign each span to a logical column ID (0=date, 1=amount, 2=operation, 3=details)
    using the x-split boundaries.
    Mutates df by adding a 'col_id' column.
    """
    def _pick_col(x: float) -> int:
        if x < splits[0]:
            return 0
        if x < splits[1]:
            return 1
        if x < splits[2]:
            return 2
        return 3

    df["col_id"] = df["x0"].apply(_pick_col)
    return df


def build_row_bands(df: pd.DataFrame) -> Dict[int, RowBand]:
    """
    For each row_id group in df, compute its vertical band (y_top, y_bottom).
    Returns {row_id: RowBand(...)}.
    """
    out: Dict[int, RowBand] = {}
    for rid, g in df.groupby("row_id"):
        out[rid] = RowBand(
            y_top=float(g["y0"].min()),
            y_bottom=float(g["y1"].max()),
        )
    return out


def find_clock_rows(row_bands: Dict[int, RowBand],
                    icon_bands: List[Tuple[float, float]]) -> Set[int]:
    """
    If row vertical band overlaps with an icon band, we mark that row_id as "clock row".
    Returns a set of row_ids.
    """
    clock_rows: Set[int] = set()
    if not icon_bands:
        return clock_rows

    for rid, band in row_bands.items():
        for (iy0, iy1) in icon_bands:
            # overlap check with a small tolerance
            if not (iy1 < band.y_top - 2.0 or iy0 > band.y_bottom + 2.0):
                clock_rows.add(rid)
                break

    return clock_rows



def rebuild_transactions_from_page(
    df: pd.DataFrame,
    page_index: int,
    clock_rows: Set[int],
) -> List[Dict[str, Any]]:
    """
    Reconstruct structured transaction rows from the span dataframe for the page.
    Steps:
    - group by row_id
    - merge texts within each logical column (0=date,1=amount,2=operation,3=details)
    - keep only rows where col 0 looks like a date dd.mm.yy or dd.mm.yyyy
    - parse amount
    - fallback parse if amount is 0.0
    - split operation/details if glued
    """
    tx_rows: List[Dict[str, Any]] = []

    for rid, g in df.groupby("row_id"):
        # collect cell text by column
        cells = {
            cid: " ".join(gg.sort_values("x0")["text"].tolist()).strip()
            for cid, gg in g.groupby("col_id")
        }

        date_str = cells.get(0, "")

        # 1. must have a date in the first col
        if not isinstance(date_str, str) or not date_str:
            continue

        # strictly require dd.mm.yy or dd.mm.yyyy
        if not re.match(r"^\d{2}\.\d{2}\.\d{2,4}$", date_str):
            continue

        amount_text = cells.get(1, "")
        operation   = cells.get(2, "")
        details     = cells.get(3, "")

        # 2. parse numeric amount
        amt_val = parse_amount(amount_text)

        # 3. fallback if amount was 0.0 but maybe it's hiding in operation/details
        if amt_val == 0.0:
            whole = "  ".join([amount_text, operation, details])
            m = AMOUNT_ROW_REGEX.search(whole)
            if m:
                amount_text = m.group(1)
                amt_val = parse_amount(amount_text)

        # 4. Sometimes operation+details are glued into col 2.
        #    ex: "Перевод  Kaspi Pay QWERTY  +1 000 ₸"
        if operation and not details:
            parts = re.split(r"\s{2,}", operation, maxsplit=1)
            if len(parts) == 2:
                operation, details = parts[0], parts[1]

        tx_rows.append({
            "page": page_index,
            "date": date_str,
            "amount": amt_val,
            "operation": operation or "",
            "details": details or "",
            "clock_icon": bool(rid in clock_rows),
            "amount_text": amount_text,
        })

    return tx_rows
# ---------- FONT REGION ANALYSIS FOR VISUAL CHECKS ----------

def collect_span_info(doc: fitz.Document) -> pd.DataFrame:
    """
    Collect font info (font, size), bbox, text for every text span in every page.
    We'll use this for font-family consistency / tampering checks.

    Returns a DataFrame with columns:
      ['page','x0','y0','x1','y1','font','size','text']
    """
    rows: List[Dict[str, Any]] = []

    for page_idx, page in enumerate(doc):
        d = page.get_text("dict")
        for b in d.get("blocks", []):
            if b.get("type") != 0:
                continue  # skip images
            for ln in b.get("lines", []):
                for sp in ln.get("spans", []):
                    raw_txt = sp.get("text") or ""
                    txt = raw_txt.strip()
                    if not txt:
                        continue
                    x0, y0, x1, y1 = sp["bbox"]
                    rows.append({
                        "page": page_idx,
                        "x0": x0,
                        "y0": y0,
                        "x1": x1,
                        "y1": y1,
                        "font": str(sp.get("font", "")).strip().lower(),
                        "size": float(sp.get("size", 0.0)),
                        "text": txt,
                    })

    if not rows:
        return pd.DataFrame(
            columns=["page","x0","y0","x1","y1","font","size","text"]
        )

    return pd.DataFrame(rows)


def define_regions(first_page: fitz.Page) -> Dict[str, Tuple[float, float]]:
    """
    Very rough heuristic for vertical zones on the first page:
      - header      (top ~25%)
      - summary     (next ~35%)
      - tx_table    (bottom ~40%)

    Returns {region_name: (y_start, y_end)}.
    """
    h = float(first_page.rect.height)

    header_max  = 0.25 * h
    summary_max = 0.60 * h

    return {
        "header":   (0.0,        header_max),
        "summary":  (header_max, summary_max),
        "tx_table": (summary_max, h + 10.0),
    }


def analyze_regions(
    span_df: pd.DataFrame,
    regions: Dict[str, Tuple[float, float]]
) -> Dict[str, Dict[str, Any]]:
    """
    For each named region ('header','summary','tx_table'), compute:
      - fonts: set of font family names in that band
      - sizes_body: set of rounded body font sizes in [8.5, 11.5]
    We mostly use page 0 (first page), because that's where scammers edit totals.
    """
    out: Dict[str, Dict[str, Any]] = {}

    first_page_spans = span_df[span_df["page"] == 0].copy()
    if first_page_spans.empty:
        # fallback (1-page pdf or weird pdf)
        first_page_spans = span_df.copy()

    for region_name, (y_start, y_end) in regions.items():
        region_spans = first_page_spans[
            (first_page_spans["y0"] >= y_start) &
            (first_page_spans["y0"] <  y_end)
        ].copy()

        fonts_set = set()
        sizes_set = set()

        for _, r in region_spans.iterrows():
            f = r["font"].lower().strip()
            s = float(r["size"])
            fonts_set.add(f)
            if 8.5 <= s <= 11.5:
                sizes_set.add(round(s, 1))

        out[region_name] = {
            "fonts": fonts_set,
            "sizes_body": sizes_set,
        }

    return out


def normalize_font_name(font_name: str) -> str:
    """
    Remove subset prefixes like 'ABCDEE+' from font names and lowercase.
    Example:
      'ABCDEE+ArialMT' -> 'arialmt'
      'Arial-BoldMT'   -> 'arial-boldmt'
    """
    f = font_name.strip().lower()
    if "+" in f:
        f = f.split("+", 1)[1]
    return f
