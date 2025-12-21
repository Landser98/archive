# src/kaspi_parser/utils.py
import re
from datetime import datetime
import numpy as np
import pandas as pd
import fitz
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any

SPACE_CHARS_CLASS = r"\u00A0\u202F "
AMOUNT_ROW_REGEX = re.compile(
    rf"([+\-]?\s*\(?\s*\d[\d{SPACE_CHARS_CLASS}]*[.,]\d{{2}}\s*\)?)(?:\s*[₸Т]|$)"
)

@dataclass
class RowBand:
    y_top: float
    y_bottom: float

def parse_amount(text: str) -> float:
    if not text:
        return 0.0
    s = re.sub(rf"[^\d,.\-()+{SPACE_CHARS_CLASS}]", "", text)
    s = s.replace("\u00A0"," ").replace("\u202F"," ").strip()
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()").replace(" ","").replace(",",".")
    m = re.match(r"^([+-]?)(\d+(?:\.\d+)?)$", s)
    if not m:
        s2 = re.sub(r"[^0-9.]", "", s)
        try:
            v = float(s2) if s2 else 0.0
        except:
            v = 0.0
        return -v if neg else v
    sign, num = m.groups()
    v = float(num)
    if neg or sign == "-":
        v = -v
    return v

def cluster_rows_by_y(y_values: np.ndarray, tol: float = 3.0) -> np.ndarray:
    order = np.argsort(y_values)
    groups = np.full_like(y_values, -1, dtype=int)
    if y_values.size == 0:
        return groups
    gid = 0
    groups[order[0]] = gid
    for idx in order[1:]:
        prev_idx = np.where(groups == gid)[0][-1]
        if abs(y_values[idx] - y_values[prev_idx]) <= tol:
            groups[idx] = gid
        else:
            gid += 1
            groups[idx] = gid
    return groups

def _safe_dt(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse PDF date strings like 'D:20251029120000+05\'00\''."""
    if not dt_str:
        return None
    s = dt_str.strip()
    if s.startswith("D:"):
        s = s[2:]
    s = re.sub(r"[+\-]\d{2}'?\d{2}'?$", "", s)
    s_digits = re.sub(r"\D", "", s)
    fmts = ["%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y%m%d%H", "%Y%m%d"]
    for f in fmts:
        try:
            return datetime.strptime(s_digits[:len(datetime.now().strftime(f))], f)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None


def to_ddmmy_date(s: str) -> Optional[datetime]:
    """
    Parse '01.09.24' or '01.09.2024' to datetime or None.
    """
    for fmt in ("%d.%m.%y", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None
