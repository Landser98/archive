# src/kaspi_parser/checks_meta.py
from typing import Dict, Any, Optional, List
import fitz
from src.kaspi_gold.utils import _safe_dt
from datetime import datetime

def extract_pdf_meta(doc: fitz.Document) -> Dict[str, Any]:
    """
    Collect PDF metadata and apply Kaspi-specific authenticity logic.
    For genuine Kaspi statements:
      - metadata is usually empty
      - no XMP section
      - no producer/creator strings
    Therefore, we flag *presence* of such data as suspicious.
    """
    meta = doc.metadata or {}
    author        = meta.get("author", "") or meta.get("Author", "")
    creator       = meta.get("creator", "") or meta.get("Creator", "")
    producer      = meta.get("producer", "") or meta.get("Producer", "")
    creation_date = meta.get("creationDate", "") or meta.get("CreationDate", "")
    mod_date      = meta.get("modDate", "") or meta.get("ModDate", "")

    dt_created = _safe_dt(creation_date)
    dt_mod     = _safe_dt(mod_date)

    flags = []
    debug = {}

    # === 1. Extra metadata presence (suspicious for genuine Kaspi PDFs) ===
    non_empty_meta_fields = [v for v in [author, creator, producer, creation_date, mod_date] if v]
    if len(non_empty_meta_fields) > 0:
        flags.append("HAS_METADATA_FIELDS")
        debug["HAS_METADATA_FIELDS"] = {
            "author": author,
            "creator": creator,
            "producer": producer,
            "creationDate": creation_date,
            "modDate": mod_date,
        }

    # === 2. If XMP XML exists -> definitely re-exported or edited ===
    try:
        xmp_data = doc.xmp_metadata
        if xmp_data and len(xmp_data.strip()) > 0:
            flags.append("HAS_XMP_DATA")
            debug["HAS_XMP_DATA"] = {"xmp_length": len(xmp_data)}
    except Exception:
        pass  # PyMuPDF returns None if no XMP

    # === 3. File appears modified after creation (rare for genuine Kaspi) ===
    if dt_created and dt_mod and dt_mod > dt_created:
        flags.append("MODIFIED_AFTER_CREATE")
        debug["MODIFIED_AFTER_CREATE"] = {
            "creation_date": creation_date,
            "mod_date": mod_date
        }

    # === 4. Creator / producer strings known from export tools ===
    suspicious_terms = [
        "microsoft", "adobe", "foxit", "wps", "nitro",
        "pdfcreator", "pdf-xchange", "safari pdf", "chrome pdf",
        "telegram", "whatsapp", "camscanner", "scanner", "scan"
    ]
    if any(t in (creator + producer).lower() for t in suspicious_terms):
        flags.append("SUSPICIOUS_SOFTWARE_CREATOR")
        debug["SUSPICIOUS_SOFTWARE_CREATOR"] = {
            "creator": creator,
            "producer": producer
        }

    # === 5. If encrypted, it’s probably OK, but note it ===
    if doc.is_encrypted:
        flags.append("ENCRYPTED_FILE")

    # score: start at 100, subtract 5 per flag
    score = 100 - 5 * len(flags)

    return {
        "author": author,
        "creator": creator,
        "producer": producer,
        "creation_date_raw": creation_date,
        "mod_date_raw": mod_date,
        "creation_date_parsed": dt_created.isoformat() if dt_created else "",
        "mod_date_parsed": dt_mod.isoformat() if dt_mod else "",
        "page_count": len(doc),
        "is_encrypted": doc.is_encrypted,
        "flags": flags,
        "score": score,
        "debug_meta": debug
    }


def check_unprotected_statement(doc: fitz.Document) -> Optional[str]:
    """Flag if PDF is not encrypted / password-protected."""
    try:
        if not doc.is_encrypted:
            return "UNPROTECTED_STATEMENT"
    except Exception:
        return None
    return None

def check_inconsistent_page_size(doc: fitz.Document) -> Optional[str]:
    """
    Flag if not all pages share (width,height) approximately.
    """
    sizes = []
    for page in doc:
        w = round(page.rect.width)
        h = round(page.rect.height)
        sizes.append((w, h))
    unique_sizes = list(dict.fromkeys(sizes))
    if len(unique_sizes) > 1:
        return "INCONSISTENT_PAGE_SIZE"
    return None


def check_odd_page_aspect(doc: fitz.Document) -> (Optional[str], list):
    """
    Slight upgrade: instead of returning just flag, return (flag, debug_pages).
    debug_pages is a list of pages that violate aspect ratio.
    """
    weird_pages = []
    for idx, page in enumerate(doc):
        w = float(page.rect.width)
        h = float(page.rect.height)
        if h == 0:
            continue
        ratio = w / h
        if ratio > 1.2 or ratio < 0.55:
            weird_pages.append({
                "page": idx,
                "w": w,
                "h": h,
                "ratio": ratio,
            })
    if weird_pages:
        return "ODD_PAGE_ASPECT", weird_pages
    return None, []

def check_footer_markers_per_page(doc: fitz.Document):
    """
    Verify that each page contains the exact Kaspi footer line:
        'АО «Kaspi Bank», БИК CASPKZKA, www.kaspi_gold.kz'

    Returns:
        ("MISSING_KASPI_FOOTER", debug) if missing on any page,
        otherwise (None, {}).
    """

    required_footer = "АО «Kaspi Bank», БИК CASPKZKA, www.kaspi_gold.kz"

    pages_missing = []

    for page_index, page in enumerate(doc):
        text = page.get_text("text") or ""
        # Normalize whitespace but preserve punctuation and case sensitivity
        normalized = " ".join(text.split())

        if required_footer not in normalized:
            pages_missing.append({
                "page": page_index,
                "sample": normalized[-200:],  # short snippet for debugging
            })

    if pages_missing:
        return "MISSING_KASPI_FOOTER", {"pages_missing": pages_missing}

    return None, {}
