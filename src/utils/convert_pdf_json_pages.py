#!/usr/bin/env python3
# src/study_pdf/convert_pdf_json_pages.py
import sys
import os
import json
import base64
import argparse
from pathlib import Path

import pdfplumber
import pikepdf

# --- Resolve project root & import DATA_DIR safely --------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # repo root (…/bank_statements_otbasy)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from src.config import DATA_DIR  # type: ignore
except Exception:
    # Fallback to <repo>/data if config import isn't available
    DATA_DIR = PROJECT_ROOT / "data"

# --- Core -------------------------------------------------------------------
def dump_pdf_pages(
    pdf_path: Path,
    out_path: Path | None = None,
    stream_preview_len: int = 4000,
    include_full_stream: bool = False,
) -> Path:
    """
    Extract per-page text, word geometry (via pdfplumber), and content-stream previews (via pikepdf).
    Writes a JSONL file, one page per line.
    """
    pdf_path = Path(pdf_path)
    out_dir = Path(DATA_DIR) / "converted_jsons"
    out_dir.mkdir(parents=True, exist_ok=True)
    if out_path is None:
        out_path = out_dir / f"{pdf_path.stem}_pages.jsonl"

    # 1) Plain text + word positions (pdfplumber)
    with pdfplumber.open(str(pdf_path)) as pl:
        words_per_page = []
        texts = []
        for p in pl.pages:
            texts.append(p.extract_text() or "")
            # geometry is critical for your downstream column banding
            words = p.extract_words(
                x_tolerance=2,
                y_tolerance=2,
                keep_blank_chars=False
            )
            # normalize floats for stability
            for w in words:
                for k in ("x0", "x1", "top", "bottom"):
                    if k in w and isinstance(w[k], (int, float)):
                        w[k] = float(w[k])
            words_per_page.append(words)

    # 2) Raw content streams (pikepdf)
    with pikepdf.open(str(pdf_path)) as pdf, open(out_path, "w", encoding="utf-8") as out:
        for i, page in enumerate(pdf.pages):
            contents = page.get("/Contents", None)
            raw_bytes = b""
            if contents is not None:
                try:
                    raw_bytes = page.contents.read_bytes()  # convenience accessor
                except Exception:
                    try:
                        if isinstance(contents, pikepdf.Array):
                            raw_bytes = b"".join(obj.read_bytes() for obj in contents)
                        else:
                            raw_bytes = contents.read_bytes()
                    except Exception:
                        raw_bytes = b""

            preview = raw_bytes[:stream_preview_len]
            record = {
                "page_num": i + 1,
                "rotate": int(page.get("/Rotate", 0) or 0),
                "media_box": [float(x) for x in page.get("/MediaBox", [])] if page.get("/MediaBox") else None,
                "procset": [str(x) for x in (page.Resources.get("/ProcSet", []) if page.Resources else [])] or None,
                "text": texts[i],
                "words": words_per_page[i],  # list of dicts: text, x0, x1, top, bottom, etc.
                "content_stream_preview_utf8": preview.decode("latin-1", errors="replace"),
                "content_stream_preview_b64": base64.b64encode(preview).decode("ascii"),
                "content_stream_len": len(raw_bytes),
            }
            if include_full_stream and raw_bytes:
                record["content_stream_full_b64"] = base64.b64encode(raw_bytes).decode("ascii")

            out.write(json.dumps(record, ensure_ascii=False) + "\n")

    return out_path

# --- CLI --------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Dump per-page text, word geometry, and PDF content-stream previews to JSONL."
    )
    ap.add_argument("pdf", help="Path to PDF")
    ap.add_argument("-o", "--out", default=None, help="Output JSONL file (optional). Defaults to DATA_DIR/converted_jsons/<pdf_stem>_pages.jsonl")
    ap.add_argument("--stream-preview-len", type=int, default=4000, help="Bytes of stream preview to include per page")
    ap.add_argument("--include-full-stream", action="store_true", help="Include full content stream (base64) per page")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    in_pdf = Path(args.pdf)
    out_path = Path(args.out) if args.out else None
    written = dump_pdf_pages(
        pdf_path=in_pdf,
        out_path=out_path,
        stream_preview_len=args.stream_preview_len,
        include_full_stream=args.include_full_stream,
    )
    print(f"Written {written}")


# python src/utils/convert_pdf_json_pages.py "data/bcc/4679699582619.pdf" --stream-preview-len 2048 --include-full-stream
