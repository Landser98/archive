#!/usr/bin/env python3
import sys, json, base64, argparse
from typing import Any, Dict, Set, Tuple
from pathlib import Path

import pikepdf
from pikepdf import Name, Dictionary, Array, Stream, Object
from src.config import DATA_DIR, FAILED


# --- add project root to sys.path so `import src.*` works when run as a script ---
from pathlib import Path
import sys as _sys
_PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../bank_statements_otbasy
if str(_PROJECT_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_PROJECT_ROOT))

from src.config import DATA_DIR, FAILED


# --- Helpers ---------------------------------------------------------------

def name_str(n: Name) -> str:
    try:
        return str(n)
    except Exception:
        return f"/{bytes(n).decode('latin-1', 'replace')}"

def obj_id(obj: Object) -> str:
    """
    Return a stable identifier for an indirect object: 'objnum:gennum'.
    For direct objects (no objgen), return a Python id-based tag.
    """
    try:
        og = obj.objgen
        if og is not None:
            return f"{og[0]}:{og[1]}"
    except Exception:
        pass
    return f"direct:{id(obj)}"

def safe_bytes_preview(b: bytes, max_bytes: int) -> Dict[str, Any]:
    preview = b[:max_bytes]
    return {
        "length": len(b),
        "preview_len": len(preview),
        "preview_hex": preview.hex(),
        "preview_b64": base64.b64encode(preview).decode("ascii"),
        "truncated": len(b) > max_bytes,
    }

# --- Core conversion -------------------------------------------------------

def to_jsonable(obj: Any,
                seen: Set[str],
                depth: int,
                max_depth: int,
                include_stream_data: bool,
                stream_max_bytes: int) -> Any:
    """
    Recursively convert pikepdf objects to JSON-serializable structures.
    Guards against cycles and huge streams.
    """
    if depth > max_depth:
        return {"__type__": "DepthLimit", "note": f"max_depth {max_depth} reached"}

    # None / primitives
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj

    # Names
    if isinstance(obj, Name):
        return {"__type__": "Name", "value": name_str(obj)}

    # Arrays
    if isinstance(obj, Array):
        return [
            to_jsonable(x, seen, depth + 1, max_depth, include_stream_data, stream_max_bytes)
            for x in obj
        ]

    # Dictionaries
    if isinstance(obj, Dictionary):
        out = {"__type__": "Dictionary"}
        for k, v in obj.items():
            key = name_str(k) if isinstance(k, Name) else str(k)
            out[key] = to_jsonable(v, seen, depth + 1, max_depth, include_stream_data, stream_max_bytes)
        return out

    # Streams
    if isinstance(obj, Stream):
        d: Dict[str, Any] = {"__type__": "Stream"}
        # Include stream dictionary (metadata)
        try:
            d["dict"] = to_jsonable(obj._dict, seen, depth + 1, max_depth, include_stream_data, stream_max_bytes)
        except Exception as e:
            d["dict_error"] = f"{type(e).__name__}: {e}"

        # Optionally include decoded data preview
        if include_stream_data:
            try:
                # decode filters if possible
                data = obj.read_bytes()
                d["data"] = safe_bytes_preview(data, stream_max_bytes)
            except Exception as e:
                d["data_error"] = f"{type(e).__name__}: {e}"
        return d

    # Indirect objects – dereference and guard cycles
    if isinstance(obj, Object):
        oid = obj_id(obj)
        if oid in seen:
            return {"__type__": "Ref", "ref": oid, "note": "already visited"}
        seen.add(oid)
        try:
            deref = obj.get_object()
        except Exception:
            deref = obj
        return {
            "__type__": "Indirect",
            "id": oid,
            "value": to_jsonable(deref, seen, depth + 1, max_depth, include_stream_data, stream_max_bytes),
        }

    # Fallback – stringify
    return {"__type__": type(obj).__name__, "repr": repr(obj)}

# --- Entry points ----------------------------------------------------------

def dump_catalog(pdf: pikepdf.Pdf, **kw) -> Dict[str, Any]:
    root = pdf.Root
    info = {
        "pdf_version": pdf.pdf_version,
        "trailer_keys": list(pdf.trailer.keys()),
        "metadata": {k: str(v) for k, v in (pdf.docinfo or {}).items()},
        "Root": to_jsonable(root, set(), 0, kw["max_depth"], kw["include_streams"], kw["stream_max_bytes"]),
    }
    return info

def dump_pages(pdf: pikepdf.Pdf, **kw) -> Dict[str, Any]:
    pages_out = []
    for i, page in enumerate(pdf.pages, start=1):
        page_obj = page.obj
        entry = {
            "index": i,
            "obj_id": obj_id(page_obj),
            "MediaBox": list(page_obj.get("/MediaBox", [])) if isinstance(page_obj.get("/MediaBox", []), Array) else page_obj.get("/MediaBox", None),
            "Rotate": int(page_obj.get("/Rotate", 0)),
            "Resources": to_jsonable(page_obj.get("/Resources", Dictionary()), set(), 0, kw["max_depth"], kw["include_streams"], kw["stream_max_bytes"]),
        }
        # Contents can be a single stream or an array of streams
        contents = page_obj.get("/Contents", None)
        entry["Contents"] = to_jsonable(contents, set(), 0, kw["max_depth"], kw["include_streams"], kw["stream_max_bytes"])
        pages_out.append(entry)
    return {"Pages": pages_out}

def main():
    ap = argparse.ArgumentParser(description="Dump a PDF's internal objects to JSON (catalog, pages, resources, streams).")
    ap.add_argument("pdf", help="Path to PDF")
    # let --out be optional; if omitted we auto-save to DATA_DIR/converted_jsons/<pdf_stem>.json
    ap.add_argument("-o", "--out", default=None, help="Output JSON file (optional)")
    ap.add_argument("--max-depth", type=int, default=6, help="Max recursion depth")
    ap.add_argument("--include-streams", action="store_true", help="Include decoded stream data previews")
    ap.add_argument("--stream-max-bytes", type=int, default=4096, help="Max decoded bytes to include per stream")
    ap.add_argument("--include-xref", action="store_true", help="Also list the cross-reference (object numbers)")
    args = ap.parse_args()

    in_path = Path(args.pdf)
    out_dir = Path(DATA_DIR) / "converted_jsons"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = Path(args.out) if args.out else (out_dir / f"{in_path.stem}.json")

    with pikepdf.open(str(in_path)) as pdf:
        out: Dict[str, Any] = {
            "file": str(in_path),
            "num_pages": len(pdf.pages),
        }

        out.update(dump_catalog(pdf,
                                max_depth=args.max_depth,
                                include_streams=args.include_streams,
                                stream_max_bytes=args.stream_max_bytes))
        out.update(dump_pages(pdf,
                              max_depth=args.max_depth,
                              include_streams=args.include_streams,
                              stream_max_bytes=args.stream_max_bytes))

        if args.include_xref:
            xref = []
            try:
                for obj in pdf.objects:
                    try:
                        og = obj.objgen
                        xref.append({"obj": og[0], "gen": og[1], "type": type(obj.get_object()).__name__})
                    except Exception:
                        pass
            except Exception as e:
                xref = {"error": f"{type(e).__name__}: {e}"}
            out["XRef"] = xref

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Dumped to {out_path}")


if __name__ == "__main__":
    main()

#  PYTHONPATH=. python src/utils/convert_pdf_json_page.py "data/kaspi_pay/Vypiska_po_scetu_KZ98722S000033980379.pdf" --include-streams --stream-max-bytes 2048 --include-xref