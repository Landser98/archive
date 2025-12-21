from __future__ import annotations

from datetime import date
from typing import List, Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from src.core.service import parse_statement
from src.core.analysis import (
    get_last_full_12m_window,
    combine_transactions,
    compute_ip_income_for_statement,
)


from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import numpy as np

def df_records_json_safe(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    df2 = df.copy()

    # Convert datetimes to strings (and NaT -> None)
    for c in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2[c]):
            df2[c] = df2[c].dt.strftime("%Y-%m-%d")

    # Replace NaN/NaT with None (JSON-safe)
    df2 = df2.replace({pd.NaT: None})
    df2 = df2.where(pd.notna(df2), None)

    return df2.to_dict(orient="records")

app = FastAPI(title="Bank Statements API", version="0.1")
app.add_middleware(GZipMiddleware, minimum_size=1000)


@app.get("/health")
def health():
    return {"status": "ok"}


def _df_to_records_safe(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    df2 = df.where(pd.notna(df), None)
    records = df2.to_dict(orient="records")
    return jsonable_encoder(records)


@app.post("/v1/analyze_all")
async def analyze_all(
    anchor_date: date = Form(...),
    bank_keys: List[str] = Form(...),
    pdfs: List[UploadFile] = File(...),
    allow_iin_mismatch: bool = Form(False),
    max_rows: Optional[int] = Form(None),
):
    try:
        if len(bank_keys) != len(pdfs):
            raise HTTPException(400, "bank_keys length must match number of pdfs")

        statements = []
        for bank_key, pdf in zip(bank_keys, pdfs):
            pdf_bytes = await pdf.read()
            try:
                stmnt = parse_statement(bank_key=bank_key, pdf_name=pdf.filename, pdf_bytes=pdf_bytes)
                statements.append(stmnt)
            except Exception as e:
                raise HTTPException(400, f"{pdf.filename}: {e}")

        session_iin = statements[0].iin_bin if statements else None
        if (not allow_iin_mismatch) and session_iin:
            for s in statements[1:]:
                if s.iin_bin != session_iin:
                    raise HTTPException(
                        400,
                        f"IIN/BIN mismatch: first={session_iin} vs {s.pdf_name}={s.iin_bin}",
                    )

        window_start, window_end = get_last_full_12m_window(anchor_date)

        tx_12m = combine_transactions(statements, window_start, window_end)
        if max_rows is not None:
            tx_12m = tx_12m.head(max_rows)

        all_enriched = []
        summary_rows = []
        for stmnt in statements:
            enriched, summary = compute_ip_income_for_statement(stmnt, window_start, window_end)
            if enriched is not None and not enriched.empty:
                all_enriched.append(enriched)
            if summary is not None:
                summary_rows.append(summary)

        tx_12m_ip = pd.concat(all_enriched, ignore_index=True) if all_enriched else pd.DataFrame()
        if max_rows is not None and not tx_12m_ip.empty:
            tx_12m_ip = tx_12m_ip.head(max_rows)

        meta = [
            {
                "bank": s.bank,
                "pdf_name": s.pdf_name,
                "iin_bin": s.iin_bin,
                "account_number": s.account_number,
                "period_from": str(s.period_from) if s.period_from else None,
                "period_to": str(s.period_to) if s.period_to else None,
                "statement_generation_date": str(s.statement_generation_date) if s.statement_generation_date else None,
            }
            for s in statements
        ]

        payload = {
            "window_start": str(window_start),
            "window_end": str(window_end),
            "metadata": meta,
            "tx_12m_rows": int(len(tx_12m)),
            "tx_12m": df_records_json_safe(tx_12m),
            "tx_12m_ip_rows": int(len(tx_12m_ip)),
            "tx_12m_ip": df_records_json_safe(tx_12m_ip),
            "income_summary": jsonable_encoder(summary_rows),
        }

        return JSONResponse(content=jsonable_encoder(payload))


    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
