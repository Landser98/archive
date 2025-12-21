from dataclasses import dataclass
from datetime import date
from typing import Optional
import pandas as pd

@dataclass
class Statement:
    bank: str
    pdf_name: str
    account_holder_name: str
    iin_bin: str
    account_number: Optional[str]
    period_from: Optional[date]
    period_to: Optional[date]
    statement_generation_date: Optional[date]
    tx_df: pd.DataFrame
    header_df: Optional[pd.DataFrame] = None
