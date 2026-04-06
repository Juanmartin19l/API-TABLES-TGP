from io import BytesIO
from typing import List, Dict, Optional

import pandas as pd
import numpy as np

COLUMNS = ["Fecha", "Origen", "Monto", "Retenido", "MEP/CTA BNA", "TOTAL"]


def json_to_df(rows: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    # Ensure columns exist and order
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNS].copy()
    # Normalize empty -> NaN
    df = df.replace({"": np.nan, None: np.nan})
    # Numeric conversion
    num_cols = ["Monto", "Retenido", "MEP/CTA BNA", "TOTAL"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Retenido always negative if present
    df["Retenido"] = df["Retenido"].apply(lambda x: -abs(x) if pd.notna(x) else x)
    # Ensure positivity for numeric columns except Retenido
    for col in ["Monto", "MEP/CTA BNA", "TOTAL"]:
        df[col] = df[col].apply(lambda x: abs(x) if pd.notna(x) else x)
    # Fecha/Origen/Monto remain as-is (Monto not summed)
    return df


def compute_totals_row(df: pd.DataFrame) -> Dict:
    # Use min_count=1 so sum returns NaN if there are no values
    retenido_sum = df["Retenido"].sum(min_count=1)
    mep_sum = df["MEP/CTA BNA"].sum(min_count=1)
    total_sum = df["TOTAL"].sum(min_count=1)

    def nan_to_none(v):
        return None if pd.isna(v) else float(v)

    totals_row = {
        "Fecha": None,
        "Origen": None,
        "Monto": None,
        "Retenido": nan_to_none(retenido_sum),
        "MEP/CTA BNA": nan_to_none(mep_sum),
        "TOTAL": nan_to_none(total_sum),
    }
    return totals_row


def append_totals_row(df: pd.DataFrame, totals_row: Dict) -> pd.DataFrame:
    df_tot = pd.concat([df, pd.DataFrame([totals_row])], ignore_index=True)
    return df_tot


def df_to_json_rows(df: pd.DataFrame) -> List[Dict]:
    out = df.where(pd.notnull(df), None)
    return out.to_dict(orient="records")


# Excel export helpers
def export_df_to_excel_stream(df: pd.DataFrame, sheet_name: str = "Sheet1") -> BytesIO:
    buf = BytesIO()
    # engine openpyxl required
    df.to_excel(buf, index=False, sheet_name=sheet_name, engine="openpyxl")
    buf.seek(0)
    return buf


def export_df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    buf = export_df_to_excel_stream(df, sheet_name)
    return buf.getvalue()
