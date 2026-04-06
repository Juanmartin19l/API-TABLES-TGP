from fastapi import FastAPI, File, UploadFile, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from io import BytesIO
from pathlib import Path

from app.ocr_client import post_file_to_ocr
from app.processor import process_ocr_response
from fastapi.responses import StreamingResponse
from datetime import datetime
import traceback

from app.services.dataframe_service import (
    json_to_df,
    apply_business_rules,
    compute_totals_row,
    append_totals_row,
    export_df_to_excel_stream,
)

app = FastAPI(
    title="API Tables TGP",
    description="API para procesar archivos OCR financieros",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/process", status_code=status.HTTP_200_OK)
async def process_file(file: UploadFile = File(...)) -> List[dict]:
    try:
        ocr_response = await post_file_to_ocr(file)
        result = process_ocr_response(ocr_response)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing file: {str(e)}",
        )


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.post("/process/export")
async def process_and_export(file: UploadFile = File(...), save: bool = False):
    try:
        ocr_resp = await post_file_to_ocr(file)
        rows = process_ocr_response(ocr_resp)

        df = json_to_df(rows)
        df = apply_business_rules(df)

        totals_row = compute_totals_row(df)
        df_with_totals = append_totals_row(df, totals_row)

        # Export to bytes so we can optionally save to disk
        buf = export_df_to_excel_stream(df_with_totals, sheet_name="Report")
        bytes_data = buf.getvalue()

        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        filename = f"report_{ts}.xlsx"

        saved_path_header = None
        if save:
            exports_dir = Path.cwd() / "exports"
            exports_dir.mkdir(parents=True, exist_ok=True)
            file_path = exports_dir / filename
            # Write file to disk
            with open(file_path, "wb") as f:
                f.write(bytes_data)
            saved_path_header = str(file_path.resolve())

        resp_buf = BytesIO(bytes_data)
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        if saved_path_header:
            headers["X-Saved-File"] = saved_path_header

        return StreamingResponse(
            resp_buf,
            media_type=(
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ),
            headers=headers,
        )
    except HTTPException:
        raise
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Error processing file: {str(exc)}"
        )
