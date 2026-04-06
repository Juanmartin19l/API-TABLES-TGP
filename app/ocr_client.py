import httpx
from typing import Optional
from fastapi import UploadFile

OCR_API_URL = "http://localhost:8000"


async def post_file_to_ocr(file: UploadFile) -> dict:
    async with httpx.AsyncClient(timeout=120.0) as client:
        files = {"files": (file.filename, file.file, file.content_type)}
        response = await client.post(f"{OCR_API_URL}/api/v1/ocr/upload", files=files)
        response.raise_for_status()
        return response.json()
