from typing import List, Optional, Union
from pydantic import BaseModel, Field


class OCRResultItem(BaseModel):
    texto: str
    confianza: float
    caja: List[int]
    pagina: int


class OCRArchivoResultado(BaseModel):
    nombre_archivo: str
    resultados: List[OCRResultItem]
    confianza_promedio: float
    total_paginas: int


class OCRArchivo(BaseModel):
    nombre_archivo: str
    ok: bool
    resultado: Optional[OCRArchivoResultado] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None


class OCRResponse(BaseModel):
    total_imagenes: int
    resultados_por_archivo: List[OCRArchivo]


class ProcessResponse(BaseModel):
    data: List[dict]


class ProcessError(BaseModel):
    detail: str
