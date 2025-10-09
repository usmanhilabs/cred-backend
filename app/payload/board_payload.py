from pydantic import BaseModel
from typing import Optional, Dict, Any


class BoardCertificateFileResponse(BaseModel):
    status: Optional[str]
    progress: Optional[int]
    ocrData: Dict[str, Any]  # or define a stricter schema if you know all OCR fields
    verification: Optional[str]
    file: Optional[str]  # base64 string


class BoardCertificateResponse(BaseModel):
    formId: Optional[str]
    boardCertificate: Optional[BoardCertificateFileResponse]
