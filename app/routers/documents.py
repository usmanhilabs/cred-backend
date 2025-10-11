from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from ..models import UploadedDocument, Application
from ..database import SessionLocal
import os

router = APIRouter(prefix="/api/documents", tags=["Documents"])
DOWNLOAD_DIR = "uploads"

@router.get("/download")
async def download_document(id: str = Query(...), type: str = Query(...)):
    db: Session = SessionLocal()
    try:
        application = db.query(Application).filter_by(id=id).first()
        if not application:
            raise HTTPException(status_code=404, detail="Application not found")

        file_upload = (
            db.query(UploadedDocument)
            .filter(
                UploadedDocument.status != 'Replaced',
                UploadedDocument.form_id == application.form_id,
                UploadedDocument.file_type == type
            )
            .first()
        )
        if not file_upload:
            raise HTTPException(status_code=404, detail="Document not found")

        filename_without_ext = ".".join(file_upload.filename.split(".")[:-1])
        file_path = os.path.join(DOWNLOAD_DIR, f"{filename_without_ext}__{application.form_id}.pdf")
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found on disk")

        return FileResponse(
            path=file_path,
            filename=f"{filename_without_ext}.pdf",
            media_type="application/pdf"
        )
    finally:
        db.close()