from fastapi import APIRouter, Query, HTTPException
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from ..models import FormFileUpload, Application, FormData
from ..database import SessionLocal
import os, uuid

router = APIRouter(prefix="/api/documents", tags=["Documents"])
DOWNLOAD_DIR = "uploads"

@router.get("/download")
async def download_document(id: str = Query(...), type: str = Query(...)):
    db: Session = SessionLocal()
    try:
        application = db.query(Application).filter_by(id=id).first()
        if not application:
            raise HTTPException(status_code=404, detail="Application not found")

        # Get related file uploads with OCR data
        file_uploads = db.query(FormFileUpload).filter(FormFileUpload.status != 'Replaced', FormFileUpload.form_id==application.form_id, FormFileUpload.file_type==type).first()
        if not file_uploads:
            raise HTTPException(status_code=404, detail="Error while fetching file uploads")

        filename_without_ext = ".".join(file_uploads.filename.split(".")[:-1])
        file_path = os.path.join(DOWNLOAD_DIR, f"{filename_without_ext}__{application.form_id}.pdf")

        print(file_path)
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found on disk")

        return FileResponse(
            path=file_path,
            filename=f"{filename_without_ext}.pdf",
            media_type="application/pdf"
        )
    finally:
        db.close()