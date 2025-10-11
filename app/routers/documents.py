from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import FileResponse, Response
from sqlalchemy.orm import Session
from ..models import UploadedDocument, Application, SavedFile
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


@router.get("/download/saved")
async def download_saved_file(
    application_id: str | None = Query(None),
    provider_id: str | None = Query(None),
    npi: str | None = Query(None),
):
    """Return a saved file blob for the given application_id/provider_id/NPI.
    The saved file is created by ingestion flows (e.g., sanctions) into SavedFile table.
    We return the most recent matching file.
    """
    db: Session = SessionLocal()
    try:
        app: Application | None = None
        if application_id:
            app = db.query(Application).filter(Application.id == application_id).first()
        elif provider_id:
            app = db.query(Application).filter(Application.provider_id == provider_id).order_by(Application.last_updt_dt.desc()).first()
        elif npi:
            app = db.query(Application).filter(Application.npi == npi).order_by(Application.last_updt_dt.desc()).first()

        if not app:
            raise HTTPException(status_code=404, detail="No matching application found")

        # Prefer attribute formatted as "sanctions:{app.id}" else any file containing app.id
        saved = (
            db.query(SavedFile)
            .filter((SavedFile.attribute == f"sanctions:{app.id}") | (SavedFile.attribute.contains(app.id)))
            .order_by(SavedFile.created_at.desc())
            .first()
        )
        if not saved:
            # fallback: any saved file matching NPI in filename
            saved = (
                db.query(SavedFile)
                .filter(SavedFile.filename.like(f"%{str(app.npi or '').strip()}%"))
                .order_by(SavedFile.created_at.desc())
                .first()
            )
        if not saved:
            raise HTTPException(status_code=404, detail="Saved file not found")

        media_type = "application/octet-stream"
        if saved.filename.lower().endswith(".pdf"):
            media_type = "application/pdf"
        elif saved.filename.lower().endswith(".zip"):
            media_type = "application/zip"

        headers = {"Content-Disposition": f"attachment; filename={saved.filename}"}
        return Response(content=saved.file_data, media_type=media_type, headers=headers)
    finally:
        db.close()