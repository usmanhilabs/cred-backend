from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from enum import Enum
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.utils import get_db
from app.services.ingest_board_certificate import ingest_board_certificate_file, ingest_boardcertificate_files
from app.services.ingest_license import LicenseService

class FileType(str, Enum):
    sanctions = "Sanctions"
    license = "License"
    board_certificate = "Board Certificate"

router = APIRouter(prefix="/api/ingest", tags=["Ingestion"])

@router.post("/upload/")
async def ingest_file(
    file: UploadFile = File(...),
    file_type: FileType = Form(...),
    zip_file: UploadFile = File(None)
):
    file_path = f"/tmp/{file.filename}"
    with open(file_path, "wb") as f:
        f.write(await file.read())
    try:
        if file_type == FileType.board_certificate:
            ingest_board_certificate_file(file_path)
            print("Ingestion succeeded")
            if zip_file:
                zip_file_path = f"/tmp/{zip_file.filename}"
                with open(zip_file_path, "wb") as zf:
                    zf.write(await zip_file.read())
                ingest_boardcertificate_files(zip_file_path)
        elif file_type == FileType.license:
            service = LicenseService()  # db session is set in __init__
            if service.ingest_licenses_from_excel(file_path):
                print("License ingestion succeeded")
            else:
                print("License ingestion failed")
            if zip_file:
                zip_file_path = f"/tmp/{zip_file.filename}"
                with open(zip_file_path, "wb") as zf:
                    zf.write(await zip_file.read())
                service.ingest_license_files_from_zip(zip_file_path)
        else:
            raise HTTPException(status_code=400, detail="Invalid file type")
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
