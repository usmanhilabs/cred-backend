from fastapi import APIRouter, UploadFile, File, Form, Query
from typing import Optional
from sqlalchemy.orm import Session
from ..models import UploadedDocument, FormData, Application, ApplicationEvent
from ..database import SessionLocal
import os
import ast
import json
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api/forms", tags=["Uploads"])
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@router.post("/upload-file")
async def upload_file(
    formId: str = Form(...),
    fileType: str = Form(...),
    file: UploadFile = File(...)
):  
    filename_without_ext = ".".join(file.filename.split(".")[:-1])
    file_ext = file.filename.split(".")[-1]
    new_filename = f"{filename_without_ext}__{formId}.{file_ext}"
    file_path = os.path.join(UPLOAD_DIR, new_filename)

    with open(file_path, "wb") as f:
        f.write(await file.read())

    db: Session = SessionLocal()

    try:
        # 1. Mark previous file as replaced, if exists
        previous_record = db.query(UploadedDocument).filter(
            UploadedDocument.form_id == formId,
            UploadedDocument.file_type == fileType,
            UploadedDocument.status != "Replaced"
        ).first()  # optional: based on latest

        if previous_record:
            previous_record.status = "Replaced"
            db.flush()

        # 2. Insert new file record
        new_file_record = UploadedDocument(
            form_id=formId,
            filename=file.filename,
            file_extension=file_ext,
            file_type=fileType,
            status="New"
        )
        db.add(new_file_record)
        db.flush()

        # 3. Update reference in FormData
        form = db.query(FormData).filter(FormData.form_id == formId).first()
        if form:
            field_name = f"{fileType}_upload_id"
            setattr(form, field_name, new_file_record.id)

        db.commit()
        db.refresh(new_file_record)

        return {
            "message": "File uploaded successfully",
            "fileId": new_file_record.id,
            "filename": file.filename,
            "fileType": fileType,
        }

    finally:
        db.close()


def get_progress(type, status):
    if status == "Approved":
        return 100
    if status == "In Progress":
        return 50
    if type == "npi":
        return 100
    elif type == "malpractice_insurance":
        return 70
    elif type == "dl":
        return 90
    elif type == "degree":
        return 75
    elif type in ("cv", "cv/resume"):
        return 60
    elif type in ("MEDICAL_TRAINING_CERTIFICATE",):
        return 60
    elif type in ("board_certification",):
        return 85
    elif type in ("license_board",):
        return 80
    elif type in ("DEA", "COI", "CV"):
        return 60
    else:
        return 45
    

def _parse_json_field(val: Optional[str]):
    if not val:
        return {}
    # try JSON first, then Python literal
    try:
        return json.loads(val)
    except Exception:
        try:
            return ast.literal_eval(val)
        except Exception:
            return {}


def _normalize_provider_type(ft: str) -> str:
    if not ft:
        return ""
    v = ft.strip()
    lu = v.lower()
    if lu in ("degree", "medical_training_certificate", "medical_training_cert", "mtc", "med_training"):
        return "MEDICAL_TRAINING_CERTIFICATE"
    if lu in ("cv", "cv/resume", "resume"):
        return "CV"
    if lu == "dea":
        return "DEA"
    if lu == "coi":
        return "COI"
    if lu == "malpractice_insurance":
        return "malpractice_insurance"
    # default: uppercase token
    return v.upper()

@router.get("/upload-info")
async def get_upload_info(
    uploadIds: Optional[str] = Query(None),
    formId: Optional[str] = Query(None),
    appId: Optional[str] = Query(None),
):
    db: Session = SessionLocal()
    if appId:
        application = db.query(Application).filter(Application.id == appId).first()
        if application:
            formId = application.form_id

    if not formId:
        db.close()
        return {"formId": None, "files": {}, "comments": []}

    # Show provider-submitted docs; accept multiple DB variants and normalize to FE keys
    provider_file_types_db = {
        "DEA", "CV", "MEDICAL_TRAINING_CERTIFICATE", "malpractice_insurance",
        "dea", "cv", "degree", "cv/resume", "medical_training_certificate", "medical_training_cert", "malpractice_insurance"
    }
    query = (
        db.query(UploadedDocument)
        .filter(UploadedDocument.form_id == formId)
        .order_by(UploadedDocument.id.desc())
    )
    all_rows = query.all()

    # Filter to provider types if any match, else fallback to all for this form
    provider_rows = [r for r in all_rows if (r.file_type or "") in provider_file_types_db]
    rows = provider_rows if provider_rows else all_rows
    # Gather recent comments/events for the application if appId given
    comments = []
    if appId:
        events = (
            db.query(ApplicationEvent)
            .filter(ApplicationEvent.application_id == appId)
            .order_by(ApplicationEvent.created_at.desc())
            .limit(10)
            .all()
        )
        for ev in events:
            comments.append({
                "id": ev.id,
                "type": ev.event_type,
                "message": ev.message,
                "createdAt": ev.created_at.isoformat() if ev.created_at else None,
            })
    db.close()

    # Build response with optional placeholders for blank sections
    response_files = {}
    for file in rows:
        key = _normalize_provider_type(file.file_type)
        if key == "COI":
            continue
        if key in response_files:
            continue
        base = {
            "filename": file.filename,
            "fileType": key,
            "fileExtension": file.file_extension,
            "fileId": file.id,
            "status": file.status,
            "progress": get_progress(key, file.status),
            "pdfMatch": _parse_json_field(file.pdf_match),
            "ocrData": _parse_json_field(file.ocr_output),
            "jsonMatch": _parse_json_field(file.json_match),
        }
        if key == "malpractice_insurance":
            base["verification"] = "Insurance Policy Verification"
            base["verificationDetails"] = _parse_json_field(file.verification_data)
        else:
            base["verification"] = _parse_json_field(file.verification_data)
        response_files[key] = base


    # Ensure placeholders for expected provider tiles
    expected_provider_types = [
        "MEDICAL_TRAINING_CERTIFICATE", "DEA", "CV", "malpractice_insurance"
    ]
    for t in expected_provider_types:
        if t not in response_files:
            placeholder = {
                "filename": None,
                "fileType": t,
                "fileExtension": None,
                "fileId": None,
                "status": None,
                "progress": get_progress(t, None),
                "pdfMatch": {},
                "ocrData": {},
                "jsonMatch": {},
            }
            if t == "malpractice_insurance":
                placeholder["verification"] = "Insurance Policy Verification"
                placeholder["verificationDetails"] = {}
            else:
                placeholder["verification"] = {}
            response_files[t] = placeholder

    return {
        "formId": formId,
        "files": response_files,
        "comments": comments,
    }


@router.get("/upload-info-psv")
async def get_upload_info_psv(
    uploadIds: Optional[str] = Query(None),
    formId: Optional[str] = Query(None),
    appId: Optional[str] = Query(None),
):
    db: Session = SessionLocal()

    if appId:
        application = db.query(Application).filter(Application.id == appId).first()
        if application:
            formId = application.form_id

    if not formId:
        db.close()
        return {"formId": None, "files": {}}

    psv_types = {
        "board_certification": {
            "verification": "Verification with Board Certificate",
            "ocr_keys": [
                "abmsuid",
                "abms_name",
                "abms_dob",
                "abms_education",
                "abms_address",
                "abms_certification_board",
                "abms_certification_type",
                "abms_status",
                "abms_duration",
                "abms_occurrence",
                "abms_start_date",
                "abms_end_date",
                "abms_reverification_date",
                "abms_participating_in_moc",
            ],
        },
        "license_board": {
            "verification": "License Number Match",
            "ocr_keys": [
                "LicenseBoard_ExtractedLicense",
                "LicenseBoard_Extracted_Name",
                "LicenseBoard_Extracted_License_Type",
                "LicenseBoard_Extracted_Primary_Status",
                "LicenseBoard_Extracted_Specialty",
                "LicenseBoard_Extracted_Qualification",
                "LicenseBoard_Extracted_School_Name",
                "LicenseBoard_Extracted_Graduation_Year",
                "LicenseBoard_Extracted_Previous_Names",
                "LicenseBoard_Extracted_Address",
                "LicenseBoard_Extracted_Issuance_Date",
                "LicenseBoard_Extracted_Expiration_Date",
                "LicenseBoard_Extracted_Current_Date_Time",
                "LicenseBoard_Extracted_Professional_Url",
                "LicenseBoard_Extracted_Disciplinary_Actions",
                "LicenseBoard_Extracted_Public_Record_Actions",
            ],
        },
        "sanctions": {"verification": None, "ocr_keys": []},
        "npi": {"verification": "NPPES Verification", "ocr_keys": []},
    }

    rows = (
        db.query(UploadedDocument)
        .filter(UploadedDocument.form_id == formId)
        .filter(UploadedDocument.file_type.in_(list(psv_types.keys())))
        .all()
    )

    files = {}
    for r in rows:
        ocr_data = _parse_json_field(r.ocr_output)
        json_match = _parse_json_field(r.json_match)
        verification_details = _parse_json_field(r.verification_data)
        legacy_verif = psv_types[r.file_type]["verification"]
        # For sanctions, surface key details in the ocrData section so the UI shows values
        if r.file_type == "sanctions" and verification_details:
            prov = verification_details.get("provider") or {}
            sanc = verification_details.get("sanction") or {}
            ocr_data = {
                "Sanction Status": sanc.get("status") or "N/A",
                "Sanction Details": sanc.get("details") or "N/A",
                "Comment 1": sanc.get("comment_1") or "",
                "Comment 2": sanc.get("comment_2") or "",
                "NPI": verification_details.get("npi") or "",
                "Provider Name": prov.get("name") or "",
            }
        files[r.file_type] = {
            "filename": r.filename,
            "fileType": r.file_type,
            "fileExtension": r.file_extension,
            "fileId": r.id,
            "status": r.status,
            "progress": get_progress(r.file_type),
            "pdfMatch": _parse_json_field(r.pdf_match),
            "ocrData": {k: ocr_data.get(k) for k in psv_types[r.file_type]["ocr_keys"]} if psv_types[r.file_type]["ocr_keys"] else ocr_data,
            "jsonMatch": json_match,
            # Keep legacy string for UI label, and provide details in separate field
            "verification": legacy_verif,
            "verificationDetails": verification_details,
        }

    # Ensure placeholders for any missing PSV sections
    for ft, meta in psv_types.items():
        if ft not in files:
            files[ft] = {
                "filename": None,
                "fileType": ft,
                "fileExtension": None,
                "fileId": None,
                "status": None,
                "progress": get_progress(f,None),
                "pdfMatch": {},
                "ocrData": {},
                "verification": meta["verification"],
            }

    return {"formId": formId, "files": files}

@router.post("/upload-status-update")
async def upload_status_update(
    formId: str = Query(...),
    statusUpdate: str = Query(...),
    fileType: str = Query(...)
):
    db: Session = SessionLocal()
    try:
        docs = db.query(UploadedDocument).filter(
            UploadedDocument.form_id == formId,
            UploadedDocument.file_type == fileType
        ).all()
        if not docs:
            db.close()
            return JSONResponse({"formId": None, "files": {}, "comments": []}, status_code=404)

        for doc in docs:
            if statusUpdate == "Accepted":
                doc.status = "Approved"
            elif statusUpdate == "Rejected":
                doc.status = "In Progress"

        db.commit()
    finally:
        db.close()

    return await get_upload_info(formId=str(formId), appId=None, uploadIds=None)
