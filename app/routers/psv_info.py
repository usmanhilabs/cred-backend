from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from ..utils import get_db
from ..models import Application, FormData, UploadedDocument
import json

router = APIRouter(prefix="/api", tags=["PSV Info"])

PROVIDER_SUBMITTED_TYPES = {"MEDICAL_TRAINING_CERTIFICATE", "CV", "COI", "DEA", "DRIVING_LICENSE", "DL"}
PSV_FETCHED_TYPES = {"board_certification", "license_board", "sanctions"}
EXCLUDE_TYPES = {"hospital_privileges", "npdb", "npi"}

DISPLAY_NAME_OVERRIDES = {
    "COI": "Certificate of Insurance",
    "CV": "CV/Resume",
    "MEDICAL_TRAINING_CERTIFICATE": "Medical Training Certificate",
    "DRIVING_LICENSE": "Driver License",
    "DL": "Driver License",
    "board_certification": "Board Certification",
    "license_board": "License / Board Status",
    "sanctions": "Sanctions Report",
    "DEA": "DEA/CDS Certificate",
}


@router.get("/psv-info/{application_id}")
def get_psv_info(application_id: str, db: Session = Depends(get_db)):
    app = db.query(Application).filter_by(id=application_id).first()
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    form = db.query(FormData).filter_by(form_id=app.form_id).first()

    docs = db.query(UploadedDocument).filter(
        UploadedDocument.form_id == app.form_id,
        UploadedDocument.status != 'Replaced'
    ).all()

    provider_docs = []
    psv_docs = []

    for d in docs:
        ft_lower = (d.file_type or '').lower()
        if ft_lower in (t.lower() for t in EXCLUDE_TYPES):
            continue
        item = {
            "id": d.id,
            "type": d.file_type,
            "displayName": DISPLAY_NAME_OVERRIDES.get(d.file_type, d.file_type.replace('_', ' ').title()),
            "status": d.status,
            "ocrOutput": json.loads(d.ocr_output) if d.ocr_output else {},
            "verification": json.loads(d.verification_data) if d.verification_data else [],
            "jsonMatch": json.loads(d.json_match) if d.json_match else {},
        }
        if d.file_type.upper() in PROVIDER_SUBMITTED_TYPES:
            provider_docs.append(item)
        elif d.file_type.lower() in PSV_FETCHED_TYPES:
            # For sanctions, send path hint (simulate link)
            if d.file_type.lower() == 'sanctions':
                item['documentLink'] = f"/uploads/sanctions_{app.form_id}.pdf"
            psv_docs.append(item)

    response = {
        "applicationId": app.id,
        "provider": {
            "name": form.provider_name if form else app.name,
            "lastName": form.provider_last_name if form else app.last_name,
            "npi": form.npi if form else app.npi,
            "specialty": form.specialty if form else app.specialty,
            "address": form.address if form else app.address,
            "market": app.market,
        },
        "providerSubmitted": provider_docs,
        "psvFetched": psv_docs,
        "stats": {
            "totalDocuments": len(provider_docs) + len(psv_docs),
            "verifiedDocuments": sum(1 for x in provider_docs + psv_docs if str(x.get('status','')).lower() in ('approved','verified')),
            "inProgressDocuments": sum(1 for x in provider_docs + psv_docs if str(x.get('status','')).lower() not in ('approved','verified','rejected')),
        }
    }
    return response
