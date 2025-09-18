from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import desc 
from sqlalchemy.orm import Session
from app.schemas import ApplicationCreate, ApplicationResponse
from app.models import Application
from typing import List
from datetime import datetime
import json
from app.models import FormData, UploadedDocument, EmailRecord, ApplicationEvent
from app.utils import get_db
import uuid
from app.services.report_service import ReportService

router = APIRouter(prefix="/api/applications", tags=["Applications"])
# Committee review endpoints now live under the main router with correct paths

def model_to_response(application: Application) -> ApplicationResponse:
    return ApplicationResponse(
        id=application.id,
        formId=application.form_id,
        providerId=application.provider_id,
        name=application.name,
        phone=application.phone,
        email=application.email,
        providerLastName=application.last_name,
        psvStatus=application.psv_status,
        committeeStatus=application.committee_status,
        progress=application.progress,
        assignee=application.assignee,
        source=application.source,
        market=application.market,
        specialty=application.specialty,
        address=application.address,
        npi=application.npi,
        create_dt=application.create_dt,
        last_updt_dt=application.last_updt_dt
    )

def update_application_model(db_obj: Application, data: dict):
    db_obj.form_id = data.get("formId")
    db_obj.provider_id = data.get("providerId")
    db_obj.name = data.get("name")
    db_obj.email = data.get("email")
    db_obj.phone = data.get("phone")
    db_obj.last_name = data.get("providerLastName")
    # statuses handled separately; keep compatibility with legacy payload keys
    db_obj.psv_status = data.get("psvStatus", data.get("psv_status", db_obj.psv_status or "NEW"))
    db_obj.committee_status = data.get("committeeStatus", data.get("committee_status", db_obj.committee_status or "NOT_STARTED"))
    db_obj.progress = data.get("progress", 0)
    db_obj.assignee = data.get("assignee")
    db_obj.source = data.get("source")
    db_obj.market = data.get("market")
    db_obj.specialty = data.get("specialty")
    db_obj.address = data.get("address")
    db_obj.npi = data.get("npi")

@router.post("/", response_model=ApplicationResponse)
def create_application(app_data: ApplicationCreate, db: Session = Depends(get_db)):
    print("Creating application with data:", app_data)
    now = datetime.now()
    existing_application = db.query(Application).filter(Application.form_id == app_data.form_id).first()
    # print("existing_application with data:", vars(existing_application))
    if existing_application:
        update_application_model(existing_application, app_data.dict(by_alias=True, exclude={"id"}))
        db.commit()
        db.refresh(existing_application)
        return model_to_response(existing_application)

    application_id = str(uuid.uuid4())
    application = Application(
        id=application_id,
        **app_data.dict(by_alias=False, exclude={"id"})
    )
    application.create_dt = now
    application.last_updt_dt = now
    db.add(application)
    db.commit()
    db.refresh(application)
    return model_to_response(application)

@router.get("/", response_model=List[ApplicationResponse])
def get_all_applications(db: Session = Depends(get_db)):
    applications = db.query(Application).order_by(desc(Application.create_dt)).all()
    if not applications:
        raise HTTPException(status_code=404, detail="No applications found")
    return [model_to_response(app) for app in applications]

@router.get("/{app_id}")
def get_application_by_id(app_id: str, db: Session = Depends(get_db)):
    application = db.query(Application).filter_by(id=app_id).first()
    if not application:
        raise HTTPException(status_code=404, detail="Application not found")

    form_data = db.query(FormData).filter_by(form_id=application.form_id).first()
    if not form_data:
        form_snapshot = {}
    else:
        form_snapshot = {
            "providerId": form_data.provider_id,
            "providerName": form_data.provider_name,
            "providerLastName": form_data.provider_last_name,
            "specialty": form_data.specialty,
            "address": form_data.address,
            "npi": form_data.npi,
            "email": form_data.email,
            "phone": form_data.phone,
        }

    docs = []
    uploads = db.query(UploadedDocument).filter(UploadedDocument.form_id == application.form_id, UploadedDocument.status != "Replaced").all()
    for u in uploads:
        docs.append({
            "id": u.id,
            "type": u.file_type,
            "filename": u.filename,
            "status": u.status,
            "ocrData": u.ocr_output and json.loads(u.ocr_output) if u.ocr_output else {},
            "jsonMatch": u.json_match and json.loads(u.json_match) if u.json_match else {},
        })

    # AI issues reuse logic from separate endpoint (light duplication to avoid extra call)
    issues = []
    first_doc = uploads[0] if uploads else None
    if first_doc and first_doc.json_match:
        try:
            json_match_data = json.loads(first_doc.json_match)
            for field, data in json_match_data.items():
                if not data.get("match"):
                    issues.append({
                        "field": field.upper(),
                        "issue": f"{field.upper()} field mismatch.",
                        "confidence": float(data.get("extracted_confident_score", 0)),
                        "value": data.get("extracted"),
                        "reasoning": f"Extracted value '{data.get('extracted')}' does not match provided value '{data.get('provided')}'."
                    })
        except Exception:
            pass

    # Add two demo issues if list empty (placeholder until ML pipeline integrated)
    if not issues:
        issues.append({
            "field": "DEA CERTIFICATE",
            "issue": "Missing Schedule II authorization",
            "confidence": 0.88,
            "value": "DEA Certificate ID: 1234567890, Authorized Schedules: III, IV, V",
            "reasoning": "Schedule II not explicitly listed for this specialty."})
        issues.append({
            "field": "CV/RESUME",
            "issue": "Gap in employment history (3 months)",
            "confidence": 0.65,
            "value": "Missing: Jan 2020 - Mar 2020",
            "reasoning": "Detected temporal gap requiring clarification."})

    # Timeline events
    events = []
    event_rows = db.query(ApplicationEvent).filter(ApplicationEvent.application_id == application.id).order_by(ApplicationEvent.created_at.asc()).all()
    for ev in event_rows:
        events.append({
            "id": ev.id,
            "type": ev.event_type,
            "message": ev.message,
            "createdAt": ev.created_at.isoformat() if ev.created_at else None
        })

    # Email summary placeholders
    emails = db.query(EmailRecord).filter_by(application_id=application.id).all()
    emails_sent = len([e for e in emails if e.status == "SENT"])

    return {
        "id": application.id,
        "npi": form_snapshot.get("npi") if form_snapshot else application.npi,
        "address": form_snapshot.get("address") if form_snapshot else application.address,
        "specialty": form_snapshot.get("specialty") if form_snapshot else application.specialty,
        "market": application.market,
        "application": {
            "id": application.id,
            "psvStatus": application.psv_status,
            "committeeStatus": application.committee_status,
            "progress": application.progress,
            "assignee": application.assignee,
            "source": application.source,
            "market": application.market,
            "createdAt": application.create_dt.isoformat() if application.create_dt else None,
            "lastUpdatedAt": application.last_updt_dt.isoformat() if application.last_updt_dt else None,
        },
        "provider": form_snapshot,
        "documents": docs,
        "aiIssues": issues,
        "summary": {
            "documentsSubmitted": len(docs),
            "documentsVerified": sum(1 for d in docs if d["status"] == "APPROVED"),
            "emailsSent": emails_sent,
            "nextAction": "Proceed to Credentialing" if application.psv_status == "COMPLETED" else "Verify outstanding documents",
        },
        "timeline": events,
    }


@router.get("/aiissues/{app_id}")
def get_ai_issues(app_id: str, db: Session = Depends(get_db)):
    # Get application with form_id
    application = db.query(Application).filter_by(id=app_id).first()
    if not application:
        raise HTTPException(status_code=404, detail="Application not found")

    # Get form data
    form_data = db.query(FormData).filter_by(form_id=application.form_id).first()
    if not form_data:
        raise HTTPException(status_code=404, detail="Form data not found")

    # Get related file upload with OCR data
    file_upload = db.query(UploadedDocument).filter_by(form_id=application.form_id).first()
    if not file_upload:
        raise HTTPException(status_code=404, detail="OCR/Match data not found")

    issues = []

    # ---------- 1. Parse JSON match ----------
    if file_upload.json_match:
        try:
            json_match_data = json.loads(file_upload.json_match)
            for field, data in json_match_data.items():
                if not data.get("match"):
                    issues.append({
                        "field": field.upper(),
                        "issue": f"{field.upper()} field mismatch.",
                        "confidence": float(data.get("extracted_confident_score", 0)),
                        "value": data.get("extracted"),
                        "reasoning": f"Extracted value '{data.get('extracted')}' does not match provided value '{data.get('provided')}'."
                    })
        except Exception:
            raise HTTPException(status_code=500, detail="Error parsing json_match")

    # # ---------- 2. Parse PDF Match ----------
    # if file_upload.pdf_match:
    #     try:
    #         pdf_data = json.loads(file_upload.pdf_match)
    #         if not pdf_data.get("match"):
    #             issues.append({
    #                 "field": "PDF Document",
    #                 "issue": "PDF and extracted layout mismatch.",
    #                 "confidence": float(pdf_data.get("confidance_score", 0)),
    #                 "value": "",
    #                 "reasoning": pdf_data.get("reason")
    #             })
    #     except Exception:
    #         raise HTTPException(status_code=500, detail="Error parsing pdf_match")

    # ---------- 3. Add mock/derived logic issues ----------
    # Example hardcoded issues — you can replace this with ML/validation logic later
    if form_data.npi == "0987654321":
        issues.append({
            "field": "NPI",
            "issue": "NPI number not found in national registry.",
            "confidence": 0.82,
            "value": form_data.npi,
            "reasoning": "The NPI provided did not return a valid result from the NPPES NPI Registry. This could be a typo or an inactive NPI."
        })

    issues.append({
        "field": "Address",
        "issue": "ZIP code and City mismatch.",
        "confidence": 0.95,
        "value": form_data.address,
        "reasoning": "(1) The ZIP code 958818 (provided in Address) does not match with the address in Driving License. However, the zip code is not valid for the California state. \n (2) The city 'Hometown' does not match with the city 'Anytown' in the Driving License."
    })

    issues.append({
        "field": "CV/Resume",
        "issue": "Gap in employment history (3 months).",
        "confidence": 0.65,
        "value": "Missing: Jan 2020 - Mar 2020",
        "reasoning": "A 3-month gap was detected between two listed employment periods. This may require clarification from the provider."
    })

    return {"issues": issues}


@router.get("/summary/{app_id}")
def get_ai_summary(app_id: str, db: Session = Depends(get_db)):
    # Get the application
    application = db.query(Application).filter_by(id=app_id).first()
    if not application:
        raise HTTPException(status_code=404, detail="Application not found")

    # Get form data
    form_data = db.query(FormData).filter_by(form_id=application.form_id).first()
    if not form_data:
        raise HTTPException(status_code=404, detail="Form data not found")

    # Get related file uploads with OCR data
    file_uploads = db.query(UploadedDocument).filter_by(form_id=application.form_id).all()
    if not file_uploads:
        raise HTTPException(status_code=404, detail="OCR/Match data not found")
    
    emails = db.query(EmailRecord).filter_by(application_id=application.id).all()

    # Example logic: count verified, approved, in progress, etc.
    total_docs = len(file_uploads)
    approved_docs = sum(1 for f in file_uploads if f.status == "APPROVED")
    in_progress_docs = sum(1 for f in file_uploads if f.status == "New" or f.status == "In Progress")

    pending_docs = total_docs - approved_docs - in_progress_docs

    # Placeholder email logic (adjust with your actual email model/status)
    if emails:
        emails_sent = len([e for e in emails if e.status == "SENT"])
        draft_emails = len([e for e in emails if e.status == "DRAFT"])
        pending_emails = len([e for e in emails if e.status == "PENDING"])
    else:
        emails_sent = 1
        draft_emails = 1
        pending_emails = 6

    return {
        "providerName": form_data.provider_name,
        "providerLastName": form_data.provider_last_name,
        "npi": form_data.npi,
    "psvStatus": application.psv_status,
    "committeeStatus": application.committee_status,
        "issues": [],
        "docsSummary": f"{approved_docs + in_progress_docs}/{total_docs} ({approved_docs} approved, {in_progress_docs} in progress)",
        "emailSummary": f"{emails_sent} sent, {draft_emails} draft, {pending_emails} pending",
        "nextActions": [
            f"Verify {pending_docs} remaining docs",
            f"Review draft, send {pending_emails} emails"
        ]
    }


@router.get("/report/{app_id}")
def generate_detailed_report(app_id: str, db: Session = Depends(get_db)):
    try:
        service = ReportService(db)
        result = service.generate_credentialing_report(app_id)
        return {"report": result["markdown"], "meta": result["data"]["session_metadata"]}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary-report/{app_id}")
def generate_short_summary_report(app_id: str, db: Session = Depends(get_db)):
    try:
        service = ReportService(db)
        result = service.generate_short_summary(app_id)
        return {"report": result["markdown"]}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Committee Review APIs (correct paths under /api/applications)
@router.get("/committee-review/", response_model=List[ApplicationResponse])
def list_committee_review_applications(db: Session = Depends(get_db)):
    rows = (
        db.query(Application)
        .filter(Application.committee_status != "NOT_STARTED")
        .order_by(desc(Application.last_updt_dt))
        .all()
    )
    return [model_to_response(a) for a in rows]

@router.post("/send-to-committee/{application_id}", response_model=ApplicationResponse)
def send_to_committee(application_id: str, db: Session = Depends(get_db)):
    app_row = db.query(Application).filter_by(id=application_id).first()
    if not app_row:
        raise HTTPException(status_code=404, detail="Application not found")
    app_row.committee_status = "IN_REVIEW"
    app_row.last_updt_dt = datetime.utcnow()
    db.commit()
    db.refresh(app_row)
    return model_to_response(app_row)