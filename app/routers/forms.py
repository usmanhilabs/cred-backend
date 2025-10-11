from fastapi import APIRouter, HTTPException, Query
from sqlalchemy.orm import Session
from ..database import SessionLocal
from ..models import FormData, Application
from ..schemas import FormDataSchema
from datetime import datetime
from typing import Optional

router = APIRouter(prefix="/api/forms", tags=["Forms"])

def update_form_model(db_obj: FormData, data: dict):
    db_obj.provider_id = data.get("providerId")
    db_obj.provider_name = data.get("providerName")
    db_obj.provider_last_name = data.get("providerLastName")
    db_obj.npi = data.get("npi")
    db_obj.email = data.get("email")
    db_obj.phone = data.get("phone")
    db_obj.dob = datetime.strptime(data.get("dob"), '%Y-%m-%d').date()
    db_obj.specialty = data.get("specialty")
    db_obj.address = data.get("address")
    db_obj.degree_type = data.get("degreeType")
    db_obj.university = data.get("university")
    db_obj.year = data.get("year")
    db_obj.training_type = data.get("training-type")
    db_obj.experience = data.get("experience")
    db_obj.last_org = data.get("lastOrg")
    db_obj.work_history_desc = data.get("work-history-desc")
    db_obj.dl_number = data.get("dl-number")
    db_obj.ml_number = data.get("ml-number")
    db_obj.other_name = data.get("other-name")
    db_obj.additional_info = data.get("additional-info")
    db_obj.info_correct = data.get("info-correct")
    db_obj.consent_verification = data.get("consent-verification")
    db_obj.dl_upload_id = data.get("dl-upload-id")
    db_obj.npi_upload_id = data.get("npi-upload-id")
    db_obj.degree_upload_id = data.get("degree-upload-id")
    db_obj.training_upload_id = data.get("training-upload-id")
    db_obj.cv_upload_id = data.get("cv-upload-id")
    db_obj.work_history_upload_id = data.get("work-history-upload-id")
    db_obj.ml_upload_id = data.get("ml-upload-id")
    db_obj.other_upload_id = data.get("other-upload-id")
    db_obj.malpractice_upload_id = data.get("malpractice-upload-id")


def model_to_reponse(db_obj: FormData):
    return {
        "providerId": db_obj.provider_id,
        "providerName": db_obj.provider_name,
        "providerLastName": db_obj.provider_last_name,
        "npi": db_obj.npi,
        "dob": db_obj.dob,
        "email": db_obj.email,
        "phone": db_obj.phone,
        "specialty": db_obj.specialty,
        "address": db_obj.address,
        "degreeType": db_obj.degree_type,
        "university": db_obj.university,
        "year": db_obj.year,
        "training_type": db_obj.training_type,
        "experience": db_obj.experience,
        "lastOrg": db_obj.last_org,
        "work_history_desc": db_obj.work_history_desc,
        "dl_number": db_obj.dl_number,
        "ml_number": db_obj.ml_number,
        "other_name": db_obj.other_name,
        "additional_info": db_obj.additional_info,
        "info-correct": db_obj.info_correct,
        "consent-verification": db_obj.consent_verification,
        "dl-upload-id": db_obj.dl_upload_id,
        "npi-upload-id": db_obj.npi_upload_id,
        "degree-upload-id": db_obj.degree_upload_id,
        "training-upload-id": db_obj.training_upload_id,
        "cv-upload-id": db_obj.cv_upload_id,
        "work_history-upload-id": db_obj.work_history_upload_id,
        "ml-upload-id": db_obj.ml_upload_id,
        "other-upload-id": db_obj.other_upload_id,
        "malpractice-upload-id": db_obj.malpractice_upload_id
    }

@router.post("/create-form")
def create_form(payload: dict):
    form_id = payload.get("formId")
    db: Session = SessionLocal()
    if db.query(FormData).filter_by(form_id=form_id).first():
        db.close()
        raise HTTPException(status_code=400, detail="Form already exists")
    db.add(FormData(form_id=form_id))
    db.commit()
    db.close()
    return {"formId": form_id}

@router.post("/save-form")
def save_form(payload: FormDataSchema):
    db: Session = SessionLocal()
    form = db.query(FormData).filter_by(form_id=payload.formId).first()
    if not form:
        form = FormData(form_id=payload.formId)
    update_form_model(form, payload.data)
    db.add(form)
    db.commit()
    db.close()
    return {"message": "Form saved"}

@router.post("/submit-form")
def submit_form(payload: FormDataSchema):
    db: Session = SessionLocal()
    form = db.query(FormData).filter_by(form_id=payload.formId).first()
    if not form:
        raise HTTPException(status_code=404, detail="Form not found")
    update_form_model(form, payload.data)
    db.add(form)
    db.commit()
    db.close()
    return {"message": "Form submitted"}

@router.get("/")
def get_form(formId: Optional[str] = Query(None),
    appId: Optional[str] = Query(None)):
    db: Session = SessionLocal()
    if appId:
        application = db.query(Application).filter(Application.id == appId).first()
        formId = application.form_id

    form = db.query(FormData).filter_by(form_id=formId).first()
    db.close()
    if not form:
        raise HTTPException(status_code=404, detail="Form not found")
    return model_to_reponse(form)