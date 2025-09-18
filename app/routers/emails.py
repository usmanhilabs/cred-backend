from fastapi import APIRouter, Query, HTTPException
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from ..schemas import EmailCreate
from ..models import EmailRecord
from ..database import SessionLocal
import uuid

router = APIRouter(prefix="/api/emails", tags=["Emails"])

def save_email_to_db(db: Session, email_data: EmailCreate):
    email_record = EmailRecord(
        id=str(uuid.uuid4()),
        application_id=email_data.application_id,
        recipient_email=email_data.recipient_email,
        subject=email_data.subject,
        body=email_data.body,
        status=email_data.status,
        sent_at=email_data.sent_at,
    )

    db.add(email_record)
    db.commit()
    db.refresh(email_record)
    return {"message": "Email saved successfully", "id": email_record.id}

@router.post("/save")
def save_email(email_data: EmailCreate):
    try:
        db: Session = SessionLocal()
        return save_email_to_db(db, email_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

