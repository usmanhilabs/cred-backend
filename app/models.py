from sqlalchemy import Column, Integer, String, Boolean, Text, ForeignKey, DateTime, Date
from .database import Base
from datetime import datetime


class FormData(Base):
    __tablename__ = "form_data"

    id = Column(Integer, primary_key=True, index=True)
    form_id = Column(String, unique=True, index=True)
    provider_id = Column(String)
    provider_name = Column(String)
    provider_last_name = Column(String)
    npi = Column(String)
    dob = Column(Date)
    email = Column(String)
    phone = Column(String)
    specialty = Column(String)
    address = Column(String)
    degree_type = Column(String)
    university = Column(String)
    year = Column(String)
    training_type = Column(String)
    experience = Column(String)
    last_org = Column(String)
    work_history_desc = Column(Text)
    dl_number = Column(String)
    ml_number = Column(String)
    other_name = Column(String)
    additional_info = Column(Text)
    info_correct = Column(Boolean)
    consent_verification = Column(Boolean)
    dl_upload_id = Column(Integer, ForeignKey("form_file_uploads.id"))
    npi_upload_id = Column(Integer, ForeignKey("form_file_uploads.id"))
    degree_upload_id = Column(Integer, ForeignKey("form_file_uploads.id"))
    training_upload_id = Column(Integer, ForeignKey("form_file_uploads.id"))
    cv_upload_id = Column(Integer, ForeignKey("form_file_uploads.id"))
    work_history_upload_id = Column(Integer, ForeignKey("form_file_uploads.id"))
    ml_upload_id = Column(Integer, ForeignKey("form_file_uploads.id"))
    other_upload_id = Column(Integer, ForeignKey("form_file_uploads.id"))
    malpractice_upload_id = Column(Integer, ForeignKey("form_file_uploads.id"))

class FormFileUpload(Base):
    __tablename__ = "form_file_uploads"

    id = Column(Integer, primary_key=True, index=True)
    form_id = Column(String, ForeignKey("form_data.form_id"))
    filename = Column(String)
    file_extension = Column(String)
    file_type = Column(String)
    status = Column(String)
    ocr_output = Column(String)
    pdf_match = Column(String)
    json_match = Column(String)


class Application(Base):
    __tablename__ = "applications"

    id = Column(String, primary_key=True, index=True)
    provider_id = Column(String)
    form_id = Column(String)
    name = Column(String)
    last_name = Column(String)
    email = Column(String)
    phone = Column(String)
    status = Column(String)
    progress = Column(Integer)
    assignee = Column(String)
    source = Column(String)
    market = Column(String)
    specialty = Column(String)
    address = Column(String)
    npi = Column(String)
    create_dt = Column(DateTime, default=datetime.utcnow)
    last_updt_dt = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class EmailRecord(Base):
    __tablename__ = "email_records"

    id = Column(String, primary_key=True, index=True)  # UUID stored as string
    application_id = Column(String, nullable=False)
    recipient_email = Column(String, nullable=False)
    subject = Column(String, nullable=False)
    body = Column(Text, nullable=False)
    status = Column(String, nullable=False)
    sent_at = Column(DateTime, default=datetime.utcnow)
