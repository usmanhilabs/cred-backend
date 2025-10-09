import uuid

from sqlalchemy import Column, Integer, String, Boolean, Text, ForeignKey, DateTime, Date, LargeBinary, UUID
from sqlalchemy.orm import declarative_mixin, relationship
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
    dl_upload_id = Column(Integer, ForeignKey("uploaded_documents.id"))
    npi_upload_id = Column(Integer, ForeignKey("uploaded_documents.id"))
    degree_upload_id = Column(Integer, ForeignKey("uploaded_documents.id"))
    training_upload_id = Column(Integer, ForeignKey("uploaded_documents.id"))
    cv_upload_id = Column(Integer, ForeignKey("uploaded_documents.id"))
    work_history_upload_id = Column(Integer, ForeignKey("uploaded_documents.id"))
    ml_upload_id = Column(Integer, ForeignKey("uploaded_documents.id"))
    other_upload_id = Column(Integer, ForeignKey("uploaded_documents.id"))
    malpractice_upload_id = Column(Integer, ForeignKey("uploaded_documents.id"))

class UploadedDocument(Base):
    """Renamed from form_file_uploads. Holds uploaded documents plus OCR/LLM outputs."""
    __tablename__ = "uploaded_documents"  # migration will rename old table

    id = Column(Integer, primary_key=True, index=True)
    form_id = Column(String, ForeignKey("form_data.form_id"))
    filename = Column(String)
    file_extension = Column(String)
    file_type = Column(String)  # logical category (e.g., DL, NPI, CV, board_certification)
    status = Column(String)  # New, In Progress, Approved, Replaced, etc.
    ocr_output = Column(String)  # raw OCR JSON/text
    pdf_match = Column(String)   # pdf structural match output
    json_match = Column(String)  # field-level match results
    llm_extraction = Column(Text)  # future: structured extraction JSON
    llm_summary = Column(Text)     # future: summarization of document
    verification_data = Column(Text)  # structured verification / matching results


class Application(Base):
    __tablename__ = "applications"

    id = Column(String, primary_key=True, index=True)
    provider_id = Column(String)
    form_id = Column(String)
    # denormalized provider snapshot fields (may be trimmed later):
    name = Column(String)
    last_name = Column(String)
    email = Column(String)
    phone = Column(String)
    specialty = Column(String)
    address = Column(String)
    npi = Column(String)

    # New split statuses
    psv_status = Column(String, default="NEW", index=True)
    committee_status = Column(String, default="NOT_STARTED", index=True)
    psv_original_label = Column(String)  # raw label from ingestion (e.g., Sanctioned, Pending Review, Needs Further Review)
    progress = Column(Integer, default=0)
    assignee = Column(String)
    source = Column(String)
    market = Column(String)

    # Legacy column 'status' may still exist in DB; not mapped here intentionally.
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


class ApplicationEvent(Base):
    __tablename__ = "application_events"

    id = Column(Integer, primary_key=True, index=True)
    application_id = Column(String, index=True)
    event_type = Column(String)  # e.g., SYSTEM, COMMENT, STATUS_CHANGE
    message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

class SavedFile(Base):
    __tablename__ = "saved_files"
    id = Column(Integer, primary_key=True)
    filename = Column(String, nullable=False)
    file_type = Column(String, nullable=False)
    attribute = Column(String, nullable=True)
    file_data = Column(LargeBinary, nullable=False)  # Store file content as BLOB
    created_at = Column(DateTime, default=datetime.utcnow)

class FileRecord(Base):
    __tablename__ = "file_records"   # 👈 consistent name

    file_id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    type = Column(String, nullable=False)
    created_by = Column(String, nullable=False)
    status = Column(String, default="UPLOADED")
    file_storage_path = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ServiceStatus(Base):
    __tablename__ = "service_status"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    file_id = Column(String, ForeignKey("file_records.file_id", ondelete="CASCADE"), nullable=False)  # 👈 match above
    service_id = Column(String, nullable=False)
    status = Column(String, default="PROCESS_STARTED")
    created_at = Column(DateTime, default=datetime.utcnow)


class BoardCertificate(Base):
    __tablename__ = "Boardcertificate"

    boardcertificate_key = Column(String, primary_key=True)
    application_id = Column(String, ForeignKey("applications.id"), nullable=False)

    abmsuid = Column(Text)
    abms_name = Column(Text)
    abms_dob = Column(Date)
    abms_education = Column(Text)
    abms_address = Column(Text)
    abms_certification_board = Column(Text)
    abms_certification_type = Column(Text)
    abms_status = Column(Text)
    abms_duration = Column(Text)
    abms_occurrence = Column(Text)
    abms_start_date = Column(Date)
    abms_end_date = Column(Date)
    abms_reverification_date = Column(Date)
    abms_participating_in_moc = Column(Text)

class BoardCertificateFile(Base):
    __tablename__ = "boardcertificate_files"

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    boardcertificate_key = Column(String, ForeignKey("Boardcertificate.boardcertificate_key"), nullable=False)

    filename = Column(String, nullable=False)
    file_type = Column(String, nullable=False)
    file_data = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime)

class License(Base):
    __tablename__ = "License"

    license_key = Column(String, primary_key=True, nullable=False)  # UUID generated at ingestion
    application_id = Column(String, ForeignKey("applications.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)

    License_Website_Name = Column(String)
    License_Provider_Match = Column(String)
    License_Provider_Match_Score = Column(String)
    License_Website_License_Number = Column(String)
    Recommendation_License_No = Column(String)
    License_Website_IssuanceDate = Column(Date)
    Match_with_input_IssuanceDate = Column(String)
    License_Website_ExpirationDate = Column(Date)
    Match_with_input_ExpirationDate = Column(String)
    License_Expiry_Issue = Column(String)
    License_Website_BoardName = Column(String)
    License_Website_LicenseType = Column(String)
    License_Website_PrimaryStatus = Column(String)
    License_Website_Specialty = Column(String)
    License_Website_Qualification = Column(String)
    License_Website_SchoolName = Column(String)
    License_Website_GraduationYear = Column(String)
    License_Website_PreviousNames = Column(String)
    License_Website_Address = Column(String)
    License_Website_CurrentDateTime = Column(DateTime)
    License_Website_ProfessionalUrl = Column(String)
    License_Website_DisciplinaryActions = Column(String)
    License_Website_Public_Record_Actions = Column(String)

class LicenseFile(Base):
    __tablename__ = "license_files"

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    license_key = Column(String, ForeignKey("License.license_key", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)

    filename = Column(String, nullable=False)
    file_type = Column(String, nullable=False)
    file_data = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime)