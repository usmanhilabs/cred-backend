from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import FormData, FormFileUpload
import json

FORM_ID = "685b2c97-fb64-4ae8-934f-da3058256fd5"

abms_ocr = {
    "abmsuid": "813890",
    "abms_name": "Munther Ayed Hijazin",
    "abms_dob": "1988",
    "abms_education": "MD (Doctor of Medicine)",
    "abms_address": "Simi Valley, CA 93063-6321 (United States)",
    "abms_certification_board": "American Board of Psychiatry & Neurology",
    "abms_certification_type": "Neurology - General",
    "abms_status": "active",
    "abms_duration": "MOC",
    "abms_occurrence": "Recertification",
    "abms_start_date": "10/30/2017",
    "abms_end_date": "",
    "abms_reverification_date": "3/1/2026",
    "abms_participating_in_moc": "TRUE",
}

license_ocr = {
    "LicenseBoard_ExtractedLicense": "A 64753",
    "LicenseBoard_Extracted_Name": "HIJAZIN, MUNTHER A",
    "LicenseBoard_Extracted_License_Type": "Physician and Surgeon A",
    "LicenseBoard_Extracted_Primary_Status": "License Renewed & Current",
    "LicenseBoard_Extracted_Specialty": "",
    "LicenseBoard_Extracted_Qualification": "",
    "LicenseBoard_Extracted_School_Name": "University of Sassari Faculty of Medicine and Surgery",
    "LicenseBoard_Extracted_Graduation_Year": "1988",
    "LicenseBoard_Extracted_Previous_Names": "",
    "LicenseBoard_Extracted_Address": "10916 Downey Ave DOWNEY CA 90241-3709 LOS ANGELES county",
    "LicenseBoard_Extracted_Issuance_Date": "3-Apr-98",
    "LicenseBoard_Extracted_Expiration_Date": "31-Jan-26",
    "LicenseBoard_Extracted_Current_Date_Time": "August 7, 2025 9:5:35 AM",
    "LicenseBoard_Extracted_Professional_Url": "",
    "LicenseBoard_Extracted_Disciplinary_Actions": "",
    "LicenseBoard_Extracted_Public_Record_Actions": "Administrative Disciplinary Actions (NO INFORMATION TO MEET THE CRITERIA FOR POSTING)",
}

# Optional: include confidence scores for frontend patterns
for k in list(abms_ocr.keys()):
    abms_ocr[f"{k}_confident_score"] = 1.0
for k in list(license_ocr.keys()):
    license_ocr[f"{k}_confident_score"] = 1.0

psv_specs = [
    {
        "file_type": "board_certification",
        "filename": "abms_screenshot.png",
        "file_extension": "png",
        "status": "New",
        "ocr_output": abms_ocr,
    },
    {
        "file_type": "license_board",
        "filename": "license_board.png",
        "file_extension": "png",
        "status": "New",
        "ocr_output": license_ocr,
    },
    {"file_type": "sanctions", "filename": None, "file_extension": None, "status": "New", "ocr_output": {}},
    {"file_type": "hospital_privileges", "filename": None, "file_extension": None, "status": "New", "ocr_output": {}},
    {"file_type": "npi", "filename": None, "file_extension": None, "status": "New", "ocr_output": {}},
]

def upsert_file(db: Session, form_id: str, spec: dict):
    row = (
        db.query(FormFileUpload)
        .filter(FormFileUpload.form_id == form_id)
        .filter(FormFileUpload.file_type == spec["file_type"])
        .first()
    )
    if row:
        row.filename = spec["filename"]
        row.file_extension = spec["file_extension"]
        row.status = spec["status"]
        row.ocr_output = json.dumps(spec["ocr_output"]) if spec.get("ocr_output") else None
        row.pdf_match = row.pdf_match or None
        row.json_match = row.json_match or None
    else:
        row = FormFileUpload(
            form_id=form_id,
            filename=spec["filename"],
            file_extension=spec["file_extension"],
            file_type=spec["file_type"],
            status=spec["status"],
            ocr_output=json.dumps(spec["ocr_output"]) if spec.get("ocr_output") else None,
            pdf_match=None,
            json_match=None,
        )
        db.add(row)


def main():
    db: Session = SessionLocal()
    try:
        form = db.query(FormData).filter(FormData.form_id == FORM_ID).first()
        if not form:
            print(f"Form with id {FORM_ID} not found.")
            return
        for spec in psv_specs:
            upsert_file(db, FORM_ID, spec)
        db.commit()
        print("PSV OCR fakes upserted with new types.")
    finally:
        db.close()

if __name__ == "__main__":
    main()
