from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import FormData, FormFileUpload
import json

FORM_ID = "685b2c97-fb64-4ae8-934f-da3058256fd5"

psv_records = [
    {
    "file_type": "board_certification",
        "filename": "abms_screenshot.png",
        "file_extension": "png",
        "status": "New",
        "ocr_output": {
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
        },
    },
    {
    "file_type": "license_board",
        "filename": "license_board.png",
        "file_extension": "png",
        "status": "New",
        "ocr_output": {
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
            "LicenseBoard_Extracted_Current_Date_Time": "August 7, 20259:5:35 AM",
            "LicenseBoard_Extracted_Professional_Url": "",
            "LicenseBoard_Extracted_Disciplinary_Actions": "",
            "LicenseBoard_Extracted_Public_Record_Actions": "Administrative Disciplinary Actions (NO INFORMATION TO MEET THE CRITERIA FOR POSTING)",
        },
    },
    {
    "file_type": "sanctions",
        "filename": None,
        "file_extension": None,
        "status": "New",
        "ocr_output": {},
    },
    {
    "file_type": "hospital_privileges",
        "filename": None,
        "file_extension": None,
        "status": "New",
        "ocr_output": {},
    },
    {
    "file_type": "npi",
        "filename": None,
        "file_extension": None,
        "status": "New",
        "ocr_output": {},
    },
]

def main():
    db: Session = SessionLocal()

    try:
        form = db.query(FormData).filter(FormData.form_id == FORM_ID).first()
        if not form:
            print(f"Form with id {FORM_ID} not found. Aborting.")
            return

        for rec in psv_records:
            exists = (
                db.query(FormFileUpload)
                .filter(FormFileUpload.form_id == FORM_ID)
                .filter(FormFileUpload.file_type == rec["file_type"])
                .first()
            )
            if exists:
                continue

            row = FormFileUpload(
                form_id=FORM_ID,
                filename=rec["filename"],
                file_extension=rec["file_extension"],
                file_type=rec["file_type"],
                status=rec["status"],
                ocr_output=json.dumps(rec["ocr_output"]) if rec.get("ocr_output") else None,
                pdf_match=None,
                json_match=None,
            )
            db.add(row)
        db.commit()
        print("PSV records seeded.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
