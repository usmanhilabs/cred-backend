import uuid
from app.database import SessionLocal
from typing import Dict, Any, List
from sqlalchemy.orm import Session
from app.models import License, Application
from app.services.utils.excel_utils import load_excel_rows, parse_excel_date, get_first_value ,normalize_keys, parse_cell_date
from app.services.utils.file_utils import FileIngestionUtils

class LicenseService:

    def __init__(self, db: Session = SessionLocal()):
        self.db = db

    def ingest_licenses_from_excel(self, excel_path: str) -> bool:
        rows = load_excel_rows(excel_path)
        print(f"Loaded {len(rows)} rows from Excel.")

        applications = self.db.query(Application).all()
        print(f"Loaded {len(applications)} applications from DB.")

        npi_to_app_id = {str(app.npi).strip(): app.id for app in applications if app.npi}
        print(f"Built NPI to Application ID map with {len(npi_to_app_id)} entries.")

        upserts = 0
        for idx, rec in enumerate(rows):
            nrec = normalize_keys(rec)
            print(f"Processing row {idx}: {nrec}")

            # Map Excel columns to DB columns directly

            license_data = {
                "License_Website_Name": nrec.get("licensewebsitename"),
                "License_Provider_Match": nrec.get("licenseprovidermatch"),
                "License_Provider_Match_Score": nrec.get("licenseprovidermatchscore"),
                "License_Website_License_Number": nrec.get("licensewebsitelicensenumber"),
                "Recommendation_License_No": nrec.get("recommendationlicenseno"),
                "License_Website_IssuanceDate": parse_cell_date(nrec.get("licensewebsiteissuancedate")),
                "Match_with_input_IssuanceDate": nrec.get("matchwithinput"),
                "License_Website_ExpirationDate": parse_cell_date(nrec.get("licensewebsiteexpirationdate")),
                "Match_with_input_ExpirationDate": nrec.get("matchwithinputexpirationdate"),
                "License_Expiry_Issue": nrec.get("licenseexpiryissue"),
                "License_Website_BoardName": nrec.get("licensewebsiteboardname"),
                "License_Website_LicenseType": nrec.get("licensewebsitelicensetype"),
                "License_Website_PrimaryStatus": nrec.get("licensewebsiteprimarystatus"),
                "License_Website_Specialty": nrec.get("licensewebsitespecialty"),
                "License_Website_Qualification": nrec.get("licensewebsitequalification"),
                "License_Website_SchoolName": nrec.get("licensewebsiteschoolname"),
                "License_Website_GraduationYear": nrec.get("licensewebsitegraduationyear"),
                "License_Website_PreviousNames": nrec.get("licensewebsitepreviousnames"),
                "License_Website_Address": nrec.get("licensewebsiteaddress"),
                "License_Website_CurrentDateTime": parse_cell_date(nrec.get("licensewebsitecurrentdatetime")),
                "License_Website_ProfessionalUrl": nrec.get("licensewebsiteprofessionalurl"),
                "License_Website_DisciplinaryActions": nrec.get("licensewebsitedisciplinaryactions"),
                "License_Website_Public_Record_Actions": nrec.get("licensewebsitepublicrecordactions"),
            }

            print(f"Mapped license data: {license_data}")

            # Determine Application ID from NPI
            npi_val = nrec.get("npi") or nrec.get("providernpi") or nrec.get("provider_npi")
            if not npi_val:
                print(f"Row {idx} skipped: No NPI value found.")
                continue
            npi_str = str(npi_val).strip()
            application_id = npi_to_app_id.get(npi_str)
            if not application_id:
                print(f"Row {idx} skipped: No application found for NPI {npi_str}.")
                continue

            # Insert License row
            license_row = License(
                license_key=str(uuid.uuid4()),
                application_id=application_id,
                **license_data
            )
            print(f"Adding License row for application_id {application_id} with license_key {license_row.license_key}")
            self.db.add(license_row)
            upserts += 1

        self.db.commit()
        print(f"Committed {upserts} license rows to the database.")
        return True if upserts > 0 else False

    def ingest_license_files_from_zip(self, zip_path: str):
        licenses = self.db.query(License).all()
        npi_to_license_key = {}
        for lic in licenses:
            app = self.db.query(Application).filter_by(id=lic.application_id).first()
            if app and app.npi:
                npi_to_license_key[str(app.npi).strip()] = lic.license_key

        FileIngestionUtils.ingest_license_zip(zip_path, npi_to_license_key, self.db)
