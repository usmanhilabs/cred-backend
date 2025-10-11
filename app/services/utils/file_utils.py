import zipfile
from datetime import datetime
from typing import List
from sqlalchemy.orm import Session
import uuid
from app.models import LicenseFile

class FileIngestionUtils:

    @staticmethod
    def ingest_license_zip(zip_path: str, npi_to_license_key: dict, db: Session):
        """
        Reads ZIP file and saves License files in DB linked to License table via license_key.
        """
        with zipfile.ZipFile(zip_path, "r") as zf:
            processed_npis = set()
            for file_info in zf.infolist():
                if file_info.is_dir():
                    continue

                filename = file_info.filename.split("/")[-1]  # Get just the file name
                npi_candidate = filename.split("_")[0]  # Assuming filename starts with NPI
                print(f"Processing file: {filename}, NPI candidate: {npi_candidate}")  # Debug

                # Skip if this NPI has already been processed
                if npi_candidate in processed_npis:
                    print(f"NPI {npi_candidate} already processed, skipping file {filename}")  # Debug
                    continue

                license_key = npi_to_license_key.get(npi_candidate)
                print(f"License key found: {license_key}")  # Debug

                if not license_key:
                    # Skip files without a matching license
                    print(f"No license key for {npi_candidate}, skipping file {filename}")  # Debug
                    continue

                # Check if file already exists
                existing_file = (
                    db.query(LicenseFile)
                    .filter_by(license_key=license_key, filename=filename)
                    .first()
                )
                if existing_file:
                    print(f"File {filename} already exists for license {license_key}, skipping.")  # Debug
                    processed_npis.add(npi_candidate)
                    continue

                # Read file data
                file_data = zf.read(file_info.filename)
                print(f"Read {len(file_data)} bytes from {filename}")  # Debug

                # Create LicenseFile record
                lic_file = LicenseFile(
                    license_key=license_key,
                    filename=filename,
                    file_type=filename.split(".")[-1],
                    file_data=file_data,
                    created_at=datetime.utcnow()
                )
                db.add(lic_file)
                print(f"Added LicenseFile record for {filename}")  # Debug

                # Mark this NPI as processed
                processed_npis.add(npi_candidate)

                db.commit()
                print("Database commit complete.")  # Debug