import uuid
from datetime import datetime
from fastapi import APIRouter, UploadFile, File as FastAPIFile, HTTPException, Depends
import boto3, pandas as pd
from io import BytesIO
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Application, FileRecord, ServiceStatus, FormData
from app.routers.applications import update_application_model, model_to_response
from app.schemas import ApplicationCreate, ApplicationResponse
from app.utils import compute_progress

router = APIRouter()
import os
import boto3

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1",
)


def create_or_update_application(app_data: ApplicationCreate, db: Session) -> ApplicationResponse:
    now = datetime.now()
    existing_application = db.query(Application).filter(Application.form_id == app_data.form_id).first()

    if existing_application:
        update_application_model(existing_application, app_data.dict(by_alias=True, exclude={"id"}))
        db.commit()
        db.refresh(existing_application)
        return model_to_response(existing_application)

    # Create Application entry
    application_id = str(uuid.uuid4())
    application = Application(
        id=application_id,
        **app_data.dict(by_alias=False, exclude={"id"})
    )
    application.create_dt = now
    application.last_updt_dt = now
    application.progress = compute_progress(application.psv_status, application.committee_status)

    db.add(application)
    db.commit()
    db.refresh(application)

    # Check if FormData already exists for this form_id
    existing_form = db.query(FormData).filter(FormData.form_id == application.form_id).first()
    if not existing_form:
        form_data = FormData(
            form_id=application.form_id,
            provider_id=application.provider_id,
            provider_name=application.name,
            provider_last_name=application.last_name,
            npi=application.npi,
            email=application.email,
            phone=application.phone,
            specialty=application.specialty,
            address=application.address
        )
        db.add(form_data)
        db.commit()
        db.refresh(form_data)

    return model_to_response(application)



import hashlib

def generate_form_id(row: dict) -> str:
    # Create a deterministic key for the provider
    unique_key = f"{row.get('Provider NPI')}-{row.get('First Name')}-{row.get('Middle Name')}-{row.get('Provider Last Name')}-{row.get('State License')}-{row.get('State Issuing License')}"
    return hashlib.md5(unique_key.encode()).hexdigest()

def map_row_to_application(row: dict) -> dict:
    address = f"{row.get('Addr Line 1', '')} {row.get('Addr Line 2', '')} {row.get('City', '')} {row.get('State', '')} {row.get(' Zip+4', '')}".strip()
    return {
        "id": str(uuid.uuid4()),
        "providerId": str(row.get("Org Enterprise ID (EID)(For Internal Use Only.)") or ""),
        "formId": str(row.get("Form ID") or generate_form_id(row)),
        "name": f"{row.get('First Name', '')} {row.get('Middle Name', '')} {row.get('Provider Last Name', '')}".strip(),
        "providerLastName": row.get("Provider Last Name") or "",
        "email": row.get("Provider Email") or "",
        "phone": str(row.get("Phone Number") or ""),
        "specialty": row.get("Provider Primary Specialty") or "",
        "address": address,
        "npi": str(row.get("Provider NPI") or ""),
        "psvStatus": "New",
        "committeeStatus": "New",
        "progress": 5,
        "assignee": "Unassigned",
        "source": "Manual Entry",
        "market": row.get("State") or "",
        "createDt": datetime.now(),
        "lastUpdtDt": datetime.now(),
        "psvOriginalLabel": ""
    }



import tempfile
import os

@router.post("/upload-to-s3/")
async def upload_to_s3(file: UploadFile = FastAPIFile(...)):
    db: Session = SessionLocal()
    try:
        contents = await file.read()
        buffer = BytesIO(contents)

        file_id = str(uuid.uuid4())
        base_name, ext = os.path.splitext(file.filename)

        # Write file to temp for pandas
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            tmp.write(contents)
            tmp_path = tmp.name

        # Load into DataFrame
        if ext.lower() == ".csv":
            df = pd.read_csv(tmp_path)
        elif ext.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(tmp_path, engine="openpyxl")
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")

        # Deduplication
        dedupe_columns = [
            "Provider NPI", "First Name", "Middle Name", "Provider Last Name",
            "Provider Primary Specialty", "State License", "State Issuing License"
        ]
        before_count = len(df)
        df = df.drop_duplicates(subset=dedupe_columns, keep="first")
        after_count = len(df)

        # Extract state
        state = str(df["Contracted State"].iloc[0]) if "Contracted State" in df.columns else "UNKNOWN"

        # Always use .csv for uploads
        bucket = "webscraping-hilabs-databucket2"
        dedup_key = f"Credentialing/Release_1/Intake/Roster/raw_file/{file_id}_deduplicated.csv"
        license_key = f"Credentialing/Release_1/PSV/License/{state}/input/{file_id}_licenseA.csv"
        sanctions_key = f"Credentialing/Release_1/PSV/Sanctions/{state}/input/{file_id}_licenseB.csv"

        # --- Upload deduplicated ISF ---
        dedup_buffer = BytesIO()
        df.to_csv(dedup_buffer, index=False)
        dedup_buffer.seek(0)
        s3_client.upload_fileobj(dedup_buffer, bucket, dedup_key)

        # --- Transformation A ---
        df_a = pd.DataFrame({
            "NPI": df["Provider NPI"],
            "FirstName": df["First Name"],
            "MiddleName": df["Middle Name"],
            "LastName": df["Provider Last Name"],
            "Specialty": df["Provider Primary Specialty"],
            "State License Number": df["State License"],
            "State Issuing License": df["State Issuing License"],
            "License Effective Date": df["License effective date"],
            "License Expiration Date": df["License Term Date"],
        })
        buf_a = BytesIO()
        df_a.to_csv(buf_a, index=False)
        buf_a.seek(0)
        s3_client.upload_fileobj(buf_a, bucket, license_key)

        # --- Transformation B ---
        df_b = pd.DataFrame({
            "RowID": df["ISFRowID"],
            "NPI": df["Provider NPI"],
            "FirstName": df["First Name"],
            "MiddleName": df["Middle Name"],
            "LastName": df["Provider Last Name"],
            "Specialty": df["Provider Primary Specialty"],
            "License": df["State License"],
            "DOB": "",  # blank
            "State": df["State Issuing License"],
        })
        buf_b = BytesIO()
        df_b.to_csv(buf_b, index=False)
        buf_b.seek(0)
        s3_client.upload_fileobj(buf_b, bucket, sanctions_key)

        print(f"✅ License File B uploaded to S3 → s3://{bucket}/{sanctions_key}")

        # --- DB record for file ---
        file_record = FileRecord(
            file_id=file_id,
            type="application_upload",
            created_by="system",
            status="UPLOADED",
            file_storage_path=dedup_key
        )
        db.add(file_record)
        db.commit()
        db.refresh(file_record)

        # Process rows into applications
        results = []
        for _, row in df.iterrows():
            try:
                mapped = map_row_to_application(row.to_dict())
                app_data = ApplicationCreate(**mapped)
                create_or_update_application(app_data, db)
                service_id = str(uuid.uuid4())
                db.add(ServiceStatus(file_id=file_id, service_id=service_id, status="PROCESS_STARTED"))
                results.append({"provider_name": app_data.name, "npi": app_data.npi, "status": "success"})
            except Exception as e:
                results.append({"npi": row.get("Provider NPI"), "error": str(e)})

        file_record.status = "PROCESSED"
        db.commit()

        os.remove(tmp_path)

        return {
            "file_id": file_id,
            "deduplicated_rows": after_count,
            "removed_duplicates": before_count - after_count,
            "s3_files": {
                "deduplicated_isf": f"s3://{bucket}/{dedup_key}",
                "license_format_A": f"s3://{bucket}/{license_key}",
                "license_format_B": f"s3://{bucket}/{sanctions_key}"
            },
            "processed_records": len(results),
            "results": results
        }

    except Exception as e:
        if 'file_record' in locals():
            file_record.status = "FAILED"
            db.commit()
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

    finally:
        db.close()


from app.models import ServiceExecution
from app.schemas import ServiceExecutionRequest, ServiceName


@router.post("/service/record")
def record_service_execution(request: ServiceExecutionRequest):
    db: Session = SessionLocal()
    # Save to DB
    new_record = ServiceExecution(
        file_id=request.file_id,
        service_id=request.service_id,
        execution_id=request.execution_id,
        service_name=request.service_name.value,
        file_path=request.file_path,
        message=request.message,
        status=request.status
    )
    db.add(new_record)
    db.commit()
    db.refresh(new_record)

    # ✅ Only proceed if status is SUCCESS
    if request.status.upper() != "SUCCESS":
        return {
            "message": f"Record saved but status={request.status}, stopping workflow.",
            "id": new_record.id
        }

    # ✅ Continue to next step if SUCCESS
    if request.service_name == ServiceName.LICENCE:
        trigger_licence_service(request)
    elif request.service_name == ServiceName.NPI:
        trigger_npi_service(request)
    elif request.service_name == ServiceName.BoardCertificate:
        trigger_credentialing_service(request)

    return {"message": "Record saved and next step triggered", "id": new_record.id}



# Dummy service triggers
def trigger_licence_service(req: ServiceExecutionRequest):
    print(f"Triggering Licence Service for {req.file_id}")

def trigger_npi_service(req: ServiceExecutionRequest):
    print(f"Triggering NPI Service for {req.file_id}")

def trigger_credentialing_service(req: ServiceExecutionRequest):
    print(f"Triggering Credentialing Service for {req.file_id}")

