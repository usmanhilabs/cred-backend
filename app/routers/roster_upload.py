import uuid
from datetime import datetime

import httpx
from fastapi import APIRouter, UploadFile, File as FastAPIFile, HTTPException, Depends
import boto3, pandas as pd
from io import BytesIO
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Application, FileRecord, ServiceStatus, FormData
from app.routers.applications import update_application_model, model_to_response
from app.schemas import ApplicationCreate, ApplicationResponse
from app.utils import compute_progress
from scripts.ingest_board_certification_from_excel import process_board_certification_from_s3
from scripts.ingest_license_board_from_excel import ingest_license_board

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
        "providerId": str(row.get("Org Enterprise ID (EID)(For Internal Use Only.)") or str(uuid.uuid4())),
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

import logging

# ✅ Setup logger
logger = logging.getLogger("app")
logger.setLevel(logging.INFO)

# Add console handler if not already
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# ------------------------
# Upload to S3 API
# ------------------------
@router.post("/upload-to-s3/")
async def upload_to_s3(file: UploadFile = FastAPIFile(...)):
    db: Session = SessionLocal()
    try:
        logger.info("📂 Starting upload-to-s3 process...")

        contents = await file.read()
        logger.info(f"📄 File received: {file.filename}, size={len(contents)} bytes")

        buffer = BytesIO(contents)
        file_id = str(uuid.uuid4())
        base_name, ext = os.path.splitext(file.filename)
        logger.info(f"🆔 Generated file_id={file_id}, extension={ext}")

        # Write file to temp
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            tmp.write(contents)
            tmp_path = tmp.name
        logger.info(f"💾 Temporary file written at {tmp_path}")

        # Load into DataFrame
        if ext.lower() == ".csv":
            df = pd.read_csv(tmp_path)
        elif ext.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(tmp_path, engine="openpyxl")
        else:
            logger.error("❌ Unsupported file format")
            raise HTTPException(status_code=400, detail="Unsupported file format")
        logger.info(f"📊 Loaded file into DataFrame with {len(df)} rows")

        # Deduplication
        before_count = len(df)
        df = df.drop_duplicates(subset=[
            "Provider NPI", "First Name", "Middle Name", "Provider Last Name",
            "Provider Primary Specialty", "State License", "State Issuing License"
        ], keep="first")
        after_count = len(df)
        logger.info(f"🧹 Deduplication done → before={before_count}, after={after_count}, removed={before_count - after_count}")

        # Extract state
        state = str(df["Contracted State"].iloc[0]) if "Contracted State" in df.columns else "UNKNOWN"
        logger.info(f"🌎 State extracted: {state}")

        # Upload deduped + transformations
        bucket = "webscraping-hilabs-databucket2"
        dedup_key = f"Credentialing/Release_1/Intake/Roster/raw_file/{file_id}_deduplicated.csv"
        license_key = f"Credentialing/Release_1/PSV/License/{state}/input/{file_id}_licenseA.csv"
        sanctions_key = f"Credentialing/Release_1/PSV/Sanctions/{state}/input/{file_id}_licenseB.csv"

        # Upload deduplicated file
        dedup_buffer = BytesIO()
        df.to_csv(dedup_buffer, index=False)
        dedup_buffer.seek(0)
        s3_client.upload_fileobj(dedup_buffer, bucket, dedup_key)
        logger.info(f"✅ Deduplicated file uploaded to s3://{bucket}/{dedup_key}")

        # Transformation A
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
        logger.info(f"📤 License A format uploaded to s3://{bucket}/{license_key}")

        # Transformation B
        df_b = pd.DataFrame({
            "RowID": df["ISFRowID"],
            "NPI": df["Provider NPI"],
            "FirstName": df["First Name"],
            "MiddleName": df["Middle Name"],
            "LastName": df["Provider Last Name"],
            "Specialty": df["Provider Primary Specialty"],
            "License": df["State License"],
            "DOB": "",
            "State": df["State Issuing License"],
        })
        buf_b = BytesIO()
        df_b.to_csv(buf_b, index=False)
        buf_b.seek(0)
        s3_client.upload_fileobj(buf_b, bucket, sanctions_key)
        logger.info(f"📤 License B format uploaded to s3://{bucket}/{sanctions_key}")

        # DB record
        file_record = FileRecord(file_id=file_id, type="application_upload",
                                 created_by="system", status="UPLOADED",
                                 file_storage_path=dedup_key)
        db.add(file_record)
        db.commit()
        logger.info("🗂️ FileRecord entry created in DB")

        # Trigger App2
        execution_id = str(uuid.uuid4())
        service_id = str(uuid.uuid4())
        app2_url = "http://127.0.0.1:8001/pipeline/pipeline/run-unified"
        run_req = {
            "file_id": file_id,
            "file_path": f"s3://{bucket}/{license_key}",
            "service_id": service_id,
            "market": state,
            "execution_id": execution_id
        }
        logger.info(f"⚡ Calling App2 API {app2_url} with payload={run_req}")
        async with httpx.AsyncClient() as client:
            resp = await client.post(app2_url, json=run_req)
            logger.info(f"📡 Response from App2: {resp.status_code}, {resp.text}")
            if resp.status_code != 200:
                logger.error("❌ Failed to trigger App2")
                raise HTTPException(status_code=500, detail=f"Failed to trigger App2: {resp.text}")

        # Process applications
        results = []
        for _, row in df.iterrows():
            try:
                mapped = map_row_to_application(row.to_dict())
                app_data = ApplicationCreate(**mapped)
                create_or_update_application(app_data, db)
                db.add(ServiceStatus(file_id=file_id, service_id=str(uuid.uuid4()), status="PROCESS_STARTED"))
                results.append({"provider_name": app_data.name, "npi": app_data.npi, "status": "success"})
            except Exception as e:
                logger.error(f"❌ Error processing row: {e}")
                results.append({"npi": row.get("Provider NPI"), "error": str(e)})

        file_record.status = "PROCESSED"
        db.commit()
        logger.info(f"✅ Processing finished, {len(results)} records saved")

        os.remove(tmp_path)
        logger.info("🧹 Temporary file removed")

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
        logger.error(f"💥 Upload failed: {e}", exc_info=True)
        if 'file_record' in locals():
            file_record.status = "FAILED"
            db.commit()
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

    finally:
        db.close()
        logger.info("🔒 DB session closed")


from app.models import ServiceExecution
from app.schemas import ServiceExecutionRequest, ServiceName


@router.post("/service/record")
def record_service_execution(request: ServiceExecutionRequest):
    db: Session = SessionLocal()
    try:
        logger.info(f"📝 Received service execution record request → {request.dict()}")

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
        logger.info(f"✅ Record saved to DB with id={new_record.id}, status={request.status}")

        # ✅ Only proceed if status is SUCCESS
        if request.status.upper() != "SUCCESS":
            logger.warning(f"⏹ Workflow stopped → status={request.status}, no further triggers")
            return {
                "message": f"Record saved but status={request.status}, stopping workflow.",
                "id": new_record.id
            }

        # ✅ Continue to next step if SUCCESS
        if request.service_name == ServiceName.LICENCE:
            logger.info("⚡ Triggering Licence Service...")
            trigger_licence_service(request)
        elif request.service_name == ServiceName.NPI:
            logger.info("⚡ Triggering NPI Service...")
            trigger_npi_service(request)
        elif request.service_name == ServiceName.BoardCertificate:
            logger.info("⚡ Triggering Board Certificate Service...")
            trigger_board_service(request)

        logger.info(f"🎯 Workflow continued for service={request.service_name.value}, record_id={new_record.id}")
        return {"message": "Record saved and next step triggered", "id": new_record.id}

    except Exception as e:
        logger.error(f"💥 Error in record_service_execution: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Service execution failed: {str(e)}")

    finally:
        db.close()
        logger.info("🔒 DB session closed")



# Dummy service triggers
def trigger_licence_service(req: ServiceExecutionRequest):
    # print(f"Triggering Licence Service for {req.file_id}")
    ingest_license_board(req.file_path)
    print(f"Licence Service completed for {req.file_id}")

def trigger_npi_service(req: ServiceExecutionRequest):
    print(f"Triggering NPI Service for {req.file_id}")

def trigger_board_service(req: ServiceExecutionRequest):
    process_board_certification_from_s3(req.file_path)
    print(f"Triggering Credentialing Service for {req.file_id}")

import boto3
import tempfile
from fastapi import APIRouter, HTTPException