# import os
# import sys
# import json
# from typing import Any, Dict, List
#
# CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
# if PROJECT_ROOT not in sys.path:
#     sys.path.insert(0, PROJECT_ROOT)
#
# import openpyxl  # type: ignore
# from sqlalchemy.orm import Session
# from app.database import SessionLocal
# from app.models import UploadedDocument, Application, FormData, SavedFile
#
# EXCEL_PATH = os.path.join(PROJECT_ROOT, "data", "BoardCertificate_License_Schema.xlsx")
# SHEET_NAME = "Board_Certification"
#
#
# def _s(val: Any) -> Any:
#     if val is None:
#         return None
#     try:
#         import datetime as _dt
#         if isinstance(val, (_dt.date, _dt.datetime)):
#             return val.isoformat()
#     except Exception:
#         pass
#     if isinstance(val, (str, int, float, bool)):
#         return val
#     return str(val)
#
#
# def detect_header_row(ws) -> int:
#     # Look for a header that contains either NPI, provider_name, or Board_ABMS_*
#     max_scan = min(8, ws.max_row)
#     for r in range(1, max_scan + 1):
#         vals = [(str(c.value) or "").strip().lower() for c in ws[r]]
#         if any("npi" in v for v in vals) or any(v.startswith("board_abms_") for v in vals) or any("provider_name" in v for v in vals):
#             return r
#     return 1
#
#
# def load_rows(path: str) -> List[Dict[str, Any]]:
#     wb = openpyxl.load_workbook(path, data_only=True)
#     if SHEET_NAME not in wb.sheetnames:
#         raise RuntimeError(f"Sheet '{SHEET_NAME}' not found in Excel file {path}")
#     ws = wb[SHEET_NAME]
#     header_row = detect_header_row(ws)
#     headers_by_idx: Dict[int, str] = {}
#     for j, cell in enumerate(ws[header_row], start=1):
#         headers_by_idx[j] = (str(cell.value) or "").strip()
#     rows: List[Dict[str, Any]] = []
#     for i in range(header_row + 1, ws.max_row + 1):
#         rec: Dict[str, Any] = {}
#         empty = True
#         for j, key in headers_by_idx.items():
#             v = ws.cell(row=i, column=j).value
#             rec[key] = v
#             if v not in (None, ""):
#                 empty = False
#         if not empty:
#             rows.append(rec)
#     return rows
#
#
# def _norm_keys(rec: Dict[str, Any]) -> Dict[str, Any]:
#     # Normalize keys for fuzzy matching: lowercase and remove non-alphanumeric
#     out = {}
#     import re
#     for k, v in rec.items():
#         nk = re.sub(r"[^a-z0-9]", "", (str(k) or "").lower())
#         out[nk] = v
#     return out
#
#
# def _get_first(nrec: Dict[str, Any], candidates: List[str]):
#     for c in candidates:
#         if c in nrec and nrec[c] not in (None, ""):
#             return nrec[c]
#     return None
#
#
# def build_payload(rec: Dict[str, Any]) -> Dict[str, Any]:
#     nrec = _norm_keys(rec)
#     # Canonical OCR keys expected by API for board_certification
#     ocr = {
#         "abmsuid": _s(_get_first(nrec, [
#             "boardabmsuid", "abmsuid"
#         ])),
#         "abms_name": _s(_get_first(nrec, [
#             "providername", "provider_name", "abmsname", "boardabmsname"
#         ])),
#         "abms_dob": _s(_get_first(nrec, [
#             "boardabmsdob", "dob"
#         ])),
#         "abms_education": _s(_get_first(nrec, [
#             "boardabmseducation"
#         ])),
#         "abms_address": _s(_get_first(nrec, [
#             "boardabmsaddress"
#         ])),
#         "abms_certification_board": _s(_get_first(nrec, [
#             "boardabmscertificationboard"
#         ])),
#         "abms_certification_type": _s(_get_first(nrec, [
#             "boardabmscertificationtype"
#         ])),
#         "abms_status": _s(_get_first(nrec, [
#             "boardabmsstatus"
#         ])),
#         "abms_duration": _s(_get_first(nrec, [
#             "boardabmsduration"
#         ])),
#         "abms_occurrence": _s(_get_first(nrec, [
#             "boardabmsoccurrence"
#         ])),
#         "abms_start_date": _s(_get_first(nrec, [
#             "boardabmsstartdate", "boardabmsstart_date"
#         ])),
#         "abms_end_date": _s(_get_first(nrec, [
#             "boardabmsenddate", "boardabmsend_date"
#         ])),
#         "abms_reverification_date": _s(_get_first(nrec, [
#             "boardabmsreverificationdate"
#         ])),
#         "abms_participating_in_moc": _s(_get_first(nrec, [
#             "boardabmsparticipatinginmoc", "boardabmsparticipatinginmoc"
#         ])),
#     }
#     # Drop None values
#     ocr = {k: v for k, v in ocr.items() if v not in (None, "")}
#
#     # Verification fields
#     pdf_match = _s(_get_first(nrec, [
#         "demo_verification_attribute1pdfformatmatch".replace("_", ""),
#         "pdfformatmatch"
#     ]))
#     cert_match = _s(_get_first(nrec, [
#         "demo_verification_attribute2boardcertificatematch".replace("_", ""),
#         "boardcertificatematch"
#     ]))
#     cert_status = _s(_get_first(nrec, [
#         "demo_verification_attribute3certificationstatus".replace("_", ""),
#         "certificationstatus"
#     ]))
#
#     # Comments typically appear as Comment 1, Comment 2, Comment 3
#     comment1 = _s(_get_first(nrec, ["comment1"]))
#     comment2 = _s(_get_first(nrec, ["comment2"]))
#     comment3 = _s(_get_first(nrec, ["comment3"]))
#
#     verification = {
#         "pdf_format_match": pdf_match,
#         "board_certificate_match": cert_match,
#         "certification_status": cert_status,
#         "comment_1": comment1,
#         "comment_2": comment2,
#         "comment_3": comment3,
#     }
#     # Remove empty entries
#     verification = {k: v for k, v in verification.items() if v not in (None, "")}
#
#     # Extract npi for traceability
#     npi_val = _get_first(nrec, ["npi", "providernpi", "provider_npi"])
#     npi_str = None
#     if npi_val is not None:
#         try:
#             npi_str = str(int(npi_val)) if isinstance(npi_val, (int, float)) else str(npi_val)
#             npi_str = npi_str.strip()
#         except Exception:
#             npi_str = str(npi_val).strip()
#
#     return {
#         "source": f"{os.path.basename(EXCEL_PATH)}:{SHEET_NAME}",
#         "npi": npi_str,
#         "ocr": ocr,
#         "verification": verification,
#     }
#
#
# def upsert_board_certification(db: Session, form_id: str, payload: Dict[str, Any], filename: str = None):
#     row = (
#         db.query(UploadedDocument)
#         .filter(UploadedDocument.form_id == form_id)
#         .filter(UploadedDocument.file_type == "board_certification")
#         .first()
#     )
#     if not row:
#         row = UploadedDocument(
#             form_id=form_id,
#             filename=filename,
#             file_extension="png",
#             file_type="board_certification",
#             status="In Progress",
#         )
#         db.add(row)
#         db.flush()
#     else:
#         row.filename = filename
#         row.file_extension = "png"
#     row.ocr_output = json.dumps(payload.get("ocr", {}))
#     row.verification_data = json.dumps(payload.get("verification", {}))
#     if not row.status:
#         row.status = "In Progress"
#     return row
#
# import zipfile
#
# def ingest_board_files(zip_path, npi_list):
#     session = SessionLocal()
#     try:
#         with zipfile.ZipFile(zip_path, "r") as zf:
#             for file_info in zf.infolist():
#                 for npi in npi_list:
#                     if npi in file_info.filename:
#                         file_data = zf.read(file_info.filename)
#                         filename = f"{npi}_Board_Certificate.png"
#                         saved_file = SavedFile(
#                             filename=filename,
#                             file_type="png",
#                             file_data=file_data,
#                             attribute="board_certificate",
#                         )
#                         session.add(saved_file)
#                         break  # Avoid duplicate uploads for the same file
#         session.commit()
#         print("Board files ingested.")
#     except Exception as e:
#         session.rollback()
#         print(f"Error: {e}")
#     finally:
#         session.close()
#
#
# def main():
#     rows = load_rows(EXCEL_PATH)
#     print(f"Loaded Board_Certification rows: {len(rows)}")
#
#     db: Session = SessionLocal()
#     try:
#         # Build NPI -> form_id index
#         npi_to_form: Dict[str, str] = {}
#         npi_list = []
#         for f in db.query(FormData).all():
#             if f.npi and f.form_id:
#                 npi_str = str(f.npi).strip()
#                 npi_to_form[npi_str] = f.form_id
#                 npi_list.append(npi_str)
#         for a in db.query(Application).all():
#             if a.npi and a.form_id:
#                 npi_str = str(a.npi).strip()
#                 npi_to_form.setdefault(npi_str, a.form_id)
#                 if npi_str not in npi_list:
#                     npi_list.append(npi_str)
#
#         # Ingest board certificate files from zip
#         zip_path = os.path.join(PROJECT_ROOT, "data", "CA_ABMS_Sample 1.zip")
#         ingest_board_files(zip_path, npi_list)
#
#         upserts = 0
#         for rec in rows:
#             # ... (existing upsert logic)
#             nrec = _norm_keys(rec)
#             npi_val = None
#             for cand in ("npi", "providernpi", "provider_npi"):
#                 if cand in nrec and nrec[cand] not in (None, ""):
#                     npi_val = nrec[cand]
#                     break
#             if not npi_val:
#                 continue
#             try:
#                 npi = str(int(npi_val)) if isinstance(npi_val, (int, float)) else str(npi_val)
#                 npi = npi.strip()
#             except Exception:
#                 npi = str(npi_val).strip()
#             form_id = npi_to_form.get(npi)
#             if not form_id:
#                 continue
#             payload = build_payload(rec)
#             filename = f"{npi}_Board_Certificate.png"
#             upsert_board_certification(db, form_id, payload, filename)
#             upserts += 1
#         db.commit()
#         print(f"Board Certification ingest complete. Upserts={upserts}")
#     finally:
#         db.close()
# def main():
#     rows = load_rows(EXCEL_PATH)
#     print(f"Loaded Board_Certification rows: {len(rows)}")
#
#     db: Session = SessionLocal()
#     try:
#         # Build NPI -> form_id index
#         npi_to_form: Dict[str, str] = {}
#         for f in db.query(FormData).all():
#             if f.npi and f.form_id:
#                 npi_to_form[str(f.npi).strip()] = f.form_id
#         for a in db.query(Application).all():
#             if a.npi and a.form_id:
#                 npi_to_form.setdefault(str(a.npi).strip(), a.form_id)
#
#         upserts = 0
#         for rec in rows:
#             # Determine form by NPI
#             nrec = _norm_keys(rec)
#             npi_val = None
#             for cand in ("npi", "providernpi", "provider_npi"):
#                 if cand in nrec and nrec[cand] not in (None, ""):
#                     npi_val = nrec[cand]
#                     break
#             if not npi_val:
#                 continue
#             try:
#                 npi = str(int(npi_val)) if isinstance(npi_val, (int, float)) else str(npi_val)
#                 npi = npi.strip()
#             except Exception:
#                 npi = str(npi_val).strip()
#             form_id = npi_to_form.get(npi)
#             if not form_id:
#                 continue
#             payload = build_payload(rec)
#             upsert_board_certification(db, form_id, payload)
#             upserts += 1
#         db.commit()
#         print(f"Board Certification ingest complete. Upserts={upserts}")
#     finally:
#         db.close()
#
#
# if __name__ == "__main__":
#     main()import csv


import os
import csv
import json
import tempfile
import logging
from typing import Any, Dict, List, Optional

import boto3
import requests
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import UploadedDocument, Application, FormData, SavedFile


# --------------------------------------------------------------------------
# Logger Setup
# --------------------------------------------------------------------------
logger = logging.getLogger("Board_Certification_Ingest")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | Board_Certification_Ingest | %(message)s"
)
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)


# --------------------------------------------------------------------------
# AWS S3 Utilities
# --------------------------------------------------------------------------
def get_s3_client():
    """Return a configured boto3 S3 client."""
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def download_from_s3(s3_path: str) -> str:
    """Download S3 file to a temporary local path."""
    assert s3_path.startswith("s3://"), f"Invalid S3 path: {s3_path}"

    bucket, key = s3_path[5:].split("/", 1)
    _, ext = os.path.splitext(key)
    ext = ext or ".csv"

    tmp_file = tempfile.mktemp(suffix=ext)
    s3 = get_s3_client()

    try:
        s3.download_file(bucket, key, tmp_file)
        logger.info(f"Downloaded from S3: {s3_path} -> {tmp_file}")
        return tmp_file
    except Exception as e:
        logger.error(f"Failed to download from S3: {s3_path} | Error: {e}", exc_info=True)
        raise


# --------------------------------------------------------------------------
# Helper Functions
# --------------------------------------------------------------------------
def _s(val: Any) -> Any:
    """Normalize and stringify any value."""
    if val is None:
        return None
    try:
        import datetime as _dt
        if isinstance(val, (_dt.date, _dt.datetime)):
            return val.isoformat()
    except Exception:
        pass
    if isinstance(val, (str, int, float, bool)):
        return str(val).strip()
    return str(val)


def load_rows(path: str) -> List[Dict[str, Any]]:
    """Load rows from a CSV file."""
    ext = os.path.splitext(path)[-1].lower()
    if ext != ".csv":
        raise ValueError(f"Unsupported file type: {ext}. Only .csv is supported.")

    rows = []
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                clean_row = {
                    (k.strip() if k else k): (_s(v) if v not in (None, "", " ") else None)
                    for k, v in row.items()
                }
                if any(v for v in clean_row.values()):
                    rows.append(clean_row)
        logger.info(f"Loaded {len(rows)} valid rows from CSV file.")
    except Exception as e:
        logger.error(f"Error reading CSV file {path}: {e}", exc_info=True)
        raise
    return rows


def build_payload(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Construct OCR and verification payload dictionaries from a CSV row."""
    ocr = {
        "abms_uid": _s(rec.get("ABMS_Uid")),
        "abms_name": _s(rec.get("ABMS_Name")),
        "abms_dob": _s(rec.get("ABMS_DOB")),
        "abms_education": _s(rec.get("ABMS_Education")),
        "abms_address": _s(rec.get("ABMS_Address")),
        "abms_certification_board": _s(rec.get("ABMS_CertificationBoard")),
        "abms_certification_type": _s(rec.get("ABMS_CertificationType")),
        "abms_status": _s(rec.get("ABMS_Status")),
        "abms_duration": _s(rec.get("ABMS_Duration")),
        "abms_occurrence": _s(rec.get("ABMS_Occurrence")),
        "abms_start_date": _s(rec.get("ABMS_StartDate")),
        "abms_end_date": _s(rec.get("ABMS_EndDate")),
        "abms_reverification_date": _s(rec.get("ABMS_ReverificationDate")),
        "abms_participating_in_moc": _s(rec.get("ABMS_ParticipatingInMoc")),
    }
    ocr = {k: v for k, v in ocr.items() if v}

    verification = {
        "board_certificate_match": _s(rec.get("ABMS_Certification_Board_Match")),
        "input_validation": _s(rec.get("ABMS_Input_Validation")),
    }
    verification = {k: v for k, v in verification.items() if v}

    npi = _s(rec.get("Provider NPI")) or _s(rec.get("ABMS_NPI"))

    return {
        "source": "ABMS_Board_Certification_CSV",
        "npi": npi,
        "ocr": ocr,
        "verification": verification,
    }


def upsert_board_certification(
    db: Session, form_id: str, payload: Dict[str, Any], npi: Optional[str] = None
):
    """Insert or update a record in UploadedDocument for a given form_id and filename."""
    filename = f"{npi}_Board_Certification.csv" if npi else None

    # Check if a row already exists for this form_id and file_type
    row = (
        db.query(UploadedDocument)
        .filter(UploadedDocument.form_id == form_id)
        .filter(UploadedDocument.file_type == "board_certification")
        .first()
    )

    if not row:
        row = UploadedDocument(
            form_id=form_id,
            file_type="board_certification",
            file_extension="csv",
            status="In Progress",
            filename=filename,
        )
        db.add(row)
        db.flush()
        logger.info(f"Inserted new UploadedDocument for form_id={form_id}, filename={filename}")
    else:
        # Update filename if missing or different
        if filename and row.filename != filename:
            row.filename = filename
            logger.info(f"Updated filename in UploadedDocument for form_id={form_id}: {filename}")

    # Update OCR/verification data
    row.ocr_output = json.dumps(payload.get("ocr", {}))
    row.verification_data = json.dumps(payload.get("verification", {}))
    if not row.status:
        row.status = "In Progress"

    return row


# --------------------------------------------------------------------------
# Save ABMS File from S3/URL
# --------------------------------------------------------------------------
def save_file_from_s3(db: Session, npi: str, s3_path: Optional[str]):
    """Fetch file from S3 or HTTP URL and store (or update) in SavedFile table."""
    if not s3_path:
        return

    try:
        file_data = None

        # Fetch file bytes
        if s3_path.startswith("s3://"):
            bucket, key = s3_path.replace("s3://", "").split("/", 1)
            s3 = get_s3_client()
            file_data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            logger.info(f"Downloaded ABMS file from S3: {s3_path}")

        elif s3_path.startswith("http"):
            resp = requests.get(s3_path, timeout=30)
            resp.raise_for_status()
            file_data = resp.content
            logger.info(f"Downloaded ABMS file from URL: {s3_path}")

        else:
            logger.warning(f"Unsupported ABMS_FilePath: {s3_path}")
            return

        # Infer file extension or default to png
        _, ext = os.path.splitext(s3_path)
        ext = ext.replace(".", "") or "png"
        filename = f"{npi}_Board_Certification.{ext}"

        # Check for existing SavedFile entry with the same filename
        existing_file = (
            db.query(SavedFile)
            .filter(SavedFile.filename == filename)
            .filter(SavedFile.attribute == "board_certification")
            .first()
        )

        if existing_file:
            logger.info(f"File already exists for NPI={npi}, updating existing SavedFile entry.")
            existing_file.file_data = file_data
            existing_file.file_type = ext
        else:
            saved_file = SavedFile(
                filename=filename,
                file_type=ext,
                file_data=file_data,
                attribute="board_certification",
            )
            db.add(saved_file)
            logger.info(f"Inserted new SavedFile for NPI={npi}, filename={filename}")

    except Exception as e:
        logger.warning(f"Could not save ABMS file for NPI={npi}: {e}", exc_info=True)

# --------------------------------------------------------------------------
# Main Processing Function
# --------------------------------------------------------------------------
def process_board_certification_from_s3(s3_path: str) -> dict:
    """
    Ingest ABMS Board Certification data from an S3 CSV file.
    - Downloads CSV from S3
    - Loads and parses rows
    - Upserts to UploadedDocument
    - Saves ABMS_FilePath files to SavedFile
    """
    logger.info(f"Starting Board Certification ingestion for file: {s3_path}")

    try:
        local_path = download_from_s3(s3_path)
        rows = load_rows(local_path)
    except Exception as e:
        logger.error(f"Failed before ingestion started: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}

    db: Session = SessionLocal()
    upserts = 0
    saved_files = 0
    skipped = 0

    try:
        # Build NPI → form_id mapping
        logger.info("Building NPI to FormID mapping.")
        npi_to_form: Dict[str, str] = {}
        for f in db.query(FormData).all():
            if f.npi and f.form_id:
                npi_to_form[str(f.npi).strip()] = f.form_id
        for a in db.query(Application).all():
            if a.npi and a.form_id:
                npi_to_form.setdefault(str(a.npi).strip(), a.form_id)
        logger.info(f"Loaded {len(npi_to_form)} mappings from database.")

        # Process rows
        for rec in rows:
            npi = _s(rec.get("Provider NPI")) or _s(rec.get("ABMS_NPI"))
            if not npi:
                skipped += 1
                continue

            form_id = npi_to_form.get(str(npi).strip())
            if not form_id:
                skipped += 1
                continue

            payload = build_payload(rec)
            upsert_board_certification(db, form_id, payload)
            upserts += 1

            # Save ABMS_FilePath if present
            abms_file_path = rec.get("ABMS_FilePath")
            if abms_file_path:
                save_file_from_s3(db, npi, abms_file_path)
                saved_files += 1

        db.commit()
        logger.info(
            f"Board Certification ingestion complete. "
            f"Processed={len(rows)}, Upserts={upserts}, SavedFiles={saved_files}, Skipped={skipped}"
        )

        return {
            "status": "success",
            "rows_processed": len(rows),
            "upserts": upserts,
            "saved_files": saved_files,
            "skipped": skipped,
        }

    except Exception as e:
        db.rollback()
        logger.error(f"Error during ingestion: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}

    finally:
        db.close()
        logger.info("Database session closed.")
