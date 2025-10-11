import csv
import io
import json
import logging
import os
import re
import uuid
from dataclasses import dataclass
from typing import Dict, Any, List, Optional

import boto3
import openpyxl
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Application, UploadedDocument, SavedFile


logger = logging.getLogger(__name__)


# Expected headers from the sanctions CSV
EXPECTED_HEADERS = [
    "Input_RowID",
    "NPI",
    "FirstName",
    "MiddleName",
    "LastName",
    "Specialty",
    "State",
    "License",
    "DOB",
    "Federal Sources",
    "Primary State Sources",
    "Secondary State Sources",
    "Primary State Evidence",
    "Secondary State Evidence",
    "Federal Evidence",
    "Sanctioned Confidence",
    "Sanctioned Flag",
    "Provider_report_path",
]


def _s3_client():
    """Use AWS default credential provider chain (env vars, shared config, IAM role)."""
    # Region defaults to us-east-1 if not provided
    return boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    if not s3_uri:
        raise ValueError("Empty S3 URI")
    if not s3_uri.lower().startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    path = s3_uri[5:]
    first_slash = path.find("/")
    if first_slash == -1:
        raise ValueError(f"Invalid S3 URI, missing key: {s3_uri}")
    return path[:first_slash], path[first_slash + 1 :]


def download_s3_bytes(s3_uri: str) -> bytes:
    bucket, key = parse_s3_uri(s3_uri)
    client = _s3_client()
    logger.info(f"Downloading sanctions file from s3://{bucket}/{key}")
    obj = client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def download_s3_binary(s3_uri: str) -> bytes:
    bucket, key = parse_s3_uri(s3_uri)
    client = _s3_client()
    logger.info(f"Downloading provider report from s3://{bucket}/{key}")
    obj = client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def _load_rows_from_bytes(data: bytes, s3_uri: str) -> List[Dict[str, Any]]:
    """Load rows from CSV or XLSX bytes using first row as header."""
    lower = s3_uri.lower()
    if lower.endswith(".csv") or lower.endswith(".txt"):
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("latin-1")
        reader = csv.DictReader(io.StringIO(text))
        return [dict(r) for r in reader]
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True)
        # Prefer `Sheet1` if present, else case-insensitive match, else first worksheet
        target_ws = None
        try:
            if "Sheet1" in wb.sheetnames:
                target_ws = wb["Sheet1"]
            else:
                # Case-insensitive / trimmed fallback
                name_map = {s.strip().lower(): s for s in wb.sheetnames}
                if "sheet1" in name_map:
                    target_ws = wb[name_map["sheet1"]]
        except Exception:
            target_ws = None
        ws = target_ws if target_ws is not None else wb.worksheets[0]
        logger.info(f"Sanctions XLSX: using worksheet '{ws.title}'")
        headers = [str(c.value) if c.value is not None else "" for c in ws[1]]
        rows: List[Dict[str, Any]] = []
        for i in range(2, ws.max_row + 1):
            rec: Dict[str, Any] = {}
            empty = True
            for j, h in enumerate(headers, start=1):
                val = ws.cell(row=i, column=j).value
                rec[h] = val
                if val not in (None, ""):
                    empty = False
            if not empty:
                rows.append(rec)
        return rows
    raise ValueError(f"Unsupported file extension for {s3_uri}")


def _split_sources(value: Optional[str]) -> List[str]:
    if not value:
        return []
    # Sources may be like: "OIG: https://... | SAM: https://..." or lines separated
    parts = re.split(r"\||\n|,\s*(?=https?://)", str(value))
    return [p.strip() for p in parts if p and p.strip()]


def _build_verification_entries(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []

    provider_name_in_doc = " ".join(
        [str(row.get("FirstName") or "").strip(), str(row.get("MiddleName") or "").strip(), str(row.get("LastName") or "").strip()]
    ).strip()
    specialty = str(row.get("Specialty") or "").strip()

    def add_entry(source: str, link: str, level: str, state: Optional[str] = None):
        if level == "federal":
            comment = (
                f"For federal {source}, the application name {{provider_name}} has been found as sanctioned "
                f"with the name {provider_name_in_doc} {specialty}"
            )
        else:
            comment = (
                f"For the state of {state}, the application name {{provider_name}} has been found as sanctioned "
                f"with the name {provider_name_in_doc} {specialty}"
            )
        entries.append({
            "level": level,
            "source": source,
            "url": link,
            "comment_template": comment,
        })

    # Federal sources
    fed_sources = _split_sources(row.get("Federal Sources"))
    for part in fed_sources:
        # expect format like "OIG: https://..." or just url
        if ":" in part:
            src, url = [x.strip() for x in part.split(":", 1)]
        else:
            src, url = "Federal", part
        if src.upper().startswith("OIG"):
            add_entry("OIG", url, level="federal")
        elif src.upper().startswith("SAM"):
            add_entry("SAM", url, level="federal")
        else:
            add_entry(src, url, level="federal")

    # Primary state sources - format like "CA: https://..."
    prim_sources = _split_sources(row.get("Primary State Sources"))
    for part in prim_sources:
        if ":" in part:
            st, url = [x.strip() for x in part.split(":", 1)]
            add_entry(st, url, level="state-primary", state=st)

    # Secondary state sources
    sec_sources = _split_sources(row.get("Secondary State Sources"))
    for part in sec_sources:
        if ":" in part:
            st, url = [x.strip() for x in part.split(":", 1)]
            add_entry(st, url, level="state-secondary", state=st)

    return entries


def _parse_evidence(value: Optional[str]) -> Any:
    if not value:
        return None
    txt = str(value).strip()
    # could be single JSON or multiple comma-separated JSONs
    if txt.startswith("{") and txt.endswith("}"):
        try:
            return json.loads(txt)
        except Exception:
            return txt
    # attempt to parse list of JSONs separated by '},{'
    try:
        if "},{" in txt and not txt.strip().startswith("["):
            return [json.loads(x) for x in re.findall(r"\{.*?\}", txt)]
        return json.loads(txt)
    except Exception:
        return txt


def ingest_sanctions_from_s3(csv_or_xlsx_s3_uri: str) -> bool:
    """Main entry: download sanctions CSV/XLSX from S3 and ingest into UploadedDocument and SavedFile."""
    db: Session = SessionLocal()
    try:
        data = download_s3_bytes(csv_or_xlsx_s3_uri)
        rows = _load_rows_from_bytes(data, csv_or_xlsx_s3_uri)
        logger.info(f"Sanctions CSV rows: {len(rows)}")

        # Build NPI => (application, provider display name)
        apps = db.query(Application).all()
        npi_to_app: Dict[str, Application] = {}
        for app in apps:
            if app.npi:
                npi_to_app[str(app.npi).strip()] = app

        inserted = 0
        for r in rows:
            npi = str(r.get("NPI") or "").strip()
            app = npi_to_app.get(npi)
            if not app:
                logger.warning(f"No application for NPI={npi}, skipping row")
                continue

            # Prepare verification_data
            verif_entries = _build_verification_entries(r)
            # Build user-facing comments
            provider_full = app.name or ""
            oig_entry = next((e for e in verif_entries if e.get("level") == "federal" and e.get("source") == "OIG"), None)
            sam_entry = next((e for e in verif_entries if e.get("level") == "federal" and e.get("source") == "SAM"), None)
            state_entry = next((e for e in verif_entries if e.get("level", "").startswith("state")), None)
            comment_1 = oig_entry["comment_template"].replace("{provider_name}", provider_full) if oig_entry else None
            comment_2 = sam_entry["comment_template"].replace("{provider_name}", provider_full) if sam_entry else None
            state_comment = (
                state_entry["comment_template"].replace("{provider_name}", provider_full) if state_entry else None
            )
            any_sanction = bool(oig_entry or sam_entry or state_entry)

            verification = {
                "npi": npi,
                "provider": {"name": app.name, "application_id": app.id},
                "sanction": {
                    "status": "SANCTIONED" if any_sanction else "NOT FOUND",
                    "details": ", ".join(
                        [
                            *( ["OIG"] if oig_entry else [] ),
                            *( ["SAM"] if sam_entry else [] ),
                            *( ["STATE"] if state_entry else [] ),
                        ]
                    ) or None,
                    "confidence": r.get("Sanctioned Confidence"),
                    "flag": r.get("Sanctioned Flag"),
                    "comment_1": comment_1,
                    "comment_2": comment_2,
                    "state_comment": state_comment,
                },
                "sources": verif_entries,
                "evidence": {
                    "federal": _parse_evidence(r.get("Federal Evidence")),
                    "primary_state": _parse_evidence(r.get("Primary State Evidence")),
                    "secondary_state": _parse_evidence(r.get("Secondary State Evidence")),
                },
            }

            # Upsert UploadedDocument for sanctions
            upload = UploadedDocument(
                form_id=app.form_id,
                filename=f"sanctions_{npi}.json",
                file_extension="json",
                file_type="sanctions",
                status="New",
                ocr_output=json.dumps({  # surface a readable subset
                    "NPI": npi,
                    "FirstName": r.get("FirstName"),
                    "LastName": r.get("LastName"),
                    "Specialty": r.get("Specialty"),
                    "State": r.get("State"),
                    "License": r.get("License"),
                    "DOB": r.get("DOB"),
                }),
                json_match=json.dumps({}),
                verification_data=json.dumps(verification),
            )
            db.add(upload)
            inserted += 1

            # Download provider report and store in SavedFile if available
            report_path = (r.get("Provider_report_path") or "").strip()
            if report_path.lower().startswith("s3://"):
                try:
                    content = download_s3_binary(report_path)
                    filename = report_path.split("/")[-1]
                    existing = db.query(SavedFile).filter(SavedFile.filename == filename).first()
                    if not existing:
                        db.add(
                            SavedFile(
                                filename=filename,
                                file_type=filename.split(".")[-1],
                                attribute=f"sanctions:{app.id}",
                                file_data=content,
                            )
                        )
                except Exception as e:
                    logger.warning(f"Failed to download provider report {report_path}: {e}")

        db.commit()
        logger.info(f"Inserted sanctions uploads: {inserted}")
        return inserted > 0
    except Exception as e:
        db.rollback()
        logger.exception(f"Sanctions ingestion failed: {e}")
        return False
    finally:
        db.close()
