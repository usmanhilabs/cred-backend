import csv, json, uuid
from pathlib import Path
from sqlalchemy.orm import Session
import sys
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.database import SessionLocal  # noqa
from app.models import Application, UploadedDocument  # noqa

CSV_PATH = Path('data/ocrDataAndVerificationSectionData.csv')

# We treat each CSV row as board certification verification (PSV-fetched doc)
# Map file_type naming consistent with front-end categories
FILE_TYPE = 'board_certification'
SANCTIONS_TYPE = 'sanctions'

FIELDS_OCR_PREFIX = 'Board_ABMS_'
VERIFICATION_PREFIXES = [
    'Demo_Verification_Attribute1 (PDF Format Match)',
    'Demo_Verification_Attribute2 (Board Certificate Match)',
    'Demo_Verification_Attribute3 (Certification Status)'
]


def row_to_struct(row):
    ocr_payload = {}
    for k, v in row.items():
        if k.startswith('Board_ABMS_') and v:
            short = k.replace('Board_ABMS_', '').lower()
            ocr_payload[short] = v
    verification = []
    for vp in VERIFICATION_PREFIXES:
        if vp in row and row[vp]:
            # Next column assumed to be comment ("Comment N")
            idx = list(row.keys()).index(vp)
            comment_key = None
            # Try find subsequent comment column name starting with 'Comment'
            for possible in list(row.keys())[idx+1:idx+3]:
                if 'Comment' in possible:
                    comment_key = possible
                    break
            verification.append({
                'name': vp.split(' (')[0],
                'value': row[vp],
                'comment': row.get(comment_key) or ''
            })
    return ocr_payload, verification


def upsert_board_cert(db: Session, app: Application, row):
    doc = db.query(UploadedDocument).filter_by(form_id=app.form_id, file_type=FILE_TYPE).order_by(UploadedDocument.id.desc()).first()
    ocr_payload, verification = row_to_struct(row)
    status_raw = (row.get('Board_ABMS_Status') or '').lower()
    computed_status = 'APPROVED' if status_raw == 'active' else ('In Progress' if status_raw else 'In Progress')
    if doc is None:
        doc = UploadedDocument(
            form_id=app.form_id,
            filename='abms_screenshot.png',
            file_extension='png',
            file_type=FILE_TYPE,
            status=computed_status,
            ocr_output=json.dumps(ocr_payload) if ocr_payload else None,
            verification_data=json.dumps(verification) if verification else None,
        )
        db.add(doc)
    else:
        # Always update; allow empty lists
        doc.ocr_output = json.dumps(ocr_payload) if ocr_payload else doc.ocr_output
        doc.verification_data = json.dumps(verification) if verification else doc.verification_data
        if status_raw:
            doc.status = computed_status


def upsert_sanctions(db: Session, app: Application, row):
    # One sanctions doc per application
    doc = db.query(UploadedDocument).filter_by(form_id=app.form_id, file_type=SANCTIONS_TYPE).first()
    # Sanctions rows in CSV have minimal ABMS data; treat as no OCR, only verification attributes derived from Demo_ attributes if present
    ocr_payload, verification = row_to_struct(row)
    if doc is None:
        doc = UploadedDocument(
            form_id=app.form_id,
            filename='sanctions_result.pdf',
            file_extension='pdf',
            file_type=SANCTIONS_TYPE,
            status='In Progress',
            ocr_output=json.dumps(ocr_payload) if ocr_payload else None,
            verification_data=json.dumps(verification) if verification else None,
        )
        db.add(doc)
    else:
        if ocr_payload:
            doc.ocr_output = json.dumps(ocr_payload)
        if verification:
            doc.verification_data = json.dumps(verification)


def ingest():
    if not CSV_PATH.exists():
        print('CSV not found', CSV_PATH)
        return
    db = SessionLocal()
    inserted = 0
    updated = 0
    sanctions_inserted = 0
    sanctions_updated = 0
    with CSV_PATH.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            npi = row.get('npi') or row.get('NPI')
            if not npi:
                continue
            app = db.query(Application).filter_by(npi=npi).first()
            if not app:
                # Skip if application not present
                continue
            flag = (row.get('Flag') or '').lower()
            if flag == 'sanctioned':
                before_s = db.query(UploadedDocument).filter_by(form_id=app.form_id, file_type=SANCTIONS_TYPE).count()
                upsert_sanctions(db, app, row)
                after_s = db.query(UploadedDocument).filter_by(form_id=app.form_id, file_type=SANCTIONS_TYPE).count()
                if after_s>before_s:
                    sanctions_inserted += 1
                else:
                    sanctions_updated += 1
            else:
                before = db.query(UploadedDocument).filter_by(form_id=app.form_id, file_type=FILE_TYPE).count()
                upsert_board_cert(db, app, row)
                after = db.query(UploadedDocument).filter_by(form_id=app.form_id, file_type=FILE_TYPE).count()
                if after>before:
                    inserted += 1
                else:
                    updated += 1
    db.commit()
    db.close()
    print(f'Ingestion complete. BoardCert Inserted={inserted} Updated={updated} Sanctions Inserted={sanctions_inserted} Updated={sanctions_updated}')

if __name__ == '__main__':
    ingest()
