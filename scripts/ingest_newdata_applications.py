import csv
from pathlib import Path
from sqlalchemy.orm import Session
from datetime import datetime
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.database import SessionLocal  # noqa: E402
from app.models import Application  # noqa: E402
import uuid

CSV_PATH = Path('data/newData.csv')

# Map CSV PSV_Status to internal allowed values
PSV_NORMALIZE = {
    'Sanctioned': 'IN_PROGRESS',  # assuming analyst still working
    'Pending Review': 'NEW',
    'Needs Further Review': 'IN_PROGRESS',
    'In-Progress': 'IN_PROGRESS',
    'In Progress': 'IN_PROGRESS',
    'Completed': 'COMPLETED',
}

ALLOWED_PSV = {"NEW","IN_PROGRESS","COMPLETED","IN_COMMITTE_REVIEW","APPROVED","DENIED"}

def normalize_psv(raw: str) -> str:
    if not raw:
        return 'NEW'
    mapped = PSV_NORMALIZE.get(raw.strip(), raw.strip().upper().replace(' ', '_'))
    if mapped not in ALLOWED_PSV:
        mapped = 'NEW'
    return mapped

def upsert_application(db: Session, row: dict):
    provider_name = row['provider_name']
    last_name = row['provider_last_name']
    npi = row['npi']
    address = row['address']
    specialty = row['specialty']
    market = row.get('Market') or row.get('market') or ''
    source = row.get('Source') or row.get('source') or 'Manual Entry'
    psv_status_raw = row.get('PSV_Status') or row.get('PSV_STATUS') or row.get('psv_status')
    psv_status = normalize_psv(psv_status_raw)

    # Find existing by NPI snapshot (assuming uniqueness in seed set)
    app = db.query(Application).filter_by(npi=npi).first()
    app_id = app.id if app else str(uuid.uuid4())
    now = datetime.utcnow()
    if app:
        app.name = provider_name
        app.last_name = last_name
        app.npi = npi
        app.address = address
        app.specialty = specialty
        app.market = market
        app.source = source
        app.psv_status = psv_status if not app.psv_status or app.psv_status == 'NEW' else app.psv_status
        if psv_status_raw:
            app.psv_original_label = psv_status_raw
        if not app.committee_status:
            app.committee_status = 'NOT_STARTED'
        app.last_updt_dt = now
    else:
        app = Application(
            id=app_id,
            provider_id=row['Row_ID'],
            form_id=row['Row_ID'],  # placeholder until real form id linkage
            name=provider_name,
            last_name=last_name,
            email=row.get('email') or None,
            phone=row.get('Phone') or None,
            specialty=specialty,
            address=address,
            npi=npi,
            psv_status=psv_status,
            committee_status='NOT_STARTED',
            progress=0,
            assignee='system',
            source=source,
            market=market,
            psv_original_label=psv_status_raw,
        )
        db.add(app)
    return app_id


def ingest():
    if not CSV_PATH.exists():
        print(f"CSV not found: {CSV_PATH}")
        return
    db = SessionLocal()
    inserted = 0
    updated = 0
    with CSV_PATH.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Harmonize headers (original CSV uses provider_first_name/last_name -> provider_name built already)
            if 'provider_name' not in row and 'provider_first_name' in row and 'provider_last_name' in row:
                row['provider_name'] = f"{row['provider_first_name']} {row['provider_last_name']}".strip()
            pre = db.query(Application).filter_by(id=row.get('npi') or row.get('Row_ID')).first()
            app_id = upsert_application(db, row)
            post = db.query(Application).filter_by(id=app_id).first()
            if not pre:
                inserted += 1
            else:
                updated += 1
    db.commit()
    db.close()
    print(f"Ingestion complete. Inserted={inserted} Updated={updated}")

if __name__ == '__main__':
    ingest()
