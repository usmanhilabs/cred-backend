import csv
import os, sys
from sqlalchemy.orm import Session

# ensure project root on path
CURRENT_DIR = os.path.dirname(os.path.abspath(__FILE__)) if '__FILE__' in globals() else os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.models import Application
from app.database import SessionLocal

def compute_progress(psv_status: str | None, committee_status: str | None) -> int:
    def norm(v: str | None) -> str:
        if not v:
            return ""
        x = v.strip().upper().replace(" ", "_")
        aliases = {
            "INPROGRESS": "IN_PROGRESS",
            "APPROVED": "COMPLETED",
        }
        return aliases.get(x, x)
    psv = norm(psv_status or "NEW")
    cmte = norm(committee_status or "NOT_STARTED")
    if psv == "SANCTIONED":
        base = 10
    elif psv == "NEW":
        base = 5
    elif psv == "IN_PROGRESS":
        base = 35
    elif psv == "COMPLETED":
        base = 70
    else:
        base = 20
    if cmte == "IN_REVIEW":
        inc = 15
    elif cmte == "DECIDED":
        inc = 30
    else:
        inc = 0
    val = base + inc
    if psv == "COMPLETED" and cmte == "DECIDED":
        val = 100
    return max(0, min(100, int(val)))

ASSIGNEES = [
    "Alex Johnson",
    "Priya Singh",
    "Marcus Lee",
    "Sofia Martinez",
]

def main():
    data_csv = os.path.join(PROJECT_ROOT, "data", "newData.csv")
    db: Session = SessionLocal()
    try:
        sanctioned_npies = set()
        with open(data_csv, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                status = (row.get("PSV_Status") or "").strip()
                npi = (row.get("npi") or "").strip()
                if status.lower() == "sanctioned" and npi:
                    sanctioned_npies.add(npi)
        print(f"Found sanctioned NPIs in CSV: {len(sanctioned_npies)}")

        # Update matching applications by NPI
        updated = 0
        assigned = 0
        assignee_index = 0
        rows = db.query(Application).all()
        for app in rows:
            changed = False
            if app.npi and app.npi in sanctioned_npies:
                if app.psv_status != "SANCTIONED":
                    app.psv_status = "SANCTIONED"
                    changed = True
                if app.committee_status != "SANCTIONED":
                    app.committee_status = "SANCTIONED"
                    changed = True
            # assign assignees round-robin if missing
            if not app.assignee:
                app.assignee = ASSIGNEES[assignee_index % len(ASSIGNEES)]
                assignee_index += 1
                assigned += 1
                changed = True
            if changed:
                app.progress = compute_progress(app.psv_status, app.committee_status)
                updated += 1
        db.commit()
        print(f"Sanctioned/assignees update complete. Applications scanned={len(rows)} updated={updated} newly_assigned={assigned}")
    finally:
        db.close()

if __name__ == "__main__":
    main()
