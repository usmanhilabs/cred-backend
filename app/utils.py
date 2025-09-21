from sqlalchemy import func
from fastapi import HTTPException, Depends
from app.models import Application
from app.database import SessionLocal
from sqlalchemy.orm import Session

reference_keys_map = {
    "dl" : ["fn", "dl", "ln", "class", "dob", "sex", "hair", "eyes", "hgt", "wgt", "exp"],
    "npi": ["npi", "Enumeration Date", "Status", "Primary Practice Address"],
    "degree": ["degree", "college name", "year", "major"]
}

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def generate_next_id(db: Session = Depends(get_db)) -> str:
    # Get the max numeric part from the existing APP-XXX ids
    last_id = db.query(
        func.max(Application.id)
    ).filter(Application.id.like("APP-%")).scalar()

    if last_id:
        try:
            last_num = int(last_id.split("-")[1])
        except (IndexError, ValueError):
            raise HTTPException(status_code=500, detail="Invalid ID format in DB.")
        next_num = last_num + 1
    else:
        next_num = 1

    return f"APP-{next_num:03d}"


def _normalize_status(val: str) -> str:
    if not val:
        return ""
    v = val.strip().upper().replace(" ", "_")
    # common variants
    aliases = {
        "IN_PROGRESS": "IN_PROGRESS",
        "INPROGRESS": "IN_PROGRESS",
        "NEW": "NEW",
        "NOT_STARTED": "NOT_STARTED",
        "STARTED": "IN_PROGRESS",
        "PENDING": "IN_PROGRESS",
        "COMPLETED": "COMPLETED",
        "APPROVED": "COMPLETED",
        "DONE": "COMPLETED",
        "IN_REVIEW": "IN_REVIEW",
        "REVIEW": "IN_REVIEW",
        "DECIDED": "DECIDED",
        "REJECTED": "DECIDED",
        "SANCTIONED": "SANCTIONED",
    }
    return aliases.get(v, v)


def compute_progress(psv_status: str | None, committee_status: str | None) -> int:
    """Compute an overall integer progress 0..100 from PSV and Committee statuses.

    Strategy (ranges):
    - PSV phase dominates 0..70; Committee adds 30..100
      • PSV NEW -> 0..10
      • PSV IN_PROGRESS -> 20..50 depending on finer committee state
      • PSV COMPLETED -> >= 60
    - Committee:
      • NOT_STARTED: +0
      • IN_REVIEW: +15
      • DECIDED: +30
    Final result is clamped to [0, 100].
    """

    psv = _normalize_status(psv_status or "NEW")
    cmte = _normalize_status(committee_status or "NOT_STARTED")

    # Base by PSV
    if psv == "SANCTIONED":
        base = 10
    elif psv == "NEW":
        base = 5
    elif psv == "IN_PROGRESS":
        base = 35
    elif psv == "COMPLETED":
        base = 70
    else:
        # Unknown PSV; default conservatively
        base = 20

    # Committee increments
    if cmte == "NOT_STARTED":
        inc = 0
    elif cmte == "IN_REVIEW":
        inc = 15
    elif cmte == "DECIDED":
        inc = 30
    else:
        inc = 0

    val = base + inc
    if psv == "COMPLETED" and cmte == "DECIDED":
        val = 100
    return max(0, min(100, int(val)))