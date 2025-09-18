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