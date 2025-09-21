import sys, os
from sqlalchemy.orm import Session

# Ensure project root is on sys.path when running directly
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.utils import get_db, compute_progress
from app.models import Application


def main():
    db: Session = next(get_db())
    try:
        rows = db.query(Application).all()
        updated = 0
        for a in rows:
            newp = compute_progress(a.psv_status, a.committee_status)
            if a.progress != newp:
                a.progress = newp
                updated += 1
        db.commit()
        print(f"Backfill complete. Applications processed={len(rows)} updated={updated}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
