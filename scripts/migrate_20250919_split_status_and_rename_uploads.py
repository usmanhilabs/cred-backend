import sqlite3
from pathlib import Path

DB_PATH = Path('credential.db')

PSV_STATUS_MAP = {
    # map old generic statuses if needed
    'New': 'NEW',
    'In-Progress': 'IN_PROGRESS',
    'In Progress': 'IN_PROGRESS',
    'Completed': 'COMPLETED',
    'Approved': 'APPROVED',
    'Denied': 'DENIED',
}

def column_exists(cur, table, column):
    cur.execute("PRAGMA table_info(%s)" % table)
    return any(row[1] == column for row in cur.fetchall())


def table_exists(cur, table):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def migrate():
    if not DB_PATH.exists():
        print(f"DB file {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1. Rename form_file_uploads to uploaded_documents if exists
    if table_exists(cur, 'form_file_uploads') and not table_exists(cur, 'uploaded_documents'):
        cur.execute("ALTER TABLE form_file_uploads RENAME TO uploaded_documents")
        print('Renamed table form_file_uploads -> uploaded_documents')

    # 2. Add new columns to uploaded_documents
    if table_exists(cur, 'uploaded_documents') and not column_exists(cur, 'uploaded_documents', 'llm_extraction'):
        cur.execute("ALTER TABLE uploaded_documents ADD COLUMN llm_extraction TEXT")
    if table_exists(cur, 'uploaded_documents') and not column_exists(cur, 'uploaded_documents', 'llm_summary'):
        cur.execute("ALTER TABLE uploaded_documents ADD COLUMN llm_summary TEXT")

    # 3. Add new status columns to applications
    if table_exists(cur, 'applications') and not column_exists(cur, 'applications', 'psv_status'):
        cur.execute("ALTER TABLE applications ADD COLUMN psv_status TEXT DEFAULT 'NEW'")
    if table_exists(cur, 'applications') and not column_exists(cur, 'applications', 'committee_status'):
        cur.execute("ALTER TABLE applications ADD COLUMN committee_status TEXT DEFAULT 'NOT_STARTED'")

    # 4. Backfill psv_status from legacy status if present
    # Determine if legacy status column still exists
    legacy_status_present = column_exists(cur, 'applications', 'status')
    if legacy_status_present:
        cur.execute("SELECT id, status FROM applications")
        rows = cur.fetchall()
        for app_id, legacy in rows:
            if legacy is None:
                continue
            mapped = PSV_STATUS_MAP.get(legacy, 'NEW')
            cur.execute("UPDATE applications SET psv_status = COALESCE(psv_status, ?), committee_status = COALESCE(committee_status, 'NOT_STARTED') WHERE id = ?", (mapped, app_id))
        print(f"Backfilled {len(rows)} application rows from legacy status")

    conn.commit()
    conn.close()
    print('Migration complete.')

if __name__ == '__main__':
    migrate()
