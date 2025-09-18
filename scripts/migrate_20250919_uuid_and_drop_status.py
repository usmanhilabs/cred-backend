import sqlite3, uuid, shutil
from pathlib import Path

DB = Path('credential.db')
BACKUP = Path('credential.db.bak.uuid')

# We will:
# 1. Backup DB
# 2. Rebuild applications table without legacy status column if it exists
# 3. Ensure id values are UUIDv4 (generate new for any row whose id looks like an NPI (all digits length 10) or matches pattern Row_ID)
# 4. Update email_records.application_id accordingly

def fetch_columns(cur, table):
    cur.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]

def table_exists(cur, name):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None


def needs_uuid(value: str) -> bool:
    if not value:
        return True
    # If it's already UUID, skip
    try:
        uuid.UUID(str(value))
        return False
    except Exception:
        pass
    # If numeric (NPI style) or short APP-like id
    if value.isdigit() and len(value) in (6, 7, 8, 9, 10, 12):
        return True
    if value.startswith('APP-'):
        return True
    return True  # fallback generate


def migrate():
    if not DB.exists():
        print('DB not found.')
        return
    shutil.copy(DB, BACKUP)
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    if not table_exists(cur, 'applications'):
        print('applications table missing.')
        return

    cols = fetch_columns(cur, 'applications')
    legacy_has_status = 'status' in cols

    # Build new schema columns list (exclude legacy status)
    new_columns = [c for c in cols if c != 'status']

    # Only rebuild if status present or we want to normalize ids
    cur.execute('SELECT id, npi FROM applications')
    rows = cur.fetchall()

    id_map = {}
    for old_id, npi in rows:
        if needs_uuid(old_id):
            new_id = str(uuid.uuid4())
            id_map[old_id] = new_id
        else:
            id_map[old_id] = old_id

    if legacy_has_status:
        print('Rebuilding applications table to drop legacy status...')
        # 1. Rename old table
        cur.execute('ALTER TABLE applications RENAME TO applications_old')
        # 2. Recreate without status
        # Reconstruct column definitions (simple approach: introspect old then copy sans status)
        # For simplicity we hardcode expected columns now (after prior migrations)
        cur.execute('''CREATE TABLE applications (
            id TEXT PRIMARY KEY,
            provider_id TEXT,
            form_id TEXT,
            name TEXT,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            specialty TEXT,
            address TEXT,
            npi TEXT,
            psv_status TEXT,
            committee_status TEXT,
            progress INTEGER,
            assignee TEXT,
            source TEXT,
            market TEXT,
            create_dt DATETIME,
            last_updt_dt DATETIME
        )''')
        # 3. Copy data
        cur.execute('''INSERT INTO applications (
            id, provider_id, form_id, name, last_name, email, phone, specialty, address, npi, psv_status, committee_status, progress, assignee, source, market, create_dt, last_updt_dt
        ) SELECT id, provider_id, form_id, name, last_name, email, phone, specialty, address, npi, psv_status, committee_status, progress, assignee, source, market, create_dt, last_updt_dt FROM applications_old''')
        cur.execute('DROP TABLE applications_old')

    # Update IDs if needed
    for old_id, new_id in id_map.items():
        if old_id != new_id:
            cur.execute('UPDATE applications SET id=? WHERE id=?', (new_id, old_id))
            # Update child tables referencing application id
            if table_exists(cur, 'email_records'):
                cur.execute('UPDATE email_records SET application_id=? WHERE application_id=?', (new_id, old_id))

    conn.commit()
    conn.close()
    print(f'Migration complete. {sum(1 for k,v in id_map.items() if k!=v)} IDs updated. Backup at {BACKUP}')

if __name__ == '__main__':
    migrate()
