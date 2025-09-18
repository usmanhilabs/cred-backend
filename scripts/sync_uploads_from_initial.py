import os
import re
import shutil
import sqlite3
from glob import glob

ROOT = os.path.dirname(os.path.dirname(__file__))
TARGET_DB = os.path.join(ROOT, 'credential.db')
INITIAL_DB = os.path.join(ROOT, 'intial_cred.db.db')
UPLOADS_DIR = os.path.join(ROOT, 'uploads')

RX = re.compile(r"^(?P<base>.+?)__(?P<form>[0-9a-f\-]{36})\.pdf$", re.I)

TYPE_MAP = {
    'dl': 'dl',
    'dl_5': 'dl',
    'npi': 'npi',
    'npi_2': 'npi',
}


def ensure_backup(path: str):
    bak = path + '.bak'
    if not os.path.exists(bak):
        shutil.copy2(path, bak)
        print(f"Backup created: {bak}")
    else:
        print(f"Backup exists: {bak}")


def get_conn(db_path: str):
    return sqlite3.connect(db_path)


def columns(cur, table: str):
    cur.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]


def upsert_application(src_cur, dst_cur, form_id: str):
    src_cur.execute("SELECT * FROM applications WHERE form_id=?", (form_id,))
    row = src_cur.fetchone()
    if not row:
        return None
    colnames = [d[0] for d in src_cur.description]
    data = dict(zip(colnames, row))
    dst_cur.execute("SELECT id FROM applications WHERE id=?", (data['id'],))
    exists = dst_cur.fetchone() is not None
    cols = [
        'id','provider_id','form_id','name','last_name','email','phone','status','progress','assignee','source','market','specialty','address','npi','create_dt','last_updt_dt'
    ]
    placeholders = ",".join([":"+c for c in cols])
    insert_sql = f"INSERT OR REPLACE INTO applications ({','.join(cols)}) VALUES ({placeholders})"
    dst_cur.execute(insert_sql, {c: data.get(c) for c in cols})
    print(f"{'Updated' if exists else 'Inserted'} application {data['id']} for form {form_id}")
    return data['id']


def upsert_form_data(src_cur, dst_cur, form_id: str):
    src_cur.execute("SELECT * FROM form_data WHERE form_id=?", (form_id,))
    row = src_cur.fetchone()
    if not row:
        return None
    colnames = [d[0] for d in src_cur.description]
    data = dict(zip(colnames, row))
    # Build upsert without copying numeric primary key id
    cols = [c for c in colnames if c != 'id']
    placeholders = ",".join([":"+c for c in cols])
    # Try REPLACE based on form_id uniqueness
    dst_cur.execute("SELECT id FROM form_data WHERE form_id=?", (form_id,))
    exists = dst_cur.fetchone() is not None
    if exists:
        set_clause = ",".join([f"{c}=:{c}" for c in cols if c != 'form_id'])
        sql = f"UPDATE form_data SET {set_clause} WHERE form_id=:form_id"
        dst_cur.execute(sql, {c: data.get(c) for c in cols})
        print(f"Updated form_data for {form_id}")
    else:
        sql = f"INSERT INTO form_data ({','.join(cols)}) VALUES ({placeholders})"
        dst_cur.execute(sql, {c: data.get(c) for c in cols})
        print(f"Inserted form_data for {form_id}")
    # Return current row id
    dst_cur.execute("SELECT id FROM form_data WHERE form_id=?", (form_id,))
    rid = dst_cur.fetchone()
    return rid[0] if rid else None


def pick_initial_upload(src_cur, form_id: str, ftype: str):
    src_cur.execute(
        "SELECT * FROM form_file_uploads WHERE form_id=? AND LOWER(file_type)=? ORDER BY id DESC",
        (form_id, ftype.lower()),
    )
    row = src_cur.fetchone()
    if not row:
        return None
    colnames = [d[0] for d in src_cur.description]
    return dict(zip(colnames, row))


def insert_upload(dst_cur, form_id: str, filename: str, ftype: str, initial: dict | None):
    cols = ['form_id','filename','file_extension','file_type','status','ocr_output','pdf_match','json_match']
    payload = {
        'form_id': form_id,
        'filename': filename,
        'file_extension': 'pdf',
        'file_type': ftype,
        'status': (initial.get('status') if initial else 'New') if initial is not None else 'New',
        'ocr_output': (initial.get('ocr_output') if initial else None),
        'pdf_match': (initial.get('pdf_match') if initial else None),
        'json_match': (initial.get('json_match') if initial else None),
    }
    placeholders = ",".join([":"+c for c in cols])
    sql = f"INSERT INTO form_file_uploads ({','.join(cols)}) VALUES ({placeholders})"
    dst_cur.execute(sql, payload)
    dst_cur.execute("SELECT last_insert_rowid()")
    uid = dst_cur.fetchone()[0]
    print(f"Inserted upload id={uid} type={ftype} file={filename}")
    return uid


def update_form_link(dst_cur, form_id: str, ftype: str, upload_id: int):
    col = f"{ftype}_upload_id"
    # Ensure column exists
    dst_cur.execute("PRAGMA table_info(form_data)")
    cols = [r[1] for r in dst_cur.fetchall()]
    if col not in cols:
        print(f"Warning: column {col} not in form_data; skipping link update")
        return
    sql = f"UPDATE form_data SET {col}=? WHERE form_id=?"
    dst_cur.execute(sql, (upload_id, form_id))


def main():
    if not os.path.exists(TARGET_DB):
        raise SystemExit(f"Target DB not found: {TARGET_DB}")
    if not os.path.exists(INITIAL_DB):
        raise SystemExit(f"Initial DB not found: {INITIAL_DB}")

    files = [os.path.basename(p) for p in glob(os.path.join(UPLOADS_DIR, '*.pdf'))]
    parsed = []
    for fn in files:
        m = RX.match(fn)
        if not m:
            print(f"Skip (no form id): {fn}")
            continue
        base = m.group('base').lower()
        
        # derive type key from base prefix
        tkey = None
        for k in TYPE_MAP:
            if base.startswith(k):
                tkey = TYPE_MAP[k]
                break
        if not tkey:
            print(f"Skip (unknown type): {fn}")
            continue
        parsed.append((fn, tkey, m.group('form')))

    if not parsed:
        print("No matching upload files found.")
        return

    ensure_backup(TARGET_DB)

    with get_conn(INITIAL_DB) as src_con, get_conn(TARGET_DB) as dst_con:
        src_cur = src_con.cursor()
        dst_cur = dst_con.cursor()

        # Cache existing form_ids to avoid redundant work
        dst_cur.execute("SELECT form_id FROM form_data")
        existing_forms = {r[0] for r in dst_cur.fetchall() if r[0]}

        for fn, ftype, form_id in parsed:
            print(f"\nProcessing {fn} -> type={ftype}, form={form_id}")
            if form_id not in existing_forms:
                app_id = upsert_application(src_cur, dst_cur, form_id)
                upsert_form_data(src_cur, dst_cur, form_id)
                # refresh cache
                existing_forms.add(form_id)
            # copy initial upload metadata if available
            init_upload = pick_initial_upload(src_cur, form_id, ftype)
            uid = insert_upload(dst_cur, form_id, fn, ftype, init_upload)
            update_form_link(dst_cur, form_id, ftype, uid)

        dst_con.commit()
        print("\n✅ Sync complete.")

if __name__ == '__main__':
    main()
