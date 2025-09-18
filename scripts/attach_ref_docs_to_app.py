import os
import shutil
import sqlite3
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(__file__))
DB = os.path.join(ROOT, 'credential.db')
REF_DIR = os.path.join(ROOT, 'ref_uploads')
UPLOADS_DIR = os.path.join(ROOT, 'uploads')

DOCS = {
    'dl': 'dl.pdf',
    'npi': 'npi.pdf',
}

def attach(provider_id: str):
    if not os.path.exists(DB):
        raise SystemExit('credential.db not found')
    os.makedirs(UPLOADS_DIR, exist_ok=True)

    with sqlite3.connect(DB) as con:
        cur = con.cursor()
        cur.execute('SELECT id, form_id, name FROM applications WHERE provider_id=?', (provider_id,))
        row = cur.fetchone()
        if not row:
            raise SystemExit(f'No application found for provider_id={provider_id}')
        app_id, form_id, name = row
        print(f'Found application {app_id} for {provider_id} -> form {form_id} ({name})')

        for ftype, ref_name in DOCS.items():
            src = os.path.join(REF_DIR, ref_name)
            if not os.path.exists(src):
                print(f'Skipping {ftype}: {src} missing')
                continue
            dest_name = f'{ftype}__{form_id}.pdf'
            dest = os.path.join(UPLOADS_DIR, dest_name)
            shutil.copy2(src, dest)
            print(f'Copied {src} -> {dest}')

            # Mark previous file as Replaced for this type
            cur.execute('''
                UPDATE form_file_uploads
                SET status='Replaced'
                WHERE form_id=? AND LOWER(file_type)=? AND status!='Replaced'
            ''', (form_id, ftype))

            # Insert new upload record
            cur.execute('''
                INSERT INTO form_file_uploads (form_id, filename, file_extension, file_type, status, ocr_output, pdf_match, json_match)
                VALUES (?, ?, 'pdf', ?, 'New', NULL, NULL, NULL)
            ''', (form_id, dest_name, ftype))
            cur.execute('SELECT last_insert_rowid()')
            upload_id = cur.fetchone()[0]
            print(f'Inserted upload id={upload_id} type={ftype}')

            # Update link in form_data
            cur.execute(f'UPDATE form_data SET {ftype}_upload_id=? WHERE form_id=?', (upload_id, form_id))

        con.commit()
        print('✓ Attachment complete')

if __name__ == '__main__':
    attach('P1073')
