import csv, json
import sqlite3
from pathlib import Path

DB = Path('credential.db')
CSV = Path('data/ocrDataAndVerificationSectionData.csv')


def fetchall_dict(cur):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def row_to_struct(row):
    ocr_payload = {}
    for k, v in row.items():
        if k.startswith('Board_ABMS_') and v:
            short = k.replace('Board_ABMS_', '').lower()
            ocr_payload[short] = v
    verification = []
    keys = list(row.keys())
    for base in (
        'Demo_Verification_Attribute1 (PDF Format Match)',
        'Demo_Verification_Attribute2 (Board Certificate Match)',
        'Demo_Verification_Attribute3 (Certification Status)'):
        val = row.get(base)
        if val:
            idx = keys.index(base)
            comment = ''
            for k2 in keys[idx+1:idx+3]:
                if 'Comment' in k2:
                    comment = row.get(k2) or ''
                    break
            verification.append({'name': base.split(' (')[0], 'value': val, 'comment': comment})
    return ocr_payload, verification


def ensure_form(cur, app):
    # Use app.id as form_id if missing
    fid = app['form_id'] or app['id']
    cur.execute('SELECT 1 FROM form_data WHERE form_id=?', (fid,))
    if not cur.fetchone():
        cur.execute(
            'INSERT INTO form_data(form_id, provider_id, provider_name, provider_last_name, npi, specialty, address) VALUES (?,?,?,?,?,?,?)',
            (fid, app.get('provider_id'), app.get('name'), app.get('last_name'), app.get('npi'), app.get('specialty'), app.get('address'))
        )
    cur.execute('UPDATE applications SET form_id=? WHERE id=?', (fid, app['id']))
    return fid


def create_doc(cur, form_id, file_type, filename, ext, status, ocr_payload, verification):
    cur.execute('SELECT id FROM uploaded_documents WHERE form_id=? AND file_type=?', (form_id, file_type))
    row = cur.fetchone()
    if row:
        cur.execute('UPDATE uploaded_documents SET status=?, ocr_output=?, verification_data=? WHERE id=?',
                    (status, json.dumps(ocr_payload) if ocr_payload else None, json.dumps(verification) if verification else None, row[0]))
    else:
        cur.execute('INSERT INTO uploaded_documents(form_id, filename, file_extension, file_type, status, ocr_output, verification_data) VALUES (?,?,?,?,?,?,?)',
                    (form_id, filename, ext, file_type, status, json.dumps(ocr_payload) if ocr_payload else None, json.dumps(verification) if verification else None))


def run():
    if not DB.exists() or not CSV.exists():
        print('Missing DB or CSV')
        return

    # Load sample rows
    with CSV.open(newline='', encoding='utf-8') as f:
        reader = list(csv.DictReader(f))
    sanctions_row = next((r for r in reader if (r.get('Flag') or '').lower() == 'sanctioned'), None)
    board_row_active = next((r for r in reader if (r.get('Flag') or '').lower() != 'sanctioned' and (r.get('Board_ABMS_Status') or '').lower() == 'active'), None)
    board_row_expired = next((r for r in reader if (r.get('Flag') or '').lower() != 'sanctioned' and (r.get('Board_ABMS_Status') or '').lower() == 'expired'), None)

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Find apps with no uploaded docs
    cur.execute('''
        SELECT a.* FROM applications a
        LEFT JOIN (
            SELECT form_id, COUNT(1) c FROM uploaded_documents GROUP BY form_id
        ) d ON a.form_id = d.form_id
        WHERE IFNULL(d.c,0)=0
    ''')
    apps = fetchall_dict(cur)

    made = 0
    for app in apps:
        fid = ensure_form(cur, app)
        label = (app.get('psv_original_label') or '').lower()
        if label == 'sanctioned' and sanctions_row:
            ocr, ver = row_to_struct(sanctions_row)
            create_doc(cur, fid, 'sanctions', 'sanctions_result.pdf', 'pdf', 'In Progress', ocr, ver)
            made += 1
        else:
            # Prefer active board to show APPROVED
            src = board_row_active or board_row_expired or sanctions_row
            if src:
                ocr, ver = row_to_struct(src)
                status_raw = (src.get('Board_ABMS_Status') or '').lower()
                status = 'APPROVED' if status_raw == 'active' else 'In Progress'
                create_doc(cur, fid, 'board_certification', 'abms_screenshot.png', 'png', status, ocr, ver)
                made += 1

    conn.commit()
    conn.close()
    print(f'Mock docs created for {made} applications')


if __name__ == '__main__':
    run()
