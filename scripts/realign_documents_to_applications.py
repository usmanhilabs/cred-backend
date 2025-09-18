import sqlite3, csv, json
from pathlib import Path

DB = Path('credential.db')
CSV = Path('data/ocrDataAndVerificationSectionData.csv')


def fetchall_dict(cur):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def ensure_form_for_app(cur, app):
    # keep existing valid form_id, else set to app.id and create form_data
    fid = app.get('form_id')
    if fid:
        cur.execute('SELECT 1 FROM form_data WHERE form_id=?', (fid,))
        if cur.fetchone():
            return fid
    fid = app['id']
    cur.execute('SELECT 1 FROM form_data WHERE form_id=?', (fid,))
    if not cur.fetchone():
        cur.execute(
            'INSERT INTO form_data(form_id, provider_id, provider_name, provider_last_name, npi, specialty, address) VALUES (?,?,?,?,?,?,?)',
            (fid, app.get('provider_id'), app.get('name'), app.get('last_name'), app.get('npi'), app.get('specialty'), app.get('address'))
        )
    cur.execute('UPDATE applications SET form_id=? WHERE id=?', (fid, app['id']))
    return fid


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


def create_doc(cur, form_id, file_type, filename, ext, status, ocr_payload, verification):
    cur.execute('INSERT INTO uploaded_documents(form_id, filename, file_extension, file_type, status, ocr_output, verification_data) VALUES (?,?,?,?,?,?,?)',
                (form_id, filename, ext, file_type, status, json.dumps(ocr_payload) if ocr_payload else None, json.dumps(verification) if verification else None))


def run():
    if not DB.exists():
        print('DB not found'); return

    # Load CSV templates
    sample_active = sample_expired = sample_sanctioned = None
    if CSV.exists():
        rows = list(csv.DictReader(CSV.open(newline='', encoding='utf-8')))
        sample_sanctioned = next((r for r in rows if (r.get('Flag') or '').lower()=='sanctioned'), None)
        sample_active = next((r for r in rows if (r.get('Flag') or '').lower()!='sanctioned' and (r.get('Board_ABMS_Status') or '').lower()=='active'), None)
        sample_expired = next((r for r in rows if (r.get('Flag') or '').lower()!='sanctioned' and (r.get('Board_ABMS_Status') or '').lower()=='expired'), None)

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Index form_data by NPI and by name
    cur.execute('SELECT * FROM form_data')
    forms = fetchall_dict(cur)
    forms_by_npi = {}
    forms_by_name = {}
    for f in forms:
        npi = (f.get('npi') or '').strip()
        if npi:
            forms_by_npi.setdefault(npi, []).append(f)
        key = ((f.get('provider_name') or '').strip().lower(), (f.get('provider_last_name') or '').strip().lower())
        forms_by_name.setdefault(key, []).append(f)

    # Load applications
    cur.execute('SELECT * FROM applications')
    apps = fetchall_dict(cur)

    reassigned = 0
    created_docs = 0
    ensured_forms = 0

    for app in apps:
        fid = ensure_form_for_app(cur, app)
        ensured_forms += 1
        # Gather candidate forms to pull docs from
        candidates = []
        npi = (app.get('npi') or '').strip()
        if npi and npi in forms_by_npi:
            candidates.extend(forms_by_npi[npi])
        key = ((app.get('name') or '').strip().lower(), (app.get('last_name') or '').strip().lower())
        if key in forms_by_name:
            candidates.extend(forms_by_name[key])
        # Reassign any docs from candidate form_ids to this app's form_id
        for c in candidates:
            src_fid = c.get('form_id')
            if not src_fid or src_fid == fid:
                continue
            cur.execute('UPDATE uploaded_documents SET form_id=? WHERE form_id=?', (fid, src_fid))
            reassigned += cur.rowcount
        # Ensure at least one document exists
        cur.execute('SELECT COUNT(1) FROM uploaded_documents WHERE form_id=?', (fid,))
        count = cur.fetchone()[0]
        if count == 0:
            label = (app.get('psv_original_label') or '').lower()
            if label == 'sanctioned' and sample_sanctioned:
                ocr, ver = row_to_struct(sample_sanctioned)
                create_doc(cur, fid, 'sanctions', 'sanctions_result.pdf', 'pdf', 'In Progress', ocr, ver)
                created_docs += 1
            else:
                src = sample_active or sample_expired or sample_sanctioned
                ocr, ver = ({}, []) if not src else row_to_struct(src)
                status = 'APPROVED' if src and (src.get('Board_ABMS_Status') or '').lower()=='active' else 'In Progress'
                create_doc(cur, fid, 'board_certification', 'abms_screenshot.png', 'png', status, ocr, ver)
                created_docs += 1

    conn.commit()
    conn.close()
    print(f'Ensured forms={ensured_forms} Reassigned docs={reassigned} Created docs={created_docs}')


if __name__ == '__main__':
    run()
