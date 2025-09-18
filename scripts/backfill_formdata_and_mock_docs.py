import csv, json
from pathlib import Path
import sqlite3

DB = Path('credential.db')
CSV_PATH = Path('data/ocrDataAndVerificationSectionData.csv')


def fetchall_dict(cur):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def ensure_form_for_app(cur, app):
    # If application.form_id is null or points to non-existent form, create a synthetic one
    app_id = app['id']
    target_form_id = app.get('form_id')
    if target_form_id:
        cur.execute("SELECT 1 FROM form_data WHERE form_id=?", (target_form_id,))
        if cur.fetchone():
            return target_form_id
    # Create a synthetic UUID-like form_id using app id suffix
    form_id = app_id
    # Insert minimal form_data row if not exists
    cur.execute("SELECT 1 FROM form_data WHERE form_id=?", (form_id,))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO form_data(form_id, provider_id, provider_name, provider_last_name, npi, specialty, address) VALUES (?,?,?,?,?,?,?)",
            (form_id, app.get('provider_id'), app.get('name'), app.get('last_name'), app.get('npi'), app.get('specialty'), app.get('address')),
        )
    # Update application to point to this form_id
    cur.execute("UPDATE applications SET form_id=? WHERE id=?", (form_id, app_id))
    return form_id


def row_to_struct(row):
    ocr_payload = {}
    for k, v in row.items():
        if k.startswith('Board_ABMS_') and v:
            short = k.replace('Board_ABMS_', '').lower()
            ocr_payload[short] = v
    verification = []
    for base in (
        'Demo_Verification_Attribute1 (PDF Format Match)',
        'Demo_Verification_Attribute2 (Board Certificate Match)',
        'Demo_Verification_Attribute3 (Certification Status)'):
        val = row.get(base)
        if val:
            # find corresponding comment
            keys = list(row.keys())
            idx = keys.index(base)
            comment = ''
            for k2 in keys[idx+1:idx+3]:
                if 'Comment' in k2:
                    comment = row.get(k2) or ''
                    break
            verification.append({'name': base.split(' (')[0], 'value': val, 'comment': comment})
    return ocr_payload, verification


def create_or_update_doc(cur, form_id, file_type, filename, extension, status, ocr_payload, verification):
    cur.execute("SELECT id FROM uploaded_documents WHERE form_id=? AND file_type=?", (form_id, file_type))
    row = cur.fetchone()
    if row:
        # update
        cur.execute(
            "UPDATE uploaded_documents SET status=?, ocr_output=?, verification_data=? WHERE id=?",
            (status, json.dumps(ocr_payload) if ocr_payload else None, json.dumps(verification) if verification else None, row[0])
        )
    else:
        cur.execute(
            "INSERT INTO uploaded_documents(form_id, filename, file_extension, file_type, status, ocr_output, verification_data) VALUES (?,?,?,?,?,?,?)",
            (form_id, filename, extension, file_type, status, json.dumps(ocr_payload) if ocr_payload else None, json.dumps(verification) if verification else None)
        )


def run():
    if not DB.exists():
        print('DB not found')
        return
    if not CSV_PATH.exists():
        print('CSV not found')
        return

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Load CSV rows by NPI
    csv_rows = {}
    with CSV_PATH.open(newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            npi = (r.get('npi') or r.get('NPI') or '').strip()
            if npi:
                csv_rows[npi] = r

    # Templates
    sample_sanctions = next((v for v in csv_rows.values() if (v.get('Flag') or '').lower()=='sanctioned'), None)
    sample_active = next((v for v in csv_rows.values() if (v.get('Flag') or '').lower()!='sanctioned' and (v.get('Board_ABMS_Status') or '').lower()=='active'), None)
    sample_other = next((v for v in csv_rows.values() if (v.get('Flag') or '').lower()!='sanctioned'), None)

    # Iterate all applications
    cur.execute("SELECT * FROM applications")
    apps = fetchall_dict(cur)

    bc_created = bc_updated = sanc_created = sanc_updated = 0

    for app in apps:
        form_id = ensure_form_for_app(cur, app)
        npi = (app.get('npi') or '').strip()
        row = csv_rows.get(npi)

        # Ensure board_certification exists/updated
        cur.execute("SELECT id FROM uploaded_documents WHERE form_id=? AND file_type='board_certification'", (form_id,))
        has_bc = cur.fetchone() is not None
        if not has_bc or row:
            src = row or sample_active or sample_other
            if src:
                ocr_payload, verification = row_to_struct(src)
                status_raw = (src.get('Board_ABMS_Status') or '').lower()
                computed_status = 'APPROVED' if status_raw == 'active' else 'In Progress'
                before = cur.execute("SELECT COUNT(1) FROM uploaded_documents WHERE form_id=? AND file_type='board_certification'", (form_id,)).fetchone()[0]
                create_or_update_doc(cur, form_id, 'board_certification', 'abms_screenshot.png', 'png', computed_status, ocr_payload, verification)
                after = cur.execute("SELECT COUNT(1) FROM uploaded_documents WHERE form_id=? AND file_type='board_certification'", (form_id,)).fetchone()[0]
                if after>before:
                    bc_created += 1
                else:
                    bc_updated += 1

        # Ensure sanctions if labeled sanctioned or CSV row flagged
        label = (app.get('psv_original_label') or '').lower()
        need_sanctions = label=='sanctioned' or (row and (row.get('Flag') or '').lower()=='sanctioned')
        if need_sanctions:
            srcs = row if (row and (row.get('Flag') or '').lower()=='sanctioned') else sample_sanctions
            if srcs:
                ocr_payload, verification = row_to_struct(srcs)
                before = cur.execute("SELECT COUNT(1) FROM uploaded_documents WHERE form_id=? AND file_type='sanctions'", (form_id,)).fetchone()[0]
                create_or_update_doc(cur, form_id, 'sanctions', 'sanctions_result.pdf', 'pdf', 'In Progress', ocr_payload, verification)
                after = cur.execute("SELECT COUNT(1) FROM uploaded_documents WHERE form_id=? AND file_type='sanctions'", (form_id,)).fetchone()[0]
                if after>before:
                    sanc_created += 1
                else:
                    sanc_updated += 1

    conn.commit()
    conn.close()
    print(f"Backfill complete. BoardCert created={bc_created} updated={bc_updated} Sanctions created={sanc_created} updated={sanc_updated}")


if __name__ == '__main__':
    run()
