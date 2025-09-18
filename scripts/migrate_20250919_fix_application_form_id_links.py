import sqlite3
from pathlib import Path

DB = Path('credential.db')


def fetchall_dict(cur):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def migrate():
    if not DB.exists():
        print('DB not found:', DB)
        return
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT id, form_id, npi, name, last_name FROM applications")
    apps = fetchall_dict(cur)

    changed = 0
    inspected = 0
    unmatched = 0

    for app in apps:
        inspected += 1
        app_id = app['id']
        app_npi = (app.get('npi') or '').strip()
        app_form_id = (app.get('form_id') or '').strip()
        app_name = (app.get('name') or '').strip().lower()
        app_lname = (app.get('last_name') or '').strip().lower()

        # Find candidate forms by NPI
        candidates = []
        if app_npi:
            cur.execute("SELECT form_id, provider_name, provider_last_name, npi FROM form_data WHERE npi = ?", (app_npi,))
            candidates = fetchall_dict(cur)
        
        # If none by NPI, try by name
        if not candidates and app_name:
            cur.execute("SELECT form_id, provider_name, provider_last_name, npi FROM form_data WHERE lower(provider_name) = ?", (app_name,))
            candidates = fetchall_dict(cur)

        # Score candidates by number of uploaded documents
        best = None
        best_docs = -1
        for c in candidates:
            fid = c['form_id']
            cur.execute("SELECT COUNT(1) FROM uploaded_documents WHERE form_id = ?", (fid,))
            cnt = cur.fetchone()[0]
            if cnt > best_docs:
                best_docs = cnt
                best = c

        if best:
            target_form_id = best['form_id']
            if target_form_id and target_form_id != app_form_id:
                cur.execute("UPDATE applications SET form_id = ? WHERE id = ?", (target_form_id, app_id))
                changed += 1
        else:
            unmatched += 1

    conn.commit()
    conn.close()
    print(f"Applications inspected={inspected} updated={changed} unmatched={unmatched}")


if __name__ == '__main__':
    migrate()
