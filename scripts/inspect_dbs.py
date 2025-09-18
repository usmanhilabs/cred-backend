import sqlite3, os

DBS = ['credential.db', 'intial_cred.db.db']

for db in DBS:
    print(f"\n=== {db} ===")
    if not os.path.exists(db):
        print('missing')
        continue
    con = sqlite3.connect(db)
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    print('tables:', tables)
    for t in tables:
        try:
            cur.execute(f'PRAGMA table_info({t})')
            cols = [c[1] for c in cur.fetchall()]
            cur.execute(f'SELECT COUNT(1) FROM {t}')
            cnt = cur.fetchone()[0]
            print(f' - {t} ({cnt} rows):', cols)
        except Exception as e:
            print('  err on', t, e)
    for t in ['applications','form_data','form_file_uploads']:
        if t in tables:
            cur.execute(f'SELECT * FROM {t} LIMIT 3')
            rows = cur.fetchall()
            print(f' sample {t}:', rows)
    con.close()
