import sqlite3
from pathlib import Path

DB = Path('credential.db')


def column_exists(cur, table, column):
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == column for r in cur.fetchall())


def migrate():
    if not DB.exists():
        print('DB not found.')
        return
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    if not column_exists(cur, 'applications', 'psv_original_label'):
        cur.execute("ALTER TABLE applications ADD COLUMN psv_original_label TEXT")
        print('Added psv_original_label column.')
    else:
        print('psv_original_label already exists.')
    conn.commit()
    conn.close()

if __name__ == '__main__':
    migrate()
