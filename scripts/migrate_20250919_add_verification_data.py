import sqlite3
from pathlib import Path

DB=Path('credential.db')

def column_exists(cur, table, col):
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1]==col for r in cur.fetchall())


def migrate():
    if not DB.exists():
        print('DB not found')
        return
    conn=sqlite3.connect(DB)
    cur=conn.cursor()
    if not column_exists(cur,'uploaded_documents','verification_data'):
        cur.execute("ALTER TABLE uploaded_documents ADD COLUMN verification_data TEXT")
        print('Added verification_data column.')
    else:
        print('verification_data already exists.')
    conn.commit()
    conn.close()

if __name__=='__main__':
    migrate()
