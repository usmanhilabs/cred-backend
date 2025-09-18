import sqlite3
from pathlib import Path

DB=Path('credential.db')

DDL='''CREATE TABLE application_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    application_id TEXT,
    event_type TEXT,
    message TEXT,
    created_at DATETIME
);'''

def table_exists(cur, name):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None


def migrate():
    if not DB.exists():
        print('DB not found')
        return
    conn=sqlite3.connect(DB)
    cur=conn.cursor()
    if not table_exists(cur,'application_events'):
        cur.execute(DDL)
        print('Created application_events table.')
        # seed a system event for existing apps
        cur.execute('SELECT id FROM applications')
        for (app_id,) in cur.fetchall():
            cur.execute('INSERT INTO application_events (application_id, event_type, message, created_at) VALUES (?,?,?,CURRENT_TIMESTAMP)', (app_id, 'SYSTEM', 'Application received.',))
    else:
        print('application_events already exists.')
    conn.commit()
    conn.close()

if __name__=='__main__':
    migrate()
