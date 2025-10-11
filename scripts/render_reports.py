import os, sys
# Ensure project root is on sys.path so 'app' package imports resolve
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database import SessionLocal
from app.services.report_service import ReportService

if __name__ == '__main__':
    app_id = 'APP-1073'
    db = SessionLocal()
    try:
        svc = ReportService(db)
        full = svc.generate_credentialing_report(app_id)
        short = svc.generate_short_summary(app_id)
        with open(f'detailed_report_{app_id}.md', 'w', encoding='utf-8') as f:
            f.write(full['markdown'])
        with open(f'summary_report_{app_id}.md', 'w', encoding='utf-8') as f:
            f.write(short['markdown'])
        print('Wrote detailed and summary reports')
    finally:
        db.close()
