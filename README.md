# 🔥 FAST API - Python

## 🚀 Getting Started

Follow these steps to run the project locally:

### 1. Clone the Repository

```bash
git clone https://github.com/omkar-hilabs/fast-cred-backend.git
cd fast-cred-backend
```

### 2. Create env and install dependencies (Python 3.11)

PyMuPDF wheels are available for Python 3.11. Using other versions (e.g., 3.13) may force a slow source build. We pin Python in `.python-version`.

Option A: pyenv + pyenv-virtualenv (recommended)
```bash
# Install Python 3.11 if needed
pyenv install 3.11.9 -s

# Create a project-local virtualenv
pyenv virtualenv 3.11.9 cred-backend-311 || true
pyenv local 3.11.9
pyenv activate cred-backend-311

# Upgrade pip tooling and install deps
python -m pip install --upgrade pip wheel setuptools
pip install -r requirements.txt
```

Option B: Built-in venv (if you have python3.11 on PATH)
```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip wheel setuptools
pip install -r requirements.txt
```


### 3. Create a .env and store the following
```bash
OPENAI_API_KEY=your_key_here
ENABLE_REPORT_LLM=true
# Optional: override default model used for report generation
# REPORT_LLM_MODEL=gpt-4o
```

### 4. Start the backend
```bash
uvicorn app.main:app --reload
```

The app should now be running at http://localhost:8000

## 🔄 Schema Revamp (Sept 2025)

Recent changes introduced a cleaner separation of application lifecycle states and document handling.

### Key Changes
- Split single `status` into two columns on `applications`:
	- `psv_status` (NEW, IN_PROGRESS, COMPLETED, IN_COMMITTE_REVIEW, APPROVED, DENIED)
	- `committee_status` (NOT_STARTED, IN_REVIEW, APPROVED, DENIED)
- Renamed table `form_file_uploads` → `uploaded_documents` and added LLM fields:
	- `llm_extraction` (raw structured JSON extraction)
	- `llm_summary` (free‑text / markdown summary)
- Added migration script and CSV ingestion for baseline provider applications.

### Migration
Run the migration script (idempotent):
```bash
python scripts/migrate_20250919_split_status_and_rename_uploads.py
```

### Ingest Seed Application Data
The file `data/newData.csv` is ingested into the `applications` table with normalized PSV statuses and `committee_status=NOT_STARTED`.
```bash
python scripts/ingest_newdata_applications.py
```

### Notes
- Legacy `status` column (if still present in SQLite) is backfilled into `psv_status`; it is now deprecated in the code.
- `UploadedDocument` keeps OCR + matching outputs and adds placeholders for future LLM processing.
- `FormData` is retained for user-entered form session granularity (edit/save vs. application snapshot) and field‑level document linkage.

### Next Enhancements (suggested)
- Introduce enumerated constraints for statuses (Enum or CHECK constraints).
- Add indexing for frequent filters: `applications(psv_status)`, `uploaded_documents(file_type, status)`.
- Add a lightweight audit trail table for status transitions.
