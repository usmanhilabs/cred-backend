import zipfile
from app.models import SavedFile  # Ensure this model is defined as per previous instructions

def ingest_files_from_zip(session, zip_path):
    with zipfile.ZipFile(zip_path, "r") as zf:
        for file_info in zf.infolist():
            if file_info.filename.lower().endswith(".png"):
                file_data = zf.read(file_info.filename)
                saved_file = SavedFile(
                    filename=file_info.filename,
                    file_type="png",
                    file_data=file_data
                )
                session.add(saved_file)
        session.commit()

# Usage example (add this to your main function or script logic):
zip_path = os.path.join(ROOT_DIR, "data", "your_zip_file.zip")
if os.path.exists(zip_path):
    session = SessionLocal()
    try:
        ingest_files_from_zip(session, zip_path)
        print("Files from zip saved to DB.")
    finally:
        session.close()