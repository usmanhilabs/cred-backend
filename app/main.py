from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .database import Base, engine
from apscheduler.schedulers.background import BackgroundScheduler
from .routers import forms, uploads, applications, documents, emails, executive_summary, psv_info
from sqlalchemy.orm import Session
from .utils import get_db, reference_keys_map
import os, json
from contextlib import asynccontextmanager
from .pipeline import run_pipeline
from .models import UploadedDocument, FormData, Application

Base.metadata.create_all(bind=engine)

# # Scheduler setup
# scheduler = BackgroundScheduler()

# def check_and_run_pipeline():
#     print("job started ------------------")
#     db: Session = next(get_db())
#     try:
#         row = db.query(UploadedDocument).filter(UploadedDocument.status == "New").first()

#         if not row:
#             print("No new rows to process.")
#             return  # or continue, or return None

#         form, application = (
#             db.query(FormData).filter(FormData.form_id == row.form_id).first(),
#             db.query(Application).filter(Application.form_id == row.form_id, Application.psv_status == "NEW").first(),
#         )

#         if form and application:
#             print("Found new row to process:", row.id)

#             row.status = "In Progress"
#             application.status = "AI Read in Progress"
#             db.commit()

#             filename_without_ext = ".".join(row.filename.split(".")[:-1])
#             file_ext = row.filename.split(".")[-1]

#             new_filename = f"{filename_without_ext}__{row.form_id}.{file_ext}"

#             # Parse JSON fields
#             folder_dir = os.path.dirname(os.path.dirname(__file__))
#             reference_pdf_path_abs = os.path.join(folder_dir, "ref_uploads", f'{row.file_type}.{row.file_extension}')
#             user_pdf_path_abs = os.path.join(folder_dir, "uploads", new_filename)

#             if row.file_type == 'dl':
#                 user_json_dict = {
#                     "fn": form.provider_name or "",
#                     "ln": form.provider_last_name or "",         
#                     "dl": form.dl_number or "",
#                     "class": "C",                         
#                     "dob": "08/31/1977",                 
#                     "sex": "F",                           
#                     "hair": "BRN",
#                     "eyes": "BRN",
#                     "hgt": "5'-05\"",
#                     "wgt": "125 lb",
#                     "exp": "08/31/2014"
#                 }

#             if row.file_type == 'npi':
#                 user_json_dict = {
#                     "fn": form.provider_name or "",
#                     "ln": form.provider_last_name or "",         
#                     "npi": form.npi or ""
#                 }


#             # Call your existing pipeline
#             result = run_pipeline(reference_keys_map[row.file_type], reference_pdf_path_abs, user_pdf_path_abs, user_json_dict)

#             # Update row status
#             row.status = "Processed"
#             row.ocr_output = json.dumps(result["extracted_json"])
#             row.pdf_match = str(result["pdf_match"])
#             row.json_match = json.dumps(result["json_match"])
#             db.commit()
#             db.refresh(row)

#         application.status = "AI Read Complete"
#         application.progress = 35

#         db.commit()
#         print("Pipeline completed successfully:")
#     except Exception as e:
#         print("Pipeline error:", str(e))
#         if row:
#             row.status = "Error"
#             row.error_message = str(e)
#             db.commit()
#     finally:
#         db.close()
#         print("finally - job finished ------------------")

# # Lifespan context manager
# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     print("Starting scheduler...")
#     scheduler.add_job(check_and_run_pipeline, 'interval', seconds=10, max_instances=1)
#     scheduler.start()
#     yield
#     print("Shutting down scheduler...")
#     scheduler.shutdown()

# app = FastAPI(lifespan=lifespan)

app = FastAPI()

origins = [
    "http://localhost:9002",
    "http://127.0.0.1:9002",
    "http://44.207.130.50:9002 ",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # You can use ["*"] for testing, but not recommended for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(forms.router)
app.include_router(uploads.router)
app.include_router(applications.router)
app.include_router(documents.router)
app.include_router(emails.router)
app.include_router(executive_summary.router)
app.include_router(psv_info.router)
