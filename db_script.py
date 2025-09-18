import os
import sys
import csv
import re
import uuid
import json
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime

# SQLite database URL
DATABASE_URL = "sqlite:///./credential.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

def _normalize_status(s: str) -> str:
    if not s:
        return "New"
    s = s.strip()
    return "In-Progress" if s.lower() == "in progress" or s.lower() == "in progress".replace(" ", "") else s

def _split_name(full: str) -> tuple[str, str]:
    if not full:
        return ("", "")
    name = full.replace("Dr. ", "").strip()
    parts = [p for p in name.split() if p]
    if len(parts) == 0:
        return (name, "")
    if len(parts) == 1:
        return (parts[0], "")
    return (" ".join(parts[:-1]), parts[-1])

def _mk_app_from_csv_row(row: dict, idx: int) -> dict:
    app_id = row.get("App ID", "").strip()
    name = row.get("Name", "").strip()
    specialty = row.get("Specialty", "").strip()
    market = row.get("Market", "").strip()
    status = _normalize_status(row.get("Status", "New"))
    progress = _parse_completion(row.get("Completion"))
    assignee = row.get("Assignee", "Unassigned").strip()
    source = row.get("Source", "Manual Entry").strip()

    first, last = _split_name(name)
    email_local = (first or "user").lower().replace(" ", ".") + "." + (last or f"{idx}").lower()
    email = f"{email_local}@example.com"
    phone = f"555-0{100+idx:03d}"  # e.g., 555-0102
    provider_id = f"P{app_id.split('-')[-1] or idx}"
    form_id = str(uuid.uuid4())
    address = f"{idx} Main St, {market or 'CA'}"
    npi = str(1000000000 + idx)

    return {
        "id": app_id,
        "provider_id": provider_id,
        "form_id": form_id,
        "name": name,
        "last_name": last,
        "email": email,
        "phone": phone,
        "status": status,
        "progress": progress,
        "assignee": assignee,
        "source": source,
        "market": market or "California",
        "specialty": specialty or "General",
        "address": address,
        "npi": npi,
    }

def load_applications_from_csv(csv_path: str) -> list[dict]:
    if not os.path.isabs(csv_path):
        csv_path = os.path.abspath(csv_path)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    apps: list[dict] = []
    with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required_cols = {
            "App ID", "Name", "Specialty", "Market", "Status",
            "Completion", "Assignee", "Source"
        }
        missing = [c for c in required_cols if c not in reader.fieldnames]
        if missing:
            raise RuntimeError(f"CSV missing required columns: {missing}")
        for i, row in enumerate(reader, start=1):
            apps.append(_mk_app_from_csv_row(row, i))
    return apps

def bulk_insert(apps: list[dict]):
    now = datetime.now()
    insert_sql = text(
        """
        INSERT OR REPLACE INTO applications (
            id, provider_id, form_id, name, last_name, email, phone, status, progress, assignee,
            source, market, specialty, address, npi,
            create_dt, last_updt_dt
        ) VALUES (
            :id, :provider_id, :form_id, :name, :last_name, :email, :phone, :status, :progress, :assignee,
            :source, :market, :specialty, :address, :npi,
            :create_dt, :last_updt_dt
        )
        """
    )

    with engine.begin() as conn:
        for app in apps:
            conn.execute(
                insert_sql,
                {
                    "id": app["id"],
                    "provider_id": app["provider_id"],
                    "form_id": app["form_id"],
                    "name": app["name"],
                    "last_name": app.get("last_name"),
                    "email": app.get("email"),
                    "phone": app.get("phone"),
                    "status": app["status"],
                    "progress": app["progress"],
                    "assignee": app["assignee"],
                    "source": app["source"],
                    "market": app["market"],
                    "specialty": app["specialty"],
                    "address": app["address"],
                    "npi": app["npi"],
                    "create_dt": now,
                    "last_updt_dt": now,
                },
            )
    print("✅ Applications inserted successfully.")

def seed_related_data(apps: list[dict]):
    now = datetime.now()

    insert_form_data = text(
        """
        INSERT OR REPLACE INTO form_data (
            form_id, provider_id, provider_name, provider_last_name, npi, email, phone, specialty, address
        ) VALUES (
            :form_id, :provider_id, :provider_name, :provider_last_name, :npi, :email, :phone, :specialty, :address
        )
        """
    )

    insert_upload = text(
        """
        INSERT INTO form_file_uploads (
            form_id, filename, file_extension, file_type, status, ocr_output, pdf_match, json_match
        ) VALUES (
            :form_id, :filename, :file_extension, :file_type, :status, :ocr_output, :pdf_match, :json_match
        )
        """
    )

    insert_email = text(
        """
        INSERT INTO email_records (
            id, application_id, recipient_email, subject, body, status, sent_at
        ) VALUES (
            :id, :application_id, :recipient_email, :subject, :body, :status, :sent_at
        )
        """
    )

    with engine.begin() as conn:
        for app in apps:
            # Seed form_data
            provider_name = app["name"].replace("Dr. ", "").strip()
            conn.execute(
                insert_form_data,
                {
                    "form_id": app["form_id"],
                    "provider_id": app["provider_id"],
                    "provider_name": provider_name,
                    "provider_last_name": app.get("last_name"),
                    "npi": app["npi"],
                    "email": app.get("email"),
                    "phone": app.get("phone"),
                    "specialty": app["specialty"],
                    "address": app["address"],
                },
            )

            # Seed two uploads (DL and NPI)
            json_match_ok = {
                "name": {"match": True, "extracted_confident_score": 0.98, "extracted": provider_name, "provided": provider_name}
            }
            json_match_bad = {
                "address": {"match": False, "extracted_confident_score": 0.75, "extracted": "123 Wrong St", "provided": app["address"]}
            }

            conn.execute(
                insert_upload,
                {
                    "form_id": app["form_id"],
                    "filename": f"dl_{app['id'].lower()}.pdf",
                    "file_extension": "pdf",
                    "file_type": "DRIVING_LICENSE",
                    "status": "APPROVED" if app["progress"] >= 75 else "In Progress",
                    "ocr_output": "{}",
                    "pdf_match": None,
                    "json_match": json.dumps(json_match_ok),
                },
            )
            conn.execute(
                insert_upload,
                {
                    "form_id": app["form_id"],
                    "filename": f"npi_{app['id'].lower()}.pdf",
                    "file_extension": "pdf",
                    "file_type": "NPI",
                    "status": "In Progress" if app["status"] != "Completed" else "APPROVED",
                    "ocr_output": "{}",
                    "pdf_match": None,
                    "json_match": json.dumps(json_match_bad),
                },
            )

            # Seed two emails (one SENT, one DRAFT)
            conn.execute(
                insert_email,
                {
                    "id": str(uuid.uuid4()),
                    "application_id": app["id"],
                    "recipient_email": app["email"],
                    "subject": f"Regarding your application {app['id']}",
                    "body": "Thanks for your submission. We'll get back to you soon.",
                    "status": "SENT",
                    "sent_at": now,
                },
            )
            conn.execute(
                insert_email,
                {
                    "id": str(uuid.uuid4()),
                    "application_id": app["id"],
                    "recipient_email": app["email"],
                    "subject": f"Additional info needed for {app['id']}",
                    "body": "Please upload your updated documents.",
                    "status": "DRAFT",
                    "sent_at": now,
                },
            )
        print("✅ Related form_data, uploads, and email_records inserted.")

def reset_all():
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM email_records"))
        conn.execute(text("DELETE FROM form_file_uploads"))
        conn.execute(text("DELETE FROM form_data"))
        conn.execute(text("DELETE FROM applications"))
        print("🧹 Cleared existing data from all tables.")

def _parse_completion(value: str) -> int:
    if value is None:
        return 0
    s = str(value).strip()
    if s == "":
        return 0
    m = re.search(r"(\d+)", s)
    try:
        return int(m.group(1)) if m else 0
    except Exception:
        return 0

def import_applications_from_csv(csv_path: str, truncate: bool = True):
    if not os.path.isabs(csv_path):
        csv_path = os.path.abspath(csv_path)
    if not os.path.exists(csv_path):
        print(f"❌ CSV not found: {csv_path}")
        return

    now = datetime.now()
    insert_sql = text(
        """
        INSERT INTO applications (
            id, provider_id, form_id, name, status, progress, assignee,
            source, market, specialty, address, npi,
            create_dt, last_updt_dt
        ) VALUES (
            :id, :provider_id, :form_id, :name, :status, :progress, :assignee,
            :source, :market, :specialty, :address, :npi,
            :create_dt, :last_updt_dt
        )
        """
    )

    with engine.begin() as conn:
        if truncate:
            conn.execute(text("DELETE FROM applications"))

        with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            required_cols = {
                "App ID", "Name", "Specialty", "Market", "Status",
                "Completion", "Assignee", "Source"
            }
            missing = [c for c in required_cols if c not in reader.fieldnames]
            if missing:
                raise RuntimeError(f"CSV missing required columns: {missing}")

            count = 0
            for row in reader:
                conn.execute(
                    insert_sql,
                    {
                        "id": row.get("App ID", "").strip(),
                        "provider_id": None,
                        "form_id": None,
                        "name": row.get("Name", "").strip(),
                        "status": row.get("Status", "").strip(),
                        "progress": _parse_completion(row.get("Completion")),
                        "assignee": row.get("Assignee", "").strip(),
                        "source": row.get("Source", "").strip(),
                        "market": row.get("Market", "").strip(),
                        "specialty": row.get("Specialty", "").strip(),
                        "address": None,
                        "npi": None,
                        "create_dt": now,
                        "last_updt_dt": now,
                    },
                )
                count += 1

        print(f"✅ Imported {count} applications from '{csv_path}'.")

def run_manual_sql():
    print("Enter SQL to run (end with semicolon `;`):")
    user_sql = ""
    while not user_sql.strip().endswith(";"):
        user_sql += input("> ")

    with engine.connect() as conn:
        try:
            result = conn.execute(text(user_sql.strip().rstrip(";")))
            if result.returns_rows:
                for row in result.fetchall():
                    print(row)
            else:
                print("✅ Statement executed successfully.")
            conn.commit()
        except Exception as e:
            print(f"❌ Error: {e}")


def run_defined_sql():
    user_sql = """
    DELETE FROM email_records;
    DELETE FROM form_file_uploads;
    DELETE FROM form_data;
    DELETE FROM applications;
    """
    with engine.connect() as conn:
        try:
            result = conn.execute(text(user_sql.strip().rstrip(";")))
            if result.returns_rows:
                for row in result.fetchall():
                    print(row)
            else:
                print("✅ Statement executed successfully.")
            conn.commit()
        except Exception as e:
            print(f"❌ Error: {e}")

def seed_demo(truncate: bool = True):
    # Backwards-compatible demo: load from Downloads/newApplications.csv if present
    default_csv = os.path.expanduser(r"~\\Downloads\\newApplications.csv")
    apps = []
    if os.path.exists(default_csv):
        apps = load_applications_from_csv(default_csv)
    else:
        print(f"⚠️ Demo CSV not found at {default_csv}. No data seeded.")
        return
    if truncate:
        reset_all()
    bulk_insert(apps)
    seed_related_data(apps)

def seed_from_csv(csv_path: str, truncate: bool = True):
    apps = load_applications_from_csv(csv_path)
    if truncate:
        reset_all()
    bulk_insert(apps)
    seed_related_data(apps)

if __name__ == "__main__":
    # Support CLI mode for automation
    if len(sys.argv) >= 2 and sys.argv[1] == "--import-csv":
        csv_arg = sys.argv[2] if len(sys.argv) >= 3 else input("CSV path: ").strip()
        truncate = True
        if len(sys.argv) >= 4:
            truncate = sys.argv[3].lower() in ("1", "true", "yes", "y")
        import_applications_from_csv(csv_arg, truncate)
        sys.exit(0)
    if len(sys.argv) >= 2 and sys.argv[1] == "--seed-demo":
        seed_demo(truncate=True)
        sys.exit(0)
    if len(sys.argv) >= 2 and sys.argv[1] == "--seed-from-csv":
        csv_arg = sys.argv[2] if len(sys.argv) >= 3 else input("CSV path: ").strip()
        truncate = True
        if len(sys.argv) >= 4:
            truncate = sys.argv[3].lower() in ("1", "true", "yes", "y")
        seed_from_csv(csv_arg, truncate)
        sys.exit(0)

    # Interactive menu
    print("Manual DB Runner")
    print("1. Insert demo application data")
    print("2. Run SQL manually")
    print("3. Run SQL defined")
    print("4. Import applications from CSV")
    print("5. Reset + Seed full demo data (from Downloads/newApplications.csv)")
    print("6. Seed from CSV path (all tables)")
    choice = input("Choose (1 2 3 4 5 6): ")

    if choice == "1":
        bulk_insert()
    elif choice == "2":
        run_manual_sql()
    elif choice == "3":
        run_defined_sql()
    elif choice == "4":
        default_csv = os.path.expanduser(r"~\\Downloads\\newApplications.csv")
        path = input(f"CSV path [{default_csv}]: ").strip() or default_csv
        import_applications_from_csv(path, truncate=True)
    elif choice == "5":
        seed_demo(truncate=True)
    elif choice == "6":
        default_csv = os.path.expanduser(r"~\\Downloads\\newApplications.csv")
        path = input(f"CSV path [{default_csv}]: ").strip() or default_csv
        seed_from_csv(path, truncate=True)
    else:
        print("Invalid choice.")
