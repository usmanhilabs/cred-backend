from datetime import datetime
from typing import Any, Dict, List, Optional
import os
import json

from sqlalchemy.orm import Session

from app.models import Application, FormData, FormFileUpload, EmailRecord

try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover - optional dependency at runtime
    OpenAI = None  # type: ignore

# Best-effort load environment from .env if available
try:  # pragma: no cover
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass


class ReportService:
    """Service for generating comprehensive credentialing reports based on current DB state"""

    def __init__(self, db: Session):
        self.db = db
        self.debug = (
            os.getenv("REPORT_LLM_DEBUG", "false").strip().lower() in {"1", "true", "yes", "on"}
        )
        self.enable_llm = (
            os.getenv("ENABLE_REPORT_LLM", "true").strip().lower() in {"1", "true", "yes", "on"}
        )
        self.report_llm_model = os.getenv("REPORT_LLM_MODEL", "gpt-4o-mini")
        self._client = None
        try:
            if self.enable_llm and OpenAI is not None and os.getenv("OPENAI_API_KEY"):
                self._client = OpenAI()
                if self.debug:
                    print("[ReportService] LLM client initialized.")
            else:
                if self.debug:
                    print(
                        f"[ReportService] Skipping LLM init. enable_llm={self.enable_llm}, OpenAI_imported={OpenAI is not None}, has_api_key={bool(os.getenv('OPENAI_API_KEY'))}"
                    )
        except Exception:
            self._client = None
            if self.debug:
                print(f"[ReportService] LLM init error: {e}")

    def generate_credentialing_report(self, app_id: str) -> Dict[str, Any]:
        """Generate a comprehensive credentialing report structure for a given application id"""
        application = self.db.query(Application).filter_by(id=app_id).first()
        if not application:
            raise ValueError("Application not found")

        form = self.db.query(FormData).filter_by(form_id=application.form_id).first()
        if not form:
            raise ValueError("Form data not found")

        uploads = (
            self.db.query(FormFileUpload)
            .filter(FormFileUpload.form_id == application.form_id)
            .all()
        )

        emails = (
            self.db.query(EmailRecord).filter(EmailRecord.application_id == application.id).all()
        )

        # Build process steps and decisions from current DB state
        steps: List[Dict[str, Any]] = self._build_steps(application, form, uploads, emails)
        llm_reasoning: List[Dict[str, Any]] = []
        decisions: List[Dict[str, Any]] = self._build_decisions(application, form, uploads, emails)

        data_points: Dict[str, Any] = {
            "uploads": [
                {
                    "id": u.id,
                    "type": u.file_type,
                    "filename": u.filename,
                    "status": u.status,
                    "ocr": self._safe_eval_json(u.ocr_output),
                    "pdf_match": self._safe_eval_json(u.pdf_match),
                    "json_match": self._safe_eval_json(u.json_match),
                }
                for u in uploads
            ],
            "emails": [
                {
                    "id": e.id,
                    "status": e.status,
                    "sent_at": e.sent_at,
                    "recipient": e.recipient_email,
                    "subject": e.subject,
                }
                for e in emails
            ],
        }

        final_result: Dict[str, Any] = {
            "result": {
                "compliance_status": application.status or "Unknown",
                "score": self._infer_score(application, uploads),
                "processing_time": None,
                "hard_regulations": {},
                "soft_regulations": {},
            }
        }

        comprehensive_data: Dict[str, Any] = {
            "session_metadata": {
                "session_id": application.id,
                "provider_id": form.provider_id or application.provider_id,
                "start_time": application.create_dt,
                "end_time": application.last_updt_dt,
                "total_steps": len(steps),
                "total_llm_interactions": len(llm_reasoning),
                "total_decisions": len(decisions),
            },
            "provider_info": {
                "name": (application.name or "").strip(),
                "specialty": application.specialty,
                "experience_years": self._safe_int(form.experience),
                "education": form.university,
                "license_number": form.ml_number,
                "board_certifications": [],
                "malpractice_insurance": "Unknown",
                "disciplinary_actions": [],
                "criminal_record": "Unknown",
                "cme_credits": 0,
                "quality_score": 3.0,
            },
            "process_steps": steps,
            "llm_reasoning": llm_reasoning,
            "data_points": data_points,
            "decisions": decisions,
            "decision_reasoning": {},
            "final_result": final_result,
            "credentialing_history": [],
            "raw_provider_data": {
                "application": self._model_as_dict(application),
                "form": self._model_as_dict(form),
            },
        }

        # Optionally enhance with LLM-generated detailed sections
        llm_section_markdown = self._maybe_generate_llm_sections(comprehensive_data)
        if llm_section_markdown:
            comprehensive_data["llm_reasoning"] = [
                {
                    "type": "report_enhancement",
                    "model": self.report_llm_model,
                    "timestamp": datetime.now().isoformat(),
                }
            ]
            comprehensive_data["session_metadata"][
                "total_llm_interactions"
            ] = len(comprehensive_data["llm_reasoning"])

        # Render markdown using the same format and append LLM details if present
        markdown = self._render_report_markdown(comprehensive_data)
        if llm_section_markdown:
            markdown += "\n\n## AI-Generated Detailed Analysis\n\n" + llm_section_markdown

        return {"markdown": markdown, "data": comprehensive_data}

    def generate_short_summary(self, app_id: str) -> Dict[str, Any]:
        full = self.generate_credentialing_report(app_id)
        md = self._render_short_summary(full["data"])
        return {"markdown": md}

    def _render_report_markdown(self, data: Dict[str, Any]) -> str:
        header = self._create_report_header(data)
        body = self._generate_enhanced_template(data)
        return header + body

    def _render_short_summary(self, data: Dict[str, Any]) -> str:
        session_meta = data["session_metadata"]
        provider_info = data["provider_info"]
        final_result = data["final_result"].get("result", {})
        uploads = data["data_points"].get("uploads", [])
        emails = data["data_points"].get("emails", [])
        status_upper = lambda s: (s or "").upper()
        approved = sum(1 for u in uploads if status_upper(u.get("status")) in {"APPROVED", "VERIFIED"})
        in_progress = sum(1 for u in uploads if status_upper(u.get("status")) in {"NEW", "IN PROGRESS"})
        total = len(uploads)

        # Narrative paragraphs
        doc_labels = [self._human_doc_label(u.get("type")) for u in uploads]
        doc_labels = sorted({d for d in doc_labels if d})
        sent_emails = sum(1 for e in emails if status_upper(e.get("status")) == "SENT")
        draft_emails = sum(1 for e in emails if status_upper(e.get("status")) == "DRAFT")

        overview_para = (
            f"Credentialing overview: {provider_info.get('name') or 'This provider'} is currently "
            f"{final_result.get('compliance_status', 'Unknown')}. We have {total} document(s) on file"
            + (f" ({', '.join(doc_labels)})" if doc_labels else "")
            + f" with {approved} approved/verified and {in_progress} in progress. "
            f"The overall completeness score is {final_result.get('score', 'N/A')}/5."
        )

        comms_para = (
            f"Communication: {sent_emails} email(s) sent and {draft_emails} draft(s) recorded. "
            f"We will proceed to validate any pending documents and follow up as needed."
        )

        return (
            f"# Credentialing Summary\n\n"
            f"Provider: {provider_info.get('name') or 'Unknown'}\n\n"
            f"Status: {final_result.get('compliance_status', 'Unknown')} | Score: {final_result.get('score', 'N/A')}/5\n\n"
            f"Docs: {approved + in_progress}/{total} ({approved} approved, {in_progress} in progress)\n\n"
            f"Last Updated: {session_meta.get('end_time')}\n\n"
            f"{overview_para}\n\n{comms_para}\n"
        )

    def _create_report_header(self, data: Dict[str, Any]) -> str:
        session_meta = data["session_metadata"]
        provider_info = data["provider_info"]
        return (
            "# Comprehensive Credentialing Report\n\n"
            f"Provider: {provider_info.get('name') or 'Unknown'}  \n"
            f"Session ID: {session_meta.get('session_id')}  \n"
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  \n"
            f"Report ID: RPT_{session_meta.get('session_id')}  \n"
            f"Process Steps: {session_meta.get('total_steps', 0)}  \n"
            f"AI Analysis: {session_meta.get('total_llm_interactions', 0)} interactions  \n"
            f"Automated Decisions: {session_meta.get('total_decisions', 0)}\n\n---\n\n"
        )

    def _generate_enhanced_template(self, data: Dict[str, Any]) -> str:
        session_meta = data["session_metadata"]
        provider_info = data["provider_info"]
        final_result = data["final_result"].get("result", {})
        steps = data["process_steps"]
        decisions = data["decisions"]

        def regs_markdown(regs: Dict[str, Any], scored: bool = False) -> str:
            if not regs:
                return "- No items"
            lines = []
            for k, v in regs.items():
                if scored:
                    lines.append(f"- {k}: score {v}")
                else:
                    lines.append(f"- {k}: {v}")
            return "\n".join(lines)

        def summarize_steps(steps: List[Dict[str, Any]]) -> str:
            if not steps:
                return "- No steps recorded"
            counts: Dict[str, int] = {}
            for s in steps:
                t = s.get("type") or s.get("name") or "step"
                counts[t] = counts.get(t, 0) + 1
            return "\n".join([f"- {k}: {v}" for k, v in counts.items()])

        def summarize_decisions(decisions: List[Dict[str, Any]]) -> str:
            if not decisions:
                return "- No decisions recorded"
            counts: Dict[str, int] = {}
            for d in decisions:
                t = d.get("type") or d.get("action") or "decision"
                counts[t] = counts.get(t, 0) + 1
            return "\n".join([f"- {k}: {v}" for k, v in counts.items()])

        compliance_status = final_result.get("compliance_status", "Unknown")
        score = final_result.get("score", 0)

        return (
            "## Executive Summary\n\n"
            f"{provider_info.get('name') or 'The provider'} has completed the credentialing process with a status of **{compliance_status}** and an overall score of **{score}/5**. "
            f"The process involved {session_meta.get('total_steps', 0)} steps with {session_meta.get('total_llm_interactions', 0)} AI-powered analyses.\n\n"
            "## Provider Assessment\n\n"
            f"- Name: {provider_info.get('name')}\n"
            f"- Specialty: {provider_info.get('specialty')}\n"
            f"- Experience: {provider_info.get('experience_years')} years\n"
            f"- Education: {provider_info.get('education')}\n"
            f"- License Number: {provider_info.get('license_number')}\n\n"
            "## Compliance Analysis\n\n"
            f"- Compliance Status: {compliance_status}\n"
            f"- Overall Score: {score}/5\n"
            f"- Processing Time: {final_result.get('processing_time', 'Unknown')} seconds\n\n"
            "### Hard Regulations Compliance\n"
            f"{regs_markdown(final_result.get('hard_regulations', {}))}\n\n"
            "### Soft Regulations Scoring\n"
            f"{regs_markdown(final_result.get('soft_regulations', {}), scored=True)}\n\n"
            "## Process Transparency\n\n"
            "### Credentialing Process Steps\n"
            f"{summarize_steps(steps)}\n\n"
            "### Automated Decision Summary\n"
            f"{summarize_decisions(decisions)}\n\n"
            "### AI Analysis Insights\n"
            f"- Total LLM Interactions: {session_meta.get('total_llm_interactions', 0)}\n"
            f"- Data Mapping Quality: Unknown\n"
            f"- Verification Confidence: Unknown\n\n"
            "## Risk Assessment\n\n"
            "- Not enough structured data to compute detailed risks.\n\n"
            "## Recommendations\n\n"
            "- Review any pending or in-progress documents.\n"
            "- Verify license numbers and NPI against registries.\n"
            "- Ensure malpractice insurance documentation is current.\n\n"
            "## Next Steps\n\n"
            f"1. {'✅ Proceed with onboarding process' if compliance_status.upper() == 'COMPLIANT' else '❌ Address compliance issues before proceeding'}\n"
            "2. Schedule follow-up review in 6 months\n"
            "3. Monitor upcoming license/certification renewals\n"
        )

    # --------- LLM enhancement helpers ---------
    def _llm_available(self) -> bool:
        return bool(self._client)

    def _compact_llm_payload(self, data: Dict[str, Any]) -> Dict[str, Any]:
        provider = data.get("provider_info", {})
        session = data.get("session_metadata", {})
        uploads = data.get("data_points", {}).get("uploads", [])
        emails = data.get("data_points", {}).get("emails", [])
        decisions = data.get("decisions", [])

        docs: List[Dict[str, Any]] = []
        for u in uploads:
            label = self._human_doc_label(u.get("type"))
            json_match = u.get("json_match")
            bad_fields: List[str] = []
            if isinstance(json_match, dict):
                try:
                    for k, v in json_match.items():
                        if not (v or {}).get("match"):
                            bad_fields.append(k)
                except Exception:
                    pass
            docs.append(
                {
                    "label": label,
                    "type": u.get("type"),
                    "status": u.get("status"),
                    "filename": u.get("filename"),
                    "mismatched_fields": bad_fields,
                    "matches": (u.get("matches") if "matches" in u else None),
                    "mismatches": (u.get("mismatches") if "mismatches" in u else None),
                }
            )

        return {
            "session": {
                "id": session.get("session_id"),
                "steps": session.get("total_steps", 0),
                "decisions": session.get("total_decisions", 0),
                "status": data.get("final_result", {}).get("result", {}).get("compliance_status", "Unknown"),
                "score": data.get("final_result", {}).get("result", {}).get("score", 0),
                "start_time": session.get("start_time"),
                "end_time": session.get("end_time"),
            },
            "provider": {
                "name": provider.get("name"),
                "specialty": provider.get("specialty"),
                "experience_years": provider.get("experience_years"),
                "education": provider.get("education"),
                "license_number": provider.get("license_number"),
            },
            "documents": docs,
            "emails": [
                {
                    "status": e.get("status"),
                    "sent_at": e.get("sent_at"),
                    "recipient": e.get("recipient"),
                    "subject": e.get("subject"),
                }
                for e in emails
            ],
            "decisions": [
                {
                    "type": d.get("type"),
                    "action": d.get("action"),
                    "subject": d.get("subject"),
                    "reason": d.get("reason"),
                }
                for d in decisions
            ],
        }

    def _maybe_generate_llm_sections(self, data: Dict[str, Any]) -> Optional[str]:
        if not self.enable_llm or not self._llm_available():
            if self.debug:
                print(
                    f"[ReportService] _maybe_generate_llm_sections skipped. enable_llm={self.enable_llm}, client_available={self._llm_available()}"
                )
            return None
        try:
            payload = self._compact_llm_payload(data)
            system = (
                "You are a senior medical credentialing analyst. Write clear, factual, and actionable markdown. "
                "Only use the provided data; if something is missing, explicitly mark it as Unknown. "
                "Be concise but thorough. Avoid duplication from the summary; focus on deeper analysis."
            )
            instructions = (
                "Using the JSON below, generate detailed report sections. Do NOT invent data. "
                "Return ONLY markdown with these top-level sections (in order):\n\n"
                "### Detailed Findings\n"
                "- Summarize key findings across all documents, approvals, and communications.\n\n"
                "### Document-by-Document Analysis\n"
                "- For each document, list status, detected issues, and what was verified.\n\n"
                "### Discrepancies & Root Causes\n"
                "- Enumerate mismatches with likely causes and what evidence is needed.\n\n"
                "### Risk & Mitigation Plan\n"
                "- Classify risks (Low/Medium/High) and give concrete mitigations.\n\n"
                "### Verification Plan\n"
                "- Exact external checks to run (e.g., NPI, state license).\n\n"
                "### Timeline & Ownership\n"
                "- Short plan with owners (Applicant/Staff) and expected dates.\n\n"
                "### Compliance Checklist\n"
                "- Checklist with [ ]/ [x] based on what is known.\n\n"
                "### Next Actions\n"
                "- 3-6 prioritized, specific next actions."
            )

            messages = [
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": instructions + "\n\nJSON:\n" + json.dumps(payload, default=str),
                },
            ]

            if self.debug:
                print("[ReportService] Calling LLM for detailed sections...")
            resp = self._client.chat.completions.create(
                model=self.report_llm_model,
                messages=messages,
                temperature=0.2,
                max_tokens=1200,
            )
            content = (
                resp.choices[0].message.content if getattr(resp, "choices", None) else None
            )
            if self.debug:
                print(f"[ReportService] LLM response received. has_content={bool(content)}")
            return content or None
        except Exception as e:
            if self.debug:
                print(f"[ReportService] LLM call failed: {e}")
            return None

    @staticmethod
    def _safe_int(v: Any) -> int:
        try:
            return int(v) if v is not None else 0
        except Exception:
            return 0

    @staticmethod
    def _safe_eval_json(s: Optional[str]) -> Any:
        if not s:
            return None
        try:
            import json

            return json.loads(s)
        except Exception:
            return None

    @staticmethod
    def _model_as_dict(obj: Any) -> Dict[str, Any]:
        try:
            # SQLAlchemy model __dict__ includes _sa_instance_state; filter it out
            d = {k: v for k, v in vars(obj).items() if not k.startswith("_")}
            # Convert datetimes to strings for safe serialization
            for k, v in list(d.items()):
                if hasattr(v, "isoformat"):
                    d[k] = v.isoformat()
            return d
        except Exception:
            return {}

    @staticmethod
    def _infer_score(application: Application, uploads: List[FormFileUpload]) -> int:
        # Heuristic: approved/verified docs boost score
        total = len(uploads) or 1
        status_u = lambda s: (s or "").upper()
        approved = sum(1 for u in uploads if status_u(u.status) in {"APPROVED", "VERIFIED"})
        ratio = approved / total
        if ratio >= 0.9:
            return 5
        if ratio >= 0.75:
            return 4
        if ratio >= 0.5:
            return 3
        if ratio >= 0.25:
            return 2
        return 1

    # --------- helpers to enrich content ---------
    def _human_doc_label(self, file_type: Optional[str]) -> str:
        t = (file_type or "").strip().lower()
        mapping = {
            "driving_license": "Driving License",
            "driver_license": "Driving License",
            "dl": "Driving License",
            "npi": "NPI",
            "degree": "Degree",
            "cv/resume": "CV/Resume",
            "ml": "Medical License",
            "malpractice": "Malpractice Insurance",
            "other": "Other",
        }
        return mapping.get(t, file_type or "Document")

    def _build_steps(
        self,
        application: Application,
        form: FormData,
        uploads: List[FormFileUpload],
        emails: List[EmailRecord],
    ) -> List[Dict[str, Any]]:
        steps: List[Dict[str, Any]] = []
        # Document steps
        for u in uploads:
            label = self._human_doc_label(u.file_type)
            json_match = self._safe_eval_json(u.json_match)
            mismatches = 0
            matches = 0
            if isinstance(json_match, dict):
                for _, d in json_match.items():
                    try:
                        if d.get("match"):
                            matches += 1
                        else:
                            mismatches += 1
                    except Exception:
                        pass
            steps.append(
                {
                    "type": "Document",
                    "name": label,
                    "status": u.status or "Unknown",
                    "timestamp": None,
                    "details": {
                        "filename": u.filename,
                        "file_type": u.file_type,
                        "matches": matches,
                        "mismatches": mismatches,
                    },
                }
            )

        # Communication steps
        for e in emails:
            steps.append(
                {
                    "type": "Communication",
                    "name": "Email",
                    "status": e.status or "Unknown",
                    "timestamp": e.sent_at,
                    "details": {
                        "recipient": e.recipient_email,
                        "subject": e.subject,
                    },
                }
            )
        return steps

    def _build_decisions(
        self,
        application: Application,
        form: FormData,
        uploads: List[FormFileUpload],
        emails: List[EmailRecord],
    ) -> List[Dict[str, Any]]:
        decisions: List[Dict[str, Any]] = []
        status_u = lambda s: (s or "").upper()
        for u in uploads:
            label = self._human_doc_label(u.file_type)
            if status_u(u.status) in {"APPROVED", "VERIFIED"}:
                decisions.append(
                    {
                        "type": "DocumentApproval",
                        "action": "Accept",
                        "subject": label,
                        "reason": f"{label} {u.status}.",
                    }
                )
            json_match = self._safe_eval_json(u.json_match)
            if isinstance(json_match, dict):
                bad = [k for k, v in json_match.items() if not v.get("match")]
                if bad:
                    decisions.append(
                        {
                            "type": "DataMismatch",
                            "action": "FollowUp",
                            "subject": label,
                            "reason": f"Field mismatch detected: {', '.join(bad)}",
                        }
                    )

        if emails:
            sent = sum(1 for e in emails if status_u(e.status) == "SENT")
            if sent:
                decisions.append(
                    {
                        "type": "Communication",
                        "action": "Notify",
                        "subject": f"{sent} email(s) sent",
                        "reason": "Applicant has been contacted with next steps.",
                    }
                )
        return decisions
