"""Billing file upload and report download API endpoints."""

from __future__ import annotations

from datetime import datetime
import logging
import os
import threading
import time

from flask import Flask, jsonify, request, send_file
from werkzeug.exceptions import HTTPException

from app.config.settings import settings
from app.agents.mail_reader_agent import MailReaderAgent
from app.services.billing_service import process_billing_file
from app.services.excel_filter_service import remove_red_rows_from_excel
from app.services.peoplesoft_output_service import generate_amer_peoplesoft_output
from app.services.sharepoint_download_service import SharePointDownloadClient
from app.services.sharepoint_move_service import SharePointMoveClient
from app.services.sharepoint_upload_service import SharePointUploadClient
from app.api.sharepoint_processor import sharepoint_download, sharepoint_upload
from app.agents.cleaning_agent import cleaning_data_prosessing
from app.services.ihg_servicenow_ticket_service import create_ticket_service_now

logger = logging.getLogger(__name__)

mail_agent = MailReaderAgent()

# Lazy-initialized SharePoint clients to avoid 401 errors at module import time
_sharepoint_download_client: SharePointDownloadClient | None = None
_sharepoint_upload_client: SharePointUploadClient | None = None
_sharepoint_move_client: SharePointMoveClient | None = None

def _get_sharepoint_download_client() -> SharePointDownloadClient:
    """Get or create SharePoint download client (lazy initialization)."""
    global _sharepoint_download_client
    if _sharepoint_download_client is None:
        _sharepoint_download_client = SharePointDownloadClient()
    return _sharepoint_download_client

def _get_sharepoint_upload_client() -> SharePointUploadClient:
    """Get or create SharePoint upload client (lazy initialization)."""
    global _sharepoint_upload_client
    if _sharepoint_upload_client is None:
        _sharepoint_upload_client = SharePointUploadClient()
    return _sharepoint_upload_client

def _get_sharepoint_move_client() -> SharePointMoveClient:
    """Get or create SharePoint move client (lazy initialization)."""
    global _sharepoint_move_client
    if _sharepoint_move_client is None:
        _sharepoint_move_client = SharePointMoveClient()
    return _sharepoint_move_client

ALLOWED_EXCEL_ATTACHMENT_EXTENSIONS = {".xlsx", ".xlsm"}
ALLOWED_EMAIL_BODY_TYPES = {"text", "html"}


def _normalize_email_addresses(value, field_name: str, *, required: bool = False) -> list[str] | None:
    if value in (None, "", []):
        if required:
            raise ValueError(f"'{field_name}' must be a non-empty email address or list of email addresses.")
        return None

    if isinstance(value, str):
        normalized = [value.strip()] if value.strip() else []
    elif isinstance(value, list):
        normalized = []
        for idx, item in enumerate(value):
            if not isinstance(item, str) or not item.strip():
                raise ValueError(
                    f"'{field_name}' item at index {idx} must be a non-empty email address string."
                )
            normalized.append(item.strip())
    else:
        raise ValueError(f"'{field_name}' must be an email address string or a list of email addresses.")

    if required and not normalized:
        raise ValueError(f"'{field_name}' must be a non-empty email address or list of email addresses.")
    return normalized or None


def _normalize_email_attachments(raw_attachments) -> list[dict] | None:
    if raw_attachments in (None, []):
        return None
    if not isinstance(raw_attachments, list):
        raise ValueError("'attachments' must be a list when provided.")

    normalized_attachments: list[dict] = []
    for idx, att in enumerate(raw_attachments):
        if isinstance(att, str):
            path = att.strip()
            name = os.path.basename(path)
        elif isinstance(att, dict):
            path = att.get("path")
            name = att.get("name")
            if name is None and isinstance(path, str):
                name = os.path.basename(path)
        else:
            raise ValueError(f"Attachment at index {idx} must be a path string or an object.")

        if not isinstance(path, str) or not path.strip():
            raise ValueError(f"Attachment at index {idx} is missing a valid 'path'.")
        if not os.path.isfile(path):
            raise ValueError(f"Attachment file not found: {path}")

        actual_name = os.path.basename(path)
        actual_ext = os.path.splitext(actual_name)[1].lower()
        if actual_ext not in ALLOWED_EXCEL_ATTACHMENT_EXTENSIONS:
            raise ValueError("Only Excel read/write attachment files are supported (.xlsx, .xlsm).")

        if name is None:
            name = actual_name
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"Attachment at index {idx} is missing a valid 'name'.")

        provided_ext = os.path.splitext(name)[1].lower()
        if provided_ext != actual_ext:
            raise ValueError(
                f"Attachment at index {idx} must use the exact file extension {actual_ext}."
            )

        normalized_attachments.append({"name": name, "path": path})

    return normalized_attachments


def register_api_routes(app: Flask) -> None:
    """Register all API routes to the Flask app.

    Args:
        app: Flask application instance
    """
    # Ensure runtime directories exist at startup
    os.makedirs(settings.upload_dir, exist_ok=True)
    os.makedirs(settings.output_dir, exist_ok=True)

    @app.errorhandler(HTTPException)
    def handle_http_exception(exc: HTTPException):
        """Return all HTTP errors as JSON instead of HTML."""
        return jsonify({"error": exc.description}), exc.code

    @app.get("/health")
    def health_check():
        """Health check endpoint."""
        return jsonify({"status": "ok"})

    @app.post("/upload")
    def upload_billing_file():
        """
        Upload a billing CSV or Excel file for monthly billing-summary processing.

        Workflow:
        1. Clean and normalize incoming rows.
        2. Split zero-data and paid-data.
        3. Split paid rows into CORP/NON-CORP.
        4. Segment records by AMER/MEXICO/AMEA/EMEAA/GC.
        5. Generate output workbooks and return file paths + counters.
        """
        logger.info("API: Received upload request")
        result = process_billing_file(request)
        logger.info("API: Upload processing complete")
        return jsonify(result), 200

    @app.get("/report/<filename>")
    def download_report(filename: str):
        """Download a previously generated Excel report by filename."""
        logger.info("API: Report download requested for %s", filename)

        # Prevent path-traversal attacks
        safe_name = os.path.basename(filename)
        report_path = os.path.join(settings.output_dir, safe_name)

        if not os.path.isfile(report_path):
            logger.warning("API: Report not found: %s", safe_name)
            return jsonify({"error": "Report not found."}), 404

        logger.info("API: Serving report file: %s", safe_name)
        return send_file(
            report_path,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=safe_name,
        )





    @app.get("/api/v1/emails")
    def list_emails():
        """Return unread emails from the mailbox (or local fallback)."""
        limit = request.args.get("limit", 25, type=int)
        emails = mail_agent.fetch_unread(limit=limit)
        return jsonify(
            [
                {
                    "id": e.id,
                    "subject": e.subject,
                    "sender": e.sender,
                    "received_at": e.received_at.isoformat(),
                    "body": e.body,
                    "attachments": list(e.attachment_paths),
                }
                for e in emails
            ]
        )

    @app.post("/api/v1/send-email")
    def send_email():
        """Send an email with dynamic recipients, sender, subject, body, and attachments.

        Request JSON body::

            {
                "from": "shared.mailbox@company.com",
                "to": ["recipient@example.com"],
                "cc": ["cc@example.com"],
                "subject": "Monthly Billing Validation",
                "body": "Hi Team, Please validate the attached files.",
                "body_type": "text",
                "attachments": [
                    "/absolute/path/to/Monthly Billing Records (April 2026).xlsx",
                    {
                        "name": "Validated Monthly Records.xlsx",
                        "path": "/absolute/path/to/Monthly Billing Records (April 2026).xlsx"
                    }
                ]
            }

        HTML body example::

            {
                "to": "recipient@example.com",
                "subject": "Validation Summary",
                "body": "<p>Hello Team,</p><p>Please review the attached workbook.</p>",
                "body_type": "html"
            }

        Template body example::

            {
                "to": ["recipient@example.com"],
                "subject": "Monthly Billing Validation",
                "template_name": "Monthly_report_validation.html",
                "template_variables": {
                    "recipient_name": "Team",
                    "message": "Please prioritize AMER validation first."
                }
            }

        Attachment rules:
        - Each attachment can be either a file path string or an object with 'path' and optional 'name'.
        - If 'name' is omitted, the file name from 'path' is used automatically.
        - If 'name' is provided, it can differ from the source file name but must keep the same extension.
        - Only Excel read/write files are allowed: .xlsx, .xlsm.

        Body rules:
        - Use 'body' with 'body_type' set to 'text' or 'html'.
        - Or use 'template_name' with optional 'template_variables' for HTML template rendering.
        - If neither is supplied, the default template Monthly_report_validation.html is used.
        """
        data = request.get_json(force=True, silent=True) or {}

        subject = data.get("subject")
        if not subject or not isinstance(subject, str):
            return jsonify({"error": "'subject' is required."}), 400

        try:
            to_addresses = _normalize_email_addresses(data.get("to"), "to", required=True)
            cc_addresses = _normalize_email_addresses(data.get("cc"), "cc")
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        from_address = data.get("from")
        if from_address is not None and (not isinstance(from_address, str) or not from_address.strip()):
            return jsonify({"error": "'from' must be a non-empty email address when provided."}), 400
        if isinstance(from_address, str):
            from_address = from_address.strip()

        recipient_name = data.get("recipient_name") or "Team"
        message = data.get("message") or ""
        body = data.get("body")
        body_type = (data.get("body_type") or "text").lower()
        template_name = data.get("template_name")
        template_variables = data.get("template_variables") or {}

        if body is not None and not isinstance(body, str):
            return jsonify({"error": "'body' must be a string when provided."}), 400
        if body_type not in ALLOWED_EMAIL_BODY_TYPES:
            return jsonify({"error": "'body_type' must be either 'text' or 'html'."}), 400
        if template_name is not None and not isinstance(template_name, str):
            return jsonify({"error": "'template_name' must be a string when provided."}), 400
        if not isinstance(template_variables, dict):
            return jsonify({"error": "'template_variables' must be an object when provided."}), 400
        if body and template_name:
            return jsonify({"error": "Provide either 'body' or 'template_name', not both."}), 400

        try:
            attachments = _normalize_email_attachments(data.get("attachments"))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        try:
            mail_agent.send_email(
                to_addresses=to_addresses,
                subject=subject,
                body=body,
                body_type=body_type,
                template_name=template_name,
                template_variables=template_variables,
                from_address=from_address,
                recipient_name=recipient_name,
                message=message,
                cc_addresses=cc_addresses,
                attachments=attachments,
            )
        except Exception as exc:
            logger.error("send_email failed: %s", exc)
            return jsonify({"error": str(exc)}), 500

        return jsonify({"status": "sent", "to": to_addresses, "subject": subject}), 200

    @app.post("/api/v1/excel/remove-red")
    def remove_red_rows_api():
        """Remove red-highlighted rows from an Excel file in data/ for testing.

        Request JSON body::

            {
                "filename": "input.xlsx",
                "output_dir": "data"  # optional
            }
        """
        data = request.get_json(force=True, silent=True) or {}
        filename = data.get("filename")

        if not filename or not isinstance(filename, str):
            return jsonify({"error": "'filename' is required and must be a string."}), 400

        safe_name = os.path.basename(filename)
        source_path = os.path.join(settings.upload_dir, safe_name)
        if not os.path.isfile(source_path):
            return jsonify({"error": f"File not found in data folder: {safe_name}"}), 404

        ext = os.path.splitext(safe_name)[1].lower()
        if ext not in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
            return jsonify({"error": "Only Excel files are supported (.xlsx, .xlsm, .xltx, .xltm)."}), 400

        output_dir = data.get("output_dir")
        if output_dir is not None and not isinstance(output_dir, str):
            return jsonify({"error": "'output_dir' must be a string when provided."}), 400

        try:
            cleaned_path = remove_red_rows_from_excel(
                input_file_path=source_path,
                output_dir=output_dir or settings.upload_dir,
            )
        except Exception as exc:
            logger.error("remove_red_rows_api failed: %s", exc)
            return jsonify({"error": str(exc)}), 500

        return (
            jsonify(
                {
                    "status": "ok",
                    "source_file": source_path,
                    "cleaned_file": cleaned_path,
                }
            ),
            200,
        )

    @app.post("/api/v1/excel/amer-peoplesoft")
    def generate_amer_peoplesoft_api():
        """Generate PeopleSoft format CSV output for AMER data.

        Request JSON body::

            {
                "input_file_path": "C:/.../output/AMER_*.xlsx",  # optional absolute path
                "filename": "AMER_sample.xlsx",  # optional file in output folder
                "output_stem": "AMER_2026.02 Global Non-Corp February 2026 - Learning Updated 2026.02.18"  # optional
            }
        """
        data = request.get_json(force=True, silent=True) or {}
        filename = data.get("filename")
        input_file_path = data.get("input_file_path")
        output_stem = data.get("output_stem")

        if filename is not None and not isinstance(filename, str):
            return jsonify({"error": "'filename' must be a string when provided."}), 400
        if input_file_path is not None and not isinstance(input_file_path, str):
            return jsonify({"error": "'input_file_path' must be a string when provided."}), 400
        if output_stem is not None and not isinstance(output_stem, str):
            return jsonify({"error": "'output_stem' must be a string when provided."}), 400

        source_path = input_file_path
        if not source_path and filename:
            safe_name = os.path.basename(filename)
            source_path = os.path.join(settings.output_dir, safe_name)

        if source_path and not os.path.isfile(source_path):
            return jsonify({"error": f"File not found: {source_path}"}), 404

        if source_path:
            ext = os.path.splitext(source_path)[1].lower()
        else:
            ext = ".xlsx"

        if ext not in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
            return jsonify({"error": "Only Excel files are supported (.xlsx, .xlsm, .xltx, .xltm)."}), 400

        try:
            result = generate_amer_peoplesoft_output(
                input_file_path=source_path,
                output_stem=output_stem,
            )
        except Exception as exc:
            logger.error("generate_amer_peoplesoft_api failed: %s", exc)
            return jsonify({"error": str(exc)}), 500

        return (
            jsonify(
                {
                    "status": "ok",
                    "source_file": source_path,
                    **result,
                }
            ),
            200,
        )

    @app.post("/api/v1/sharepoint/download")
    def sharepoint_download_api():
        """Download all files from the configured SharePoint folder to local data storage.

        No request body is required. Files are downloaded from the configured
        SharePoint folder into the local data directory.
        """
        remote_path = settings.sharepoint_download_root_path.rstrip("/")
        local_dir = settings.upload_dir

        try:
            downloaded_files = sharepoint_download()
        except Exception as exc:
            logger.error("sharepoint_download_api failed: %s", exc)
            return jsonify({"error": str(exc)}), 500
        
        thread1 = threading.Thread(target=cleaning_data_prosessing, args=())
        thread1.start()
        
        time.sleep(5)  # Add delay to ensure files are fully written to disk before responding 
        thread2 = threading.Thread(target=sharepoint_upload, args=(remote_path, local_dir))
        thread2.start()

        time.sleep(15)  # Add delay to allow upload to start before responding 
        pyaload = {
            "requested_by": "AMER\\USM3PA",
            "requested_for": "AMER\\USM3PA",
            "location": "ATLR3",
            "situation": "Merge profiles",
            "business_service": "IHG University",
            "service_category": "Application Support",
            "assignment_group": "IY-GLBL-LMS Support Accenture",
            "short_description": "LMS billing",
            "description": "LMS billing",
            "internal_notes": "",
            "source": "RCC Tech Intake Form"
        } 
        thread3 = threading.Thread(target=create_ticket_service_now, args=(pyaload,))
        thread3.start()

        return (
            jsonify(
                {
                    "status": "ok",
                    "remote_path": remote_path,
                    "local_directory": os.path.abspath(local_dir),
                    "downloaded_files": [os.path.abspath(path) for path in downloaded_files],
                    "downloaded_count": len(downloaded_files),
                }
            ),
            200,
        )

    @app.post("/api/v1/sharepoint/upload")
    def sharepoint_upload_api():
        """Upload a local file to SharePoint.

        Request JSON body::

            {
                "remote_path": "reports/2026/output.xlsx",
                "local_file_path": "output/output.xlsx",  # optional
                "filename": "output.xlsx",                # optional alternative
                "overwrite": true                           # optional
            }
        """
        data = request.get_json(force=True, silent=True) or {}
        remote_path = data.get("remote_path")
        local_file_path = data.get("local_file_path")
        filename = data.get("filename")
        overwrite = data.get("overwrite", True)

        if not remote_path or not isinstance(remote_path, str):
            return jsonify({"error": "'remote_path' is required and must be a string."}), 400
        if local_file_path is not None and not isinstance(local_file_path, str):
            return jsonify({"error": "'local_file_path' must be a string when provided."}), 400
        if filename is not None and not isinstance(filename, str):
            return jsonify({"error": "'filename' must be a string when provided."}), 400
        if not isinstance(overwrite, bool):
            return jsonify({"error": "'overwrite' must be a boolean when provided."}), 400

        remote_path = remote_path.strip().lstrip("/")
        if not remote_path:
            return jsonify({"error": "'remote_path' cannot be empty."}), 400

        source_path = local_file_path
        if not source_path and filename:
            safe_name = os.path.basename(filename)
            output_candidate = os.path.join(settings.output_dir, safe_name)
            upload_candidate = os.path.join(settings.upload_dir, safe_name)
            source_path = output_candidate if os.path.isfile(output_candidate) else upload_candidate

        if not source_path:
            return jsonify({"error": "Provide either 'local_file_path' or 'filename'."}), 400
        if not os.path.isfile(source_path):
            return jsonify({"error": f"Local file not found: {source_path}"}), 404

        try:
            result = _get_sharepoint_upload_client().upload_file(source_path, remote_path, overwrite=overwrite)
        except Exception as exc:
            logger.error("sharepoint_upload_api failed: %s", exc)
            return jsonify({"error": str(exc)}), 500

        return (
            jsonify(
                {
                    "status": "ok",
                    "local_file": os.path.abspath(source_path),
                    "remote_path": remote_path,
                    "sharepoint_result": result,
                }
            ),
            200,
        )

    @app.post("/api/v1/sharepoint/move")
    def sharepoint_move_api():
        """Move a file within SharePoint.

        Request JSON body::

            {
                "source_path": "LMS Billing/Monthly Billing/source.csv",
                "destination_path": "LMS Billing/Monthly Billing/Archive",
                "overwrite": true
            }

        Behavior:
        - Keeps original source filename (no rename).
        - Creates a MonthYYYY folder under destination_path (for example April2026).
        - Moves source file into destination_path/MonthYYYY/.
        """
        data = request.get_json(force=True, silent=True) or {}
        source_path = data.get("source_path")
        destination_path = data.get("destination_path")
        overwrite = data.get("overwrite", True)

        if not source_path or not isinstance(source_path, str):
            return jsonify({"error": "'source_path' is required and must be a string."}), 400
        if not destination_path or not isinstance(destination_path, str):
            return jsonify({"error": "'destination_path' is required and must be a string."}), 400
        if not isinstance(overwrite, bool):
            return jsonify({"error": "'overwrite' must be a boolean when provided."}), 400

        source_path = source_path.strip().lstrip("/")
        destination_path = destination_path.strip().lstrip("/")
        if not source_path:
            return jsonify({"error": "'source_path' cannot be empty."}), 400
        if not destination_path:
            return jsonify({"error": "'destination_path' cannot be empty."}), 400

        try:
            result = _get_sharepoint_move_client().move_file(
                source_path=source_path,
                destination_path=destination_path,
                overwrite=overwrite,
            )
        except Exception as exc:
            logger.error("sharepoint_move_api failed: %s", exc)
            return jsonify({"error": str(exc)}), 500

        return (
            jsonify(
                {
                    "status": "ok",
                    "source_path": source_path,
                    "destination_path": destination_path,
                    "destination_month_folder": datetime.now().strftime("%B%Y"),
                    "sharepoint_result": result,
                }
            ),
            200,
        )