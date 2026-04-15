"""Billing file upload and report download API endpoints."""

from __future__ import annotations

from datetime import datetime
import json
import logging
import os
import threading
import time

from flask import Flask, jsonify, request, send_file
from werkzeug.exceptions import HTTPException

from app.config.settings import settings
from app.agents.mail_reader_agent import MailReaderAgent
from app.services.billing_service import process_billing_file
from app.services.cleanup_service import cleanup_all_outputs, cleanup_specific_folder
from app.services.excel_filter_service import remove_red_rows_from_excel
from app.services.peoplesoft_output_service import generate_amer_peoplesoft_output
from app.services.sharepoint_download_service import SharePointDownloadClient
from app.services.sharepoint_move_service import SharePointMoveClient
from app.services.sharepoint_upload_service import SharePointUploadClient
from app.api.sharepoint_processor import sharepoint_download, sharepoint_upload
from app.agents.cleaning_agent import cleaning_data_prosessing
from app.services.ihg_servicenow_ticket_service import create_ticket_service_now
from app.api.mail_processor import get_available_mail_payload_templates, process_mail_for_post_validation_billing   

logger = logging.getLogger(__name__)

DEFAULT_REMOVE_RED_FILENAME = (
    "2026.02 Global Corp & Non-Corp February 2026 - Learning Updated 2026.02.18.xlsx"
)

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

ALLOWED_EXCEL_ATTACHMENT_EXTENSIONS = {".xlsx", ".xlsm", ".csv"}
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
            raise ValueError("Only Excel read/write attachment files are supported (.xlsx, .xlsm, .csv).")

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


def _extract_incident_id(servicenow_result: dict | None) -> str | None:
    """Extract incident id from ServiceNow create ticket response payload."""
    if not isinstance(servicenow_result, dict):
        return None

    response_body = servicenow_result.get("response")
    if isinstance(response_body, dict):
        for key in ("incident_id", "incidentId", "incidentID", "number", "ticket_id", "ticketId"):
            value = response_body.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        result_payload = response_body.get("result")
        if isinstance(result_payload, dict):
            for key in ("incident_id", "incidentId", "incidentID", "number", "ticket_id", "ticketId"):
                value = result_payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()

    return None

def _run_clean_data_flow() -> None:
    try:
        base_remote_path = "/Monthly Billing Clean Data/".rstrip("/")  # Ensure no trailing slash
        local_dir = settings.output_dir
        
        logger.info("Step 1: SharePoint download started")
        download_result = sharepoint_download()
        logger.info("Step 1: SharePoint download completed: %s", download_result)

        logger.info("Step 2: Cleaning process started")
        cleaning_data_prosessing()
        logger.info("Step 2: Cleaning process completed")

        

        logger.info("Step 3: SharePoint upload started")
        month_folder = datetime.now().strftime("%B_%Y")
        remote_path = f"{base_remote_path}/{month_folder}"
        local_dir = local_dir+"/Monthly_cleaned_report"
        upload_result = sharepoint_upload(remote_path, local_dir)
        logger.info("Step 3: SharePoint upload completed: %s", upload_result)

        payload = {
            "requested_by": "AMER\\USM3PA",
            "requested_for": "AMER\\USM3PA",
            "location": "ATLR3",
            "situation": "other",
            "business_service": "IHG University",
            "service_category": "Application Support",
            "assignment_group": "IY-GLBL-LMS Support Accenture",
            "short_description": "LMS Monthly Billing Process - MyID Data Retrieve",
            "description": "LMS Monthly Billing Process - MyID Data Retrieve",
            "internal_notes": "",
            "source": "RCC Tech Intake Form",
        }

        logger.info("Step 4: ServiceNow ticket creation started")
        response = create_ticket_service_now(payload)
        logger.info("Step 4: ServiceNow ticket creation completed: %s", response)

    except Exception as exc:
        logger.exception("Background flow failed: %s", exc)      


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
        attachment_dir = "data/Post_Validation_Data"
        emails = mail_agent.fetch_unread(limit=limit, attachment_dir=attachment_dir)
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
        data = request.get_json(silent=True)
        if data is None:
            raw_body = request.get_data(cache=False, as_text=False)
            if raw_body:
                try:
                    data = json.loads(raw_body.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    return jsonify({"error": "Invalid JSON request body."}), 400

        if data is None:
            data = {}
        if not isinstance(data, dict):
            return jsonify({"error": "Request body must be a JSON object."}), 400

        subject = data.get("subject")
        if not subject or not isinstance(subject, str):
            return jsonify({"error": "'subject' is required."}), 400
        subject = subject.strip()
        if not subject:
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

        body = data.get("body")
        body_type = (data.get("body_type") or "text").lower()
        template_name = data.get("template_name")

        raw_template_variables = data.get("template_variables")
        if raw_template_variables is None:
            template_variables: dict = {}
        elif isinstance(raw_template_variables, dict):
            template_variables = raw_template_variables
        else:
            return jsonify({"error": "'template_variables' must be an object when provided."}), 400

        recipient_name = template_variables.get("recipient_name") or "Team"
        message = template_variables.get("message") or ""

        if body is not None and not isinstance(body, str):
            return jsonify({"error": "'body' must be a string when provided."}), 400
        if body_type not in ALLOWED_EMAIL_BODY_TYPES:
            return jsonify({"error": "'body_type' must be either 'text' or 'html'."}), 400
        if template_name is not None and not isinstance(template_name, str):
            return jsonify({"error": "'template_name' must be a string when provided."}), 400
        if isinstance(template_name, str):
            template_name = template_name.strip()
            if template_name and not os.path.splitext(template_name)[1]:
                template_name = f"{template_name}.html"
        if body is not None and template_name:
            return jsonify({"error": "Provide either 'body' or 'template_name', not both."}), 400
        if template_name:
            template_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
            template_path = os.path.join(template_dir, template_name)
            if not os.path.isfile(template_path):
                available_templates = sorted(
                    item for item in os.listdir(template_dir) if item.lower().endswith(".html")
                )
                return (
                    jsonify(
                        {
                            "error": f"Template not found: {template_name}",
                            "available_templates": available_templates,
                        }
                    ),
                    400,
                )

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
    


    @app.post("/api/v1/post_validation_send_email")
    def post_validation_send_email():
        """Send post-validation emails for all templates one by one."""

        templates_to_send = get_available_mail_payload_templates()

        if not templates_to_send:
            return jsonify({"error": "No templates available to send."}), 400

        sent_templates: list[str] = []
        failed_templates: list[dict[str, str]] = []

        for template_name in templates_to_send:
            payload = process_mail_for_post_validation_billing({"template_name": template_name})

            if isinstance(payload, dict) and payload.get("error"):
                failed_templates.append({"template": template_name, "error": str(payload.get("error"))})
                continue

            try:
                attachments = _normalize_email_attachments(payload.get("attachments"))
                to_addresses = _normalize_email_addresses(payload.get("to"), "to", required=True)
                cc_addresses = _normalize_email_addresses(payload.get("cc"), "cc")
            except ValueError as exc:
                failed_templates.append({"template": template_name, "error": str(exc)})
                continue

            try:
                template_variables = payload.get("template_variables")
                if not isinstance(template_variables, dict):
                    template_variables = {}

                mail_agent.send_email(
                    to_addresses=to_addresses,
                    subject=str(payload.get("subject", "")).strip(),
                    body=None,
                    body_type=str(payload.get("body_type") or "html").lower(),
                    template_name=str(payload.get("template_name") or template_name),
                    template_variables=template_variables,
                    from_address=str(payload.get("from", "")).strip() or None,
                    recipient_name=str(template_variables.get("recipient_name") or "Team"),
                    message=str(template_variables.get("message") or ""),
                    cc_addresses=cc_addresses,
                    attachments=attachments,
                )
                sent_templates.append(template_name)
            except Exception as exc:
                logger.error("post_validation_send_email failed for template %s: %s", template_name, exc)
                failed_templates.append({"template": template_name, "error": str(exc)})

        status_code = 200 if not failed_templates else 207
        return (
            jsonify(
                {
                    "status": "completed" if sent_templates else "failed",
                    "sent_count": len(sent_templates),
                    "failed_count": len(failed_templates),
                    "sent_templates": sent_templates,
                    "failed_templates": failed_templates,
                }
            ),
            status_code,
        )

    # This endpoint is designed to trigger post validation part of a long-running background flow that downloads files from SharePoint, processes them, uploads results back to SharePoint, creates a ServiceNow ticket, and sends notification emails. It returns immediately with a 202 Accepted status while the flow continues asynchronously.
    @app.post("/api/v1/excel/remove-red")
    def post_validation_flow_api():
        """Remove red-highlighted rows from an Excel file in data/ for testing.

        Request JSON body::

            {
                "filename": "input.xlsx",
                "output_dir": "data"  # optional
            }
        """
        data = request.get_json(force=True, silent=True) or {}
        filename = data.get("filename")

        if filename is not None and not isinstance(filename, str):
            return jsonify({"error": "'filename' must be a string when provided."}), 400

        if filename and filename.strip():
            safe_name = os.path.basename(filename.strip())
            source_path = os.path.join(settings.upload_dir, safe_name)
        else:
            source_path = os.path.join(
                settings.upload_dir,
                "Post_validation_data",
                DEFAULT_REMOVE_RED_FILENAME,
            )
            safe_name = os.path.basename(source_path)

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
        
        upload_result = None
        try:
            upload_result = sharepoint_upload_post_validation_record_api()
            if isinstance(upload_result, tuple) and len(upload_result) >= 2:
                status_code = upload_result[1]
                if isinstance(status_code, int) and status_code >= 400:
                    logger.warning(
                        "Post validation SharePoint upload returned non-success status: %s",
                        status_code,
                    )
                else:
                    logger.info("Uploaded all output files to SharePoint")
            else:
                logger.info("Uploaded all output files to SharePoint")
        except Exception as exc:
            logger.warning("Failed to upload post validation data file: %s", exc)

        time.sleep(15)  # Add delay to allow upload to start before responding 
        payload = {
            "requested_by": "AMER\\USM3PA",
            "requested_for": "AMER\\USM3PA",
            "location": "ATLR3",
            "situation": "other [incident]",
            "business_service": "IHG University",
            "service_category": "Application Support",
            "assignment_group": "IY-GLBL-LMS Support Accenture",
            "short_description": "LMS Monthly Billing Process - PS Upload",
            "description": "LMS Monthly Billing Process - PS Upload",
            "internal_notes": "",
            "source": "RCC Tech Intake Form"
        }
        service_now_result: dict | None = None
        incident_id: str | None = None
        try:
            service_now_result = create_ticket_service_now(payload)
            incident_id = _extract_incident_id(service_now_result)
        except Exception as exc:
            logger.warning("Failed to create ServiceNow ticket: %s", exc)

        try:
            post_validation_send_email()
        except Exception as exc:
            logger.warning("Failed to send post validation emails: %s", exc)  



        return (
            jsonify(
                {
                    "status": "ok",
                    "source_file": source_path,
                    "cleaned_file": cleaned_path,
                    "incident_id": incident_id,
                    "servicenow_status_code": (
                        service_now_result.get("status_code") if isinstance(service_now_result, dict) else None
                    ),
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

    # This endpoint is designed to trigger a long-running background flow that downloads files from SharePoint, processes them, uploads results back to SharePoint, creates a ServiceNow ticket, and sends notification emails. It returns immediately with a 202 Accepted status while the flow continues asynchronously.
    @app.post("/api/v1/sharepoint/download")
    def initial_clean_data_flow_api():
        """Download all files from the configured SharePoint folder to local data storage.

        No request body is required. Files are downloaded from the configured
        SharePoint folder into the local data directory.
        """
        worker = threading.Thread(target=_run_clean_data_flow, daemon=True)
        worker.start()

        return (
            jsonify(
                {
                    "status": "accepted",
                    "message": "Background flow started",
                }
            ),
            202,
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
    
    @app.post("/api/v1/sharepoint/upload/validation_records")
    def sharepoint_upload_post_validation_record_api():
        """Upload a local file to SharePoint.

        Request JSON body::

            {
                "remote_path": "reports/2026/output.xlsx",
                "local_file_path": "output/output.xlsx",  # optional
                "filename": "output.xlsx",                # optional alternative
                "overwrite": true                           # optional
            }
        """
        remote_path = settings.sharepoint_download_root_path.rstrip("/") + "/Output"
        local_dir = settings.output_dir
        month_folder = datetime.now().strftime("%B_%Y")

        # Mapping: SharePoint destination folder -> local output subfolder
        upload_targets = {
            "AMER PeopleSoft": os.path.join("AMER", "AMER_Output"),
            "AMER_InterCompany": os.path.join("AMER_Intercompny", "Output"),
            "APAC_GC Intercompany": os.path.join("APAC", "APAC_Intercompny", "Output"),
            "APAC_GC_GAF": os.path.join("APAC", "GAF_APAC_Processor", "Output"),
            "APAC_GC_RIR": os.path.join("APAC", "APAC_GC_RIR", "Output"),
            "EMEAA_Intercompany": os.path.join("EMEAA", "EMEAA_Intercompany", "Output"),
            "Standard_Journal": os.path.join("JRF", "Output"),
        }

        try:
            count = 0
            skipped_directories: list[str] = []
            skipped_files: list[str] = []
            used_remote_month_paths: list[str] = []
            upload_client = _get_sharepoint_upload_client()

            for remote_folder, local_subdir in upload_targets.items():
                exact_remote_path = f"{remote_path}/{remote_folder}/{month_folder}"
                local_target_dir = os.path.join(local_dir, local_subdir)

                if not os.path.isdir(local_target_dir):
                    skipped_directories.append(local_target_dir)
                    logger.warning("Skipping missing output folder: %s", local_target_dir)
                    continue

                used_remote_month_paths.append(exact_remote_path)

                for file_name in os.listdir(local_target_dir):
                    file_path = os.path.join(local_target_dir, file_name)
                    if not os.path.isfile(file_path):
                        continue
                    remote_file_path = f"{exact_remote_path}/{file_name}"
                    try:
                        upload_client.upload_file(file_path, remote_file_path, overwrite=True)
                        count += 1
                    except FileNotFoundError:
                        skipped_files.append(file_path)
                        logger.warning("Skipping missing local file during upload: %s", file_path)
                        continue

        except Exception as exc:
            logger.error("sharepoint_download_api failed: %s", exc)
            return jsonify({"error": str(exc)}), 500
        
        

        return (
            jsonify(
                {
                    "status": "ok",
                    "remote_path": remote_path,
                    "month_folder": month_folder,
                    "Total_upload_file": count,
                    "skipped_directories": skipped_directories,
                    "skipped_files": skipped_files,
                    "used_remote_month_paths": used_remote_month_paths,
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

    @app.post("/api/v1/cleanup/all")
    def cleanup_all_api():
        """Remove all flow output files and cleanup generated files from data folder.

        This endpoint recursively deletes:
        - ALL files in output/ folder (preserves folder structure & templates)
        - cleaned_no_red_*.xlsx files from data/ folder (preserves original inputs)

        Preserves:
        - Folder structure (directories are not deleted, only contents)
        - Template files (files/folders containing 'template' in the name)
        - Original input files in data/ folder (only removes generated cleanup files)

        Request JSON body (optional)::

            {
                "confirm": true  # Safe to omit - just call the endpoint
            }

        Response::

            {
                "status": "success",
                "message": "Cleanup completed successfully (output/ and data/cleaned files removed)",
                "files_deleted": 125,
                "folders_scanned": 45,
                "size_freed_mb": 156.34,
                "locations_cleaned": ["output", "data"],
                "removed_paths": [
                    "output/AMER_Intercompny/Output/AMER_Intercompany billing lines_April 2026.xlsx",
                    "output/APAC/APAC_Output/APAC Processing_APAC_GC_CROP.xlsx",
                    "data/cleaned_no_red_2026.02 Global Corp & Non-Corp February 2026 - Learning Updated 2026.02.18.xlsx",
                    ...
                ]
            }
        """
        try:
            result = cleanup_all_outputs()
        except Exception as exc:
            logger.error("cleanup_all_api failed: %s", exc)
            return jsonify({"error": str(exc), "status": "error"}), 500

        status_code = 200 if result.get("status") == "success" else 500
        return jsonify(result), status_code

    @app.post("/api/v1/cleanup/folder")
    def cleanup_folder_api():
        """Remove all files from a specific output subfolder.

        Preserves template files within the folder.

        Request JSON body::

            {
                "folder_name": "AMER_Intercompny"  # required: subfolder in output/
            }

        Supported folder names:
        - AMER_Intercompny
        - APAC
        - EMEAA
        - Region_Wise_Split
        - GAF_APAC_PROCESSER
        - JRF
        - Monthly_cleaned_report
        - RIR_APAC

        Response::

            {
                "status": "success",
                "message": "Cleanup completed for folder 'AMER_Intercompny'",
                "files_deleted": 15,
                "size_freed_mb": 12.45,
                "removed_paths": [
                    "AMER_Intercompny/Output/AMER_Intercompany billing lines_April 2026.xlsx"
                ]
            }
        """
        data = request.get_json(force=True, silent=True) or {}
        folder_name = data.get("folder_name")

        if not folder_name or not isinstance(folder_name, str):
            return jsonify(
                {
                    "error": "'folder_name' is required and must be a string.",
                    "status": "error",
                }
            ), 400

        try:
            result = cleanup_specific_folder(folder_name.strip())
        except Exception as exc:
            logger.error("cleanup_folder_api failed for folder '%s': %s", folder_name, exc)
            return jsonify({"error": str(exc), "status": "error"}), 500

        status_code = 200 if result.get("status") == "success" else 500
        return jsonify(result), status_code