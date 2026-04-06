# app/api/upload_api.py
"""Billing file upload and report download API endpoints."""

from __future__ import annotations

import logging
import os

from flask import Flask, jsonify, request, send_file
from werkzeug.exceptions import HTTPException

from app.config.settings import settings
from app.services.billing_service import process_billing_file
from app.services.excel_filter_service import remove_red_rows_from_excel
from app.services.peoplesoft_output_service import generate_amer_peoplesoft_output
from app.services.mail_service import MicrosoftGraphMailboxClient

logger = logging.getLogger(__name__)

pipeline = MicrosoftGraphMailboxClient(
    tenant_id=os.getenv("GRAPH_TENANT_ID"),
    client_id=os.getenv("GRAPH_CLIENT_ID"),
    client_secret=os.getenv("GRAPH_CLIENT_SECRET"),
    mailbox_user=os.getenv("GRAPH_MAILBOX_USER"),
    mailbox_password=os.getenv("GRAPH_MAILBOX_PASSWORD"),
    timeout_seconds=int(os.getenv("GRAPH_TIMEOUT_SECONDS", "20")),
)


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
        print("hello")
        limit = request.args.get("limit", 25, type=int)
        emails = pipeline.fetch_unread(limit=limit)
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
        """Send an email using the static HTML template at app/templates/email_body.html.

        Request JSON body::

            {
                "to": ["recipient@example.com"],
                "subject": "Invoice INV-3021 Due",
                "recipient_name": "John",
                "message": "Your invoice INV-3021 of $1,500 is due by April 15.",
                "cc": ["cc@example.com"],
                "attachments": [
                    {"name": "invoice.pdf", "path": "/absolute/path/to/file.pdf"}
                ]
            }
        """
        data = request.get_json(silent=True) or {}

        to_addresses = data.get("to")
        subject = data.get("subject")

        if not to_addresses or not isinstance(to_addresses, list):
            return jsonify({"error": "'to' must be a non-empty list of email addresses."}), 400
        if not subject or not isinstance(subject, str):
            return jsonify({"error": "'subject' is required."}), 400

        recipient_name = data.get("recipient_name") or "Team"
        message = data.get("message") or ""

        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "templates", "email_body.html"
        )
        with open(template_path, encoding="utf-8") as f:
            html_body = f.read()

        html_body = (
            html_body
            .replace("{{subject}}", subject)
            .replace("{{recipient_name}}", recipient_name)
            .replace("{{message}}", message)
        )

        try:
            pipeline.send_email(
                to_addresses=to_addresses,
                subject=subject,
                body=html_body,
                cc_addresses=data.get("cc") or None,
                attachments=data.get("attachments") or None,
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
        data = request.get_json(silent=True) or {}
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
        data = request.get_json(silent=True) or {}
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
