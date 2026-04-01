# app/main.py
from __future__ import annotations

import logging
import os

from flask import Flask, jsonify, request, send_file
from werkzeug.exceptions import HTTPException

from app.config.settings import settings
from app.services.billing_service import process_billing_file

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)

# ------------------------------------------------------------------
# App
# ------------------------------------------------------------------
app = Flask(__name__)

# Ensure runtime directories exist at startup
os.makedirs(settings.upload_dir, exist_ok=True)
os.makedirs(settings.output_dir, exist_ok=True)


@app.errorhandler(HTTPException)
def handle_http_exception(exc: HTTPException):
    """Return all HTTP errors as JSON instead of HTML."""
    return jsonify({"error": exc.description}), exc.code


@app.get("/health")
def health_check():
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
    result = process_billing_file(request)
    return jsonify(result), 200


@app.get("/report/<filename>")
def download_report(filename: str):
    """Download a previously generated Excel report by filename."""
    # Prevent path-traversal attacks
    safe_name = os.path.basename(filename)
    report_path = os.path.join(settings.output_dir, safe_name)
    if not os.path.isfile(report_path):
        return jsonify({"error": "Report not found."}), 404
    return send_file(
        report_path,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=safe_name,
    )


if __name__ == "__main__":
    app.run(debug=True, port=8000)
