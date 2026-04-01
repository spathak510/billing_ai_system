# app/services/billing_service.py
from __future__ import annotations

import io

import pandas as pd
from flask import Request, abort

from app.config.settings import settings
from app.processing.pipeline import BillingPipeline

_ALLOWED_CONTENT_TYPES = {
    "text/csv",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

_pipeline = BillingPipeline()


def process_billing_file(request: Request) -> dict:
    """Validate, process, and generate use-case billing output files.

    Returns
    -------
    dict
        Output files and processing counters.
    """
    if "file" not in request.files:
        abort(400, description="No file part in the request.")

    file = request.files["file"]

    if not file.filename:
        abort(400, description="No file selected.")

    _validate_upload(file)

    contents = file.read()
    filename = file.filename or "upload"

    # Enforce max upload size
    if len(contents) > settings.max_upload_size_mb * 1024 * 1024:
        abort(400, description=f"File exceeds maximum size of {settings.max_upload_size_mb} MB.")

    df = _read_file(contents, filename)

    try:
        result = _pipeline.run(df, filename)
    except ValueError as exc:
        abort(422, description=str(exc))

    return result


def _validate_upload(file) -> None:
    if file.content_type not in _ALLOWED_CONTENT_TYPES:
        abort(
            400,
            description=(
                f"Unsupported file type '{file.content_type}'. "
                "Please upload a CSV or Excel file."
            ),
        )


def _read_file(contents: bytes, filename: str) -> pd.DataFrame:
    if filename.endswith(".csv"):
        return pd.read_csv(io.BytesIO(contents))
    return pd.read_excel(io.BytesIO(contents), engine="openpyxl")
