from __future__ import annotations

import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from app.config.settings import settings

logger = logging.getLogger(__name__)

_RIR_REQUIRED_SHEETS = ("upload sheet", "Recharge Form")
_TIMESTAMPED_OUTPUT_PATTERN = re.compile(r"_\d{8}_\d{6}\.xlsx$", re.IGNORECASE)


def _resolve_input_path(input_file_path: str | None) -> Path:
    """Resolve the input file path for RIR APAC processing."""
    if input_file_path:
        return Path(input_file_path)

    output_dir = Path(settings.output_dir)
    cleaned_files = sorted(
        output_dir.glob("cleaned_no_red_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    if cleaned_files:
        return cleaned_files[0]

    raise FileNotFoundError("No cleaned_no_red_*.xlsx file found in output folder.")


def _find_col(columns: list[str], candidates: list[str]) -> str | None:
    """Find a column by trying multiple candidate names (case-insensitive)."""
    normalized = {str(col).strip().upper().replace(" ", ""): col for col in columns}
    for candidate in candidates:
        key = candidate.upper().replace(" ", "")
        if key in normalized:
            return normalized[key]
    return None


def _to_decimal(value: object) -> Decimal:
    """Convert a value to Decimal, handling commas and empty strings."""
    text = str(value).strip().replace(",", "")
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except InvalidOperation:
        return Decimal("0")


def _set_cell_value_safe(ws, cell_ref: str, value: object) -> None:
    """Set cell value safely, handling merged cells."""
    cell = ws[cell_ref]
    if cell.__class__.__name__ != "MergedCell":
        cell.value = value
        return

    for merged_range in ws.merged_cells.ranges:
        if cell_ref in merged_range:
            ws.cell(row=merged_range.min_row, column=merged_range.min_col).value = value
            return

    ws[cell_ref].value = value


def _is_generated_rir_output(path: Path) -> bool:
    """Return True when filename matches generated RIR output timestamp pattern."""
    return bool(_TIMESTAMPED_OUTPUT_PATTERN.search(path.name))


def _resolve_template_path(output_root: Path) -> Path:
    """Find a valid RIR template workbook.

    The folder can contain both templates and previously generated outputs.
    We only accept workbooks that contain the required sheets and prefer
    non-generated template files over timestamped output files.
    """
    template_candidates = sorted(
        output_root.glob("RIR_*.xlsx"),
        key=lambda path: (_is_generated_rir_output(path), path.stat().st_mtime),
    )
    if not template_candidates:
        raise FileNotFoundError(
            f"RIR APAC template not found in {output_root}. "
            f"Please place a template file named 'RIR_*.xlsx' in the output/RIR_APAC directory."
        )

    for candidate in template_candidates:
        try:
            workbook = load_workbook(candidate, read_only=True)
        except Exception as exc:
            logger.warning("Skipping RIR candidate %s: failed to open workbook (%s)", candidate, exc)
            continue

        missing_sheets = [sheet for sheet in _RIR_REQUIRED_SHEETS if sheet not in workbook.sheetnames]
        extra_sheets = [sheet for sheet in workbook.sheetnames if sheet not in _RIR_REQUIRED_SHEETS]
        workbook.close()

        if missing_sheets:
            logger.warning(
                "Skipping RIR candidate %s: missing required sheets %s",
                candidate,
                ", ".join(missing_sheets),
            )
            continue

        if extra_sheets:
            logger.info(
                "RIR template %s contains extra sheets that will be ignored: %s",
                candidate,
                ", ".join(extra_sheets),
            )

        return candidate

    raise ValueError(
        "No valid RIR template found. The workbook must contain both 'upload sheet' and 'Recharge Form'."
    )


def generate_rir_apac_output(
    input_file_path: str | None = None,
    submitted_by: str = "",
    output_file_name: str = "RIR_GC_APAC_NON-CORP",
) -> dict[str, str | int | float]:
    """Generate RIR APAC workbook from cleaned data using the provided template format.
    
    Args:
        input_file_path: Path to the cleaned data file (optional, auto-resolves if None)
        submitted_by: Name of the user submitting the data
        output_file_name: Base name for the output file
    
    Returns:
        Dictionary with output_path, record count, and total amount
    """
    source_path = _resolve_input_path(input_file_path)
    df = pd.read_excel(source_path, sheet_name=0)

    cols = list(df.columns)
    
    # Find required columns with multiple candidate names
    bu_col = _find_col(cols, ["BU"])
    currency_col = _find_col(cols, ["CURRENCYCODE", "CURRENCY CODE", "CURRENCY"])
    amount_col = _find_col(cols, ["AMOUNT", "BILL_AMOUNT", "BILLING_AMOUNT", "TOTAL_AMOUNT", "NET_AMOUNT", "VALUE"])
    holidex_col = _find_col(cols, ["HOLIDEX", "CUSTID", "CUSTID #"])
    course_col = _find_col(cols, ["COURSE_NAME", "COURSE NAME", "DESCRIPTION"])
    order_col = _find_col(cols, ["ORDER_NO", "ORDER NO", "ORDER_NUMBER", "ORDER NUMBER"])
    offering_date_col = _find_col(cols, ["OFFERING_DATE", "OFFERING DATE"])
    employee_col = _find_col(cols, ["EMPLOYEE", "LEARNERS NAME", "LEARNER", "USERNAME"])

    # Check for required columns
    if not bu_col or not currency_col or not amount_col:
        missing = [
            key
            for key, value in {
                "BU": bu_col,
                "CURRENCYCODE": currency_col,
                "AMOUNT": amount_col,
            }.items()
            if value is None
        ]
        raise ValueError(f"Missing required columns for RIR APAC processing: {', '.join(missing)}")

    # Find APPLY REVENUE column
    apply_revenue_col = None
    for col in cols:
        if "APPLYREVENUE" in str(col).upper().replace(" ", ""):
            apply_revenue_col = col
            break

    # Filter rows for RIR APAC processing
    rir_rows: list[dict[str, object]] = []

    for _, row in df.iterrows():
        bu = str(row.get(bu_col, "")).strip().upper()
        amount = _to_decimal(row.get(amount_col, "0"))

        # Skip zero amounts and certain BU types
        if amount == 0:
            continue
        if bu.startswith("H") or bu.startswith("A"):
            continue
        if not bu.startswith("P"):
            continue

        # Add to RIR rows
        row_dict = row.to_dict()
        row_dict[amount_col] = float(amount)
        rir_rows.append(row_dict)

    # Find or create output directory
    output_root = Path(settings.output_dir) / "RIR_APAC"
    output_root.mkdir(parents=True, exist_ok=True)

    # Load a valid template workbook from the RIR folder.
    template_path = _resolve_template_path(output_root)
    
    logger.info("Using RIR APAC template: %s", template_path)

    workbook = load_workbook(template_path)
    
    # Verify required sheets exist
    if "upload sheet" not in workbook.sheetnames:
        raise ValueError("Template must contain 'upload sheet' sheet")
    if "Recharge Form" not in workbook.sheetnames:
        raise ValueError("Template must contain 'Recharge Form' sheet")

    extra_sheets = [sheet for sheet in workbook.sheetnames if sheet not in _RIR_REQUIRED_SHEETS]
    if extra_sheets:
        logger.info(
            "Ignoring extra sheets in RIR workbook %s: %s",
            template_path,
            ", ".join(extra_sheets),
        )

    upload_sheet = workbook["upload sheet"]
    recharge_sheet = workbook["Recharge Form"]

    # Clear old data rows (rows 10-10000)
    for row_idx in range(10, 10001):
        for col_idx in range(1, 30):
            cell = upload_sheet.cell(row=row_idx, column=col_idx)
            cell.value = None

    # Set header information
    current_dt = datetime.now()
    date_str = f"{current_dt.month}/{current_dt.day}/{current_dt.year}"
    
    _set_cell_value_safe(recharge_sheet, "F8", date_str)
    _set_cell_value_safe(recharge_sheet, "O8", submitted_by)

    # Process data rows
    total = Decimal("0")
    row_index = 10
    line_no = 1
    record_count = 0

    for row in rir_rows:
        amount = _to_decimal(row.get(amount_col, 0))

        # Write row data
        upload_sheet.cell(row=row_index, column=1).value = line_no
        upload_sheet.cell(row=row_index, column=2).value = row.get(bu_col, "")
        upload_sheet.cell(row=row_index, column=3).value = row.get(holidex_col, "") if holidex_col else ""
        upload_sheet.cell(row=row_index, column=5).value = date_str
        upload_sheet.cell(row=row_index, column=6).value = "LMS Training"
        upload_sheet.cell(row=row_index, column=7).value = row.get(course_col, "") if course_col else ""
        upload_sheet.cell(row=row_index, column=8).value = row.get(currency_col, "")
        upload_sheet.cell(row=row_index, column=9).value = float(amount)
        upload_sheet.cell(row=row_index, column=11).value = row.get(apply_revenue_col, "") if apply_revenue_col else ""
        
        # Column 15 (O): COURSE_NAME
        upload_sheet.cell(row=row_index, column=15).value = row.get(course_col, "") if course_col else ""
        
        # Column 26: ORDER_NO
        upload_sheet.cell(row=row_index, column=26).value = row.get(order_col, "") if order_col else ""
        
        # Column 28 (AB): OFFERING_DATE
        upload_sheet.cell(row=row_index, column=28).value = row.get(offering_date_col, "") if offering_date_col else ""
        
        # Column 29 (AC): EMPLOYEE
        upload_sheet.cell(row=row_index, column=29).value = row.get(employee_col, "") if employee_col else ""

        total += amount
        record_count += 1
        row_index += 1
        line_no += 1

    # Set totals in upload sheet (I5)
    _set_cell_value_safe(upload_sheet, "I5", float(total))

    # Set totals in recharge form (I33)
    _set_cell_value_safe(recharge_sheet, "I33", float(total))

    # Save the workbook with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_root / f"{output_file_name}_{timestamp}.xlsx"
    workbook.save(output_path)

    logger.info("Generated RIR APAC output: %s (records: %d, total: %.2f)", output_path, record_count, float(total))
    return {
        "rir_apac_output_path": str(output_path),
        "rir_apac_records": record_count,
        "rir_apac_total": float(total),
    }
