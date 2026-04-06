from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from app.config.settings import settings

logger = logging.getLogger(__name__)


def _resolve_input_path(input_file_path: str | None) -> Path:
    if input_file_path:
        return Path(input_file_path)

    output_dir = Path(settings.output_dir)
    cleaned_files = sorted(output_dir.glob("cleaned_no_red_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if cleaned_files:
        return cleaned_files[0]

    raise FileNotFoundError("No cleaned_no_red_*.xlsx file found in output folder.")


def _to_decimal(value: object) -> Decimal:
    text = str(value).strip().replace(",", "")
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except InvalidOperation:
        return Decimal("0")


def _output_stem(source_stem: str, custom_stem: str | None) -> str:
    if custom_stem and custom_stem.strip():
        return custom_stem.strip()
    if source_stem.upper().startswith("CLEANED_NO_RED_"):
        return "APAC Processing"
    return source_stem


def _generate_apac_gc_noncorp_formatted_output(
    output_dir: Path,
    rows: list[dict[str, object]],
    source_columns: list[str],
) -> str | None:
    template_path = output_dir.parent / "APAC_INP_FORMAT" / "APAC_GC_Intercompany billing lines_January26.xlsx"
    if not template_path.exists():
        logger.warning("APAC template not found: %s", template_path)
        return None

    wb = load_workbook(template_path)
    if "BILLING LINES" not in wb.sheetnames:
        logger.warning("APAC template missing 'BILLING LINES' sheet: %s", template_path)
        return None

    ws = wb["BILLING LINES"]
    header_cells = [cell.value for cell in ws[1] if cell.value not in (None, "")]
    if not header_cells:
        logger.warning("APAC template has no headers in BILLING LINES: %s", template_path)
        return None

    header_names = [str(h).strip() for h in header_cells]

    # Clear existing data rows while preserving the workbook format and headers.
    if ws.max_row > 1:
        ws.delete_rows(2, ws.max_row - 1)

    source_map = {str(col).strip().upper(): col for col in source_columns}

    for row_dict in rows:
        output_row: list[object] = []
        for header in header_names:
            source_col = source_map.get(header.upper())
            output_row.append(row_dict.get(source_col) if source_col else None)
        ws.append(output_row)

    if "RIR" in wb.sheetnames:
        wb["RIR"]["F10"] = datetime.now().date()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"APAC_GC_NON CROP_{timestamp}.xlsx"
    wb.save(output_path)
    logger.info("Generated APAC formatted output: %s", output_path)
    return str(output_path)


def generate_apac_processing_output(
    input_file_path: str | None = None,
    output_stem: str | None = None,
) -> dict[str, str | int]:
    """Process APAC rules from cleaned data and write formatted APAC output workbook."""
    source_path = _resolve_input_path(input_file_path)
    df = pd.read_excel(source_path, sheet_name=0)

    col_map = {str(col).upper(): col for col in df.columns}
    bu_col = col_map.get("BU")
    currency_col = col_map.get("CURRENCYCODE")
    user_type_col = col_map.get("USER_TYPE")
    amount_col = col_map.get("AMOUNT")

    if not bu_col or not currency_col or not user_type_col or not amount_col:
        missing = [
            key
            for key, col in {
                "BU": bu_col,
                "CURRENCYCODE": currency_col,
                "USER_TYPE": user_type_col,
                "AMOUNT": amount_col,
            }.items()
            if col is None
        ]
        raise ValueError(f"Missing required columns for APAC processing: {', '.join(missing)}")

    apac_rir_rows: list[dict[str, object]] = []
    apac_gaf_rows: list[dict[str, object]] = []
    emeaa_corp_rows: list[dict[str, object]] = []
    emeaa_noncorp_rows: list[dict[str, object]] = []
    amer_merged_rows: list[dict[str, object]] = []
    rir_apac_corp_rows: list[dict[str, object]] = []
    rir_gc_corp_rows: list[dict[str, object]] = []
    rir_noncorp_rows: list[dict[str, object]] = []

    for _, row in df.iterrows():
        bu = str(row.get(bu_col, "")).strip().upper()
        currency = str(row.get(currency_col, "")).strip().upper()
        user_type = str(row.get(user_type_col, "")).strip().upper()
        amount = _to_decimal(row.get(amount_col, "0"))

        row_dict = row.to_dict()

        if bu.startswith("H"):
            if user_type == "C":
                emeaa_corp_rows.append(row_dict)
            else:
                emeaa_noncorp_rows.append(row_dict)
            continue

        if bu.startswith("A"):
            amer_merged_rows.append(row_dict)
            continue

        if not bu.startswith("P"):
            continue

        if currency == "EUR":
            amount = amount / Decimal("0.86")
            currency = "USD"

        row_dict[amount_col] = float(amount)
        row_dict[currency_col] = currency

        if currency not in {"USD", "CNY"}:
            continue

        if amount > 0:
            apac_rir_rows.append(row_dict)
        elif amount < 0 and user_type != "C":
            apac_gaf_rows.append(row_dict)

    for row_dict in apac_rir_rows:
        bu = str(row_dict.get(bu_col, "")).strip().upper()
        user_type = str(row_dict.get(user_type_col, "")).strip().upper()

        region = ""
        if bu.startswith("P5"):
            region = "APAC"
        elif bu.startswith("P6"):
            region = "GC"

        if user_type == "C" and region == "APAC":
            rir_apac_corp_rows.append(row_dict)
        elif user_type == "C" and region == "GC":
            rir_gc_corp_rows.append(row_dict)
        elif user_type != "C":
            rir_noncorp_rows.append(row_dict)

    output_dir = Path(settings.output_dir) / "APAC" / "APAC_Output"
    output_dir.mkdir(parents=True, exist_ok=True)

    result: dict[str, str | int] = {
        "apac_rir_rows": len(apac_rir_rows),
        "apac_gaf_rows": len(apac_gaf_rows),
        "emeaa_corp_rows": len(emeaa_corp_rows),
        "emeaa_noncorp_rows": len(emeaa_noncorp_rows),
        "amer_merged_rows": len(amer_merged_rows),
        "rir_apac_corp_rows": len(rir_apac_corp_rows),
        "rir_gc_corp_rows": len(rir_gc_corp_rows),
        "rir_noncorp_rows": len(rir_noncorp_rows),
    }

    formatted_path = _generate_apac_gc_noncorp_formatted_output(
        output_dir=output_dir,
        rows=rir_noncorp_rows,
        source_columns=list(df.columns),
    )
    if formatted_path:
        result["apac_gc_noncorp_formatted_path"] = formatted_path
        result["apac_gc_noncorp_formatted_rows"] = len(rir_noncorp_rows)

    logger.info("Generated APAC processing outputs in %s", output_dir)
    return result
