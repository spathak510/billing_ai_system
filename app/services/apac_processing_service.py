from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from app.config.settings import settings

logger = logging.getLogger(__name__)


def _set_cell_value_safe(ws, cell_ref: str, value: object) -> None:
    cell = ws[cell_ref]
    if cell.__class__.__name__ != "MergedCell":
        cell.value = value
        return

    for merged_range in ws.merged_cells.ranges:
        if cell_ref in merged_range:
            ws.cell(row=merged_range.min_row, column=merged_range.min_col).value = value
            return

    ws[cell_ref].value = value


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


def _value_from_row(row_dict: dict[str, object], column_name: str) -> object:
    normalized_target = column_name.upper().replace(" ", "")
    for key, value in row_dict.items():
        if str(key).upper().replace(" ", "") == normalized_target:
            return value
    return ""


def _generate_rir_file_from_rows(
    template_path: Path,
    output_dir: Path,
    base_file_name: str,
    rows: list[dict[str, object]],
    request_date: str,
    request_name: str,
    account_number: str,
) -> str | None:
    if not rows:
        return None

    wb = load_workbook(template_path)
    if "RIR" not in wb.sheetnames or "BILLING LINES" not in wb.sheetnames:
        logger.warning("Template missing required sheets (RIR/BILLING LINES): %s", template_path)
        return None

    rir_sheet = wb["RIR"]
    billing_sheet = wb["BILLING LINES"]

    _set_cell_value_safe(rir_sheet, "C10", request_date)
    _set_cell_value_safe(rir_sheet, "F10", request_name)
    _set_cell_value_safe(rir_sheet, "F11", account_number)

    if billing_sheet.max_row > 1:
        billing_sheet.delete_rows(2, billing_sheet.max_row - 1)

    revenue_col = ""
    if rows:
        for key in rows[0].keys():
            if "REVEN" in str(key).upper().replace(" ", ""):
                revenue_col = str(key)
                break

    write_row = 2
    for row_dict in rows:
        billing_sheet.cell(write_row, 1).value = _value_from_row(row_dict, "USERNAME")
        billing_sheet.cell(write_row, 2).value = _value_from_row(row_dict, "EMPLOYEE")
        billing_sheet.cell(write_row, 3).value = _value_from_row(row_dict, "HOLIDEX")
        billing_sheet.cell(write_row, 4).value = _value_from_row(row_dict, "AMOUNT")
        billing_sheet.cell(write_row, 5).value = _value_from_row(row_dict, "CURRENCYCODE")
        billing_sheet.cell(write_row, 6).value = _value_from_row(row_dict, "COST_CENTER")
        billing_sheet.cell(write_row, 7).value = _value_from_row(row_dict, "ORDER_NO")
        billing_sheet.cell(write_row, 8).value = _value_from_row(row_dict, "COURSE_NAME")
        billing_sheet.cell(write_row, 9).value = _value_from_row(row_dict, "FACILITY")
        billing_sheet.cell(write_row, 10).value = _value_from_row(row_dict, "OFFERING_ID")
        billing_sheet.cell(write_row, 11).value = _value_from_row(row_dict, "INSTRUCTOR")
        billing_sheet.cell(write_row, 12).value = _value_from_row(row_dict, "OFFERING_DATE")
        billing_sheet.cell(write_row, 13).value = _value_from_row(row_dict, "COUNTRY")
        billing_sheet.cell(write_row, 14).value = _value_from_row(row_dict, "REGION")
        billing_sheet.cell(write_row, 15).value = _value_from_row(row_dict, "USER_TYPE")
        billing_sheet.cell(write_row, 16).value = _value_from_row(row_dict, "TRANSTYPECODE")
        billing_sheet.cell(write_row, 17).value = _value_from_row(row_dict, "PAY_DATE")
        billing_sheet.cell(write_row, 18).value = _value_from_row(row_dict, "NAME")
        billing_sheet.cell(write_row, 19).value = _value_from_row(row_dict, "DELIVERED_ON")
        billing_sheet.cell(write_row, 20).value = row_dict.get(revenue_col, "") if revenue_col else ""
        billing_sheet.cell(write_row, 21).value = _value_from_row(row_dict, "BU")
        write_row += 1

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"{base_file_name}_{timestamp}.xlsx"
    wb.save(output_path)
    logger.info("Generated RIR file: %s", output_path)
    return str(output_path)


def generate_rir_files_from_apac_output(
    request_date: str | None = None,
    request_name: str = "",
    account_number: str = "",
) -> dict[str, str | int]:
    """Generate RIR files using all APAC collection files from APAC_Output."""
    base_dir = Path(settings.output_dir) / "APAC"
    output_dir = base_dir / "APAC_Output"
    template_path = base_dir / "APAC_INP_FORMAT" / "APAC_GC_Intercompany billing lines_January26.xlsx"

    if not template_path.exists():
        raise FileNotFoundError(f"APAC template not found: {template_path}")

    collection_files = sorted(
        output_dir.glob("*_APAC_RIR.xlsx")
    ) + sorted(
        output_dir.glob("*_APAC_GAF.xlsx")
    ) + sorted(
        output_dir.glob("*_EMEAA_CORP.xlsx")
    ) + sorted(
        output_dir.glob("*_EMEAA_NONCORP.xlsx")
    ) + sorted(
        output_dir.glob("*_AMER_MERGED.xlsx")
    ) + sorted(
        output_dir.glob("*_RIR_APAC_CORP.xlsx")
    ) + sorted(
        output_dir.glob("*_RIR_GC_CORP.xlsx")
    ) + sorted(
        output_dir.glob("*_RIR_NONCORP.xlsx")
    )

    if not collection_files:
        raise FileNotFoundError("No APAC collection files found in APAC_Output.")

    generated: dict[str, str | int] = {"rir_files_generated": 0}
    request_date_value = request_date or datetime.now().strftime("%Y-%m-%d")

    for collection_file in collection_files:
        df = pd.read_excel(collection_file)
        rows = df.to_dict(orient="records")
        base_name = collection_file.stem

        generated_path = _generate_rir_file_from_rows(
            template_path=template_path,
            output_dir=output_dir,
            base_file_name=base_name,
            rows=rows,
            request_date=request_date_value,
            request_name=request_name,
            account_number=account_number,
        )

        if generated_path:
            generated[f"{base_name}_rir_path"] = generated_path
            generated["rir_files_generated"] = int(generated["rir_files_generated"]) + 1

    return generated


def generate_apac_processing_output(
    input_file_path: str | None = None,
    output_stem: str | None = None,
) -> dict[str, str | int]:
    """Process APAC rules from cleaned data and write all APAC collection outputs."""
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
    stem = _output_stem(source_path.stem, output_stem)

    collections: dict[str, list[dict[str, object]]] = {
        "APAC_RIR": apac_rir_rows,
        "APAC_GAF": apac_gaf_rows,
        "EMEAA_CORP": emeaa_corp_rows,
        "EMEAA_NONCORP": emeaa_noncorp_rows,
        "AMER_MERGED": amer_merged_rows,
        "RIR_APAC_CORP": rir_apac_corp_rows,
        "RIR_GC_CORP": rir_gc_corp_rows,
        "RIR_NONCORP": rir_noncorp_rows,
    }

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

    for name, rows in collections.items():
        collection_path = output_dir / f"{stem}_{name}.xlsx"
        pd.DataFrame(rows, columns=df.columns).to_excel(collection_path, index=False)
        result[f"{name.lower()}_path"] = str(collection_path)

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
