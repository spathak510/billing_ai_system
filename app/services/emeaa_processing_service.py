from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd

from app.config.settings import settings

logger = logging.getLogger(__name__)


def _resolve_input_path(input_file_path: str | None) -> Path:
    if input_file_path:
        return Path(input_file_path)

    region_split_dir = Path(settings.output_dir) / "Region_Wise_Split"
    region_split_emeaa_files = sorted(
        region_split_dir.glob("EMEAA_*.xlsx"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if region_split_emeaa_files:
        return region_split_emeaa_files[0]

    output_dir = Path(settings.output_dir)
    emeaa_files = sorted(output_dir.glob("EMEAA_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if emeaa_files:
        return emeaa_files[0]

    cleaned_files = sorted(output_dir.glob("cleaned_no_red_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if cleaned_files:
        return cleaned_files[0]

    raise FileNotFoundError("No EMEAA_*.xlsx or cleaned_no_red_*.xlsx file found in output folder.")


def _resolve_template_path(template_path: str | None) -> Path | None:
    if template_path:
        resolved = Path(template_path)
        if not resolved.exists():
            raise FileNotFoundError(f"Template file not found: {resolved}")
        return resolved

    template_dir = Path(settings.output_dir) / "EMEAA" / "Template_Formate"
    preferred_template = template_dir / "EMEAA_Intercompany billing lines_January26.xlsx"
    if preferred_template.exists():
        return preferred_template

    template_files = sorted(template_dir.glob("*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if template_files:
        return template_files[0]

    return None


def _to_decimal(value: object) -> Decimal:
    text = str(value).strip().replace(",", "")
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except InvalidOperation:
        return Decimal("0")


def generate_emeaa_processing_output(
    input_file_path: str | None = None,
    template_path: str | None = None,
    output_folder_path: str | None = None,
) -> dict[str, str | int]:
    """Run EMEAA V1/V2/GAF logic and generate three EMEAA collection outputs."""
    source_path = _resolve_input_path(input_file_path)
    resolved_template = _resolve_template_path(template_path)
    if resolved_template is None:
        logger.warning("EMEAA template not found; continuing collection generation without template.")
    df = pd.read_excel(source_path, sheet_name=0)

    # Match VBO behavior: ensure invoice columns exist in all output collections.
    if "INVOICE_NO" not in df.columns:
        df["INVOICE_NO"] = ""
    if "INVOICE_DATE" not in df.columns:
        df["INVOICE_DATE"] = ""

    col_map = {str(col).upper(): col for col in df.columns}
    bu_col = col_map.get("BU")
    currency_col = col_map.get("CURRENCYCODE")
    amount_col = col_map.get("AMOUNT")
    user_type_col = col_map.get("USER_TYPE")

    if not bu_col or not currency_col or not amount_col or not user_type_col:
        missing = [
            key
            for key, col in {
                "BU": bu_col,
                "CURRENCYCODE": currency_col,
                "AMOUNT": amount_col,
                "USER_TYPE": user_type_col,
            }.items()
            if col is None
        ]
        raise ValueError(f"Missing required columns for EMEAA processing: {', '.join(missing)}")

    emeaa_v1_rows: list[dict[str, object]] = []
    emeaa_v2_rows: list[dict[str, object]] = []
    emeaa_gaf_rows: list[dict[str, object]] = []
    corp_count = 0
    noncorp_count = 0

    for _, row in df.iterrows():
        bu = str(row.get(bu_col, "")).strip().upper()
        if not bu.startswith("H"):
            continue

        currency = str(row.get(currency_col, "")).strip().upper()
        amount = _to_decimal(row.get(amount_col, "0"))
        user_type = str(row.get(user_type_col, "")).strip().upper()

        if currency == "EUR":
            amount = amount / Decimal("0.86")
            currency = "USD"

        base_row = row.to_dict()
        base_row[amount_col] = float(amount)
        base_row[currency_col] = currency

        if user_type == "C":
            corp_count += 1
            emeaa_v1_rows.append(dict(base_row))

        if user_type in {"F", "H"}:
            noncorp_count += 1
            v2_row = dict(base_row)
            if amount >= 0:
                v2_row["INVOICE_NO"] = "N/A"
                v2_row["INVOICE_DATE"] = "N/A"
            emeaa_v2_rows.append(v2_row)

            if amount < 0:
                emeaa_gaf_rows.append(dict(base_row))

    output_dir = Path(output_folder_path) if output_folder_path else Path(settings.output_dir) / "EMEAA" / "Output"
    output_dir.mkdir(parents=True, exist_ok=True)

    emeaa_v1_path = output_dir / "EMEAA_V1.xlsx"
    emeaa_v2_path = output_dir / "EMEAA_V2.xlsx"
    emeaa_gaf_path = output_dir / "EMEAA_GAF.xlsx"

    pd.DataFrame(emeaa_v1_rows, columns=df.columns).to_excel(emeaa_v1_path, index=False)
    pd.DataFrame(emeaa_v2_rows, columns=df.columns).to_excel(emeaa_v2_path, index=False)
    pd.DataFrame(emeaa_gaf_rows, columns=df.columns).to_excel(emeaa_gaf_path, index=False)

    logger.info("Generated EMEAA collections: %s, %s, %s", emeaa_v1_path, emeaa_v2_path, emeaa_gaf_path)

    result: dict[str, str | int] = {
        "corp_count": corp_count,
        "noncorp_count": noncorp_count,
        "emeaa_v1_rows": len(emeaa_v1_rows),
        "emeaa_v2_rows": len(emeaa_v2_rows),
        "emeaa_gaf_rows": len(emeaa_gaf_rows),
        "emeaa_v1_path": str(emeaa_v1_path),
        "emeaa_v2_path": str(emeaa_v2_path),
        "emeaa_gaf_path": str(emeaa_gaf_path),
        "template_file": str(resolved_template) if resolved_template else "",
        "source_file": str(source_path),
    }

    return result
