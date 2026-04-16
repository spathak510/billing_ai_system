from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from app.config.settings import settings

logger = logging.getLogger(__name__)


def _resolve_input_path(input_file_path: str | None) -> Path:
    if input_file_path:
        return Path(input_file_path)

    output_dir = Path(settings.output_dir)
    cleaned_files = sorted(output_dir.glob("cleaned_no_red_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if cleaned_files:
        return cleaned_files[0]

    amer_files = sorted(output_dir.glob("AMER_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if amer_files:
        return amer_files[0]

    raise FileNotFoundError("No cleaned_no_red_*.xlsx or AMER_*.xlsx file found in output folder.")


def _date_suffix() -> str:
    """Return today's date as mmddyyyy string, e.g. 04132026."""
    return date.today().strftime("%m%d%Y")


def _filter_amer_rows(df: pd.DataFrame) -> pd.DataFrame:
    col_map = {str(col).upper(): col for col in df.columns}
    bu_col = col_map.get("BU")
    region_col = col_map.get("REGION")

    if region_col:
        region_series = df[region_col].fillna("").astype(str).str.strip().str.upper()
        return df[region_series.eq("AMER")].copy()

    if bu_col:
        bu_series = df[bu_col].fillna("").astype(str).str.strip().str.upper()
        return df[bu_series.str.startswith("A")].copy()

    return df.copy()


def generate_amer_peoplesoft_output(
    input_file_path: str | None = None,
    output_stem: str | None = None,
) -> dict[str, str | int]:
    """
    Process AMER data and generate PeopleSoft format CSV files (CORP and NONCORP).

    Expected input columns (flexible mapping):
    - OWNER_ID or EMPLOYEE → owner_id
    - ID or ORDER_NO → id_value
    - HOLIDEX → holidex
    - TRANSTYPECODE → class_type
    - AMOUNT → amount
    - USER_TYPE → user_type (for CORP/NON-CORP split)

    Output format:
    - CORP: idValue|ownerId||amount
    - NON-CORP: idValue|ownerId|holidex|amount
    """
    source_path = _resolve_input_path(input_file_path)

    # Read Excel file
    try:
        df = pd.read_excel(source_path, sheet_name=0)
    except Exception as exc:
        logger.error("Failed to read Excel file %s: %s", source_path, exc)
        raise

    df = _filter_amer_rows(df)

    # Only generate output if there is at least one data row
    if df.empty:
        logger.info("No data found for AMER PeopleSoft output; skipping file generation.")
        return {}

    # Ensure output directory exists under the current AMER folder structure.
    output_dir = Path(settings.output_dir) / "AMER" / "AMER_Output"
    output_dir.mkdir(parents=True, exist_ok=True)

    date_str = _date_suffix()
    corp_csv_path = output_dir / f"CORP_BILLING_{date_str}.csv"
    noncorp_csv_path = output_dir / f"NONCORP_BILLING_{date_str}.csv"
    combined_csv_path = output_dir / f"NONCORP&CORP_BILLING_{date_str}.csv"

    # Open CSV writers
    corp_writer = open(corp_csv_path, "w", encoding="utf-8")
    noncorp_writer = open(noncorp_csv_path, "w", encoding="utf-8")
    combined_writer = open(combined_csv_path, "w", encoding="utf-8")

    try:
        corp_count = 0
        noncorp_count = 0
        combined_count = 0

        # Write headers
        corp_writer.write("HDR|08|2019\n")
        noncorp_writer.write("HDR|08|2019\n")
        combined_writer.write("HDR|08|2019\n")

        # Normalize column names (case-insensitive lookup)
        col_map = {col.upper(): col for col in df.columns}

        owner_id_col = col_map.get("OWNER_ID") or col_map.get("EMPLOYEE")
        id_col = col_map.get("ID") or col_map.get("ORDER_NO")
        holidex_col = col_map.get("HOLIDEX")
        class_type_col = col_map.get("TRANSTYPECODE")
        amount_col = col_map.get("AMOUNT")
        user_type_col = col_map.get("USER_TYPE")

        # Process rows
        for _, row in df.iterrows():
            owner_id = ""
            holidex = ""
            class_type = ""
            id_value = ""
            amount = ""
            user_type = ""

            # Extract OWNER_ID
            if owner_id_col:
                owner_id = str(row.get(owner_id_col, "")).strip()

            # Extract ID
            if id_col:
                id_value = str(row.get(id_col, "")).strip()

            if not id_value.lower().startswith("intor"):
                id_value = "intor00000000" + id_value

            # Extract HOLIDEX
            if holidex_col:
                holidex = str(row.get(holidex_col, "")).strip()

            # Extract CLASS TYPE
            if class_type_col:
                class_type = str(row.get(class_type_col, "")).strip()

            # Extract AMOUNT
            if amount_col:
                amount = str(row.get(amount_col, "")).strip()

            # Extract USER_TYPE
            if user_type_col:
                user_type = str(row.get(user_type_col, "")).strip().upper()

            # Apply prefix logic based on class type
            class_type_upper = class_type.upper()
            if "VIRTUAL" in class_type_upper or "INSTRUCTOR" in class_type_upper:
                owner_id = "ioreg00000000" + owner_id
            elif "WEB" in class_type_upper:
                owner_id = "iodwn00000000" + owner_id

            # Truncate owner_id to max 20 chars
            if len(owner_id) > 20:
                owner_id = owner_id.replace("0", "")
                if len(owner_id) > 20:
                    owner_id = owner_id[:20]

            # Split by USER_TYPE
            if user_type == "C":
                # CORP: idValue|ownerId||amount
                line = f"{id_value}|{owner_id}||{amount}\n"
                corp_writer.write(line)
                combined_writer.write(line)
                corp_count += 1
                combined_count += 1

            elif user_type in {"F", "H"}:
                # NON-CORP: idValue|ownerId|holidex|amount
                # Only write if HOLIDEX length is exactly 5
                if len(holidex) == 5:
                    line = f"{id_value}|{owner_id}|{holidex}|{amount}\n"
                    noncorp_writer.write(line)
                    combined_writer.write(line)
                    noncorp_count += 1
                    combined_count += 1

        # Write trailers
        corp_writer.write(f"TRL|{corp_count:010d}\n")
        noncorp_writer.write(f"TRL|{noncorp_count:010d}\n")
        combined_writer.write(f"TRL|{combined_count:010d}\n")

        logger.info(
            "Generated PeopleSoft CORP output: %s (rows=%d)",
            corp_csv_path,
            corp_count,
        )
        logger.info(
            "Generated PeopleSoft NON-CORP output: %s (rows=%d)",
            noncorp_csv_path,
            noncorp_count,
        )
        logger.info(
            "Generated PeopleSoft COMBINED output: %s (rows=%d)",
            combined_csv_path,
            combined_count,
        )

        return {
            "corp_file": str(corp_csv_path),
            "noncorp_file": str(noncorp_csv_path),
            "combined_file": str(combined_csv_path),
            "corp_count": corp_count,
            "noncorp_count": noncorp_count,
            "combined_count": combined_count,
        }

    finally:
        corp_writer.close()
        noncorp_writer.close()
        combined_writer.close()
