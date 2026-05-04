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

    if df.empty:
        logger.info("No data found for AMER PeopleSoft output; skipping file generation.")
        return {}

    # Output folder logic (ensure trailing slash, create if missing)
    output_dir = Path(settings.output_dir) / "AMER" / "AMER_Output"
    output_dir.mkdir(parents=True, exist_ok=True)

    date_str = _date_suffix()
    corp_file_name = f"CORP_BILLING_{date_str}.csv"
    noncorp_file_name = f"NONCORP_BILLING_{date_str}.csv"
    combined_file_name = f"NONCORP&CORP_BILLING_{date_str}.csv"
    corp_csv_path = output_dir / corp_file_name
    noncorp_csv_path = output_dir / noncorp_file_name
    combined_csv_path = output_dir / combined_file_name

    # Open writers
    corp_writer = open(corp_csv_path, "w", encoding="utf-8")
    noncorp_writer = open(noncorp_csv_path, "w", encoding="utf-8")
    combined_writer = open(combined_csv_path, "w", encoding="utf-8")

    try:
        corp_count = 0
        noncorp_count = 0
        combined_count = 0

        # Header: HDR|MM|YYYY
        from datetime import datetime
        now = datetime.now()
        header = f"HDR|{now.strftime('%m')}|{now.strftime('%Y')}"
        corp_writer.write(header + "\n")
        noncorp_writer.write(header + "\n")
        combined_writer.write(header + "\n")

        # Normalize column names (case-insensitive lookup)
        col_map = {col.upper(): col for col in df.columns}
        owner_id_col = col_map.get("OWNER_ID") or col_map.get("EMPLOYEE")
        id_col = col_map.get("ID") or col_map.get("ORDER_NO")
        holidex_col = col_map.get("HOLIDEX")
        amount_col = col_map.get("AMOUNT")
        user_type_col = col_map.get("USER_TYPE")
        description_col = col_map.get("COURSE_NAME")
        trans_date_col = col_map.get("PAY_DATE")
        currency_col = col_map.get("CURRENCYCODE")
        country_col = col_map.get("COUNTRY")

        for _, row in df.iterrows():
            owner_id = str(row.get(owner_id_col, "")).strip() if owner_id_col else ""
            id_value = str(row.get(id_col, "")).strip() if id_col else ""
            holidex = str(row.get(holidex_col, "")).strip() if holidex_col else ""
            amount = str(row.get(amount_col, "")).strip() if amount_col else ""
            user_type = str(row.get(user_type_col, "")).strip().upper() if user_type_col else ""
            description = str(row.get(description_col, "")).strip() if description_col else ""
            trans_date = str(row.get(trans_date_col, "")).strip() if trans_date_col else ""
            currency = str(row.get(currency_col, "")).strip() if currency_col else ""
            country = str(row.get(country_col, "")).strip() if country_col else ""

            # Defaults/fixes
            if not currency:
                currency = "USD"

            # EUR → USD conversion
            if currency.upper() == "EUR":
                try:
                    amt = float(amount)
                    amount = f"{amt / 0.86:.2f}"
                    currency = "USD"
                except Exception:
                    pass

            if not id_value.lower().startswith("intor"):
                id_value = "intor00000000" + id_value

            if len(owner_id) > 20:
                owner_id = owner_id[:20]

            # Date format
            from datetime import datetime
            try:
                if trans_date:
                    dt = pd.to_datetime(trans_date, errors="coerce")
                    if pd.notnull(dt):
                        trans_date = dt.strftime("%m/%d/%Y")
            except Exception:
                pass

            # CORPORATE (C)
            if user_type == "C":
                line = (
                    f"DTL|"
                    f"{owner_id}|"
                    f"{id_value}||"
                    f"{trans_date}|"
                    f"Training||83|"
                    f"{currency}||"
                    f"{amount}|"
                    f"{description}"
                )
                corp_writer.write(line + "\n")
                combined_writer.write(line + "\n")
                corp_count += 1
                combined_count += 1

            # NON-CORPORATE (F/H)
            elif user_type in ("F", "H"):
                if len(holidex) != 5:
                    continue
                line = (
                    f"DTL|"
                    f"{owner_id}|"
                    f"{id_value}||"
                    f"{trans_date}|"
                    f"Training|"
                    f"{holidex}|83|"
                    f"{currency}|"
                    f"{country}|"
                    f"{amount}|"
                    f"{description}"
                )
                noncorp_writer.write(line + "\n")
                combined_writer.write(line + "\n")
                noncorp_count += 1
                combined_count += 1

        # Trailer
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
