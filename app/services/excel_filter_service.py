from __future__ import annotations

import logging
from pathlib import Path

from openpyxl import Workbook, load_workbook

from app.config.settings import settings
from app.services.apac_processing_service import (
    generate_apac_gc_intewrcompany_output,
    generate_apac_processing_output,
)
from app.services.amer_intercompany_service import generate_amer_intercompany_output
from app.services.emeaa_intercompany_service import generate_emeaa_intercompany_output
from app.services.emeaa_processing_service import generate_emeaa_processing_output
from app.services.gaf_apac_processor_service import generate_gaf_apac_output
from app.services.rir_apac_processor_service import generate_rir_apac_output
from app.services.jrf_processor_service import generate_jrf_output
from app.services.peoplesoft_output_service import generate_amer_peoplesoft_output

logger = logging.getLogger(__name__)


def _ensure_expected_output_dirs() -> None:
    output_root = Path(settings.output_dir)
    expected_dirs = [
        output_root / "Region_Wise_Split",
        output_root / "AMER" / "AMER_Output",
        output_root / "AMER_Intercompny" / "Output",
        output_root / "APAC" / "APAC_Output",
        output_root / "APAC" / "APAC_Intercompny" / "Output",
        output_root / "APAC" / "GAF_APAC_Processor" / "Output",
        output_root / "APAC" / "APAC_GC_RIR" / "Output",
        output_root / "EMEAA" / "Output",
        output_root / "EMEAA" / "EMEAA_Intercompany" / "Output",
        output_root / "JRF",
    ]
    for directory in expected_dirs:
        directory.mkdir(parents=True, exist_ok=True)


def _find_column(header_row: list[object], column_name: str) -> int | None:
    for idx, value in enumerate(header_row):
        if str(value).strip().upper() == column_name:
            return idx
    return None


def _split_user_type_collections(cleaned_workbook: Workbook, source_stem: str) -> None:
    output_dir = Path(settings.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    corp_workbook = Workbook()
    non_corp_workbook = Workbook()
    corp_workbook.remove(corp_workbook.active)
    non_corp_workbook.remove(non_corp_workbook.active)

    for cleaned_sheet in cleaned_workbook.worksheets:
        corp_sheet = corp_workbook.create_sheet(title=cleaned_sheet.title)
        non_corp_sheet = non_corp_workbook.create_sheet(title=cleaned_sheet.title)

        rows = list(
            cleaned_sheet.iter_rows(
                min_row=1,
                max_row=cleaned_sheet.max_row,
                max_col=cleaned_sheet.max_column,
                values_only=True,
            )
        )

        if not rows:
            continue

        header = list(rows[0])
        corp_sheet.append(header)
        non_corp_sheet.append(header)

        user_type_index = _find_column(header, "USER_TYPE")

        for row in rows[1:]:
            row_values = list(row)
            user_type = ""
            if user_type_index is not None and user_type_index < len(row_values):
                user_type = str(row_values[user_type_index]).strip().upper()

            if user_type == "C":
                corp_sheet.append(row_values)
            else:
                non_corp_sheet.append(row_values)

    corp_path = _next_available_path(output_dir / f"CorpCollection_{source_stem}.xlsx")
    non_corp_path = _next_available_path(output_dir / f"NonCorpCollection_{source_stem}.xlsx")
    corp_workbook.save(corp_path)
    non_corp_workbook.save(non_corp_path)

    logger.info("Created CorpCollection file: %s", corp_path)
    logger.info("Created NonCorpCollection file: %s", non_corp_path)


def _split_region_collections(cleaned_workbook: Workbook, source_stem: str) -> dict[str, str]:
    output_dir = Path(settings.output_dir) / "Region_Wise_Split"
    output_dir.mkdir(parents=True, exist_ok=True)

    region_workbooks = {
        "AMER": Workbook(),
        "APAC": Workbook(),
        "GC": Workbook(),
        "APAC_GC": Workbook(),
        "EMEAA": Workbook(),
    }

    for workbook in region_workbooks.values():
        workbook.remove(workbook.active)

    for cleaned_sheet in cleaned_workbook.worksheets:
        region_sheets = {
            name: workbook.create_sheet(title=cleaned_sheet.title)
            for name, workbook in region_workbooks.items()
        }

        rows = list(
            cleaned_sheet.iter_rows(
                min_row=1,
                max_row=cleaned_sheet.max_row,
                max_col=cleaned_sheet.max_column,
                values_only=True,
            )
        )

        if not rows:
            continue

        header = list(rows[0])
        for sheet in region_sheets.values():
            sheet.append(header)

        bu_index = _find_column(header, "BU")
        region_index = _find_column(header, "REGION")

        for row in rows[1:]:
            row_values = list(row)
            bu = ""
            region = ""

            if bu_index is not None and bu_index < len(row_values):
                bu = str(row_values[bu_index]).strip().upper()
            if region_index is not None and region_index < len(row_values):
                region = str(row_values[region_index]).strip().upper()

            # AMER: BU starts with A
            if bu.startswith("A"):
                region_sheets["AMER"].append(row_values)
            # APAC_GC (combined) + split to APAC & GC: BU starts with P
            elif bu.startswith("P"):
                region_sheets["APAC_GC"].append(row_values)
                if region == "GC":
                    region_sheets["GC"].append(row_values)
                else:
                    region_sheets["APAC"].append(row_values)
            # EMEAA: BU starts with H
            elif bu.startswith("H"):
                region_sheets["EMEAA"].append(row_values)

    output_paths: dict[str, str] = {}
    for region_name, workbook in region_workbooks.items():
        output_path = _next_available_path(output_dir / f"{region_name}_{source_stem}.xlsx")
        workbook.save(output_path)
        output_paths[region_name] = str(output_path)
        logger.info("Created %s region file: %s", region_name, output_path)

    return output_paths


def _split_intercompany_collections(cleaned_workbook: Workbook, source_stem: str) -> None:
    output_dir = Path(settings.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    intercompany_workbook = Workbook()
    normal_workbook = Workbook()
    intercompany_workbook.remove(intercompany_workbook.active)
    normal_workbook.remove(normal_workbook.active)

    for cleaned_sheet in cleaned_workbook.worksheets:
        intercompany_sheet = intercompany_workbook.create_sheet(title=cleaned_sheet.title)
        normal_sheet = normal_workbook.create_sheet(title=cleaned_sheet.title)

        rows = list(
            cleaned_sheet.iter_rows(
                min_row=1,
                max_row=cleaned_sheet.max_row,
                max_col=cleaned_sheet.max_column,
                values_only=True,
            )
        )

        if not rows:
            continue

        header = list(rows[0])
        intercompany_sheet.append(header)
        normal_sheet.append(header)

        bu_index = _find_column(header, "BU")

        for row in rows[1:]:
            row_values = list(row)
            bu = ""

            if bu_index is not None and bu_index < len(row_values):
                bu = str(row_values[bu_index]).strip().upper()

            if bu.startswith("H") or bu.startswith("A"):
                intercompany_sheet.append(row_values)
            else:
                normal_sheet.append(row_values)

    intercompany_path = _next_available_path(output_dir / f"Intercompany_{source_stem}.xlsx")
    normal_path = _next_available_path(output_dir / f"Normal_{source_stem}.xlsx")
    intercompany_workbook.save(intercompany_path)
    normal_workbook.save(normal_path)

    logger.info("Created Intercompany file: %s", intercompany_path)
    logger.info("Created Normal file: %s", normal_path)


def _next_available_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    index = 1
    while True:
        candidate = path.parent / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def _is_red_fill(cell) -> bool:
    fill = getattr(cell, "fill", None)
    if not fill or fill.fill_type is None:
        return False

    color = fill.start_color
    if not color:
        return False

    # RGB colors (most common in modern .xlsx files)
    if color.type == "rgb" and color.rgb:
        rgb = color.rgb.upper()
        return rgb in {"FFFF0000", "FF0000", "00FF0000"}

    # Indexed palette red (legacy formats/styles)
    if color.type == "indexed" and color.indexed is not None:
        return int(color.indexed) == 10

    return False


def remove_red_rows_from_excel(
    input_file_path: str,
    output_dir: str | None = None,
) -> str:
    """Create a new Excel file excluding rows with red-filled cells."""
    _ensure_expected_output_dirs()

    source_path = Path(input_file_path)
    target_dir = Path(output_dir or settings.upload_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    input_workbook = load_workbook(source_path)
    output_workbook = Workbook()
    output_workbook.remove(output_workbook.active)

    for input_sheet in input_workbook.worksheets:
        output_sheet = output_workbook.create_sheet(title=input_sheet.title)

        for row in input_sheet.iter_rows(
            min_row=1,
            max_row=input_sheet.max_row,
            max_col=input_sheet.max_column,
        ):
            # Keep header row as-is.
            if row[0].row == 1:
                output_sheet.append([cell.value for cell in row])
                continue

            if not any(_is_red_fill(cell) for cell in row):
                output_sheet.append([cell.value for cell in row])

    output_path = target_dir / f"cleaned_no_red_{source_path.stem}.xlsx"
    output_path = _next_available_path(output_path)
    output_workbook.save(output_path)

    _split_user_type_collections(cleaned_workbook=output_workbook, source_stem=source_path.stem)
    region_output_paths = _split_region_collections(cleaned_workbook=output_workbook, source_stem=source_path.stem)
    _split_intercompany_collections(cleaned_workbook=output_workbook, source_stem=source_path.stem)

    try:
        generate_amer_peoplesoft_output(input_file_path=str(output_path))
    except Exception as exc:
        logger.warning("Failed AMER PeopleSoft generation for cleaned file %s: %s", output_path, exc)

    try:
        amer_file = region_output_paths.get("AMER")
        if amer_file:
            generate_amer_intercompany_output(input_file_path=amer_file)
        else:
            logger.warning("AMER region file not found in region split output")
    except Exception as exc:
        logger.warning("Failed AMER Intercompany generation for cleaned file %s: %s", output_path, exc)

    apac_processing_result: dict[str, str | int] | None = None
    try:
        apac_processing_result = generate_apac_processing_output(input_file_path=str(output_path))
    except Exception as exc:
        logger.warning("Failed APAC processing generation for cleaned file %s: %s", output_path, exc)

    try:
        apac_gc_noncorp_file = (
            str(apac_processing_result["apac_gc_noncrop_path"])
            if apac_processing_result and apac_processing_result.get("apac_gc_noncrop_path")
            else None
        )
        if apac_gc_noncorp_file:
            generate_apac_gc_intewrcompany_output(input_file_path=apac_gc_noncorp_file)
        else:
            logger.warning("APAC GC NONCROP collection not found in APAC processing output")
    except Exception as exc:
        logger.warning("Failed APAC GC Intercompany generation for cleaned file %s: %s", output_path, exc)

    try:
        rir_noncrop_file = (
            str(apac_processing_result["rir_noncrop_path"])
            if apac_processing_result and apac_processing_result.get("rir_noncrop_path")
            else None
        )
        if rir_noncrop_file:
            generate_gaf_apac_output(input_file_path=rir_noncrop_file)
        else:
            logger.warning("RIR NONCROP collection not found in APAC processing output")
    except Exception as exc:
        logger.warning("Failed GAF APAC generation for cleaned file %s: %s", output_path, exc)

    try:
        rir_noncrop_file = (
            str(apac_processing_result["rir_noncrop_path"])
            if apac_processing_result and apac_processing_result.get("rir_noncrop_path")
            else None
        )
        if rir_noncrop_file:
            generate_rir_apac_output(input_file_path=rir_noncrop_file)
        else:
            logger.warning("RIR NONCROP collection not found in APAC processing output")
    except Exception as exc:
        logger.warning("Failed APAC_GC_RIR generation for cleaned file %s: %s", output_path, exc)

    emeaa_processing_result: dict[str, str | int] | None = None
    try:
        emeaa_file = region_output_paths.get("EMEAA")
        if emeaa_file:
            emeaa_processing_result = generate_emeaa_processing_output(input_file_path=emeaa_file)
        else:
            logger.warning("EMEAA region file not found in region split output")
    except Exception as exc:
        logger.warning("Failed EMEAA processing generation for cleaned file %s: %s", output_path, exc)

    try:
        emeaa_v2_file = (
            str(emeaa_processing_result["emeaa_v2_path"])
            if emeaa_processing_result and emeaa_processing_result.get("emeaa_v2_path")
            else None
        )
        if emeaa_v2_file:
            generate_emeaa_intercompany_output(input_file_path=emeaa_v2_file)
        else:
            logger.warning("EMEAA_V2 collection not found in EMEAA processing output")
    except Exception as exc:
        logger.warning("Failed EMEAA Intercompany generation for cleaned file %s: %s", output_path, exc)

    try:
        generate_jrf_output(input_file_path=str(output_path))
    except Exception as exc:
        logger.warning("Failed JRF generation for cleaned file %s: %s", output_path, exc)

    logger.info("Created cleaned Excel file without red rows: %s", output_path)
    return str(output_path)