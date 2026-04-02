from __future__ import annotations

import logging
from pathlib import Path

from openpyxl import Workbook, load_workbook

from app.config.settings import settings

logger = logging.getLogger(__name__)


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
    logger.info("Created cleaned Excel file without red rows: %s", output_path)
    return str(output_path)