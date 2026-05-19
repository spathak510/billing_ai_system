"""Cleanup service for removing all flow output files."""

from __future__ import annotations

import logging
from pathlib import Path

from app.config.settings import settings

logger = logging.getLogger(__name__)

_TEMPLATE_DIR_NAMES = {"template_format", "template_formate", "template_formatted", "template"}
_TEMPLATE_KEYWORDS = ("template", "format")


def _is_template_path(path: Path) -> bool:
    return any(part.lower() in _TEMPLATE_DIR_NAMES for part in path.parts)


def _delete_matching_files(
    base_dir: Path,
    *,
    recursive: bool = True,
    delete_all_files: bool = True,
    allowed_patterns: tuple[str, ...] = (),
    removed_paths_limit: int = 50,
) -> tuple[int, int, list[str]]:
    files_deleted = 0
    total_size_bytes = 0
    removed_paths: list[str] = []

    if not base_dir.exists() or not base_dir.is_dir():
        logger.warning("Folder does not exist: %s", base_dir)
        return files_deleted, total_size_bytes, removed_paths

    iterator = base_dir.rglob("*") if recursive else base_dir.iterdir()

    for file_path in iterator:
        if not file_path.is_file():
            continue

        if _is_template_path(file_path):
            logger.debug("Skipping template file: %s", file_path)
            continue

        if not delete_all_files:
            if not any(file_path.match(pattern) for pattern in allowed_patterns):
                continue

        try:
            total_size_bytes += file_path.stat().st_size
            file_path.unlink()
            files_deleted += 1
            if len(removed_paths) < removed_paths_limit:
                removed_paths.append(str(file_path))
            logger.debug("Deleted file: %s", file_path)
        except OSError as exc:
            logger.warning("Failed to delete file %s: %s", file_path, exc)

    return files_deleted, total_size_bytes, removed_paths


def cleanup_all_outputs() -> dict[str, object]:
    """
    Remove generated output files while preserving templates and folder structure.

    Cleans:
    - output/: generated files recursively
    - data/: generated post-validation and transient files only

    Preserves:
    - template folders and their contents
    - original input folders and folder structure
    """
    data_base = Path(settings.upload_dir)
    output_base = Path(settings.output_dir)

    data_folders_to_clean: list[tuple[Path, tuple[str, ...]]] = [
        (data_base, ("cleaned_no_red_*.xlsx",)),
        (data_base / "Post_validation_data", ("*.xlsx", "*.xlsm", "*.xltx", "*.xltm")),
        (data_base / "Monthly_data", ("*.xlsx", "*.xlsm", "*.csv")),
        (data_base / "Manual_entry", ("*.xlsx", "*.xlsm", "*.csv")),
    ]

    output_folders_to_clean = [
        output_base,
        output_base / "AMER" / "AMER_Output",
        output_base / "AMER_Intercompny" / "Output",
        output_base / "AMER_Intercompny" / "Input",
        output_base / "APAC" / "APAC_GC_RIR" / "Output",
        output_base / "APAC" / "APAC_Intercompny" / "Output",
        output_base / "APAC" / "APAC_Intercompny" / "Input",
        output_base / "APAC" / "APAC_Output",
        output_base / "APAC" / "GAF_APAC_Processor" / "Output",
        output_base / "EMEAA" / "EMEAA_Intercompany" / "Output",
        output_base / "EMEAA" / "EMEAA_Intercompany" / "Input",
        output_base / "EMEAA" / "Output",
        output_base / "JRF" / "Output",
        output_base / "Monthly_cleaned_report",
        output_base / "Corp_NonCorp_Split",
        output_base / "Region_Wise_Split",
    ]

    files_deleted = 0
    folders_scanned = 0
    total_size_bytes = 0
    removed_paths: list[str] = []
    locations_cleaned: list[str] = []

    try:
        for folder in output_folders_to_clean:
            if folder.exists() and folder.is_dir():
                folders_scanned += 1
                locations_cleaned.append(str(folder))
                deleted, size_bytes, removed = _delete_matching_files(folder, recursive=True, delete_all_files=True)
                files_deleted += deleted
                total_size_bytes += size_bytes
                remaining_slots = max(0, 50 - len(removed_paths))
                removed_paths.extend(removed[:remaining_slots])

        for folder, patterns in data_folders_to_clean:
            if folder.exists() and folder.is_dir():
                folders_scanned += 1
                locations_cleaned.append(str(folder))
                deleted, size_bytes, removed = _delete_matching_files(
                    folder,
                    recursive=False,
                    delete_all_files=False,
                    allowed_patterns=patterns,
                )
                files_deleted += deleted
                total_size_bytes += size_bytes
                remaining_slots = max(0, 50 - len(removed_paths))
                removed_paths.extend(removed[:remaining_slots])

        size_freed_mb = total_size_bytes / (1024 * 1024)

        logger.info(
            "Cleanup completed: deleted %d files, scanned %d folders, freed %.2f MB",
            files_deleted,
            folders_scanned,
            size_freed_mb,
        )
        return {
            "status": "success",
            "message": "Cleanup completed successfully",
            "files_deleted": files_deleted,
            "folders_scanned": folders_scanned,
            "size_freed_mb": round(size_freed_mb, 2),
            "removed_paths": removed_paths,
            "locations_cleaned": locations_cleaned,
        }
    except Exception as exc:
        error_msg = f"Cleanup failed: {exc}"
        logger.error(error_msg)
        return {
            "status": "error",
            "message": error_msg,
            "files_deleted": files_deleted,
            "folders_scanned": folders_scanned,
            "size_freed_mb": round(total_size_bytes / (1024 * 1024), 2),
            "removed_paths": removed_paths,
            "locations_cleaned": locations_cleaned,
        }