"""Cleanup service for removing all flow output files."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from app.config.settings import settings

logger = logging.getLogger(__name__)


def cleanup_all_outputs() -> dict[str, object]:
    """
    Remove all flow output files and cleanup files from data/ folder.
    
    Cleans:
    - output/ folder: All files except templates
    - data/ folder: cleaned_no_red_*.xlsx files only (preserves original inputs)
    
    Preserves:
    - Template folders and their contents (Template_Format, Template_Formate)
    - Original input files in data/ folder
    - Folder structure itself (only deletes files)
    
    Returns:
        Dictionary with cleanup statistics:
        - status: success/error
        - files_deleted: count of files removed
        - folders_scanned: count of folders scanned
        - size_freed_mb: approximate space freed in MB
        - removed_paths: list of removed file paths (first 50)
        - locations_cleaned: list of folders that were cleaned
    """
    # Build folders to clean using dynamic base paths from settings
    data_base = Path(settings.upload_dir)
    output_base = Path(settings.output_dir)
    folders_to_clean = [
        data_base,
        data_base / "History_data" / "Crop",
        data_base / "History_data" / "NonCrop",
        data_base / "Monthly_data",
        data_base / "Post_validation_data",
        output_base,
        output_base / "AMER" / "AMER_Output",
        output_base / "AMER_Intercompny" / "Output",
        output_base / "APAC" / "APAC_GC_RIR" / "Output",
        output_base / "APAC" / "APAC_Intercompny" / "Output",
        output_base / "APAC" / "APAC_Output",
        output_base / "APAC" / "GAF_APAC_Processor" / "Output",
        output_base / "EMEAA" / "EMEAA_Intercompany" / "Output",
        output_base / "EMEAA" / "Output",
        output_base / "JRF" / "Output",
        output_base / "Monthly_cleaned_report",
        output_base / "Region_Wise_Split",
    ]

    files_deleted = 0
    folders_scanned = 0
    total_size_bytes = 0
    removed_paths = []
    locations_cleaned = []

    try:
        for folder in folders_to_clean:
            folder_path = Path(folder)
            if folder_path.exists() and folder_path.is_dir():
                locations_cleaned.append(str(folder_path))
                folders_scanned += 1
                for file in folder_path.iterdir():
                    if file.is_file():
                        try:
                            file_size = file.stat().st_size
                            total_size_bytes += file_size
                            file.unlink()
                            files_deleted += 1
                            if len(removed_paths) < 50:
                                removed_paths.append(str(file))
                            logger.debug("Deleted file: %s (%.2f KB)", file, file_size / 1024)
                        except OSError as e:
                            logger.warning("Failed to delete file %s: %s", file, e)
                            continue
            else:
                logger.warning("Folder does not exist: %s", folder_path)

        size_freed_mb = total_size_bytes / (1024 * 1024)
        logger.info(
            "Cleanup completed: deleted %d files, scanned %d folders, freed %.2f MB",
            files_deleted,
            folders_scanned,
            size_freed_mb,
        )
        return {
            "status": "success",
            "message": "Cleanup completed successfully (only specified folders cleaned)",
            "files_deleted": files_deleted,
            "folders_scanned": folders_scanned,
            "size_freed_mb": round(size_freed_mb, 2),
            "removed_paths": removed_paths,
            "locations_cleaned": locations_cleaned,
        }
    except Exception as exc:
        error_msg = f"Cleanup failed: {str(exc)}"
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


def cleanup_specific_folder(folder_name: str) -> dict[str, object]:
    """
    Remove files from a specific output subfolder.
    
    Args:
        folder_name: Name of the subfolder (e.g., 'AMER_Intercompny', 'APAC', 'EMEAA')
    
    Returns:
        Dictionary with cleanup statistics
    """
    output_dir = Path(settings.output_dir)
    target_folder = output_dir / folder_name
    
    if not target_folder.exists():
        logger.warning("Target folder does not exist: %s", target_folder)
        return {
            "status": "error",
            "message": f"Folder '{folder_name}' does not exist",
            "files_deleted": 0,
            "size_freed_mb": 0.0,
        }
    
    files_deleted = 0
    total_size_bytes = 0
    removed_paths = []
    
    try:
        # Recursively walk through the target folder
        for file_path in target_folder.rglob("*"):
            if file_path.is_file():
                # Skip template files
                if any(template_keyword in str(file_path).lower() for template_keyword in ["template", "format"]):
                    logger.debug("Skipping template file: %s", file_path)
                    continue
                
                try:
                    file_size = file_path.stat().st_size
                    total_size_bytes += file_size
                    file_path.unlink()
                    files_deleted += 1
                    
                    if len(removed_paths) < 20:
                        removed_paths.append(str(file_path.relative_to(output_dir)))
                    
                    logger.debug("Deleted file: %s", file_path)
                except OSError as e:
                    logger.warning("Failed to delete file %s: %s", file_path, e)
                    continue
        
        size_freed_mb = total_size_bytes / (1024 * 1024)
        
        logger.info(
            "Folder cleanup completed for '%s': deleted %d files, freed %.2f MB",
            folder_name,
            files_deleted,
            size_freed_mb,
        )
        
        return {
            "status": "success",
            "message": f"Cleanup completed for folder '{folder_name}'",
            "files_deleted": files_deleted,
            "size_freed_mb": round(size_freed_mb, 2),
            "removed_paths": removed_paths,
        }
    
    except Exception as exc:
        error_msg = f"Cleanup failed for folder '{folder_name}': {str(exc)}"
        logger.error(error_msg)
        return {
            "status": "error",
            "message": error_msg,
            "files_deleted": files_deleted,
            "size_freed_mb": round(total_size_bytes / (1024 * 1024), 2),
            "removed_paths": removed_paths,
        }
