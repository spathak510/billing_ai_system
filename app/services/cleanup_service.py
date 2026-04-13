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
    output_dir = Path(settings.output_dir)
    data_dir = Path(settings.upload_dir)
    
    files_deleted = 0
    folders_scanned = 0
    total_size_bytes = 0
    removed_paths = []
    locations_cleaned = []
    
    try:
        # Clean output/ directory - remove all files except templates
        if output_dir.exists():
            locations_cleaned.append(str(output_dir))
            for folder_path in output_dir.rglob("*"):
                if folder_path.is_dir():
                    folders_scanned += 1
                elif folder_path.is_file():
                    # Skip template files - preserve template folders
                    if "template" in str(folder_path).lower():
                        logger.debug("Skipping template file: %s", folder_path)
                        continue
                    
                    try:
                        file_size = folder_path.stat().st_size
                        total_size_bytes += file_size
                        folder_path.unlink()
                        files_deleted += 1
                        
                        if len(removed_paths) < 50:
                            removed_paths.append(f"output/{folder_path.relative_to(output_dir)}")
                        
                        logger.debug("Deleted file: %s (%.2f KB)", folder_path, file_size / 1024)
                    except OSError as e:
                        logger.warning("Failed to delete file %s: %s", folder_path, e)
                        continue
        else:
            logger.warning("Output directory does not exist: %s", output_dir)
        
        # Clean data/ directory - remove only cleaned_no_red_*.xlsx files
        if data_dir.exists():
            locations_cleaned.append(str(data_dir))
            for file_path in data_dir.glob("cleaned_no_red_*.xlsx"):
                if file_path.is_file():
                    try:
                        file_size = file_path.stat().st_size
                        total_size_bytes += file_size
                        file_path.unlink()
                        files_deleted += 1
                        
                        if len(removed_paths) < 50:
                            removed_paths.append(f"data/{file_path.name}")
                        
                        logger.debug("Deleted file: %s (%.2f KB)", file_path, file_size / 1024)
                    except OSError as e:
                        logger.warning("Failed to delete file %s: %s", file_path, e)
                        continue
        else:
            logger.warning("Data directory does not exist: %s", data_dir)
        
        size_freed_mb = total_size_bytes / (1024 * 1024)
        
        logger.info(
            "Cleanup completed: deleted %d files, scanned %d folders, freed %.2f MB",
            files_deleted,
            folders_scanned,
            size_freed_mb,
        )
        
        return {
            "status": "success",
            "message": "Cleanup completed successfully (output/ and data/cleaned files removed)",
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
                if "template" in str(file_path).lower():
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
