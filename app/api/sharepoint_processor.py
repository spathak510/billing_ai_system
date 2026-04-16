"""Billing file upload and report download API endpoints."""

from __future__ import annotations

from datetime import datetime
import logging
import os

from flask import request

from app.services.sharepoint_download_service import SharePointDownloadClient
from app.services.sharepoint_upload_service import SharePointUploadClient
from app.config.settings import settings


logger = logging.getLogger(__name__)

# Lazy-initialized SharePoint clients to avoid 401 errors at module import time
_sharepoint_download_client: SharePointDownloadClient | None = None
_sharepoint_upload_client: SharePointUploadClient | None = None

def _get_sharepoint_download_client() -> SharePointDownloadClient:
    """Get or create SharePoint download client (lazy initialization)."""
    global _sharepoint_download_client
    if _sharepoint_download_client is None:
        _sharepoint_download_client = SharePointDownloadClient()
    return _sharepoint_download_client

def _get_sharepoint_upload_client() -> SharePointUploadClient:
    """Get or create SharePoint upload client (lazy initialization)."""
    global _sharepoint_upload_client
    if _sharepoint_upload_client is None:
        _sharepoint_upload_client = SharePointUploadClient()
    return _sharepoint_upload_client

def download_file_from_sharepoint(remote_path: str, local_dir: str) -> str:
    """Download a file from SharePoint to a local directory.

    Args:
        remote_path: The path to the file on SharePoint (relative to the configured root).
        local_dir: The local directory to save the downloaded file. Must exist. The file will be saved as local_dir/filename.ext.   
    Returns:
        The full local path to the downloaded file.
    """
    client = _get_sharepoint_download_client()
    downloaded_files = client.download_all_files(remote_path, local_dir)
    if not downloaded_files:
        raise FileNotFoundError(f"No files found at SharePoint path: {remote_path}")
    if len(downloaded_files) > 1:
        raise ValueError(f"Multiple files found at SharePoint path: {remote_path}. Expected exactly one file.")
    return downloaded_files[0]



def sharepoint_download():
    """Download all files from the configured SharePoint folder to local data storage.

    No request body is required. Files are downloaded from the configured
    SharePoint folder into the local data directory.
    """
    remote_path = ''
    local_dir = ''
    status = ""
    errors = []
    download_count = 0
    # We attempt to download the monthly report files first since they are the most critical for the billing process. The history folders are expected to have more files and be more likely to encounter issues, so we attempt them after the monthly report to ensure we get the critical billing files downloaded even if there are issues with the history folders.
    try:
        remote_path = settings.sharepoint_download_root_path.rstrip("/")+"/Monthly Billing"
        local_dir = settings.upload_dir+"/Monthly_data"
        downloaded_monthly_report_files = download_file_from_sharepoint(remote_path, local_dir)
        status = status + "Monthly report files downloaded. "
        download_count += 1
    except Exception as exc:
        logger.error("sharepoint_download_api failed: %s", exc)
        errors.append(f"Monthly: {exc}")
    
    # The HISTORY_CORP folder is expected to have the main historical billing files, so we attempt it first to ensure those critical files are downloaded even if there are issues with the NON-CORP history folder.
    try:
        logger.info("History of CROP files download started...........................................")
        corp = ['AMER CROP', 'EMEAA CROP', 'APAC GC CROP', 'MEXICO CROP']
        remote_path =''
        local_dir = settings.upload_dir+"/History_data/Crop"
        for path in corp:
            remote_path = settings.sharepoint_download_root_path.rstrip("/")+"/History Data/Crop" + "/" + path
            downloaded_history_corp_files = download_file_from_sharepoint(remote_path, local_dir)
            download_count += 1
        status = status + "History CROP files downloaded. " 
        logger.info("History of CROP files download completed...........................................:%s",str(download_count))
    except Exception as exc:
        logger.error("sharepoint_download_api failed: %s", exc)
        errors.append(f"History CROP: {exc}")
    
    # The NON-CORP folder is expected to have fewer files, so we attempt it last to ensure the main monthly report files are downloaded even if there are issues with the history folders.
    try:
        logger.info("History of NONCROP files download started...........................................")
        non_crop = ['AMER NON CROP', 'EMEAA NON CROP', 'APAC GC NON CROP', 'MEXICO NON CROP']
        remote_path =''
        local_dir = settings.upload_dir+"/History_data/NonCrop"
        for path in non_crop:
            remote_path = settings.sharepoint_download_root_path.rstrip("/")+"/History Data/Non Crop" + "/" + path
            downloaded_history_NonCrop_files = download_file_from_sharepoint(remote_path, local_dir)
            download_count += 1
        status = status + "History NON-CROP files downloaded. "
        logger.info("History of NONCROP files download completed...........................................:%s",str(download_count))
    except Exception as exc:
        logger.error("sharepoint_download_api failed: %s", exc)
        errors.append(f"History NON-CROP: {exc}")
        
    return {"status": status, "download_count": download_count, "errors": errors}
        



def sharepoint_upload(remote_path: str, local_file_path: str) -> dict:
        """Upload a local file to SharePoint.

        The caller provides the source file via local_file_path and the target
        SharePoint location via remote_path.
        """
        print("Sharepoint upload api Initiated...............................")     
        if not remote_path or not isinstance(remote_path, str):
            return {"error": "'remote_path' is required and must be a string."}
        if not local_file_path or not isinstance(local_file_path, str):
            return {"error": "'local_file_path' must be a string when provided."}

        remote_path = "/".join(
            segment for segment in remote_path.strip().replace("\\", "/").split("/") if segment
        )
        if not remote_path:
            return {"error": "'remote_path' cannot be empty."}

        # Resolve local_file_path relative to the project root (cwd).
        resolved = os.path.normpath(
            os.path.join(os.getcwd(), local_file_path.lstrip("/\\"))
        )

        # If caller passed a directory, pick the first file inside it.
        if os.path.isdir(resolved):
            candidates = [
                os.path.join(resolved, f)
                for f in os.listdir(resolved)
                if os.path.isfile(os.path.join(resolved, f))
            ]
            if not candidates:
                return {"error": f"No files found in directory: {resolved}"}
            source_path = candidates[0]
        elif os.path.isfile(resolved):
            source_path = resolved
        else:
            return {"error": f"Local file not found: {resolved}"}

        source_file_name = os.path.basename(source_path)
        remote_file_name = os.path.basename(remote_path)
        normalized_root_path = "/".join(
            segment
            for segment in settings.sharepoint_download_root_path.strip().replace("\\", "/").split("/")
            if segment
        )

        if os.path.splitext(remote_file_name)[1]:
            final_remote_path = f"{normalized_root_path}/{remote_path}"
        else:
            final_remote_path = (
                f"{normalized_root_path}/{remote_path}/{source_file_name}"
            )

        try:
            result = _get_sharepoint_upload_client().upload_file(source_path, final_remote_path, overwrite=True)
        except Exception as exc:
            logger.error("sharepoint_upload_api failed: %s", exc)
            return {"error": str(exc)}
        print("Sharepoint upload api completed............................")
        return {
            "status": "ok",
            "local_file_path": source_path,
            "remote_path": final_remote_path,
            **result,
        }

def sharepoint_upload_post_validation_records():
        """Upload a local file to SharePoint."""
        remote_path = settings.sharepoint_download_root_path.rstrip("/") + "/Output"
        local_dir = settings.output_dir
        month_folder = datetime.now().strftime("%B_%Y")

        # Mapping: SharePoint destination folder -> local output subfolder
        upload_targets = {
            "AMER PeopleSoft": os.path.join("AMER", "AMER_Output"),
            "AMER_InterCompany": os.path.join("AMER_Intercompny", "Output"),
            "APAC_GC Intercompany": os.path.join("APAC", "APAC_Intercompny", "Output"),
            "APAC_GC_GAF": os.path.join("APAC", "GAF_APAC_Processor", "Output"),
            "APAC_GC_RIR": os.path.join("APAC", "APAC_GC_RIR", "Output"),
            "EMEAA_Intercompany": os.path.join("EMEAA", "EMEAA_Intercompany", "Output"),
            "Standard_Journal": os.path.join("JRF", "Output"),
        }

        try:
            count = 0
            skipped_directories: list[str] = []
            skipped_files: list[str] = []
            used_remote_month_paths: list[str] = []
            upload_client = _get_sharepoint_upload_client()

            for remote_folder, local_subdir in upload_targets.items():
                exact_remote_path = f"{remote_path}/{remote_folder}/{month_folder}"
                local_target_dir = os.path.join(local_dir, local_subdir)

                if not os.path.isdir(local_target_dir):
                    skipped_directories.append(local_target_dir)
                    logger.warning("Skipping missing output folder: %s", local_target_dir)
                    continue

                used_remote_month_paths.append(exact_remote_path)

                for file_name in os.listdir(local_target_dir):
                    file_path = os.path.join(local_target_dir, file_name)
                    if not os.path.isfile(file_path):
                        continue
                    remote_file_path = f"{exact_remote_path}/{file_name}"
                    try:
                        result = upload_client.upload_file(file_path, remote_file_path, overwrite=True)
                        count += 1
                    except FileNotFoundError:
                        skipped_files.append(file_path)
                        logger.warning("Skipping missing local file during upload: %s", file_path)
                        continue

        except Exception as exc:
            logger.error("sharepoint_download_api failed: %s", exc)
            return ({"error": str(exc)}), 500
        
        

        return (
            {
                "status": "ok",
                "remote_path": remote_path,
                "month_folder": month_folder,
                    "Total_upload_file": count,
                    "skipped_directories": skipped_directories,
                    "skipped_files": skipped_files,
                    "used_remote_month_paths": used_remote_month_paths,
                }
            ),
        
