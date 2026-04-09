"""Billing file upload and report download API endpoints."""

from __future__ import annotations

from datetime import datetime
import logging
import os
import threading

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
    download_count = 0
    # We attempt to download the monthly report files first since they are the most critical for the billing process. The history folders are expected to have more files and be more likely to encounter issues, so we attempt them after the monthly report to ensure we get the critical billing files downloaded even if there are issues with the history folders.
    try:
        remote_path = settings.sharepoint_download_root_path.rstrip("/")+"/Monthly Billing"
        local_dir = settings.upload_dir+"/Monthly_data"
        downloaded_monthly_report_files = download_file_from_sharepoint(remote_path, local_dir)
        status = status + "Monthly report files downloaded. "
        download_count += len(downloaded_monthly_report_files)
    except Exception as exc:
        logger.error("sharepoint_download_api failed: %s", exc)
        return {"error": str(exc)}
    
    # The HISTORY_CORP folder is expected to have the main historical billing files, so we attempt it first to ensure those critical files are downloaded even if there are issues with the NON-CORP history folder.
    try:
        corp = ['AMER CORP', 'EMEAA CORP', 'APAC GC CORP', 'MEXICO CORP']
        remote_path =''
        local_dir = settings.upload_dir+"/History_data/Corp"
        for path in corp:
            remote_path = settings.sharepoint_download_root_path.rstrip("/")+"/History Data/Corp" + "/" + path
            downloaded_history_corp_files = download_file_from_sharepoint(remote_path, local_dir)
            download_count += len(downloaded_history_corp_files)
        status = status + "History CORP files downloaded. " 
    except Exception as exc:
        logger.error("sharepoint_download_api failed: %s", exc)
        return {"error": str(exc)}
    
    # The NON-CORP folder is expected to have fewer files, so we attempt it last to ensure the main monthly report files are downloaded even if there are issues with the history folders.
    try:
        non_corp = ['AMER NON CORP', 'EMEAA NON CORP', 'APAC GC NON CORP', 'MEXICO NON CORP']
        remote_path =''
        local_dir = settings.upload_dir+"/History_data/NonCorp"
        for path in non_corp:
            remote_path = settings.sharepoint_download_root_path.rstrip("/")+"/History Data/NonCorp" + "/" + path
            downloaded_history_NonCorp_files = download_file_from_sharepoint(remote_path, local_dir)
            download_count += len(downloaded_history_NonCorp_files)
        status = status + "History NON-CORP files downloaded. "
    except Exception as exc:
        logger.error("sharepoint_download_api failed: %s", exc)
        return {"error": str(exc)}
    
    return {"status": status, "download_count": download_count}
        



def sharepoint_upload(remote_path: str, local_file_path: str) -> dict:
        """Upload a local file to SharePoint.

        The caller provides the source file via local_file_path and the target
        SharePoint location via remote_path.
        """

        if not remote_path or not isinstance(remote_path, str):
            return {"error": "'remote_path' is required and must be a string."}
        if not local_file_path or not isinstance(local_file_path, str):
            return {"error": "'local_file_path' must be a string when provided."}

        remote_path = remote_path.strip().lstrip("/")
        if not remote_path:
            return {"error": "'remote_path' cannot be empty."}
        
        final_remote_path = settings.sharepoint_download_root_path.rstrip("/")+ remote_path
        final_local_path = settings.upload_dir+local_file_path

        source_path = final_local_path.strip()
        if not os.path.isfile(source_path):
            return {"error": f"Local file not found: {source_path}"}

        try:
            result = _get_sharepoint_upload_client().upload_file(source_path, final_remote_path, overwrite=True)
        except Exception as exc:
            logger.error("sharepoint_upload_api failed: %s", exc)
            return {"error": str(exc)}

        return {
            "status": "ok",
            "local_file_path": source_path,
            "remote_path": final_remote_path,
            **result,
        }