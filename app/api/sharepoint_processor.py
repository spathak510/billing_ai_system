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



def sharepoint_download(place=None) -> str:
    """Download all files from the configured SharePoint folder to local data storage.

    No request body is required. Files are downloaded from the configured
    SharePoint folder into the local data directory.
    """
    remote_path = ''
    local_dir = ''
    status = ""
    errors = []
    download_count = 0
    month_folder = datetime.now().strftime("%B_%Y")
    if place == "Cleaning_Agent":
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
    
    elif place == "Post_validation_Agent":
        month_folder = datetime.now().strftime("%B %Y") 
        try:
            remote_path = settings.sharepoint_download_root_path.rstrip("/")+"/Manual Entry/{month_folder}".format(month_folder=month_folder)
            local_dir = settings.upload_dir+"/Manual_entry"
            download_file_from_sharepoint(remote_path, local_dir)
            status = status + "Manual entry report files downloaded. "
            download_count += 1
        except Exception as exc:
            logger.error("sharepoint_download_api failed: %s", exc)
            errors.append(f"Monthly: {exc}")

        



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





def sharepoint_download_history_data():
    """Download all files from the configured SharePoint folder to local data storage.

    No request body is required. Files are downloaded from the configured
    SharePoint folder into the local data directory.
    """
    remote_path = ''
    local_dir = ''
    status = ""
    errors = []
    download_count = 0
    
    # The HISTORY_CORP folder is expected to have the main historical billing files, so we attempt it first to ensure those critical files are downloaded even if there are issues with the NON-CORP history folder.
    try:
        logger.info("History of CROP files download started...........................................")
        corp = ['AMER CROP', 'EMEAA CROP', 'APAC GC CROP', 'MEXICO CROP']
        remote_path =''
        local_dir = "feedback/Crop"
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
        local_dir = "feedback/NonCrop"
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
        

def sharepoint_upload_processed_data():
    """Upload processed files to SharePoint:
    - All files from output/Corp_NonCorp_Split → 'Corp NonCorp Processed Data'
    - All files from output/Region_Wise_Split → 'Region Wise Processed Data'
    - The first .xlsx file from data/Post_validation_data/ → 'Post Validation Business Data' (filename preserved)
    - The first .xlsx file starting with 'cleaned_no_red' from data/ → 'Post Validation Input Data' (filename preserved)
    """
    remote_path = settings.sharepoint_download_root_path.rstrip("/")
    local_dir = settings.output_dir

    try:
        logger.info("Starting sharepoint_upload_processed_data flow...")
        count = 0
        skipped_directories = []
        skipped_files = []
        used_remote_paths = []
        upload_client = _get_sharepoint_upload_client()

        # Upload Corp_NonCorp_Split to 'Corp NonCorp Processed Data'
        corp_nc_local = os.path.join(local_dir, "Corp_NonCorp_Split")
        corp_nc_remote = f"{remote_path}/Corp NonCorp Processed Data"
        logger.info(f"Uploading files from {corp_nc_local} to {corp_nc_remote}")
        month_year = datetime.now().strftime("%B_%Y")
        if os.path.isdir(corp_nc_local):
            used_remote_paths.append(corp_nc_remote)
            for file_name in os.listdir(corp_nc_local):
                file_path = os.path.join(corp_nc_local, file_name)
                if not os.path.isfile(file_path):
                    continue
                remote_file_path = f"{corp_nc_remote}/{month_year}/{file_name}"
                logger.info(f"Uploading {file_path} to {remote_file_path}")
                try:
                    upload_client.upload_file(file_path, remote_file_path, overwrite=True)
                    count += 1
                except FileNotFoundError:
                    skipped_files.append(file_path)
                    logger.warning("Skipping missing local file during upload: %s", file_path)
        else:
            skipped_directories.append(corp_nc_local)
            logger.warning("Skipping missing output folder: %s", corp_nc_local)

        # Upload Region_Wise_Split to 'Region Wise Processed Data'
        region_wise_local = os.path.join(local_dir, "Region_Wise_Split")
        region_wise_remote = f"{remote_path}/Region Wise Processed Data"
        logger.info(f"Uploading files from {region_wise_local} to {region_wise_remote}")
        if os.path.isdir(region_wise_local):
            used_remote_paths.append(region_wise_remote)
            for file_name in os.listdir(region_wise_local):
                file_path = os.path.join(region_wise_local, file_name)
                if not os.path.isfile(file_path):
                    continue
                remote_file_path = f"{region_wise_remote}/{month_year}/{file_name}"
                logger.info(f"Uploading {file_path} to {remote_file_path}")
                try:
                    upload_client.upload_file(file_path, remote_file_path, overwrite=True)
                    count += 1
                except FileNotFoundError:
                    skipped_files.append(file_path)
                    logger.warning("Skipping missing local file during upload: %s", file_path)
        else:
            skipped_directories.append(region_wise_local)
            logger.warning("Skipping missing output folder: %s", region_wise_local)

        # Upload EMEAA/Output to 'EMEAA Processed Data'
        emeaa_output_local = os.path.join(os.getcwd(), "output", "EMEAA", "Output")
        emeaa_output_remote = f"{remote_path}/EMEAA Processed Data"
        logger.info(f"Uploading files from {emeaa_output_local} to {emeaa_output_remote}")
        if os.path.isdir(emeaa_output_local):
            used_remote_paths.append(emeaa_output_remote)
            for file_name in os.listdir(emeaa_output_local):
                file_path = os.path.join(emeaa_output_local, file_name)
                if not os.path.isfile(file_path):
                    continue
                remote_file_path = f"{emeaa_output_remote}/{month_year}/{file_name}"
                logger.info(f"Uploading {file_path} to {remote_file_path}")
                try:
                    upload_client.upload_file(file_path, remote_file_path, overwrite=True)
                    count += 1
                except FileNotFoundError:
                    skipped_files.append(file_path)
                    logger.warning("Skipping missing local file during upload: %s", file_path)
        else:
            skipped_directories.append(emeaa_output_local)
            logger.warning("Skipping missing EMEAA output folder: %s", emeaa_output_local)

        # Upload Monthly_data to 'Monthly Billing Archive'
        monthly_data_local = os.path.join(os.getcwd(), "data", "Monthly_data")
        monthly_data_remote = f"{remote_path}/Monthly Billing Archive"
        logger.info(f"Uploading files from {monthly_data_local} to {monthly_data_remote}")
        if os.path.isdir(monthly_data_local):
            used_remote_paths.append(monthly_data_remote)
            for file_name in os.listdir(monthly_data_local):
                file_path = os.path.join(monthly_data_local, file_name)
                if not os.path.isfile(file_path):
                    continue
                remote_file_path = f"{monthly_data_remote}/{file_name}"
                logger.info(f"Uploading {file_path} to {remote_file_path}")
                try:
                    upload_client.upload_file(file_path, remote_file_path, overwrite=True)
                    count += 1
                except FileNotFoundError:
                    skipped_files.append(file_path)
                    logger.warning("Skipping missing local file during upload: %s", file_path)
        else:
            skipped_directories.append(monthly_data_local)
            logger.warning("Skipping missing Monthly_data folder: %s", monthly_data_local)    

        # Upload dynamic Post Validation Business Data file
        pv_business_dir = os.path.join(os.getcwd(), "data", "Post_validation_data")
        pv_business_uploaded = False
        logger.info(f"Uploading first .xlsx from {pv_business_dir} to Post Validation Business Data")
        if os.path.isdir(pv_business_dir):
            for fname in os.listdir(pv_business_dir):
                if fname.lower().endswith('.xlsx'):
                    pv_business_file = os.path.join(pv_business_dir, fname)
                    pv_business_remote = f"{remote_path}/Post Validation Business Data/{month_year}/{fname}"
                    if os.path.isfile(pv_business_file):
                        used_remote_paths.append(pv_business_remote)
                        logger.info(f"Uploading {pv_business_file} to {pv_business_remote}")
                        try:
                            upload_client.upload_file(pv_business_file, pv_business_remote, overwrite=True)
                            count += 1
                            pv_business_uploaded = True
                        except FileNotFoundError:
                            skipped_files.append(pv_business_file)
                            logger.warning("Skipping missing local file during upload: %s", pv_business_file)
                    break
            if not pv_business_uploaded:
                logger.warning("No .xlsx file found in Post_validation_data folder.")
        else:
            skipped_directories.append(pv_business_dir)
            logger.warning("Missing Post_validation_data folder: %s", pv_business_dir)

        # Upload dynamic Post Validation Input Data file
        pv_input_dir = os.path.join(os.getcwd(), "data")
        pv_input_uploaded = False
        logger.info(f"Uploading first cleaned_no_red*.xlsx from {pv_input_dir} to Post Validation Input Data")
        if os.path.isdir(pv_input_dir):
            for fname in os.listdir(pv_input_dir):
                if fname.lower().startswith('cleaned_no_red') and fname.lower().endswith('.xlsx'):
                    pv_input_file = os.path.join(pv_input_dir, fname)
                    pv_input_remote = f"{remote_path}/Post Validation Input Data/{month_year}/{fname}"
                    if os.path.isfile(pv_input_file):
                        used_remote_paths.append(pv_input_remote)
                        logger.info(f"Uploading {pv_input_file} to {pv_input_remote}")
                        try:
                            upload_client.upload_file(pv_input_file, pv_input_remote, overwrite=True)
                            count += 1
                            pv_input_uploaded = True
                        except FileNotFoundError:
                            skipped_files.append(pv_input_file)
                            logger.warning("Skipping missing local file during upload: %s", pv_input_file)
                    break
            if not pv_input_uploaded:
                logger.warning("No cleaned_no_red .xlsx file found in data folder.")
        else:
            skipped_directories.append(pv_input_dir)
            logger.warning("Missing data folder: %s", pv_input_dir)

        logger.info(f"Completed sharepoint_upload_processed_data. Total uploaded: {count}")
    except Exception as exc:
        logger.error("sharepoint_upload_processed_data failed: %s", exc)
        
    try:
        logger.info("Starting upload of updated history data to SharePoint...............................")
        sharepoint_upload_updated_history_data()
    except Exception as exc:        
        logger.error("sharepoint_upload_updated_history_data failed: %s", exc)      
        return ({"error": str(exc)}), 500

    return {
        "status": "ok",
        "remote_path": remote_path,
        "Total_upload_file": count,
        "skipped_directories": skipped_directories,
        "skipped_files": skipped_files,
        "used_remote_paths": used_remote_paths,
    }

def sharepoint_upload_updated_history_data():
    """
    Upload all files from output/history_data_output/Corp to each remote folder in corp list.
    """
    remote_path = settings.sharepoint_download_root_path.rstrip("/")
    local_dir = os.path.join(os.getcwd(), "output", "history_data_output", "Corp")
    corp = ['AMER CROP', 'EMEAA CROP', 'APAC GC CROP', 'MEXICO CROP']
    non_crop = ['AMER NON CROP', 'EMEAA NON CROP', 'APAC GC NON CROP', 'MEXICO NON CROP']
    count = 0
    skipped_directories = []
    skipped_files = []
    used_remote_paths = []
    upload_client = _get_sharepoint_upload_client()

    # Upload Corp files: map each file to its correct remote folder
    if not os.path.isdir(local_dir):
        skipped_directories.append(local_dir)
        logger.warning("Missing local Corp history data folder: %s", local_dir)
    else:
        for file_name in os.listdir(local_dir):
            file_path = os.path.join(local_dir, file_name)
            if not os.path.isfile(file_path):
                continue
            # Determine which corp folder this file belongs to
            matched = False
            for corp_folder in corp:
                # Use a robust match: folder name (without spaces) in file name (case-insensitive, ignore spaces/underscores/dashes)
                folder_key = corp_folder.replace(' ', '').replace('_', '').replace('-', '').lower()
                file_key = file_name.replace(' ', '').replace('_', '').replace('-', '').lower()
                if folder_key in file_key:
                    remote_corp_path = f"{remote_path}/History Data/Crop/{corp_folder}"
                    used_remote_paths.append(remote_corp_path)
                    remote_file_path = f"{remote_corp_path}/{file_name}"
                    try:
                        upload_client.upload_file(file_path, remote_file_path, overwrite=True)
                        count += 1
                        matched = True
                        break
                    except FileNotFoundError:
                        skipped_files.append(file_path)
                        logger.warning("Skipping missing local file during upload: %s", file_path)
            if not matched:
                logger.warning(f"No matching Corp folder for file: {file_name}")

    # Upload NonCorp files: map each file to its correct remote folder
    noncorp_local_dir = os.path.join(os.getcwd(), "output", "history_data_output", "NonCorp")
    if not os.path.isdir(noncorp_local_dir):
        skipped_directories.append(noncorp_local_dir)
        logger.warning("Missing local NonCorp history data folder: %s", noncorp_local_dir)
    else:
        for file_name in os.listdir(noncorp_local_dir):
            file_path = os.path.join(noncorp_local_dir, file_name)
            if not os.path.isfile(file_path):
                continue
            matched = False
            for noncorp_folder in non_crop:
                folder_key = noncorp_folder.replace(' ', '').replace('_', '').replace('-', '').lower()
                file_key = file_name.replace(' ', '').replace('_', '').replace('-', '').lower()
                if folder_key in file_key:
                    remote_noncorp_path = f"{remote_path}/History Data/Non Crop/{noncorp_folder}"
                    used_remote_paths.append(remote_noncorp_path)
                    remote_file_path = f"{remote_noncorp_path}/{file_name}"
                    try:
                        upload_client.upload_file(file_path, remote_file_path, overwrite=True)
                        count += 1
                        matched = True
                        break
                    except FileNotFoundError:
                        skipped_files.append(file_path)
                        logger.warning("Skipping missing local file during upload: %s", file_path)
            if not matched:
                logger.warning(f"No matching NonCorp folder for file: {file_name}")

    return {
        "status": "ok",
        "remote_path": remote_path,
        "corp_folders": corp,
        "noncorp_folders": non_crop,
        "Total_upload_file": count,
        "skipped_directories": skipped_directories,
        "skipped_files": skipped_files,
        "used_remote_paths": used_remote_paths,
    }