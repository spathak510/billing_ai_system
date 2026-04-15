from __future__ import annotations

import logging,os
from datetime import datetime, timezone
from app.celery_app import celery_app

# For post-validation flow
from app.config.settings import settings
from app.services.ihg_servicenow_ticket_service import create_ticket_service_now
from app.api.sharepoint_processor import sharepoint_download, sharepoint_upload
from app.agents.cleaning_agent import cleaning_data_prosessing
from app.services.excel_filter_service import remove_red_rows_from_excel
from app.api.sharepoint_processor import sharepoint_upload_post_validation_records
from app.api.mail_processor import post_validation_send_email

logger = logging.getLogger(__name__)

@celery_app.task(name="app.tasks.beat_heartbeat")
def beat_heartbeat() -> str:
    ts = datetime.now(timezone.utc).isoformat()
    logger.info("Celery Beat heartbeat at %s", ts)
    return ts


# Celery task to run the clean data flow
@celery_app.task(name="app.tasks.run_clean_data_flow")
def run_clean_data_flow_task():
    """Run the SharePoint download, cleaning, upload, and ServiceNow flow asynchronously."""
    try:
        base_remote_path = "/Monthly Billing Clean Data/".rstrip("/")  # Ensure no trailing slash
        local_dir = settings.output_dir
        
        logger.info("Step 1: SharePoint download started")
        download_result = sharepoint_download()
        logger.info("Step 1: SharePoint download completed: %s", download_result)

        logger.info("Step 2: Cleaning process started")
        cleaning_data_prosessing()
        logger.info("Step 2: Cleaning process completed")

        
        logger.info("Step 3: SharePoint upload started")
        month_folder = datetime.now().strftime("%B_%Y")
        remote_path = f"{base_remote_path}/{month_folder}"
        local_dir = local_dir+"/Monthly_cleaned_report"
        upload_result = sharepoint_upload(remote_path, local_dir)
        logger.info("Step 3: SharePoint upload completed: %s", upload_result)

        payload = {
            "requested_by": "AMER\\USM3PA",
            "requested_for": "AMER\\USM3PA",
            "location": "ATLR3",
            "situation": "other",
            "business_service": "IHG University",
            "service_category": "Application Support",
            "assignment_group": "IY-GLBL-LMS Support Accenture",
            "short_description": "LMS Monthly Billing Process - MyID Data Retrieve",
            "description": "LMS Monthly Billing Process - MyID Data Retrieve",
            "internal_notes": "",
            "source": "RCC Tech Intake Form",
        }

        logger.info("Step 4: ServiceNow ticket creation started")
        response = create_ticket_service_now(payload)
        logger.info("Step 4: ServiceNow ticket creation completed: %s", response)

        logger.info("Cleaning data Background flow completed successfully")
        return True 

    except Exception as exc:
        logger.exception("Background flow failed: %s", exc)
             


# Celery task to run post-validation flow in strict order
@celery_app.task(name="app.tasks.run_post_validation_flow")
def run_post_validation_flow_task():
    """
    Celery task: Run post-validation flow in strict order.
    Steps:
      1. remove_red_rows_from_excel
      2. sharepoint_upload_post_validation_record_api
      3. create_ticket_service_now
      4. post_validation_send_email
    Returns a summary dict or error.
    """

    # Step 1: Remove red-highlighted rows
    filename = os.listdir(settings.upload_dir+"/Post_validation_data/")[0]
    source_path = os.path.join(settings.upload_dir, "Post_validation_data", filename)
    output_dir = settings.upload_dir
    try:
        cleaned_path = remove_red_rows_from_excel(
            input_file_path=source_path,
            output_dir=output_dir,
        )
        logger.info("Step 1: remove_red_rows_from_excel completed: %s", cleaned_path)
    except Exception as exc:
        logger.error("Step 1 failed: %s", exc)
        return {"error": f"remove_red_rows_from_excel failed: {exc}"}

    # Step 2: SharePoint upload
    try:
        upload_result = sharepoint_upload_post_validation_records()
        logger.info("Step 2: sharepoint_upload_post_validation_records completed: %s", upload_result)
    except Exception as exc:
        logger.error("Step 2 failed: %s", exc)
        return {"error": f"sharepoint_upload_post_validation_records failed: {exc}"}

    # Step 3: ServiceNow ticket
    payload = {
        "requested_by": "AMER\\USM3PA",
        "requested_for": "AMER\\USM3PA",
        "location": "ATLR3",
        "situation": "other",
        "business_service": "IHG University",
        "service_category": "Application Support",
        "assignment_group": "IY-GLBL-LMS Support Accenture",
        "short_description": "LMS Monthly Billing Process - PS Upload",
        "description": "LMS Monthly Billing Process - PS Upload",
        "internal_notes": "",
        "source": "RCC Tech Intake Form"
    }
    try:
        servicenow_result = create_ticket_service_now(payload)
        logger.info("Step 3: create_ticket_service_now completed: %s", servicenow_result)
    except Exception as exc:
        logger.error("Step 3 failed: %s", exc)
        return {"error": f"create_ticket_service_now failed: {exc}"}

    # Step 4: Post-validation send email
    try:
        post_validation_send_email()
        logger.info("Step 4: post_validation_send_email completed")
    except Exception as exc:
        logger.error("Step 4 failed: %s", exc)
        return {"error": f"post_validation_send_email failed: {exc}"}

    return {
        "status": "ok",
        "cleaned_file": cleaned_path,
        "upload_result": str(upload_result),
        "servicenow_result": str(servicenow_result),
    }



