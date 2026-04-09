"""
SharePoint Services Usage Examples

This file demonstrates how to use the SharePoint download and upload services.
"""

from app.services.sharepoint_download_service import SharePointDownloadClient
from app.services.sharepoint_upload_service import SharePointUploadClient


# ============================================================================
# EXAMPLE 1: Using SharePoint Download Service
# ============================================================================

def example_download_single_file():
    """Download a single file from SharePoint."""
    download_client = SharePointDownloadClient()

    # Download file and save to local path
    file_path = "billing_reports/Q1_2025_Report.xlsx"
    local_save_path = "data/Q1_2025_Report.xlsx"

    try:
        result = download_client.download_file(file_path, local_save_path)
        print(f"File saved to {local_save_path}")
    except Exception as e:
        print(f"Error downloading file: {e}")


def example_download_file_as_bytes():
    """Download a file from SharePoint as bytes (without saving to disk)."""
    download_client = SharePointDownloadClient()

    file_path = "templates/billing_template.xlsx"

    try:
        file_bytes = download_client.download_file(file_path)
        # Use file_bytes in memory (e.g., read with pandas)
        import pandas as pd
        import io

        df = pd.read_excel(io.BytesIO(file_bytes))
        print(f"Downloaded file has {len(df)} rows")
    except Exception as e:
        print(f"Error downloading file: {e}")


def example_download_multiple_by_extension():
    """Download all files with specific extension from SharePoint directory."""
    download_client = SharePointDownloadClient()

    directory = "billing_reports"
    extension = ".xlsx"
    local_save_dir = "data/downloaded_reports"

    try:
        downloaded_files = download_client.download_files_by_extension(
            directory, extension, local_save_dir
        )
        print(f"Downloaded {len(downloaded_files)} files:")
        for file_path in downloaded_files:
            print(f"  - {file_path}")
    except Exception as e:
        print(f"Error downloading files: {e}")


# ============================================================================
# EXAMPLE 2: Using SharePoint Upload Service
# ============================================================================

def example_upload_single_file():
    """Upload a single file to SharePoint."""
    upload_client = SharePointUploadClient()

    local_file = "output/AMER/AMER_Output/billing_summary.xlsx"
    remote_path = "billing_reports/Q1_2025/billing_summary.xlsx"

    try:
        result = upload_client.upload_file(local_file, remote_path, overwrite=True)
        print(f"File uploaded successfully: {result.get('webUrl')}")
    except Exception as e:
        print(f"Error uploading file: {e}")


def example_upload_file_from_bytes():
    """Upload file contents (bytes) to SharePoint."""
    upload_client = SharePointUploadClient()

    # Example: generating file in memory and uploading
    import pandas as pd
    import io

    # Create Excel file in memory
    df = pd.DataFrame({"Name": ["John", "Jane"], "Amount": [1000, 2000]})
    excel_bytes = io.BytesIO()
    df.to_excel(excel_bytes, index=False)
    file_bytes = excel_bytes.getvalue()

    remote_path = "billing_reports/generated_report.xlsx"

    try:
        result = upload_client.upload_bytes(file_bytes, remote_path, overwrite=True)
        print(f"File uploaded: {result.get('webUrl')}")
    except Exception as e:
        print(f"Error uploading file: {e}")


def example_upload_multiple_files():
    """Upload multiple files to SharePoint."""
    upload_client = SharePointUploadClient()

    files = {
        "output/AMER/AMER_Output/PeopleSoft Format For AMER_CORP.csv": "AMER_CORP.csv",
        "output/AMER/AMER_Output/PeopleSoft Format For AMER_NONCORP.csv": "AMER_NONCORP.csv",
    }
    target_directory = "billing_reports/Q1_2025/AMER"

    try:
        results = upload_client.upload_multiple_files(files, target_directory, overwrite=True)
        print(f"Upload results:")
        for result in results:
            if "error" not in result:
                print(f"  ✓ {result.get('name')} - {result.get('webUrl')}")
            else:
                print(f"  ✗ {result.get('file')} - Error: {result.get('error')}")
    except Exception as e:
        print(f"Error uploading files: {e}")


# ============================================================================
# EXAMPLE 3: Advanced Operations
# ============================================================================

def example_create_folder():
    """Create a folder structure in SharePoint."""
    upload_client = SharePointUploadClient()

    folder_path = "billing_reports/Q2_2025/AMER/processed"

    try:
        result = upload_client.create_folder(folder_path)
        print(f"Folder created: {result.get('webUrl')}")
    except Exception as e:
        print(f"Error creating folder: {e}")


def example_delete_file():
    """Delete a file from SharePoint."""
    upload_client = SharePointUploadClient()

    remote_path = "billing_reports/old_report.xlsx"

    try:
        upload_client.delete_file(remote_path)
        print(f"File deleted: {remote_path}")
    except Exception as e:
        print(f"Error deleting file: {e}")


def example_integrated_workflow():
    """Complete workflow: Download, process, and upload files."""
    download_client = SharePointDownloadClient()
    upload_client = SharePointUploadClient()

    try:
        # 1. Download billing template
        print("Downloading template...")
        template_bytes = download_client.download_file("templates/billing_template.xlsx")

        # 2. Process the file
        print("Processing data...")
        import pandas as pd
        import io

        template_df = pd.read_excel(io.BytesIO(template_bytes))
        # Do some processing on the data
        template_df["processed"] = True

        # 3. Save processed file
        output_bytes = io.BytesIO()
        template_df.to_excel(output_bytes, index=False)
        processed_bytes = output_bytes.getvalue()

        # 4. Upload processed file to SharePoint
        print("Uploading processed file...")
        result = upload_client.upload_bytes(
            processed_bytes, "billing_reports/processed/template_processed.xlsx"
        )

        print(f"Workflow completed: {result.get('webUrl')}")

    except Exception as e:
        print(f"Error in workflow: {e}")


# ============================================================================
# CONFIGURATION NOTE
# ============================================================================

"""
Before using these services, ensure you have the following environment variables set in your .env file:

SHAREPOINT_TENANT_ID=your_azure_tenant_id
SHAREPOINT_CLIENT_ID=your_client_id
SHAREPOINT_CLIENT_SECRET=your_client_secret
SHAREPOINT_SITE_URL=https://company.sharepoint.com/sites/yoursite
SHAREPOINT_LIBRARY_NAME=Documents  # Usually "Documents" or "Shared Documents"
SHAREPOINT_TIMEOUT_SECONDS=30

To register an app in Azure AD for SharePoint access:

1. Go to https://portal.azure.com
2. Navigate to Azure AD > App registrations
3. Click "New registration"
4. Set name, supported account types, and redirect URI
5. In the app's settings:
   - Add a client secret (copy both ID and secret to .env)
   - Grant permissions:
     - SharePoint (Sites.ReadWrite.All)
     - Microsoft Graph (Files.ReadWrite.All, Sites.ReadWrite.All)
6. Grant admin consent for these permissions

SharePoint Site URL format:
- https://company.sharepoint.com/sites/sitename
- or https://company.sharepoint.com (for default site)

Library name is usually "Documents" by default, but can be any document library you have access to.
"""


if __name__ == "__main__":
    # Uncomment to run examples
    # example_download_single_file()
    # example_download_file_as_bytes()
    # example_download_multiple_by_extension()
    # example_upload_single_file()
    # example_upload_file_from_bytes()
    # example_upload_multiple_files()
    # example_create_folder()
    # example_delete_file()
    # example_integrated_workflow()
    pass
