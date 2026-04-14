"""SharePoint file move service using Microsoft Graph API."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from urllib.error import HTTPError
from urllib.request import Request

from app.services.sharepoint_upload_service import SharePointUploadClient

logger = logging.getLogger(__name__)


class SharePointMoveClient(SharePointUploadClient):
    """Client for moving files within SharePoint using Microsoft Graph API."""

    def move_file(self, source_path: str, destination_path: str, overwrite: bool = True) -> dict:
        """Move a file within SharePoint.

        Parameters
        ----------
        source_path : str
            Source file path in SharePoint library.
        destination_path : str
            Destination parent folder path in SharePoint library.
            A month folder in format MonthYYYY (e.g., April2026) is created under this path,
            and the source file name is preserved.
        overwrite : bool, optional
            Whether to overwrite destination when it already exists.

        Returns
        -------
        dict
            Metadata of moved file.
        """
        if not self._is_enabled:
            raise ValueError(
                "SharePoint is not configured. Set tenant_id, client_id, and either "
                "client_secret or username/password."
            )

        source_path = source_path.strip().lstrip("/")
        destination_path = destination_path.strip().lstrip("/")
        if not source_path or not destination_path:
            raise ValueError("Both source_path and destination_path are required.")

        try:
            token = self._get_access_token()
            result = self._move_file_in_sharepoint(token, source_path, destination_path, overwrite)
            logger.info("File moved in SharePoint: %s -> %s", source_path, destination_path)
            return result
        except Exception as e:
            logger.error("Error moving file in SharePoint: %s", e)
            raise

    def _move_file_in_sharepoint(
        self,
        token: str,
        source_path: str,
        destination_path: str,
        overwrite: bool,
    ) -> dict:
        """Move file into destination_path/MonthYYYY while preserving source filename."""
        site_id = self._get_site_id(token)
        drive_id = self._get_drive_id(token, site_id)

        source_item_id = self._get_file_id(token, drive_id, source_path)

        source_name = source_path.rsplit("/", 1)[-1].strip()
        if not source_name:
            raise ValueError("source_path must include a file name.")

        month_folder_name = datetime.now().strftime("%B%Y")
        destination_folder_path = f"{destination_path.rstrip('/')}/{month_folder_name}" if destination_path else month_folder_name
        destination_file_path = f"{destination_folder_path}/{source_name}"

        self._ensure_directory_exists(token, drive_id, destination_folder_path)

        if overwrite:
            try:
                self._delete_file_from_sharepoint(token, destination_file_path)
            except HTTPError as e:
                if e.code != 404:
                    raise

        patch_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{source_item_id}"
        destination_folder_id = self._get_folder_id(token, drive_id, destination_folder_path)
        parent_reference = {"id": destination_folder_id}

        body = json.dumps(
            {
                "name": source_name,
                "parentReference": parent_reference,
            }
        ).encode()

        return json.loads(
            self._execute_graph_request(
                lambda current_token: Request(
                    patch_url,
                    data=body,
                    headers={
                        "Authorization": f"Bearer {current_token}",
                        "Content-Type": "application/json",
                    },
                    method="PATCH",
                ),
                f"Error moving file {source_path} to {destination_path}",
            )
        )
