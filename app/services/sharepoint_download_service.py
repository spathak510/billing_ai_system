"""SharePoint file download service using Microsoft Graph API."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from app.config.settings import settings

logger = logging.getLogger(__name__)


class SharePointDownloadClient:
    """Client for downloading files from SharePoint using Microsoft Graph API."""

    def __init__(
        self,
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        username: str | None = None,
        password: str | None = None,
        site_url: str | None = None,
        site_id: str | None = None,
        library_name: str | None = None,
        timeout_seconds: int | None = None,
    ) -> None:
        """Initialize SharePoint download client.

        Parameters
        ----------
        tenant_id : str, optional
            Azure AD tenant ID
        client_id : str, optional
            Client ID of the registered app
        client_secret : str, optional
            Client secret of the registered app
        username : str, optional
            Username for delegated Graph auth (password grant)
        password : str, optional
            Password for delegated Graph auth (password grant)
        site_url : str, optional
            SharePoint site URL (e.g., https://company.sharepoint.com/sites/sitename)
        site_id : str, optional
            SharePoint site ID (optional alternative to site_url)
        library_name : str, optional
            Document library name (default: "Documents")
        timeout_seconds : int, optional
            Request timeout in seconds (default: 30)
        """
        self._tenant_id = tenant_id or settings.sharepoint_tenant_id or os.getenv("GRAPH_TENANT_ID", "")
        self._client_id = client_id or settings.sharepoint_client_id or os.getenv("GRAPH_CLIENT_ID", "")
        self._client_secret = (
            client_secret or settings.sharepoint_client_secret or os.getenv("GRAPH_CLIENT_SECRET", "")
        )
        self._username = username or settings.sharepoint_username or os.getenv("GRAPH_MAILBOX_USER", "")
        self._password = password or settings.sharepoint_password or os.getenv("GRAPH_MAILBOX_PASSWORD", "")
        self._site_url = site_url or settings.sharepoint_site_url
        self._site_id = site_id or settings.sharepoint_site_id
        self._library_name = library_name or settings.sharepoint_library_name
        self._timeout_seconds = timeout_seconds or settings.sharepoint_timeout_seconds

        self._token: str | None = None
        self._token_expires_at: datetime | None = None
        self._auth_mode: str | None = None

        self._has_client_secret_auth = bool(self._client_secret)
        self._has_password_auth = bool(self._username and self._password)
        self._is_enabled = bool(self._tenant_id and self._client_id) and (
            self._has_client_secret_auth or self._has_password_auth
        )

    def download_file(self, file_path: str, local_save_path: Optional[str] = None) -> bytes | None:
        """Download a file from SharePoint.

        Parameters
        ----------
        file_path : str
            Path to file in SharePoint library (e.g., "folder/subfolder/filename.xlsx")
        local_save_path : str, optional
            Local path to save the file. If None, returns bytes.

        Returns
        -------
        bytes | None
            File contents as bytes if local_save_path is None, otherwise None after saving.

        Raises
        ------
        ValueError
            If SharePoint credentials are not configured.
        Exception
            If file download fails.
        """
        if not self._is_enabled:
            raise ValueError(
                "SharePoint is not configured. Set tenant_id, client_id, and either "
                "client_secret or username/password in settings or as parameters."
            )

        try:
            token = self._get_access_token()
            normalized_file_path = self._normalize_file_path(file_path)
            file_contents = self._download_file_from_sharepoint(token, normalized_file_path)

            if local_save_path:
                Path(local_save_path).parent.mkdir(parents=True, exist_ok=True)
                with open(local_save_path, "wb") as f:
                    f.write(file_contents)
                logger.info(f"File downloaded and saved to {local_save_path}")
                return None
            else:
                logger.info(f"File downloaded from SharePoint: {normalized_file_path}")
                return file_contents

        except HTTPError as e:
            logger.error(f"HTTP error downloading file from SharePoint: {e}")
            raise
        except Exception as e:
            logger.error(f"Error downloading file from SharePoint: {e}")
            raise

    def _normalize_file_path(self, file_path: str) -> str:
        """Normalize a SharePoint path relative to the selected document library."""
        normalized = file_path.strip().lstrip("/").replace("\\", "/")
        library_prefix = f"{self._library_name}/"
        if normalized == self._library_name:
            return ""
        if normalized.startswith(library_prefix):
            return normalized[len(library_prefix):]
        return normalized

    def _encode_graph_path(self, path: str) -> str:
        """Encode a SharePoint path for Graph URLs while preserving path separators."""
        return quote(path.strip("/"), safe="/")

    def download_files_by_extension(
        self, directory: str, extension: str, local_save_dir: str
    ) -> list[str]:
        """Download all files with specific extension from SharePoint directory.

        Parameters
        ----------
        directory : str
            Directory path in SharePoint library
        extension : str
            File extension to filter (e.g., ".xlsx", ".csv")
        local_save_dir : str
            Local directory to save downloaded files

        Returns
        -------
        list[str]
            List of paths of downloaded files.
        """
        if not self._is_enabled:
            raise ValueError(
                "SharePoint is not configured. Set tenant_id, client_id, and either "
                "client_secret or username/password."
            )

        try:
            token = self._get_access_token()
            normalized_directory = self._normalize_file_path(directory)
            files = self._list_files_in_directory(token, normalized_directory, extension)
            downloaded_paths = []

            for file_name in files:
                file_path = f"{normalized_directory}/{file_name}" if normalized_directory else file_name
                local_path = str(Path(local_save_dir) / file_name)

                self.download_file(file_path, local_path)
                downloaded_paths.append(local_path)

            logger.info(f"Downloaded {len(downloaded_paths)} files with extension {extension}")
            return downloaded_paths

        except Exception as e:
            logger.error(f"Error downloading files by extension: {e}")
            raise

    def download_all_files(self, directory: str, local_save_dir: str) -> list[str]:
        """Download all files from a SharePoint directory into a local directory."""
        return self.download_files_by_extension(directory, "", local_save_dir)

    def _get_access_token(self) -> str:
        """Get Microsoft Graph access token using supported authentication flows."""
        return self._get_access_token_for_mode()

    def _get_access_token_for_mode(
        self, *, prefer_password: bool = False, force_refresh: bool = False
    ) -> str:
        """Get Microsoft Graph access token using the requested authentication preference."""
        if (
            not force_refresh
            and self._token
            and self._token_expires_at
            and datetime.now(timezone.utc) < self._token_expires_at
            and (not prefer_password or self._auth_mode == "password")
        ):
            return self._token

        if not (self._has_client_secret_auth or self._has_password_auth):
            raise ValueError(
                "SharePoint auth is not configured. Provide client_secret or username/password."
            )

        token_url = f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/token"

        client_secret_data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "scope": "https://graph.microsoft.com/.default",
        }
        password_data = {
            "grant_type": "password",
            "client_id": self._client_id,
            "username": self._username,
            "password": self._password,
            "scope": "https://graph.microsoft.com/.default offline_access",
        }
        if self._client_secret:
            password_data["client_secret"] = self._client_secret

        try:
            import json

            def _request_token(auth_mode: str, payload: dict[str, str | None]) -> str:
                data = urlencode(payload).encode()
                req = Request(token_url, data=data)
                try:
                    with urlopen(req, timeout=self._timeout_seconds) as response:
                        token_response = json.loads(response.read())
                        self._token = token_response["access_token"]
                        self._auth_mode = auth_mode
                        expires_in = token_response.get("expires_in", 3600)
                        self._token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 60)
                        return self._token
                except HTTPError as e:
                    error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
                    logger.error(f"Token request failed. Status: {e.code}. Response: {error_body}")
                    raise

            if prefer_password and self._has_password_auth:
                try:
                    return _request_token("password", password_data)
                except HTTPError as e:
                    if not self._has_client_secret_auth:
                        raise
                    logger.warning(
                        f"Password-grant token failed (status {e.code}); retrying with client-credentials flow."
                    )

            if self._has_client_secret_auth:
                try:
                    return _request_token("client_credentials", client_secret_data)
                except HTTPError as e:
                    if not self._has_password_auth:
                        raise
                    logger.warning(
                        f"Client-credentials token failed (status {e.code}); retrying with username/password flow."
                    )

            if self._has_password_auth:
                return _request_token("password", password_data)

            raise ValueError("Could not acquire SharePoint access token with configured auth modes.")
        except HTTPError as e:
            logger.error(f"Error obtaining access token: {e}")
            raise

    def _execute_graph_request(self, request_factory, operation: str) -> bytes:
        """Execute a Graph request and retry with delegated auth on auth failures when available."""
        attempts = [False]
        if self._has_password_auth:
            attempts.append(True)

        for prefer_password in attempts:
            token = self._get_access_token_for_mode(
                prefer_password=prefer_password,
                force_refresh=prefer_password,
            )
            req = request_factory(token)
            try:
                with urlopen(req, timeout=self._timeout_seconds) as response:
                    return response.read()
            except HTTPError as e:
                error_body = e.read().decode('utf-8', errors='replace') if hasattr(e, 'read') else str(e)
                should_retry = (
                    e.code in {401, 403}
                    and not prefer_password
                    and self._has_password_auth
                    and self._auth_mode != "password"
                )
                if should_retry:
                    logger.warning(
                        "%s failed with status %s using %s auth; retrying with username/password flow. Response: %s",
                        operation,
                        e.code,
                        self._auth_mode or "unknown",
                        error_body,
                    )
                    self._token = None
                    self._token_expires_at = None
                    self._auth_mode = None
                    continue
                logger.error("%s failed: Status %s. Response: %s", operation, e.code, error_body)
                raise

        raise RuntimeError(f"{operation} failed unexpectedly without returning a response.")

    def _download_file_from_sharepoint(self, token: str, file_path: str) -> bytes:
        """Download file from SharePoint using Graph API."""
        # Parse site URL to extract site ID
        site_id = self._get_site_id(token)

        # Construct the download URL
        drive_id = self._get_drive_id(token, site_id)
        file_id = self._get_file_id(token, drive_id, file_path)

        download_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}/content"

        return self._execute_graph_request(
            lambda current_token: Request(download_url, headers={"Authorization": f"Bearer {current_token}"}),
            f"Error downloading file {file_path}",
        )

    def _get_site_id(self, token: str) -> str:
        """Get SharePoint site ID."""
        if self._site_id:
            return self._site_id

        if not self._site_url:
            root_site_url = "https://graph.microsoft.com/v1.0/sites/root"
            headers = {"Authorization": f"Bearer {token}"}
            req = Request(root_site_url, headers=headers)

            try:
                import json

                site_response = json.loads(
                    self._execute_graph_request(
                        lambda current_token: Request(
                            root_site_url,
                            headers={"Authorization": f"Bearer {current_token}"},
                        ),
                        "Error getting root site ID",
                    )
                )
                return site_response["id"]
            except HTTPError as e:
                error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
                if e.code == 401:
                    raise ValueError(
                        "SharePoint site discovery failed because GET /sites/root returned 401 Unauthorized. "
                        "Set SHAREPOINT_SITE_URL or SHAREPOINT_SITE_ID explicitly for this tenant/app registration."
                    ) from e
                logger.error(f"Error getting root site ID: Status {e.code}. Response: {error_body}")
                raise

        # Extract host and path from site URL
        url_parts = self._site_url.replace("https://", "").split("/")
        host = url_parts[0]
        site_path = "/".join(url_parts[1:])

        site_url = f"https://graph.microsoft.com/v1.0/sites/{host}:/{site_path}"
        headers = {"Authorization": f"Bearer {token}"}
        req = Request(site_url, headers=headers)

        try:
            import json

            site_response = json.loads(
                self._execute_graph_request(
                    lambda current_token: Request(
                        site_url,
                        headers={"Authorization": f"Bearer {current_token}"},
                    ),
                    f"Error getting site ID from {site_url}",
                )
            )
            return site_response["id"]
        except HTTPError as e:
            error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
            logger.error(f"Error getting site ID from {site_url}: Status {e.code}. Response: {error_body}")
            raise

    def _get_drive_id(self, token: str, site_id: str) -> str:
        """Get document library drive ID."""
        drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        headers = {"Authorization": f"Bearer {token}"}
        req = Request(drive_url, headers=headers)

        try:
            import json

            drives_response = json.loads(
                self._execute_graph_request(
                    lambda current_token: Request(
                        drive_url,
                        headers={"Authorization": f"Bearer {current_token}"},
                    ),
                    f"Error getting drive ID for site {site_id}",
                )
            )
            for drive in drives_response.get("value", []):
                if drive["name"] == self._library_name:
                    return drive["id"]
            # If library not found, return first drive (usually Documents)
            if drives_response.get("value"):
                return drives_response["value"][0]["id"]
            raise ValueError(f"Could not find library: {self._library_name}")
        except HTTPError as e:
            error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
            logger.error(f"Error getting drive ID for site {site_id}: Status {e.code}. Response: {error_body}")
            raise

    def _get_file_id(self, token: str, drive_id: str, file_path: str) -> str:
        """Get file ID by path."""
        encoded_file_path = self._encode_graph_path(file_path)
        file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_file_path}"
        headers = {"Authorization": f"Bearer {token}"}
        req = Request(file_url, headers=headers)

        try:
            import json

            file_response = json.loads(
                self._execute_graph_request(
                    lambda current_token: Request(
                        file_url,
                        headers={"Authorization": f"Bearer {current_token}"},
                    ),
                    f"Error getting file ID for '{file_path}'",
                )
            )
            return file_response["id"]
        except HTTPError as e:
            error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
            logger.error(f"Error getting file ID for '{file_path}': Status {e.code}. Response: {error_body}")
            raise

    def _list_files_in_directory(self, token: str, directory: str, extension: str) -> list[str]:
        """List files in SharePoint directory with specific extension."""
        site_id = self._get_site_id(token)
        drive_id = self._get_drive_id(token, site_id)

        encoded_directory = self._encode_graph_path(directory)
        list_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_directory}:/children"
        headers = {"Authorization": f"Bearer {token}"}
        req = Request(list_url, headers=headers)

        try:
            import json

            items_response = json.loads(
                self._execute_graph_request(
                    lambda current_token: Request(
                        list_url,
                        headers={"Authorization": f"Bearer {current_token}"},
                    ),
                    f"Error listing files in directory '{directory}'",
                )
            )
            files = []
            for item in items_response.get("value", []):
                if "file" in item and item["name"].endswith(extension):
                    files.append(item["name"])
            return files
        except HTTPError as e:
            error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
            logger.error(f"Error listing files in directory '{directory}': Status {e.code}. Response: {error_body}")
            raise
