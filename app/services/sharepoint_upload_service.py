"""SharePoint file upload service using Microsoft Graph API."""
from __future__ import annotations

import json
import logging
import os
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from app.config.settings import settings

logger = logging.getLogger(__name__)


class SharePointUploadClient:
    """Client for uploading files to SharePoint using Microsoft Graph API."""

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
        """Initialize SharePoint upload client.

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

    def _encode_graph_path(self, path: str) -> str:
        """Encode a SharePoint path for Graph URLs while preserving path separators."""
        return quote(path.strip("/"), safe="/")

    def _normalize_library_path(self, path: str) -> str:
        """Normalize a SharePoint path relative to the configured document library."""
        normalized = path.strip().lstrip("/").replace("\\", "/")
        library_prefix = f"{self._library_name}/"
        if normalized == self._library_name:
            return ""
        if normalized.startswith(library_prefix):
            return normalized[len(library_prefix):]
        return normalized

    def upload_file(
        self, file_path: str, remote_path: str, overwrite: bool = True
    ) -> dict:
        """Upload a file to SharePoint.

        Parameters
        ----------
        file_path : str
            Local file path to upload
        remote_path : str
            Target path in SharePoint library (e.g., "folder/subfolder/filename.xlsx")
        overwrite : bool, optional
            Whether to overwrite if file exists (default: True)

        Returns
        -------
        dict
            Metadata about the uploaded file including id, webUrl, etc.

        Raises
        ------
        ValueError
            If SharePoint credentials are not configured.
        Exception
            If file upload fails.
        """
        if not self._is_enabled:
            raise ValueError(
                "SharePoint is not configured. Set tenant_id, client_id, and either "
                "client_secret or username/password in settings or as parameters."
            )

        if not Path(file_path).exists():
            raise FileNotFoundError(f"Local file not found: {file_path}")

        try:
            with open(file_path, "rb") as f:
                file_contents = f.read()

            token = self._get_access_token()
            remote_path = self._normalize_library_path(remote_path)
            result = self._upload_file_to_sharepoint(
                token, remote_path, file_contents, overwrite
            )

            logger.info(f"File uploaded to SharePoint: {remote_path}")
            return result

        except HTTPError as e:
            logger.error(f"HTTP error uploading file to SharePoint: {e}")
            raise
        except Exception as e:
            logger.error(f"Error uploading file to SharePoint: {e}")
            raise

    def upload_bytes(
        self, file_bytes: bytes, remote_path: str, overwrite: bool = True
    ) -> dict:
        """Upload file contents (as bytes) to SharePoint.

        Parameters
        ----------
        file_bytes : bytes
            File contents as bytes
        remote_path : str
            Target path in SharePoint library (e.g., "folder/subfolder/filename.xlsx")
        overwrite : bool, optional
            Whether to overwrite if file exists (default: True)

        Returns
        -------
        dict
            Metadata about the uploaded file.
        """
        if not self._is_enabled:
            raise ValueError(
                "SharePoint is not configured. Set tenant_id, client_id, and either "
                "client_secret or username/password."
            )

        try:
            token = self._get_access_token()
            remote_path = self._normalize_library_path(remote_path)
            result = self._upload_file_to_sharepoint(token, remote_path, file_bytes, overwrite)

            logger.info(f"File uploaded to SharePoint: {remote_path}")
            return result

        except Exception as e:
            logger.error(f"Error uploading file bytes to SharePoint: {e}")
            raise

    def upload_multiple_files(
        self, files: dict[str, str], target_directory: str, overwrite: bool = True
    ) -> list[dict]:
        """Upload multiple files to SharePoint.

        Parameters
        ----------
        files : dict[str, str]
            Dictionary mapping local file paths to remote file names
        target_directory : str
            Target directory in SharePoint library
        overwrite : bool, optional
            Whether to overwrite if files exist (default: True)

        Returns
        -------
        list[dict]
            List of uploaded file metadata.
        """
        if not self._is_enabled:
            raise ValueError(
                "SharePoint is not configured. Set tenant_id, client_id, and either "
                "client_secret or username/password."
            )

        results = []
        for local_path, remote_name in files.items():
            remote_path = f"{target_directory}/{remote_name}" if target_directory else remote_name
            try:
                result = self.upload_file(local_path, remote_path, overwrite)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to upload {local_path}: {e}")
                results.append({"error": str(e), "file": remote_path})

        logger.info(f"Uploaded {len([r for r in results if 'error' not in r])}/{len(results)} files")
        return results

    def create_folder(self, folder_path: str) -> dict:
        """Create a folder in SharePoint library.

        Parameters
        ----------
        folder_path : str
            Folder path to create (e.g., "folder/subfolder")

        Returns
        -------
        dict
            Metadata about the created folder.
        """
        if not self._is_enabled:
            raise ValueError(
                "SharePoint is not configured. Set tenant_id, client_id, and either "
                "client_secret or username/password."
            )

        try:
            token = self._get_access_token()
            folder_path = self._normalize_library_path(folder_path)
            result = self._create_folder_in_sharepoint(token, folder_path)

            logger.info(f"Folder created in SharePoint: {folder_path}")
            return result

        except Exception as e:
            logger.error(f"Error creating folder in SharePoint: {e}")
            raise

    def delete_file(self, remote_path: str) -> None:
        """Delete a file from SharePoint.

        Parameters
        ----------
        remote_path : str
            Path to file in SharePoint library
        """
        if not self._is_enabled:
            raise ValueError(
                "SharePoint is not configured. Set tenant_id, client_id, and either "
                "client_secret or username/password."
            )

        try:
            token = self._get_access_token()
            remote_path = self._normalize_library_path(remote_path)
            self._delete_file_from_sharepoint(token, remote_path)

            logger.info(f"File deleted from SharePoint: {remote_path}")

        except Exception as e:
            logger.error(f"Error deleting file from SharePoint: {e}")
            raise

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

    def _execute_graph_request(
        self,
        request_factory,
        operation: str,
        suppress_http_statuses: set[int] | None = None,
    ) -> bytes:
        """Execute a Graph request and retry with delegated auth on auth failures when available."""
        attempts = [False]
        if self._has_password_auth:
            attempts.append(True)
        max_timeout_retries = 2
        suppressed_statuses = suppress_http_statuses or set()

        def _is_timeout_error(exc: BaseException) -> bool:
            if isinstance(exc, (TimeoutError, socket.timeout)):
                return True
            if isinstance(exc, URLError):
                reason = exc.reason
                if isinstance(reason, (TimeoutError, socket.timeout)):
                    return True
                return "timed out" in str(reason).lower()
            return "timed out" in str(exc).lower()

        for prefer_password in attempts:
            token = self._get_access_token_for_mode(
                prefer_password=prefer_password,
                force_refresh=prefer_password,
            )
            for timeout_retry_index in range(max_timeout_retries + 1):
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
                        break
                    if e.code in suppressed_statuses:
                        raise
                    logger.error("%s failed: Status %s. Response: %s", operation, e.code, error_body)
                    raise
                except Exception as e:
                    if _is_timeout_error(e) and timeout_retry_index < max_timeout_retries:
                        logger.warning(
                            "%s timed out (attempt %s/%s); retrying with timeout=%ss.",
                            operation,
                            timeout_retry_index + 1,
                            max_timeout_retries + 1,
                            self._timeout_seconds,
                        )
                        continue
                    if _is_timeout_error(e):
                        logger.error(
                            "%s timed out after %s attempts with timeout=%ss. "
                            "Consider increasing SHAREPOINT_TIMEOUT_SECONDS.",
                            operation,
                            max_timeout_retries + 1,
                            self._timeout_seconds,
                        )
                    raise

        raise RuntimeError(f"{operation} failed unexpectedly without returning a response.")

    def _upload_file_to_sharepoint(
        self, token: str, file_path: str, file_contents: bytes, overwrite: bool
    ) -> dict:
        """Upload file to SharePoint using Graph API."""
        file_path = self._normalize_library_path(file_path)
        site_id = self._get_site_id(token)
        drive_id = self._get_drive_id(token, site_id)

        # Ensure parent directory exists
        parent_path = "/".join(file_path.split("/")[:-1])
        if parent_path:
            self._ensure_directory_exists(token, drive_id, parent_path)

        encoded_file_path = self._encode_graph_path(file_path)
        upload_url = (
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_file_path}:"
            f"/content?@microsoft.graph.conflictBehavior={'replace' if overwrite else 'fail'}"
        )

        return json.loads(
            self._execute_graph_request(
                lambda current_token: Request(
                    upload_url,
                    data=file_contents,
                    headers={
                        "Authorization": f"Bearer {current_token}",
                        "Content-Type": "application/octet-stream",
                    },
                    method="PUT",
                ),
                f"Error uploading file {file_path}",
            )
        )

    def _create_folder_in_sharepoint(self, token: str, folder_path: str) -> dict:
        """Create a folder in SharePoint."""
        folder_path = self._normalize_library_path(folder_path)
        site_id = self._get_site_id(token)
        drive_id = self._get_drive_id(token, site_id)

        # Split path into parent and folder name
        path_parts = folder_path.rstrip("/").split("/")
        folder_name = path_parts[-1]
        parent_path = "/".join(path_parts[:-1]) if len(path_parts) > 1 else ""

        parent_id = "root"
        if parent_path:
            parent_id = self._get_folder_id(token, drive_id, parent_path)

        create_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{parent_id}/children"

        body = json.dumps(
            {
                "name": folder_name,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "rename",
            }
        ).encode()

        return json.loads(
            self._execute_graph_request(
                lambda current_token: Request(
                    create_url,
                    data=body,
                    headers={
                        "Authorization": f"Bearer {current_token}",
                        "Content-Type": "application/json",
                    },
                ),
                f"Error creating folder {folder_path}",
            )
        )

    def _delete_file_from_sharepoint(self, token: str, file_path: str) -> None:
        """Delete a file from SharePoint."""
        file_path = self._normalize_library_path(file_path)
        site_id = self._get_site_id(token)
        drive_id = self._get_drive_id(token, site_id)
        file_id = self._get_file_id(token, drive_id, file_path)

        delete_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}"

        self._execute_graph_request(
            lambda current_token: Request(
                delete_url,
                headers={"Authorization": f"Bearer {current_token}"},
                method="DELETE",
            ),
            f"Error deleting file {file_path}",
        )


    def _ensure_directory_exists(self, token: str, drive_id: str, folder_path: str) -> None:
        """Ensure all parent directories exist, creating them if necessary."""
        folder_path = self._normalize_library_path(folder_path)
        path_parts = folder_path.rstrip("/").split("/")
        current_path = ""

        for part in path_parts:
            current_path = f"{current_path}/{part}" if current_path else part
            try:
                self._get_folder_id(token, drive_id, current_path)
            except HTTPError:
                # Folder doesn't exist, create it
                self._create_folder_in_sharepoint(token, current_path)

    def _get_site_id(self, token: str) -> str:
        """Get SharePoint site ID."""
        if self._site_id:
            return self._site_id

        if not self._site_url:
            root_site_url = "https://graph.microsoft.com/v1.0/sites/root"
            headers = {"Authorization": f"Bearer {token}"}
            req = Request(root_site_url, headers=headers)

            try:
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

        url_parts = self._site_url.replace("https://", "").split("/")
        host = url_parts[0]
        site_path = "/".join(url_parts[1:])

        site_url = f"https://graph.microsoft.com/v1.0/sites/{host}:/{site_path}"
        headers = {"Authorization": f"Bearer {token}"}
        req = Request(site_url, headers=headers)

        try:
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
            if drives_response.get("value"):
                return drives_response["value"][0]["id"]
            raise ValueError(f"Could not find library: {self._library_name}")
        except HTTPError as e:
            error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
            logger.error(f"Error getting drive ID for site {site_id}: Status {e.code}. Response: {error_body}")
            raise

    def _get_file_id(self, token: str, drive_id: str, file_path: str) -> str:
        """Get file ID by path."""
        file_path = self._normalize_library_path(file_path)
        encoded_file_path = self._encode_graph_path(file_path)
        file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_file_path}"
        headers = {"Authorization": f"Bearer {token}"}
        req = Request(file_url, headers=headers)

        try:
            file_response = json.loads(
                self._execute_graph_request(
                    lambda current_token: Request(
                        file_url,
                        headers={"Authorization": f"Bearer {current_token}"},
                    ),
                    f"Error getting file ID for '{file_path}'",
                    suppress_http_statuses={404},
                )
            )
            return file_response["id"]
        except HTTPError as e:
            if e.code != 404:
                error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
                logger.error(f"Error getting file ID for '{file_path}': Status {e.code}. Response: {error_body}")
            raise

    def _get_folder_id(self, token: str, drive_id: str, folder_path: str) -> str:
        """Get folder ID by path."""
        folder_path = self._normalize_library_path(folder_path)
        encoded_folder_path = self._encode_graph_path(folder_path)
        folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_folder_path}"
        headers = {"Authorization": f"Bearer {token}"}
        req = Request(folder_url, headers=headers)

        try:
            folder_response = json.loads(
                self._execute_graph_request(
                    lambda current_token: Request(
                        folder_url,
                        headers={"Authorization": f"Bearer {current_token}"},
                    ),
                    f"Error getting folder ID for '{folder_path}'",
                    suppress_http_statuses={404},
                )
            )
            return folder_response["id"]
        except HTTPError as e:
            if e.code != 404:
                error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
                logger.error(f"Error getting folder ID for '{folder_path}': Status {e.code}. Response: {error_body}")
            raise
