from __future__ import annotations

from base64 import b64decode, b64encode
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)
from app.services.base import EmailMessage, MailboxClient




class MicrosoftGraphMailboxClient(MailboxClient):
    """Mailbox client backed by Microsoft Graph with safe local fallback."""

    def __init__(
        self,
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        mailbox_user: str | None = None,
        mailbox_password: str | None = None,
        timeout_seconds: int = 20,
    ) -> None:
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._mailbox_user = mailbox_user
        self._mailbox_password = mailbox_password
        self._timeout_seconds = timeout_seconds
        self._token: str | None = None
        self._token_expires_at: datetime | None = None

        self._graph_enabled = all(
            [self._tenant_id, self._client_id, self._client_secret, self._mailbox_user, self._mailbox_password]
        )
        # self._graph_enabled = False  # Temporarily disabled — using local fallback
        self._job_title_cache: dict[str, str | None] = {}  # sender_email → jobTitle (per-run cache)
        self._job_title_lookup_available = True  # set False on first 403 (missing User.ReadBasic.All)

        self._emails = [
            # ── Rule-matched emails (keywords: invoice/payment/reimbursement/billing,
            #    meeting/lunch/standup, offer/sale/discount) ──────────────────────────
            # EmailMessage(
            #     id="1",
            #     subject="Invoice for March",
            #     body="Please process payment for invoice INV-3021 by end of month.",
            #     sender="billing@vendor.com",
            #     received_at=datetime.now(timezone.utc),
            # ),
            
        ]

    def fetch_unread(self, limit: int = 25) -> list[EmailMessage]:
        if not self._graph_enabled:
            return self._emails[:limit]

        mailbox = quote(self._mailbox_user or "", safe="@.-_")
        endpoint = (
            f"/users/{mailbox}/mailFolders/inbox/messages"
            f"?$filter=isRead%20eq%20false"
            "&$select=id,subject,bodyPreview,from,receivedDateTime,hasAttachments"
            "&$orderby=receivedDateTime%20desc"
        )
        payload = self._graph_get(endpoint)
        values = payload.get("value", [])
        emails: list[EmailMessage] = []
        for item in values:
            sender = (
                item.get("from", {})
                .get("emailAddress", {})
                .get("address", "unknown@unknown")
            )
            sender_name = (
                item.get("from", {})
                .get("emailAddress", {})
                .get("name", "")
            )
            # Enrich sender_name with Azure AD job title so VIP detection
            # can match titles like "VP Engineering" even when the display
            # name in the email header doesn't include a title.
            job_title = None
            try:
                job_title = self._get_sender_job_title(sender)
            except Exception:
                job_title = None
            if job_title and job_title.lower() not in sender_name.lower():
                sender_name = f"{sender_name}, {job_title}" if sender_name else job_title

            attachment_paths: tuple[str, ...] = ()
            if item.get("hasAttachments") and item.get("id"):
                try:
                    attachment_paths = tuple(
                        self._download_message_attachments(item.get("id", ""))
                    )
                except Exception as exc:
                    logger.warning(
                        "Attachment download failed for message %s: %s",
                        item.get("id", ""),
                        exc,
                    )

            emails.append(
                EmailMessage(
                    id=item.get("id", ""),
                    subject=item.get("subject") or "(no subject)",
                    body=item.get("bodyPreview") or "",
                    sender=sender,
                    received_at=self._parse_graph_datetime(item.get("receivedDateTime")),
                    sender_name=sender_name,
                    attachment_paths=attachment_paths,
                )
            )
        return emails

    def _download_message_attachments(self, message_id: str) -> list[str]:
        mailbox = quote(self._mailbox_user or "", safe="@.-_")
        endpoint = (
            f"/users/{mailbox}/messages/{quote(message_id, safe='')}/attachments"
            "?$select=id,name,contentType,@odata.type,contentBytes"
        )
        payload = self._graph_get(endpoint)
        values = payload.get("value", [])

        # Keep inbound attachments separate from regular data/processed files.
        attachment_dir = Path("email_attachments")
        attachment_dir.mkdir(parents=True, exist_ok=True)

        saved_paths: list[str] = []
        for item in values:
            if item.get("@odata.type") != "#microsoft.graph.fileAttachment":
                continue

            content_b64 = item.get("contentBytes")
            if not content_b64:
                continue

            raw_name = item.get("name") or "attachment.bin"
            safe_name = Path(raw_name).name
            target_path = self._next_available_path(attachment_dir / safe_name)
            target_path.write_bytes(b64decode(content_b64))
            saved_paths.append(str(target_path))

        return saved_paths

    @staticmethod
    def _next_available_path(path: Path) -> Path:
        if not path.exists():
            return path
        stem = path.stem
        suffix = path.suffix
        index = 1
        while True:
            candidate = path.parent / f"{stem}_{index}{suffix}"
            if not candidate.exists():
                return candidate
            index += 1

    def reply_email(
        self,
        email_id: str,
        body: str,
        cc_addresses: list[str] | None = None,
    ) -> None:
        if not self._graph_enabled:
            cc_display = ", ".join(cc_addresses or []) or "(none)"
            print(f"[mailbox] reply to email {email_id} | cc: {cc_display}")
            return

        mailbox = quote(self._mailbox_user or "", safe="@.-_")
        endpoint = f"/users/{mailbox}/messages/{quote(email_id, safe='')}/reply"
        payload: dict = {
            "message": {
                "body": {
                    "contentType": "HTML",
                    "content": body,
                },
                "ccRecipients": [
                    {"emailAddress": {"address": addr}}
                    for addr in (cc_addresses or [])
                ]
            }
        }
        self._graph_post(endpoint, payload)

    def send_email(
        self,
        to_addresses: list[str],
        subject: str,
        body: str,
        cc_addresses: list[str] | None = None,
        attachments: list[dict] | None = None,
    ) -> None:
        """Send a new email with an HTML body and optional file attachments.

        Parameters
        ----------
        to_addresses:
            List of recipient email addresses.
        subject:
            Email subject line.
        body:
            HTML string for the email body (pass a rendered Jinja2 template here).
        cc_addresses:
            Optional CC recipients.
        attachments:
            Optional list of dicts with keys ``name`` (filename shown to recipient)
            and ``path`` (absolute or relative path to the file on disk).
            Example::

                [{"name": "invoice.pdf", "path": "/tmp/invoice.pdf"}]

        Notes
        -----
        Requires ``Mail.ReadWrite`` and ``Mail.Send`` Graph API permissions.
        When Graph is not configured the call prints a local fallback line.
        """
        if not self._graph_enabled:
            cc_display = ", ".join(cc_addresses or []) or "(none)"
            att_names = ", ".join(a["name"] for a in (attachments or [])) or "(none)"
            print(
                f"[mailbox] send email to {to_addresses} | subject: {subject!r} "
                f"| cc: {cc_display} | attachments: {att_names}"
            )
            return

        mailbox = quote(self._mailbox_user or "", safe="@.-_")

        # Step 1 — create a draft message
        draft_payload: dict = {
            "subject": subject,
            "body": {"contentType": "HTML", "content": body},
            "toRecipients": [
                {"emailAddress": {"address": addr}} for addr in to_addresses
            ],
            "ccRecipients": [
                {"emailAddress": {"address": addr}} for addr in (cc_addresses or [])
            ],
        }
        draft = self._graph_post(f"/users/{mailbox}/messages", draft_payload)
        draft_id = draft.get("id")
        if not draft_id:
            raise RuntimeError("Creating draft message did not return a message id.")

        draft_base = f"/users/{mailbox}/messages/{quote(draft_id, safe='')}"

        # Step 2 — attach files (base64-encoded)
        for att in (attachments or []):
            file_bytes = Path(att["path"]).read_bytes()
            self._graph_post(
                f"{draft_base}/attachments",
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": att["name"],
                    "contentBytes": b64encode(file_bytes).decode("ascii"),
                },
            )

        # Step 3 — send the draft
        self._graph_post(f"{draft_base}/send", {})
        logger.info("Email sent to %s | subject: %s", to_addresses, subject)

    # ------------------------------------------------------------------ #
    # Graph Change Notifications (Webhook subscription management)        #
    # ------------------------------------------------------------------ #

    def register_webhook_subscription(
        self,
        notification_url: str,
        client_state: str,
    ) -> dict:
        """Create a Graph change-notification subscription for new inbox messages.

        Returns the subscription dict from Graph (contains 'id' and 'expirationDateTime').
        Raises RuntimeError ifs Graph is not enabled or the call fails.

        Required permissions: Mail.Read (delegated) — already in your token scope.
        Max subscription lifetime for mail: 4230 minutes (~2.9 days). Renew before expiry.
        """
        if not self._graph_enabled:
            raise RuntimeError("Graph client is not configured.")

        mailbox = quote(self._mailbox_user or "", safe="@.-_")
        from datetime import timedelta

        expiry = (
            datetime.now(timezone.utc) + timedelta(minutes=4230)
        ).strftime("%Y-%m-%dT%H:%M:%S.0000000Z")

        payload = {
            "changeType": "created",
            "notificationUrl": notification_url,
            "resource": f"/users/{mailbox}/mailFolders/inbox/messages",
            "expirationDateTime": expiry,
            "clientState": client_state,
        }
        subscription = self._graph_post("/subscriptions", payload)
        logger.info(
            "Graph webhook subscription created: id=%s expires=%s",
            subscription.get("id"),
            subscription.get("expirationDateTime"),
        )
        return subscription

    def renew_webhook_subscription(
        self,
        subscription_id: str,
        client_state: str,
    ) -> dict:
        """Extend the expiry of an existing subscription to avoid it expiring."""
        if not self._graph_enabled:
            raise RuntimeError("Graph client is not configured.")

        from datetime import timedelta

        expiry = (
            datetime.now(timezone.utc) + timedelta(minutes=4230)
        ).strftime("%Y-%m-%dT%H:%M:%S.0000000Z")

        token = self._get_access_token()
        url = f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
        body = json.dumps({"expirationDateTime": expiry}).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        request = Request(url, data=body, headers=headers, method="PATCH")
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                result = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Subscription renewal failed: {detail}") from exc
        logger.info("Graph webhook subscription renewed: id=%s", subscription_id)
        return result

    def _get_access_token(self) -> str:
        if not self._graph_enabled:
            raise RuntimeError("Graph client is not configured.")

        now = datetime.now(timezone.utc)
        if self._token and self._token_expires_at and now < self._token_expires_at:
            return self._token

        token_url = (
            f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/token"
        )
        body = urlencode(
            {   
                "username": self._mailbox_user,
                "password": self._mailbox_password,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "password",
            }
        ).encode("utf-8")
        request = Request(
            token_url,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Token request failed: {detail}") from exc

        token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 3600))
        if not token:
            raise RuntimeError("Token response did not include access_token.")

        self._token = token
        self._token_expires_at = now + timedelta(seconds=max(60, expires_in - 60))
        return token

    def _get_sender_job_title(self, sender_email: str) -> str | None:
        """Look up the Azure AD job title for an internal sender.

        Returns None silently when:
        - Graph is disabled (local fallback mode)
        - The token lacks User.ReadBasic.All (403) — disables feature for the run
        - Sender is external to the tenant (404)

        Requires: User.ReadBasic.All or User.Read.All delegated/application permission.
        Current tokens with only User.Read can only read the signed-in user's own profile.
        """
        if not self._graph_enabled or not self._job_title_lookup_available:
            return None
        if sender_email in self._job_title_cache:
            return self._job_title_cache[sender_email]
        try:
            encoded = quote(sender_email, safe="@.-_")
            data = self._graph_get(f"/users/{encoded}?$select=jobTitle")
            title = data.get("jobTitle") or None
        except RuntimeError as exc:
            cause = exc.__cause__
            if hasattr(cause, "code") and cause.code == 403:
                # Token has User.Read only — cannot look up other users.
                # Disable for the rest of this run to avoid 403s on every email.
                self._job_title_lookup_available = False
                logger.warning(
                    "Graph job title lookup disabled: token is missing "
                    "'User.ReadBasic.All' permission. "
                    "Grant this delegated permission in Azure AD to enable VIP "
                    "detection via Azure AD job titles. "
                    "VIP detection via sender display name and email body signature "
                    "will continue to work."
                )
                return None
            # 404 = external sender not in the tenant directory — skip silently
            title = None
        self._job_title_cache[sender_email] = title
        return title

    def _graph_get(self, endpoint: str) -> dict:
        return self._graph_request("GET", endpoint)

    def _graph_post(self, endpoint: str, payload: dict) -> dict:
        return self._graph_request("POST", endpoint, payload)

    def _graph_request(self, method: str, endpoint: str, payload: dict | None = None) -> dict:
        token = self._get_access_token()
        url = f"https://graph.microsoft.com/v1.0{endpoint}"
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        if payload is not None:
            headers["Content-Type"] = "application/json"

        request = Request(url, data=body, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                raw = response.read()
                if not raw:
                    return {}
                return json.loads(raw.decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Graph API request failed ({method} {endpoint}): {detail}") from exc

    @staticmethod
    def _parse_graph_datetime(value: str | None) -> datetime:
        if not value:
            return datetime.now(timezone.utc)
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            return datetime.now(timezone.utc)
