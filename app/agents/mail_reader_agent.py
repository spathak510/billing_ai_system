from __future__ import annotations

import os
from pathlib import Path

from app.services.mail_service import MicrosoftGraphMailboxClient


class MailReaderAgent:
	"""Orchestrates mailbox workflows while delegating Graph calls to service."""

	def __init__(self, client: MicrosoftGraphMailboxClient | None = None) -> None:
		self._client = client or MicrosoftGraphMailboxClient(
			tenant_id=os.getenv("GRAPH_TENANT_ID"),
			client_id=os.getenv("GRAPH_CLIENT_ID"),
			client_secret=os.getenv("GRAPH_CLIENT_SECRET"),
			mailbox_user=os.getenv("GRAPH_MAILBOX_USER"),
			mailbox_password=os.getenv("GRAPH_MAILBOX_PASSWORD"),
			timeout_seconds=int(os.getenv("GRAPH_TIMEOUT_SECONDS", "20")),
		)

	def fetch_unread(self, limit: int = 25):
		return self._client.fetch_unread(limit=limit)

	def send_email(
		self,
		to_addresses: list[str],
		subject: str,
		recipient_name: str = "Team",
		message: str = "",
		cc_addresses: list[str] | None = None,
		attachments: list[dict] | None = None,
	) -> None:
		template_path = Path(__file__).resolve().parents[1] / "templates" / "email_body.html"
		html_body = template_path.read_text(encoding="utf-8")
		html_body = (
			html_body.replace("{{subject}}", subject)
			.replace("{{recipient_name}}", recipient_name)
			.replace("{{message}}", message)
		)

		self._client.send_email(
			to_addresses=to_addresses,
			subject=subject,
			body=html_body,
			cc_addresses=cc_addresses,
			attachments=attachments,
		)

	def reply_email(
		self,
		email_id: str,
		body: str,
		cc_addresses: list[str] | None = None,
	) -> None:
		self._client.reply_email(email_id=email_id, body=body, cc_addresses=cc_addresses)
