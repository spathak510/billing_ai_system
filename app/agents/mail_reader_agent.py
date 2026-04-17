from __future__ import annotations

import os
from pathlib import Path

from app.services.mail_service import MicrosoftGraphMailboxClient
from dotenv import load_dotenv
load_dotenv()

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

	def fetch_unread(
		self,
		limit: int = 25,
		attachment_dir: str | None = None,
		subject: str | None = None,
	):
		return self._client.fetch_unread(limit=limit, attachment_dir=attachment_dir, subject=subject)

	def send_email(
		self,
		to_addresses: list[str],
		subject: str,
		body: str | None = None,
		body_type: str = "text",
		template_name: str | None = None,
		template_variables: dict | None = None,
		from_address: str | None = None,
		recipient_name: str = "Team",
		message: str = "",
		cc_addresses: list[str] | None = None,
		attachments: list[dict] | None = None,
	) -> None:
		rendered_body = body or ""
		rendered_body_type = body_type.upper()

		selected_template = template_name
		if not rendered_body and not selected_template:
			selected_template = "Monthly_report_validation.html"

		if selected_template:
			template_path = Path(__file__).resolve().parents[1] / "templates" / selected_template
			html_body = template_path.read_text(encoding="utf-8")
			merged_variables = {
				"subject": subject,
				"recipient_name": recipient_name,
				"message": message,
			}
			if template_variables:
				merged_variables.update({key: str(value) for key, value in template_variables.items()})

			for key, value in merged_variables.items():
				html_body = html_body.replace(f"{{{{{key}}}}}", value)

			rendered_body = html_body
			rendered_body_type = "HTML"

		self._client.send_email(
			to_addresses=to_addresses,
			subject=subject,
			body=rendered_body,
			body_content_type=rendered_body_type,
			from_address=from_address,
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
