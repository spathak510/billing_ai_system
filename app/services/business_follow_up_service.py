from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote, urlencode

from dotenv import load_dotenv

from app.services.mail_service import MicrosoftGraphMailboxClient

load_dotenv()

logger = logging.getLogger(__name__)

_SUBJECT_PREFIX_PATTERN = re.compile(r"^(?:(?:re|fw|fwd)\s*:\s*)+", re.IGNORECASE)
_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")


def _build_graph_client() -> MicrosoftGraphMailboxClient:
	return MicrosoftGraphMailboxClient(
		tenant_id=os.getenv("GRAPH_TENANT_ID"),
		client_id=os.getenv("GRAPH_CLIENT_ID"),
		client_secret=os.getenv("GRAPH_CLIENT_SECRET"),
		mailbox_user=os.getenv("GRAPH_MAILBOX_USER"),
		mailbox_password=os.getenv("GRAPH_MAILBOX_PASSWORD"),
		timeout_seconds=int(os.getenv("GRAPH_TIMEOUT_SECONDS", "20")),
	)


def _canonical_subject(subject: str) -> str:
	normalized = _SUBJECT_PREFIX_PATTERN.sub("", (subject or "").strip().casefold())
	return _NON_ALNUM_PATTERN.sub("", normalized)


def _is_reply_like_subject(subject: str) -> bool:
	return bool(_SUBJECT_PREFIX_PATTERN.match((subject or "").strip()))


def _select_latest_matching_message(messages: list[dict[str, Any]], subject: str) -> dict[str, Any] | None:
	expected_subject = _canonical_subject(subject)
	matching_messages = [
		item
		for item in messages
		if _canonical_subject(str(item.get("subject") or "")) == expected_subject
	]
	if not matching_messages:
		return None

	conversations: dict[str, list[dict[str, Any]]] = {}
	for item in matching_messages:
		conversation_key = str(item.get("conversationId") or item.get("id") or "")
		conversations.setdefault(conversation_key, []).append(item)

	def _message_timestamp(item: dict[str, Any]) -> str:
		return str(item.get("sentDateTime") or item.get("receivedDateTime") or "")

	target_conversation = max(
		conversations.values(),
		key=lambda items: max(_message_timestamp(item) for item in items),
	)
	original_candidates = [
		item
		for item in target_conversation
		if not _is_reply_like_subject(str(item.get("subject") or ""))
	]
	if original_candidates:
		return min(original_candidates, key=_message_timestamp)

	return min(target_conversation, key=_message_timestamp)


def _find_original_message(
	client: MicrosoftGraphMailboxClient,
	*,
	mailbox: str,
	subject: str,
	lookback_days: int,
) -> dict[str, Any] | None:
	folder_name = "inbox"
	endpoint = _build_messages_endpoint(mailbox, folder_name, days_back=lookback_days)
	messages = client._graph_get_collection(endpoint)
	return _select_latest_matching_message(messages, subject)


def _default_reminder_body(original_subject: str) -> str:
	return (
		"<p>Hello Team,</p>"
		f"<p>This is a reminder for the email subject <strong>{original_subject}</strong>.</p>"
		"<p>Please review the monthly billing validation and share your response at your earliest convenience.</p>"
		"<p>Regards,<br>Billing AI System</p>"
	)


def _build_messages_endpoint(mailbox: str, folder_name: str, *, days_back: int) -> str:
	since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
	query = urlencode(
		{
			"$select": "id,subject,conversationId,sentDateTime,receivedDateTime,from,toRecipients,ccRecipients",
			"$filter": f"receivedDateTime ge {since}" if folder_name.lower() != "sentitems" else f"sentDateTime ge {since}",
			"$orderby": "receivedDateTime desc" if folder_name.lower() != "sentitems" else "sentDateTime desc",
			"$top": "100",
		}
	)
	return f"/users/{mailbox}/mailFolders/{folder_name}/messages?{query}"


def _has_reply_in_conversation(
	client: MicrosoftGraphMailboxClient,
	*,
	mailbox: str,
	conversation_id: str,
	sent_at: datetime,
	original_sender: str,
	only_unread: bool,
) -> dict[str, Any] | None:
	query = urlencode(
		{
			"$select": "id,subject,conversationId,receivedDateTime,from,isRead,bodyPreview",
			"$filter": f"conversationId eq '{conversation_id}'",
			"$top": "50",
		}
	)
	endpoint = f"/users/{mailbox}/mailFolders/inbox/messages?{query}"
	mailbox_address = (client._mailbox_user or "").strip().casefold()
	original_sender_address = (original_sender or "").strip().casefold()
	reply_candidates = sorted(
		client._graph_get_collection(endpoint),
		key=lambda item: str(item.get("receivedDateTime") or ""),
		reverse=True,
	)

	for item in reply_candidates:
		if only_unread and bool(item.get("isRead")):
			continue
		sender = (
			item.get("from", {})
			.get("emailAddress", {})
			.get("address", "")
			.strip()
			.casefold()
		)
		if original_sender_address and sender == original_sender_address:
			continue
		received_raw = item.get("receivedDateTime")
		received_at = client._parse_graph_datetime(received_raw) if received_raw else None
		if sender and sender != mailbox_address and received_at and received_at > sent_at:
			return item
	return None


def _send_threaded_reminder(
	client: MicrosoftGraphMailboxClient,
	*,
	mailbox: str,
	original_message_id: str,
	reminder_body: str,
) -> str:
	message_key = quote(original_message_id, safe="")
	create_reply_endpoint = f"/users/{mailbox}/messages/{message_key}/createReplyAll"
	draft = client._graph_post(create_reply_endpoint, {})
	draft_id = draft.get("id")
	if not draft_id:
		raise RuntimeError("Graph createReplyAll did not return a draft message id.")

	draft_key = quote(draft_id, safe="")
	client._graph_patch(
		f"/users/{mailbox}/messages/{draft_key}",
		{
			"body": {
				"contentType": "HTML",
				"content": reminder_body,
			}
		},
	)
	client._graph_post(f"/users/{mailbox}/messages/{draft_key}/send", {})
	return draft_id


def send_validation_reply_reminder(
	subject: str,
	reminder_body: str | None = None,
	*,
	lookback_days: int = 2,
	only_unread_replies: bool = True,
) -> dict[str, Any]:
	"""Send a reminder in the existing mail thread when no reply is found.

	The function looks for the latest inbox email whose subject matches the
	provided subject after normalizing common reply/forward prefixes and
	punctuation. It then checks the inbox for any later message in the same
	conversation. By default it only checks unread replies from the last
	2-day inbox search window. If no reply is found, it sends a reminder
	into the same thread using Microsoft Graph.
	"""
	if not subject or not subject.strip():
		raise ValueError("subject is required")

	try:
		client = _build_graph_client()
		if not client._graph_enabled:
			return {
				"status": "skipped",
				"message": "Graph client is not configured.",
				"subject": subject,
				"reply_found": False,
				"reminder_sent": False,
			}

		mailbox = quote(client._mailbox_user or "", safe="@.-_")
		original_message = _find_original_message(
			client,
			mailbox=mailbox,
			subject=subject,
			lookback_days=lookback_days,
		)

		if not original_message:
			return {
				"status": "not_found",
				"message": "No matching email was found in inbox.",
				"subject": subject,
				"reply_found": False,
				"reminder_sent": False,
			}

		conversation_id = str(original_message.get("conversationId") or "").strip()
		sent_at_raw = original_message.get("sentDateTime") or original_message.get("receivedDateTime")
		sent_at = client._parse_graph_datetime(sent_at_raw)
		original_sender = (
			original_message.get("from", {})
			.get("emailAddress", {})
			.get("address", "")
		)

		if not conversation_id:
			raise RuntimeError("The inbox email does not include a conversationId.")

		reply_message = _has_reply_in_conversation(
			client,
			mailbox=mailbox,
			conversation_id=conversation_id,
			sent_at=sent_at,
			original_sender=original_sender,
			only_unread=only_unread_replies,
		)
		if reply_message:
			reply_sender = (
				reply_message.get("from", {})
				.get("emailAddress", {})
				.get("address", "")
			)
			return {
				"status": "reply_found",
				"message": "A reply was found in the existing thread.",
				"subject": str(original_message.get("subject") or subject),
				"reply_found": True,
				"reminder_sent": False,
				"reply_message_id": reply_message.get("id"),
				"reply_subject": reply_message.get("subject"),
				"reply_sender": reply_sender,
				"reply_body_preview": reply_message.get("bodyPreview", ""),
				"reply_received_at": reply_message.get("receivedDateTime"),
			}

		body = reminder_body or _default_reminder_body(str(original_message.get("subject") or subject))
		draft_id = _send_threaded_reminder(
			client,
			mailbox=mailbox,
			original_message_id=str(original_message.get("id") or ""),
			reminder_body=body,
		)
		logger.info("Reminder email sent for subject '%s' in conversation %s", subject, conversation_id)
		return {
			"status": "reminder_sent",
			"message": "No reply was found. Reminder sent successfully.",
			"subject": str(original_message.get("subject") or subject),
			"reply_found": False,
			"reminder_sent": True,
			"original_folder": "inbox",
			"conversation_id": conversation_id,
			"original_message_id": original_message.get("id"),
			"reminder_draft_id": draft_id,
		}
	except Exception as exc:
		logger.exception("send_validation_reply_reminder failed for subject '%s'", subject)
		return {
			"status": "error",
			"message": f"Follow-up check failed: {exc}",
			"subject": subject,
			"reply_found": False,
			"reminder_sent": False,
		}
