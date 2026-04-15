from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime

@dataclass(frozen=True)
class EmailMessage:
    id: str
    subject: str
    body: str
    sender: str
    received_at: datetime.datetime
    sender_name: str = ""
    attachment_paths: tuple[str, ...] = ()


class MailboxClient(ABC):
    @abstractmethod
    def fetch_unread(
        self,
        limit: int = 25,
        attachment_dir: str | None = None,
    ) -> list[EmailMessage]:
        raise NotImplementedError

    @abstractmethod
    def reply_email(
        self,
        email_id: str,
        body: str,
        cc_addresses: list[str] | None = None,
    ) -> None:
        """Send a reply to the given email, optionally CC-ing additional recipients."""
        raise NotImplementedError
