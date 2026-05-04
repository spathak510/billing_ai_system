from app.agents.mail_reader_agent import MailReaderAgent
from app.config.settings import settings
import logging
import traceback
import os

def send_error_notification(subject: str, error: Exception, context: str = ""):
    """Send an error notification email to the admin(s)."""
    try:
        mail_agent = MailReaderAgent()
        admin_emails = settings.error_notifications_mail  # List of main recipients
        cc_emails = getattr(settings, 'error_notifications_cc', None)  # Optional CC list
        tb = traceback.format_exc()
        body = f"""
        <h2>Error Notification</h2>
        <b>Context:</b> {context}<br>
        <b>Exception:</b> {str(error)}<br>
        <b>Traceback:</b><pre>{tb}</pre>
        <b>Host:</b> {os.uname() if hasattr(os, 'uname') else os.getenv('COMPUTERNAME', 'Unknown')}<br>
        """
        mail_agent.send_email(
            to_addresses=admin_emails,
            subject=subject,
            body=body,
            body_type="html",
            cc_addresses=cc_emails,
        )
    except Exception as exc:
        logging.getLogger(__name__).error(f"Failed to send error notification: {exc}")
