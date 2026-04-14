import logging

from app.config.settings import settings


logger = logging.getLogger(__name__)


def process_mail_for_amea_and_europe_billing(mail):
    """Process the given mail object and extract the relevant information."""
    try:
        payload = {
            "from": settings.amea_europe_mail_from,
            "to": settings.amea_europe_mail_to,
            "cc": settings.amea_europe_mail_cc,
            "subject": settings.amea_europe_mail_subject,
            "template_name": settings.amea_europe_mail_template_name,
            "template_variables": {
                "recipient_name": settings.amea_europe_mail_recipient_name,
                "message": settings.amea_europe_mail_message,
            },
            "body_type": settings.amea_europe_mail_body_type,
            "attachments": settings.amea_europe_mail_attachments,
        }
        return payload
    except Exception as exc:
        logger.error("Error processing mail: %s", exc)
        return {"error": str(exc)}