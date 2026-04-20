from datetime import datetime
import logging
from pathlib import Path
import re, os

from app.config.settings import settings
from app.agents.mail_reader_agent import MailReaderAgent


logger = logging.getLogger(__name__)

mail_agent = MailReaderAgent()

ALLOWED_EXCEL_ATTACHMENT_EXTENSIONS = {".xlsx", ".xlsm", ".csv"}
ALLOWED_EMAIL_BODY_TYPES = {"text", "html"}

TEMPLATE_PAYLOAD_PROFILES: dict[str, dict[str, object]] = {
    "AMEA_and_Europe_Billing_Files.html": {
        "subject": "AMEA and Europe Billing Files",
        "message": "AMEA and Europe Billing Files. ",
        "attachments": [
            {"path": "EMEAA/Output/EMEAA_V2.xlsx", "name": "EMEAA_NON_CROP.xlsx"},
            "APAC/APAC_Output/APAC Processing_APAC_GC_NONCROP.xlsx",
        ],
    },
    "LMS DATA Feb'26 - myLearning training billing RIR & GAF- February 2026.html": {
        "subject": "LMS DATA {month_short_yy} -myLearning training billing RIR and GAF",
        "message": "Please process the attached RIR and GAF files.",
        "attachments": [
            "APAC/APAC_GC_RIR/Output",
            "APAC/GAF_APAC_Processor/Output",
        ],
    },
    "myLearning billing files for EMEA Region -February 2026.html": {
        "subject": "myLearning billing files for EMEA Region- {month_year}",
        "message": "",
        "attachments": [
            {"path": "EMEAA/Output/EMEAA_V2.xlsx", "name": "EMEAA_NON_CROP_{month_year}.xlsx"},
            {"path": "EMEAA/Output/EMEAA_V1.xlsx", "name": "EMEAA_CROP_{month_year}.xlsx"},
            {"path": "EMEAA/Output/EMEAA_GAF.xlsx", "name": "GAF_EMEAA_NON_CROP_{month_year}.xlsx"},
        ],
    },
    "MyLearning Billing files for February 2026.html": {
        "subject": "MyLearning Billing files - {month_year}",
        "message": "Please process the attached PeopleSoft billing files and let us know if any issues.",
        "attachments": [
            "AMER/AMER_Output/CORP_BILLING_{date_suffix}.csv",
            "AMER/AMER_Output/NONCORP_BILLING_{date_suffix}.csv",
        ],
    },
    "myLearning training billing file GC - January 2026.html": {
        "subject": "myLearning training billing file GC - {month_year}",
        "message": "",
        "attachments": [
            {"path": "Region_Wise_Split/GC_*.xlsx", "name": "GC_NO_CORP_PAID_{month_year}.xlsx"},
        ],
    },
}


def _normalize_template_name(template_name: str) -> str:
    normalized = template_name.strip()
    if normalized and not normalized.endswith(".html"):
        normalized = f"{normalized}.html"
    return normalized


def _resolve_attachment_base_path(raw_path: str) -> Path:
    embedded_absolute_path = re.search(r"[A-Za-z]:[\\/].+", raw_path)
    normalized_raw_path = embedded_absolute_path.group(0) if embedded_absolute_path else raw_path

    path = Path(normalized_raw_path)
    if path.is_absolute():
        return path
    return Path(settings.output_dir) / path


def _resolve_latest_matching_file(raw_path: str, pattern: str) -> Path | None:
    path = _resolve_attachment_base_path(raw_path)
    if path.is_file() and path.match(pattern):
        return path
    if not path.is_dir():
        return None

    matching_files = sorted(
        (candidate for candidate in path.glob(pattern) if candidate.is_file()),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    if matching_files:
        return matching_files[0]
    return None


def _count_generated_rows(file_path: Path | None) -> str:
    if file_path is None or not file_path.exists():
        return "0"

    with file_path.open("r", encoding="utf-8") as file_handle:
        line_count = sum(1 for _ in file_handle)
    return str(max(line_count - 2, 0))


def _format_attachment_paths(raw_paths: list[str], now: datetime) -> list[str]:
    format_values = {
        "date_suffix": now.strftime("%m%d%Y"),
        "month_year": now.strftime("%B %Y"),
    }
    return [raw_path.format(**format_values) for raw_path in raw_paths]


def _format_attachment_entries(raw_attachments: list[object], now: datetime) -> list[object]:
    format_values = {
        "date_suffix": now.strftime("%m%d%Y"),
        "month_year": now.strftime("%B %Y"),
    }
    formatted_entries: list[object] = []

    for raw_attachment in raw_attachments:
        if isinstance(raw_attachment, str):
            formatted_entries.append(raw_attachment.format(**format_values))
            continue

        if isinstance(raw_attachment, dict):
            formatted_entry = dict(raw_attachment)
            path_value = formatted_entry.get("path")
            name_value = formatted_entry.get("name")
            if isinstance(path_value, str):
                formatted_entry["path"] = path_value.format(**format_values)
            if isinstance(name_value, str):
                formatted_entry["name"] = name_value.format(**format_values)
            formatted_entries.append(formatted_entry)
            continue

        formatted_entries.append(raw_attachment)

    return formatted_entries


def _build_template_variables(template_name: str, profile: dict[str, object], now: datetime) -> dict[str, str]:
    template_variables = {
        "recipient_name": "Team",
        "message": str(profile.get("message", "")).format(month_day=now.strftime("%B%d")),
    }

    if template_name == "MyLearning Billing files for February 2026.html":
        corp_file = _resolve_latest_matching_file("AMER/AMER_Output", "CORP_BILLING_*.csv")
        noncorp_file = _resolve_latest_matching_file("AMER/AMER_Output", "NONCORP_BILLING_*.csv")
        template_variables.update(
            {
                "server_path": str((Path(settings.output_dir) / "AMER" / "AMER_Output").resolve()),
                "corp_count": _count_generated_rows(corp_file),
                "non_corp_count": _count_generated_rows(noncorp_file),
            }
        )

    return template_variables


def _check_attachment_files_exist(raw_attachments: list[object]) -> bool:
    """Check if all attachment files exist. Returns True if all files found, False otherwise."""
    for raw_attachment in raw_attachments:
        raw_path: str | None = None

        if isinstance(raw_attachment, str):
            raw_path = raw_attachment
        elif isinstance(raw_attachment, dict):
            path_value = raw_attachment.get("path")
            if isinstance(path_value, str):
                raw_path = path_value

        if not raw_path:
            continue

        path = _resolve_attachment_base_path(raw_path)
        if raw_path.startswith("Region_Wise_Split/GC_") and any(token in raw_path for token in ("*", "?", "[")):
            matching_files = sorted(
                (candidate for candidate in path.parent.glob(path.name) if candidate.is_file()),
                key=lambda item: item.stat().st_mtime,
                reverse=True,
            )
            if not matching_files:
                return False
            continue

        if path.is_file():
            continue

        if path.is_dir():
            files_in_folder = [candidate for candidate in path.iterdir() if candidate.is_file()]
            if not files_in_folder:
                return False
            continue

        return False

    return True


def _resolve_mail_attachments(raw_attachments: list[object]) -> list[object]:
    attachments: list[object] = []
    missing_paths: list[str] = []

    for raw_attachment in raw_attachments:
        attachment_name: str | None = None
        raw_path: str | None = None

        if isinstance(raw_attachment, str):
            raw_path = raw_attachment
        elif isinstance(raw_attachment, dict):
            path_value = raw_attachment.get("path")
            name_value = raw_attachment.get("name")
            if isinstance(path_value, str):
                raw_path = path_value
            if isinstance(name_value, str):
                attachment_name = name_value

        if not raw_path:
            missing_paths.append(str(raw_attachment))
            continue

        path = _resolve_attachment_base_path(raw_path)
        if raw_path.startswith("Region_Wise_Split/GC_") and any(token in raw_path for token in ("*", "?", "[")):
            matching_files = sorted(
                (candidate for candidate in path.parent.glob(path.name) if candidate.is_file()),
                key=lambda item: item.stat().st_mtime,
                reverse=True,
            )
            if matching_files:
                resolved_path = str(matching_files[0])
                attachments.append({"path": resolved_path, "name": attachment_name} if attachment_name else resolved_path)
            else:
                missing_paths.append(str(path))
            continue

        if path.is_file():
            resolved_path = str(path)
            attachments.append({"path": resolved_path, "name": attachment_name} if attachment_name else resolved_path)
            continue

        if path.is_dir():
            files_in_folder = [
                candidate
                for candidate in sorted(
                    path.iterdir(),
                    key=lambda item: item.stat().st_mtime,
                    reverse=True,
                )
                if candidate.is_file()
            ]
            if files_in_folder:
                for candidate in files_in_folder:
                    resolved_path = str(candidate)
                    attachments.append({"path": resolved_path, "name": attachment_name} if attachment_name else resolved_path)
            else:
                missing_paths.append(str(path))
            continue

        missing_paths.append(str(path))

    if missing_paths:
        missing_summary = ", ".join(missing_paths)
        raise FileNotFoundError(f"Attachment path(s) not found: {missing_summary}")

    if not attachments:
        raise FileNotFoundError("No attachments resolved for this template.")

    return attachments


def _build_payload(template_name: str, overrides: dict | None = None) -> dict:
    now = datetime.now()
    profile = TEMPLATE_PAYLOAD_PROFILES.get(template_name)
    if profile is None:
        raise ValueError(f"Unsupported mail template: {template_name}")

    overrides = overrides or {}
    
    attachment_entries = overrides.get("attachment_paths")
    if not isinstance(attachment_entries, list):
        attachment_entries = profile.get("attachments") or settings.amea_europe_mail_attachments
    attachment_entries = _format_attachment_entries(list(attachment_entries), now)
    
    # Check if all attachment files exist before building payload
    if not _check_attachment_files_exist(attachment_entries):
        raise FileNotFoundError(f"Required attachment files do not exist for template: {template_name}")
    
    subject_format_values = {
        "date_suffix": now.strftime("%m%d%Y"),
        "month_year": now.strftime("%B %Y"),
        "month_short_yy": now.strftime("%b'%y"),
    }
    subject_template = str(overrides.get("subject") or profile.get("subject") or settings.amea_europe_mail_subject)
    subject = subject_template.format(**subject_format_values)
    subject = f"{subject} - {now.strftime('%B %Y')}"

    template_variables = _build_template_variables(template_name, profile, now)
    raw_template_variables = overrides.get("template_variables")
    if isinstance(raw_template_variables, dict):
        template_variables.update({key: str(value) for key, value in raw_template_variables.items()})

    return {
        "from": overrides.get("from") or settings.amea_europe_mail_from,
        "to": overrides.get("to") or settings.amea_europe_mail_to,
        "cc": overrides.get("cc") or settings.amea_europe_mail_cc,
        "subject": subject,
        "template_name": template_name,
        "template_variables": template_variables,
        "body_type": overrides.get("body_type") or settings.amea_europe_mail_body_type,
        "attachments": _resolve_mail_attachments(attachment_entries),
    }


def _normalize_email_attachments(raw_attachments) -> list[dict] | None:
    if raw_attachments in (None, []):
        return None
    if not isinstance(raw_attachments, list):
        raise ValueError("'attachments' must be a list when provided.")

    normalized_attachments: list[dict] = []
    for idx, att in enumerate(raw_attachments):
        if isinstance(att, str):
            path = att.strip()
            name = os.path.basename(path)
        elif isinstance(att, dict):
            path = att.get("path")
            name = att.get("name")
            if name is None and isinstance(path, str):
                name = os.path.basename(path)
        else:
            raise ValueError(f"Attachment at index {idx} must be a path string or an object.")

        if not isinstance(path, str) or not path.strip():
            raise ValueError(f"Attachment at index {idx} is missing a valid 'path'.")
        if not os.path.isfile(path):
            raise ValueError(f"Attachment file not found: {path}")

        actual_name = os.path.basename(path)
        actual_ext = os.path.splitext(actual_name)[1].lower()
        if actual_ext not in ALLOWED_EXCEL_ATTACHMENT_EXTENSIONS:
            raise ValueError("Only Excel read/write attachment files are supported (.xlsx, .xlsm, .csv).")

        if name is None:
            name = actual_name
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"Attachment at index {idx} is missing a valid 'name'.")

        provided_ext = os.path.splitext(name)[1].lower()
        if provided_ext != actual_ext:
            raise ValueError(
                f"Attachment at index {idx} must use the exact file extension {actual_ext}."
            )

        normalized_attachments.append({"name": name, "path": path})

    return normalized_attachments

def _normalize_email_addresses(value, field_name: str, *, required: bool = False) -> list[str] | None:
    if value in (None, "", []):
        if required:
            raise ValueError(f"'{field_name}' must be a non-empty email address or list of email addresses.")
        return None

    if isinstance(value, str):
        normalized = [value.strip()] if value.strip() else []
    elif isinstance(value, list):
        normalized = []
        for idx, item in enumerate(value):
            if not isinstance(item, str) or not item.strip():
                raise ValueError(
                    f"'{field_name}' item at index {idx} must be a non-empty email address string."
                )
            normalized.append(item.strip())
    else:
        raise ValueError(f"'{field_name}' must be an email address string or a list of email addresses.")

    if required and not normalized:
        raise ValueError(f"'{field_name}' must be a non-empty email address or list of email addresses.")
    return normalized or None


def get_available_mail_payload_templates() -> list[str]:
    return sorted(TEMPLATE_PAYLOAD_PROFILES)


def process_mail_for_post_validation_billing(mail):
    """Build an email payload for one of the configured HTML templates.
    
    Returns:
        - Email payload dict if successful
        - {"skipped": True, "reason": str} if template is skipped (files don't exist)
        - {"error": str} if an error occurs
    """
    try:
        overrides = mail if isinstance(mail, dict) else {}
        requested_template = overrides.get("template_name") or settings.amea_europe_mail_template_name
        template_name = _normalize_template_name(str(requested_template))
        return _build_payload(template_name=template_name, overrides=overrides)
    except FileNotFoundError as exc:
        logger.info("Skipping mail template: %s", exc)
        return {"skipped": True, "reason": str(exc)}
    except Exception as exc:
        logger.error("Error processing mail: %s", exc)
        return {"error": str(exc)}
    


def post_validation_send_email():
    """Send post-validation emails for all templates one by one."""

    templates_to_send = get_available_mail_payload_templates()

    if not templates_to_send:
        return ({"error": "No templates available to send."}, 400)

    sent_templates: list[str] = []
    failed_templates: list[dict[str, str]] = []

    for template_name in templates_to_send:
        payload = process_mail_for_post_validation_billing({"template_name": template_name})

        if isinstance(payload, dict) and payload.get("error"):
            failed_templates.append({"template": template_name, "error": str(payload.get("error"))})
            continue

        try:
            attachments = _normalize_email_attachments(payload.get("attachments"))
            to_addresses = _normalize_email_addresses(payload.get("to"), "to", required=True)
            cc_addresses = _normalize_email_addresses(payload.get("cc"), "cc")
        except ValueError as exc:
            failed_templates.append({"template": template_name, "error": str(exc)})
            continue

        try:
            template_variables = payload.get("template_variables")
            if not isinstance(template_variables, dict):
                template_variables = {}

            mail_agent.send_email(
                to_addresses=to_addresses,
                subject=str(payload.get("subject", "")).strip(),
                body=None,
                body_type=str(payload.get("body_type") or "html").lower(),
                template_name=str(payload.get("template_name") or template_name),
                template_variables=template_variables,
                from_address=str(payload.get("from", "")).strip() or None,
                recipient_name=str(template_variables.get("recipient_name") or "Team"),
                message=str(template_variables.get("message") or ""),
                cc_addresses=cc_addresses,
                attachments=attachments,
            )
            sent_templates.append(template_name)
        except Exception as exc:
            logger.error("post_validation_send_email failed for template %s: %s", template_name, exc)
            failed_templates.append({"template": template_name, "error": str(exc)})

    
    return {
        "status": "completed" if sent_templates else "failed",
        "sent_count": len(sent_templates),
        "failed_count": len(failed_templates),
            "sent_templates": sent_templates,
                "failed_templates": failed_templates,
            } 





def send_text_email():
    try:
        mail_agent.send_email(
            to_addresses=["GWZ_IA_RPA@ihg.com"],
            cc_addresses=["sono.pathak2@ihg.com"],
            subject=f"IHG University Post Validation check Initiated - {datetime.now().strftime('%B %Y')}",
            body="Hi Team,\n\nWe have received a response from the Business, and the Agent has initiated post-validation checks.\n\n\n\nBest regards,\nGenWizard Automation Team ",
            body_type="text"
        )

        return {
            "status": "success"
        }

    except Exception as exc:
        logger.error("post_validation_send_email failed: %s", exc)
        return {
            "status": "failed",
            "error": str(exc)
        }
        