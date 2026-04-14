from __future__ import annotations

import os
from typing import Any

import requests


DEFAULT_SERVICENOW_URL = "https://ihguat.service-now.com/api/x_ihgih_intake/v1/ticket/createTicket"


def create_ticket_service_now(
    payload: dict[str, Any],
    *,
    url: str | None = None,
    username: str | None = None,
    password: str | None = None,
    timeout: int = 30,
) -> dict[str, Any]:
    """Create a ticket in IHG ServiceNow and return response details.

    Credentials are resolved from arguments first, then environment variables:
    - IHG_SERVICENOW_USERNAME
    - IHG_SERVICENOW_PASSWORD
    """
    resolved_url = url or os.getenv("IHG_SERVICENOW_URL", DEFAULT_SERVICENOW_URL)
    resolved_username = username or os.getenv("IHG_SERVICENOW_USERNAME", "")
    resolved_password = password or os.getenv("IHG_SERVICENOW_PASSWORD", "")

    if not resolved_username or not resolved_password:
        raise ValueError(
            "ServiceNow credentials are not configured. Provide username/password "
            "or set IHG_SERVICENOW_USERNAME and IHG_SERVICENOW_PASSWORD."
        )

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(
            resolved_url,
            json=payload,
            headers=headers,
            auth=(resolved_username, resolved_password),
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Error calling ServiceNow: {exc}") from exc

    try:
        response_body: Any = response.json()
    except ValueError:
        response_body = response.text

    return {
        "status_code": response.status_code,
        "response": response_body,
    }
