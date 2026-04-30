import json
import os
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from utils.envelope import build_tool_response

ENDPOINT_POWER_AUTOMATE_EMAIL = os.getenv(
    "URL_SEND_EMAIL_PA",
    "https://defaultdd3881f7d5fe4f9083f0dd74977579.51.environment.api.powerplatform.com/powerautomate/automations/direct/workflows/e1be2ea245d24e39bc4dbebfb70b218b/triggers/manual/paths/invoke/send_email?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=BtNK6OEITK5E-eAOf9JeVfaadiUXyJI2_VdE4lZ5V30",
)


def _parse_response_body(raw_body: bytes) -> Any:
    """Parse a Power Automate response body when possible."""
    if not raw_body:
        return None

    text = raw_body.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def send_email(
    subject: str,
    body: str,
    recipients: str,
    is_teams_message: bool = False,
    from_email: Optional[str] = None,
) -> Dict[str, Any]:
    """Send an email through the configured Power Automate email flow.

    Use this tool when the user asks to send an email or Teams notification.

    Args:
        subject: Email subject.
        body: Email body/content in HTML string format.
        recipients: Recipient email address(es), as expected by the flow (for example, semicolon-separated).
        is_teams_message: Whether the flow should treat this as a Teams message as well.
        from_email: Sender identifier/email. It is mapped to the Power Automate field "from".

    Returns:
        Dict[str, Any]: Delivery request status returned by Power Automate.
    """
    subject = (subject or "").strip()
    recipients = (recipients or "").strip()

    if not subject:
        raise ValueError("subject is required")
    if not body:
        raise ValueError("body is required")
    if not recipients:
        raise ValueError("recipients is required")

    payload = {
        "Subject": subject,
        "Body": body,
        "Recipients": recipients,
        "isTeamsMessage": bool(is_teams_message),
        "from": (from_email or "").strip(),
    }

    request = Request(
        ENDPOINT_POWER_AUTOMATE_EMAIL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request, timeout=30) as response:
            status_code = response.getcode()
            response_body = _parse_response_body(response.read())
    except HTTPError as error:
        error_body = _parse_response_body(error.read())
        raise RuntimeError(
            f"Power Automate email flow failed with status {error.code}: {error_body}"
        ) from error
    except URLError as error:
        raise RuntimeError(f"Power Automate email flow could not be reached: {error.reason}") from error

    return build_tool_response(
        tool_name="send_email",
        summary={
            "sent": 200 <= status_code < 300,
            "status_code": status_code,
            "recipients": recipients,
            "subject": subject,
            "is_teams_message": bool(is_teams_message),
        },
        filters={
            "recipients": recipients,
            "is_teams_message": bool(is_teams_message),
            "from_email": (from_email or "").strip() or None,
        },
        source_systems=["power_automate"],
        details={"power_automate_response": response_body},
    )


send_email.MCP_ANNOTATIONS = {
    "readOnlyHint": False,
    "destructiveHint": False,
    "openWorldHint": True,
}

NOTIFICATION_TOOLS: List = [send_email]
