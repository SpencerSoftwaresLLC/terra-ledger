import os
import base64
import mimetypes
import requests

from ..db import get_db_connection


def get_company_email_settings(company_id, user_id=None):
    conn = get_db_connection()

    profile = conn.execute(
        """
        SELECT
            display_name,
            email,
            email_from_name,
            reply_to_email,
            platform_sender_enabled,
            reply_to_mode
        FROM company_profile
        WHERE company_id = ?
        """,
        (company_id,),
    ).fetchone()

    company = conn.execute(
        """
        SELECT name, email
        FROM companies
        WHERE id = ?
        """,
        (company_id,),
    ).fetchone()

    user = None
    if user_id:
        user = conn.execute(
            """
            SELECT name, email
            FROM users
            WHERE id = ? AND company_id = ?
            """,
            (user_id, company_id),
        ).fetchone()

    conn.close()

    from_name = None
    reply_to_email = None
    enabled = True
    reply_to_mode = "company"

    if profile:
        from_name = profile["email_from_name"] or profile["display_name"]
        reply_to_email = profile["reply_to_email"] or profile["email"]
        enabled = bool(profile["platform_sender_enabled"])
        if "reply_to_mode" in profile.keys() and profile["reply_to_mode"]:
            reply_to_mode = profile["reply_to_mode"]

    if not from_name and company:
        from_name = company["name"]

    if not reply_to_email and company:
        reply_to_email = company["email"]

    if reply_to_mode == "logged_in_user" and user and user["email"]:
        reply_to_email = user["email"]

    return {
        "from_name": from_name or "TerraLedger",
        "reply_to_email": reply_to_email,
        "enabled": enabled,
    }


def send_company_email(
    to_email,
    subject,
    html=None,
    body=None,
    company_id=None,
    user_id=None,
    attachments=None,
    pdf_path=None,
    attachment_bytes=None,
    attachment_filename=None,
):
    resend_api_key = os.environ.get("RESEND_API_KEY", "").strip()
    if "invoice" in subject.lower():
        from_email = os.environ.get("INVOICE_FROM_EMAIL") or os.environ.get("FROM_EMAIL")
    else:
        from_email = os.environ.get("FROM_EMAIL")

    if not resend_api_key:
        raise Exception("Missing RESEND_API_KEY in .env")

    if not from_email:
        raise Exception("Missing FROM_EMAIL in .env")

    if not to_email:
        raise Exception("Missing recipient email address")

    html = html or body or ""

    settings = {
        "from_name": "TerraLedger",
        "reply_to_email": None,
        "enabled": True,
    }

    if company_id:
        settings = get_company_email_settings(company_id, user_id)

    if not settings.get("enabled", True):
        raise Exception("Platform sender is disabled for this company")

    from_name = settings.get("from_name") or "TerraLedger"
    reply_to_email = settings.get("reply_to_email")

    resend_attachments = []

    all_attachments = []
    if attachments:
        all_attachments.extend(attachments)
    if pdf_path:
        all_attachments.append(pdf_path)

    for path in all_attachments:
        if not path or not os.path.exists(path):
            continue

        filename = os.path.basename(path)

        with open(path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode("utf-8")

        resend_attachments.append({
            "filename": filename,
            "content": content_b64,
        })

    if attachment_bytes:
        if isinstance(attachment_bytes, str):
            attachment_bytes = attachment_bytes.encode("utf-8")

        resend_attachments.append({
            "filename": attachment_filename or "attachment.pdf",
            "content": base64.b64encode(attachment_bytes).decode("utf-8"),
        })

    payload = {
        "from": f"{from_name} <{from_email}>",
        "to": [to_email] if isinstance(to_email, str) else to_email,
        "subject": subject,
        "html": html,
    }

    if reply_to_email:
        payload["reply_to"] = reply_to_email

    if resend_attachments:
        payload["attachments"] = resend_attachments

    response = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {resend_api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )

    if response.status_code not in (200, 201):
        raise Exception(f"Resend API error {response.status_code}: {response.text}")

    return response.json()