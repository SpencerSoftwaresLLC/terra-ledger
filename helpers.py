from flask import session
from datetime import datetime, date, timedelta
from urllib.parse import quote
from .db import get_db_connection
from flask_mail import Message
from .db import get_db_connection
from .extensions import mail

def get_employee_display_name(employee):
    if not employee:
        return "Unknown Employee"

    cols = employee.keys()

    first_name = employee["first_name"] if "first_name" in cols and employee["first_name"] else ""
    last_name = employee["last_name"] if "last_name" in cols and employee["last_name"] else ""
    full_name = employee["full_name"] if "full_name" in cols and employee["full_name"] else ""
    name = employee["name"] if "name" in cols and employee["name"] else ""

    display_name = f"{first_name} {last_name}".strip()

    if display_name:
        return display_name
    if full_name:
        return full_name
    if name:
        return name

    return f"Employee #{employee['id']}"


def get_company_email_settings(company_id):
    conn = get_db_connection()

    company = conn.execute(
        """
        SELECT name, email
        FROM companies
        WHERE id = ?
        """,
        (company_id,),
    ).fetchone()

    profile = conn.execute(
        """
        SELECT
            display_name,
            email,
            email_from_name,
            reply_to_email,
            platform_sender_enabled
        FROM company_profile
        WHERE company_id = ?
        """,
        (company_id,),
    ).fetchone()

    conn.close()

    company_name = "Your Company"
    if profile and profile["display_name"]:
        company_name = profile["display_name"]
    elif company and company["name"]:
        company_name = company["name"]

    reply_to_email = None
    if profile and profile["reply_to_email"]:
        reply_to_email = profile["reply_to_email"]
    elif profile and profile["email"]:
        reply_to_email = profile["email"]
    elif company and company["email"]:
        reply_to_email = company["email"]

    from_name = company_name
    if profile and profile["email_from_name"]:
        from_name = profile["email_from_name"]

    platform_sender_enabled = 1
    if profile and "platform_sender_enabled" in profile.keys():
        platform_sender_enabled = int(profile["platform_sender_enabled"] or 0)

    return {
        "from_name": from_name,
        "reply_to_email": reply_to_email,
        "platform_sender_enabled": platform_sender_enabled,
    }


def send_company_email(
    *,
    company_id,
    to_email,
    subject,
    body,
    attachment_bytes=None,
    attachment_filename=None,
    attachment_content_type="application/pdf",
):
    settings = get_company_email_settings(company_id)

    if not settings["platform_sender_enabled"]:
        raise Exception("Platform email sending is disabled for this company.")

    msg = Message(
        subject=subject,
        recipients=[to_email],
        body=body,
        reply_to=settings["reply_to_email"],
    )

    # Optional: set sender name if your Flask-Mail/mail backend supports it cleanly
    # If MAIL_DEFAULT_SENDER is configured globally, Flask-Mail will use that automatically.
    # To force a custom sender name, uncomment below and make sure MAIL_DEFAULT_SENDER is a real mailbox.
    #
    # from flask import current_app
    # default_sender = current_app.config.get("MAIL_DEFAULT_SENDER")
    # if default_sender:
    #     msg.sender = (settings["from_name"], default_sender)

    if attachment_bytes and attachment_filename:
        msg.attach(
            attachment_filename,
            attachment_content_type,
            attachment_bytes,
        )

    mail.send(msg)


def company_scope_query(base_query, params=()):
    return base_query, (session["company_id"], *params)


def get_period_range(view_type, anchor_date=None):
    if anchor_date:
        if isinstance(anchor_date, str):
            anchor_date = datetime.strptime(anchor_date, "%Y-%m-%d").date()
    else:
        anchor_date = date.today()

    if view_type == "daily":
        start = anchor_date
        end = anchor_date

    elif view_type == "weekly":
        start = anchor_date - timedelta(days=anchor_date.weekday())
        end = start + timedelta(days=6)

    elif view_type == "monthly":
        start = anchor_date.replace(day=1)

        if start.month == 12:
            end = date(start.year, 12, 31)
        else:
            next_month = date(start.year, start.month + 1, 1)
            end = next_month - timedelta(days=1)

    elif view_type == "quarterly":
        quarter = (anchor_date.month - 1) // 3 + 1
        start_month = (quarter - 1) * 3 + 1
        start = date(anchor_date.year, start_month, 1)

        if start_month + 3 > 12:
            end = date(anchor_date.year, 12, 31)
        else:
            next_quarter = date(anchor_date.year, start_month + 3, 1)
            end = next_quarter - timedelta(days=1)

    elif view_type == "yearly":
        start = date(anchor_date.year, 1, 1)
        end = date(anchor_date.year, 12, 31)

    else:
        start = date(anchor_date.year, 1, 1)
        end = date(anchor_date.year, 12, 31)

    return start.isoformat(), end.isoformat()


def mailto_link(subject, body, recipient=""):
    return f"mailto:{recipient}?subject={quote(subject)}&body={quote(body)}"

def ensure_customer_name_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(customers)")
    cols = [row[1] for row in cur.fetchall()]

    if "first_name" not in cols:
        cur.execute("ALTER TABLE customers ADD COLUMN first_name TEXT")

    if "last_name" not in cols:
        cur.execute("ALTER TABLE customers ADD COLUMN last_name TEXT")

    conn.commit()
    conn.close()