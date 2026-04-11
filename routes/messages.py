import os
import re
from datetime import datetime, date, time, timedelta
from urllib.parse import quote_plus
from flask_wtf.csrf import generate_csrf

from flask import (
    Blueprint,
    request,
    redirect,
    url_for,
    flash,
    session,
    render_template_string,
    Response,
    abort,
)
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
from twilio.twiml.messaging_response import MessagingResponse
from twilio.request_validator import RequestValidator

from db import get_db_connection, table_columns
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page
from extensions import csrf
from routes.notifications import create_notification


messages_bp = Blueprint("messages", __name__)

MAX_MESSAGE_LENGTH = 1600
THREAD_PREVIEW_LIMIT = 140

STOP_WORDS = {"stop", "stopall", "unsubscribe", "cancel", "end", "quit"}
HELP_WORDS = {"help", "info"}
START_WORDS = {"start", "unstop", "yes"}


def _safe_text(value, default=""):
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _safe_int(value, default=None):
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value, default=0.0):
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _digits_only(value):
    return re.sub(r"\D", "", _safe_text(value))


def _normalize_phone(value):
    text = _safe_text(value)
    if not text:
        return ""

    digits = _digits_only(text)
    if not digits:
        return ""

    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if 10 <= len(digits) <= 15:
        return f"+{digits}"
    return ""


def _is_reasonable_phone(value):
    digits = _digits_only(value)
    return 10 <= len(digits) <= 15


def _utcnow():
    return datetime.utcnow()


def _coerce_datetime(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime.combine(value, time.min)

    text = str(value).strip()
    if not text:
        return None

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
    ):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt in ("%Y-%m-%d", "%m/%d/%Y"):
                return datetime.combine(parsed.date(), time.min)
            return parsed
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _format_datetime_for_message(value):
    dt = _coerce_datetime(value)
    if not dt:
        return ""
    return dt.strftime("%m/%d/%Y at %I:%M %p")


def _format_datetime_short(value):
    dt = _coerce_datetime(value)
    if not dt:
        return ""
    return dt.strftime("%m/%d/%Y %I:%M %p")


def _format_currency(value):
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return "$0.00"


def _preview_text(value, limit=THREAD_PREVIEW_LIMIT):
    text = _safe_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _get_app_base_url():
    base = _safe_text(os.environ.get("APP_BASE_URL"))
    if not base:
        return ""
    if not base.startswith(("http://", "https://")):
        base = "https://" + base
    return base.rstrip("/")


def _get_external_base_url_from_request(req):
    forwarded_proto = _safe_text(req.headers.get("X-Forwarded-Proto"))
    forwarded_host = _safe_text(req.headers.get("X-Forwarded-Host"))
    host = forwarded_host or _safe_text(req.host)
    if not host:
        return _get_app_base_url()

    scheme = forwarded_proto or req.scheme or "https"
    return f"{scheme}://{host}".rstrip("/")


def _candidate_request_urls(req):
    candidates = []

    try:
        if req.url:
            candidates.append(req.url)
    except Exception:
        pass

    external_base = _get_external_base_url_from_request(req)
    if external_base and req.path:
        candidates.append(f"{external_base}{req.path}")

    app_base = _get_app_base_url()
    if app_base and req.path:
        candidates.append(f"{app_base}{req.path}")

    seen = set()
    final = []
    for url in candidates:
        url = _safe_text(url)
        if url and url not in seen:
            seen.add(url)
            final.append(url)
    return final


def _validate_twilio_request(req):
    auth_token = _safe_text(os.environ.get("TWILIO_AUTH_TOKEN"))
    if not auth_token:
        return False

    signature = _safe_text(req.headers.get("X-Twilio-Signature"))
    if not signature:
        return False

    validator = RequestValidator(auth_token)
    form_data = req.form

    for candidate_url in _candidate_request_urls(req):
        try:
            if validator.validate(candidate_url, form_data, signature):
                return True
        except Exception:
            continue

    return False


def _validate_automation_request(req):
    token = _safe_text(os.environ.get("MESSAGING_AUTOMATION_TOKEN"))
    if not token:
        return False

    provided = (
        _safe_text(req.headers.get("X-Automation-Token"))
        or _safe_text(req.args.get("token"))
        or _safe_text(req.form.get("token"))
    )
    return provided == token


def _get_twilio_client():
    account_sid = _safe_text(os.environ.get("TWILIO_ACCOUNT_SID"))
    auth_token = _safe_text(os.environ.get("TWILIO_AUTH_TOKEN"))

    if not account_sid or not auth_token:
        return None, "Twilio credentials are missing."

    try:
        return Client(account_sid, auth_token), None
    except Exception as e:
        return None, f"Twilio client error: {e}"


def _get_from_number():
    return _normalize_phone(os.environ.get("TWILIO_FROM_NUMBER"))


def _get_full_inbound_webhook_url():
    base = _get_app_base_url()
    return f"{base}/messages/webhook" if base else "/messages/webhook"


def _get_full_status_callback_url():
    env_url = _safe_text(os.environ.get("TWILIO_STATUS_CALLBACK_URL"))
    if env_url:
        return env_url
    base = _get_app_base_url()
    return f"{base}/messages/status-callback" if base else ""


def _get_customer_phone_column(conn):
    cols = table_columns(conn, "customers")
    if not cols:
        return None
    if "phone" in cols:
        return "phone"
    if "phone_number" in cols:
        return "phone_number"
    return None


def _customer_sms_columns_exist(conn):
    cols = set(table_columns(conn, "customers") or [])
    return {
        "sms_opt_in",
        "sms_opt_in_at",
        "sms_opt_in_method",
        "sms_opt_in_ip",
        "sms_opt_out_at",
    }.issubset(cols)


def _thread_key(company_id, phone_number):
    digits = _digits_only(phone_number)
    if not company_id or not digits:
        return ""
    return f"{int(company_id)}__{digits}"


def _thread_link_by_key(thread_key):
    return url_for("messages.view_thread", thread_key=thread_key)


def _full_thread_link(thread_key):
    base = _get_app_base_url()
    path = url_for("messages.view_thread", thread_key=thread_key)
    if not base:
        return path
    return f"{base}{path}"


def ensure_customer_sms_consent_columns():
    conn = get_db_connection()
    try:
        conn.execute("""
            ALTER TABLE customers
            ADD COLUMN IF NOT EXISTS sms_opt_in BOOLEAN NOT NULL DEFAULT FALSE
        """)
        conn.execute("""
            ALTER TABLE customers
            ADD COLUMN IF NOT EXISTS sms_opt_in_at TIMESTAMP NULL
        """)
        conn.execute("""
            ALTER TABLE customers
            ADD COLUMN IF NOT EXISTS sms_opt_in_method TEXT
        """)
        conn.execute("""
            ALTER TABLE customers
            ADD COLUMN IF NOT EXISTS sms_opt_in_ip TEXT
        """)
        conn.execute("""
            ALTER TABLE customers
            ADD COLUMN IF NOT EXISTS sms_opt_out_at TIMESTAMP NULL
        """)
        conn.commit()
    finally:
        conn.close()


def ensure_messaging_tables():
    ensure_customer_sms_consent_columns()

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messaging_settings (
                id SERIAL PRIMARY KEY,
                company_id INTEGER NOT NULL UNIQUE,
                messaging_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                send_job_updates BOOLEAN NOT NULL DEFAULT TRUE,
                send_invoice_reminders BOOLEAN NOT NULL DEFAULT FALSE,
                send_manual_messages BOOLEAN NOT NULL DEFAULT TRUE,
                enable_job_reminders BOOLEAN NOT NULL DEFAULT TRUE,
                job_reminder_hours INTEGER NOT NULL DEFAULT 24,
                enable_late_invoice_reminders BOOLEAN NOT NULL DEFAULT FALSE,
                late_invoice_days INTEGER NOT NULL DEFAULT 30,
                forward_inbound_to_owner BOOLEAN NOT NULL DEFAULT TRUE,
                owner_forward_phone TEXT,
                default_on_the_way_template TEXT,
                default_job_started_template TEXT,
                default_job_completed_template TEXT,
                default_invoice_reminder_template TEXT,
                default_job_reminder_template TEXT,
                default_late_invoice_reminder_template TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'messaging_settings'
        """)
        settings_cols = {row["column_name"] for row in cur.fetchall()}

        required_settings_cols = {
            "company_id": "INTEGER NOT NULL UNIQUE",
            "messaging_enabled": "BOOLEAN NOT NULL DEFAULT FALSE",
            "send_job_updates": "BOOLEAN NOT NULL DEFAULT TRUE",
            "send_invoice_reminders": "BOOLEAN NOT NULL DEFAULT FALSE",
            "send_manual_messages": "BOOLEAN NOT NULL DEFAULT TRUE",
            "enable_job_reminders": "BOOLEAN NOT NULL DEFAULT TRUE",
            "job_reminder_hours": "INTEGER NOT NULL DEFAULT 24",
            "enable_late_invoice_reminders": "BOOLEAN NOT NULL DEFAULT FALSE",
            "late_invoice_days": "INTEGER NOT NULL DEFAULT 30",
            "forward_inbound_to_owner": "BOOLEAN NOT NULL DEFAULT TRUE",
            "owner_forward_phone": "TEXT",
            "default_on_the_way_template": "TEXT",
            "default_job_started_template": "TEXT",
            "default_job_completed_template": "TEXT",
            "default_invoice_reminder_template": "TEXT",
            "default_job_reminder_template": "TEXT",
            "default_late_invoice_reminder_template": "TEXT",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }

        for col_name, col_def in required_settings_cols.items():
            if col_name not in settings_cols:
                cur.execute(f"ALTER TABLE messaging_settings ADD COLUMN {col_name} {col_def}")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS message_log (
                id SERIAL PRIMARY KEY,
                company_id INTEGER NOT NULL,
                customer_id INTEGER,
                job_id INTEGER,
                invoice_id INTEGER,
                phone_number TEXT,
                direction TEXT NOT NULL DEFAULT 'outbound',
                message_body TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                provider TEXT DEFAULT 'twilio',
                provider_message_id TEXT,
                automation_key TEXT,
                conversation_key TEXT,
                sent_by_user_id INTEGER,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sent_at TIMESTAMP
            )
        """)

        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'message_log'
        """)
        message_cols = {row["column_name"] for row in cur.fetchall()}

        required_message_cols = {
            "company_id": "INTEGER NOT NULL",
            "customer_id": "INTEGER",
            "job_id": "INTEGER",
            "invoice_id": "INTEGER",
            "phone_number": "TEXT",
            "direction": "TEXT NOT NULL DEFAULT 'outbound'",
            "message_body": "TEXT NOT NULL",
            "status": "TEXT NOT NULL DEFAULT 'queued'",
            "provider": "TEXT DEFAULT 'twilio'",
            "provider_message_id": "TEXT",
            "automation_key": "TEXT",
            "conversation_key": "TEXT",
            "sent_by_user_id": "INTEGER",
            "error_message": "TEXT",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "sent_at": "TIMESTAMP",
        }

        for col_name, col_def in required_message_cols.items():
            if col_name not in message_cols:
                cur.execute(f"ALTER TABLE message_log ADD COLUMN {col_name} {col_def}")

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_message_log_company_created
            ON message_log (company_id, created_at DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_message_log_phone_created
            ON message_log (phone_number, created_at DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_message_log_conversation_key
            ON message_log (conversation_key)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_message_log_provider_message_id
            ON message_log (provider_message_id)
        """)

        conn.commit()
    finally:
        conn.close()


def get_messaging_settings(company_id):
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT *
            FROM messaging_settings
            WHERE company_id = %s
        """, (company_id,)).fetchone()

        if row:
            row["messaging_enabled"] = _to_bool(row.get("messaging_enabled"))
            row["send_job_updates"] = _to_bool(row.get("send_job_updates"))
            row["send_invoice_reminders"] = _to_bool(row.get("send_invoice_reminders"))
            row["send_manual_messages"] = _to_bool(row.get("send_manual_messages"))
            row["enable_job_reminders"] = _to_bool(row.get("enable_job_reminders"))
            row["enable_late_invoice_reminders"] = _to_bool(row.get("enable_late_invoice_reminders"))
            row["forward_inbound_to_owner"] = _to_bool(row.get("forward_inbound_to_owner"))
            row["job_reminder_hours"] = _safe_int(row.get("job_reminder_hours"), 24)
            row["late_invoice_days"] = _safe_int(row.get("late_invoice_days"), 30)
            row["owner_forward_phone"] = _normalize_phone(row.get("owner_forward_phone"))
        return row
    finally:
        conn.close()


def get_message_history(company_id, limit=100):
    conn = get_db_connection()
    try:
        return conn.execute("""
            SELECT
                ml.*,
                c.name AS customer_name,
                j.title AS job_title,
                i.invoice_number AS invoice_number
            FROM message_log ml
            LEFT JOIN customers c ON ml.customer_id = c.id
            LEFT JOIN jobs j ON ml.job_id = j.id
            LEFT JOIN invoices i ON ml.invoice_id = i.id
            WHERE ml.company_id = %s
            ORDER BY ml.created_at DESC, ml.id DESC
            LIMIT %s
        """, (company_id, limit)).fetchall()
    finally:
        conn.close()


def get_message_threads(company_id, limit=100):
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            WITH ranked AS (
                SELECT
                    ml.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY ml.conversation_key
                        ORDER BY ml.created_at DESC, ml.id DESC
                    ) AS rn
                FROM message_log ml
                WHERE ml.company_id = %s
                  AND ml.conversation_key IS NOT NULL
                  AND ml.conversation_key <> ''
            )
            SELECT
                r.conversation_key,
                r.phone_number,
                r.customer_id,
                c.name AS customer_name,
                r.message_body AS last_message_body,
                r.direction AS last_direction,
                r.status AS last_status,
                r.created_at AS last_created_at,
                (
                    SELECT COUNT(*)
                    FROM message_log ml2
                    WHERE ml2.company_id = %s
                      AND ml2.conversation_key = r.conversation_key
                ) AS message_count
            FROM ranked r
            LEFT JOIN customers c ON r.customer_id = c.id
            WHERE r.rn = 1
            ORDER BY r.created_at DESC
            LIMIT %s
        """, (company_id, company_id, limit)).fetchall()
        return rows
    finally:
        conn.close()


def get_thread_messages(company_id, thread_key):
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT
                ml.*,
                c.name AS customer_name,
                j.title AS job_title,
                i.invoice_number AS invoice_number
            FROM message_log ml
            LEFT JOIN customers c ON ml.customer_id = c.id
            LEFT JOIN jobs j ON ml.job_id = j.id
            LEFT JOIN invoices i ON ml.invoice_id = i.id
            WHERE ml.company_id = %s
              AND ml.conversation_key = %s
            ORDER BY ml.created_at ASC, ml.id ASC
        """, (company_id, thread_key)).fetchall()
        return rows
    finally:
        conn.close()


def get_thread_context(company_id, thread_key):
    messages = get_thread_messages(company_id, thread_key)
    if not messages:
        return None

    first = messages[0]
    last = messages[-1]
    return {
        "thread_key": thread_key,
        "phone_number": first.get("phone_number"),
        "customer_id": first.get("customer_id"),
        "customer_name": first.get("customer_name"),
        "last_message_body": last.get("message_body"),
        "last_created_at": last.get("created_at"),
        "message_count": len(messages),
    }


def get_customers_for_messages(company_id):
    conn = get_db_connection()
    try:
        phone_col = _get_customer_phone_column(conn)
        if not phone_col:
            return conn.execute("""
                SELECT id, name, NULL AS phone, FALSE AS sms_opt_in
                FROM customers
                WHERE company_id = %s
                ORDER BY name ASC
            """, (company_id,)).fetchall()

        if _customer_sms_columns_exist(conn):
            return conn.execute(f"""
                SELECT
                    id,
                    name,
                    {phone_col} AS phone,
                    sms_opt_in,
                    sms_opt_in_at,
                    sms_opt_out_at
                FROM customers
                WHERE company_id = %s
                ORDER BY name ASC
            """, (company_id,)).fetchall()

        return conn.execute(f"""
            SELECT
                id,
                name,
                {phone_col} AS phone,
                FALSE AS sms_opt_in,
                NULL AS sms_opt_in_at,
                NULL AS sms_opt_out_at
            FROM customers
            WHERE company_id = %s
            ORDER BY name ASC
        """, (company_id,)).fetchall()
    finally:
        conn.close()


def find_customer_by_phone(company_id, phone_number):
    conn = get_db_connection()
    try:
        phone_col = _get_customer_phone_column(conn)
        if not phone_col:
            return None

        search_digits = _digits_only(phone_number)
        if not search_digits:
            return None

        if _customer_sms_columns_exist(conn):
            sql = f"""
                SELECT
                    id,
                    name,
                    {phone_col} AS phone,
                    sms_opt_in,
                    sms_opt_in_at,
                    sms_opt_in_method,
                    sms_opt_in_ip,
                    sms_opt_out_at
                FROM customers
                WHERE company_id = %s
                ORDER BY id DESC
            """
        else:
            sql = f"""
                SELECT
                    id,
                    name,
                    {phone_col} AS phone,
                    FALSE AS sms_opt_in,
                    NULL AS sms_opt_in_at,
                    NULL AS sms_opt_in_method,
                    NULL AS sms_opt_in_ip,
                    NULL AS sms_opt_out_at
                FROM customers
                WHERE company_id = %s
                ORDER BY id DESC
            """

        rows = conn.execute(sql, (company_id,)).fetchall()
        for row in rows:
            existing_digits = _digits_only(row["phone"])
            if not existing_digits:
                continue
            if existing_digits == search_digits:
                return row
            if existing_digits.endswith(search_digits) or search_digits.endswith(existing_digits):
                return row
        return None
    finally:
        conn.close()


def get_customer_by_id(company_id, customer_id):
    if not company_id or not customer_id:
        return None

    conn = get_db_connection()
    try:
        phone_col = _get_customer_phone_column(conn)
        if not phone_col:
            return None

        if _customer_sms_columns_exist(conn):
            sql = f"""
                SELECT
                    id,
                    name,
                    {phone_col} AS phone,
                    sms_opt_in,
                    sms_opt_in_at,
                    sms_opt_in_method,
                    sms_opt_in_ip,
                    sms_opt_out_at
                FROM customers
                WHERE company_id = %s AND id = %s
                LIMIT 1
            """
        else:
            sql = f"""
                SELECT
                    id,
                    name,
                    {phone_col} AS phone,
                    FALSE AS sms_opt_in,
                    NULL AS sms_opt_in_at,
                    NULL AS sms_opt_in_method,
                    NULL AS sms_opt_in_ip,
                    NULL AS sms_opt_out_at
                FROM customers
                WHERE company_id = %s AND id = %s
                LIMIT 1
            """

        return conn.execute(sql, (company_id, customer_id)).fetchone()
    finally:
        conn.close()


def customer_has_sms_consent(customer_row):
    if not customer_row:
        return False

    phone_number = _normalize_phone(customer_row.get("phone"))
    if not _is_reasonable_phone(phone_number):
        return False

    if not _to_bool(customer_row.get("sms_opt_in")):
        return False

    if customer_row.get("sms_opt_out_at"):
        return False

    return True


def set_customer_sms_consent(company_id, phone_number, is_opted_in, method=None):
    if not company_id or not phone_number:
        return None

    normalized_phone = _normalize_phone(phone_number)
    if not normalized_phone:
        return None

    customer = find_customer_by_phone(company_id, normalized_phone)
    if not customer:
        return None

    conn = get_db_connection()
    try:
        conn.execute("""
            UPDATE customers
            SET sms_opt_in = %s,
                sms_opt_in_at = CASE
                    WHEN %s = TRUE AND sms_opt_in = FALSE THEN CURRENT_TIMESTAMP
                    WHEN %s = TRUE AND sms_opt_in_at IS NULL THEN CURRENT_TIMESTAMP
                    ELSE sms_opt_in_at
                END,
                sms_opt_in_method = CASE
                    WHEN %s = TRUE THEN COALESCE(%s, sms_opt_in_method, 'sms_reply')
                    ELSE sms_opt_in_method
                END,
                sms_opt_out_at = CASE
                    WHEN %s = FALSE THEN CURRENT_TIMESTAMP
                    WHEN %s = TRUE THEN NULL
                    ELSE sms_opt_out_at
                END
            WHERE company_id = %s
              AND id = %s
        """, (
            is_opted_in,
            is_opted_in,
            is_opted_in,
            is_opted_in,
            method,
            is_opted_in,
            is_opted_in,
            company_id,
            customer["id"],
        ))
        conn.commit()
    finally:
        conn.close()

    return customer["id"]


def get_last_conversation_for_phone(phone_number):
    normalized_phone = _normalize_phone(phone_number)
    if not normalized_phone:
        return None

    conn = get_db_connection()
    try:
        return conn.execute("""
            SELECT
                company_id,
                customer_id,
                conversation_key,
                phone_number,
                created_at
            FROM message_log
            WHERE phone_number = %s
              AND company_id IS NOT NULL
            ORDER BY created_at DESC, id DESC
            LIMIT 1
        """, (normalized_phone,)).fetchone()
    finally:
        conn.close()


def has_automation_message(company_id, automation_key):
    if not company_id or not automation_key:
        return False

    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT 1
            FROM message_log
            WHERE company_id = %s
              AND automation_key = %s
              AND status IN ('sent', 'delivered', 'received', 'queued')
            LIMIT 1
        """, (company_id, automation_key)).fetchone()
        return row is not None
    finally:
        conn.close()


def insert_message_log(
    company_id,
    phone_number,
    message_body,
    direction="outbound",
    status="queued",
    customer_id=None,
    job_id=None,
    invoice_id=None,
    provider="twilio",
    provider_message_id=None,
    automation_key=None,
    conversation_key=None,
    sent_by_user_id=None,
    error_message=None,
):
    normalized_phone = _normalize_phone(phone_number)
    if not conversation_key and company_id and normalized_phone:
        conversation_key = _thread_key(company_id, normalized_phone)

    conn = get_db_connection()
    try:
        conn.execute("""
            INSERT INTO message_log (
                company_id,
                customer_id,
                job_id,
                invoice_id,
                phone_number,
                direction,
                message_body,
                status,
                provider,
                provider_message_id,
                automation_key,
                conversation_key,
                sent_by_user_id,
                error_message,
                sent_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                CASE WHEN %s IN ('sent', 'received', 'delivered') THEN CURRENT_TIMESTAMP ELSE NULL END
            )
        """, (
            company_id,
            customer_id,
            job_id,
            invoice_id,
            normalized_phone,
            direction,
            message_body,
            status,
            provider,
            provider_message_id,
            automation_key,
            conversation_key,
            sent_by_user_id,
            error_message,
            status,
        ))
        conn.commit()
    finally:
        conn.close()


def update_message_status_by_provider_id(provider_message_id, status, error_message=None):
    if not provider_message_id:
        return

    conn = get_db_connection()
    try:
        conn.execute("""
            UPDATE message_log
            SET status = %s,
                error_message = COALESCE(%s, error_message),
                sent_at = CASE
                    WHEN %s IN ('sent', 'delivered') THEN CURRENT_TIMESTAMP
                    ELSE sent_at
                END
            WHERE provider_message_id = %s
        """, (
            status,
            error_message,
            status,
            provider_message_id,
        ))
        conn.commit()
    finally:
        conn.close()


def send_text_message(to_number, message_body, settings_row=None):
    if not to_number:
        return False, None, "Recipient phone number is missing."

    if not message_body:
        return False, None, "Message body is empty."

    if settings_row and not _to_bool(settings_row.get("messaging_enabled")):
        return False, None, "Messaging is disabled for this company."

    from_number = _get_from_number()
    if not from_number:
        return False, None, "TWILIO_FROM_NUMBER is missing or invalid."

    client, client_error = _get_twilio_client()
    if client_error:
        return False, None, client_error

    status_callback_url = _get_full_status_callback_url()

    try:
        create_kwargs = {
            "body": message_body,
            "from_": from_number,
            "to": to_number,
        }

        if status_callback_url:
            create_kwargs["status_callback"] = status_callback_url

        msg = client.messages.create(**create_kwargs)
        return True, msg.sid, None

    except TwilioRestException as e:
        return False, None, e.msg or str(e)
    except Exception as e:
        return False, None, str(e)


def _render_job_reminder_message(template, job_row):
    scheduled_text = _format_datetime_for_message(job_row.get("scheduled_date"))
    customer_name = _safe_text(job_row.get("customer_name"), "Customer")
    company_name = _safe_text(job_row.get("company_name"), "our company")
    job_title = _safe_text(job_row.get("job_title"), "your scheduled job")

    message = template or (
        "Hello {{customer_name}} — this is a reminder from {{company_name}} "
        "about {{job_title}} scheduled for {{scheduled_date}}. "
        "Reply STOP to opt out, HELP for help. Msg&data rates may apply."
    )

    replacements = {
        "{{customer_name}}": customer_name,
        "{{company_name}}": company_name,
        "{{job_title}}": job_title,
        "{{scheduled_date}}": scheduled_text,
    }

    for key, value in replacements.items():
        message = message.replace(key, value)

    return message.strip()


def _render_invoice_late_message(template, invoice_row):
    customer_name = _safe_text(invoice_row.get("customer_name"), "Customer")
    company_name = _safe_text(invoice_row.get("company_name"), "our company")
    invoice_number = _safe_text(invoice_row.get("invoice_number"), "your invoice")
    balance_due = _format_currency(invoice_row.get("balance_due"))
    due_date_text = _format_datetime_for_message(invoice_row.get("due_date"))

    message = template or (
        "Hello {{customer_name}} — this is a reminder from {{company_name}} that "
        "invoice {{invoice_number}} is now past due. Remaining balance: {{balance_due}}. "
        "Due date: {{due_date}}. Reply STOP to opt out, HELP for help. Msg&data rates may apply."
    )

    replacements = {
        "{{customer_name}}": customer_name,
        "{{company_name}}": company_name,
        "{{invoice_number}}": invoice_number,
        "{{balance_due}}": balance_due,
        "{{due_date}}": due_date_text,
    }

    for key, value in replacements.items():
        message = message.replace(key, value)

    return message.strip()


def _send_owner_relay_alert(company_id, customer_id, phone_number, inbound_body, thread_key):
    settings = get_messaging_settings(company_id)
    if not settings:
        return

    if not _to_bool(settings.get("messaging_enabled")):
        return

    if not _to_bool(settings.get("forward_inbound_to_owner")):
        return

    owner_phone = _normalize_phone(settings.get("owner_forward_phone"))
    if not _is_reasonable_phone(owner_phone):
        return

    customer = get_customer_by_id(company_id, customer_id) if customer_id else None
    customer_name = _safe_text(customer.get("name")) if customer else ""
    display_name = customer_name or phone_number
    thread_link = _full_thread_link(thread_key)

    relay_message = (
        f"TerraLedger reply from {display_name}: "
        f"{_preview_text(inbound_body, 110)}\n"
        f"Reply: {thread_link}"
    )

    success, provider_message_id, error_message = send_text_message(
        to_number=owner_phone,
        message_body=relay_message,
        settings_row=settings,
    )

    insert_message_log(
        company_id=company_id,
        customer_id=None,
        phone_number=owner_phone,
        message_body=relay_message,
        direction="outbound",
        status="sent" if success else "failed",
        provider="twilio",
        provider_message_id=provider_message_id,
        conversation_key=None,
        sent_by_user_id=None,
        error_message=error_message,
    )


def process_job_reminders():
    ensure_messaging_tables()

    now = _utcnow()
    sent_count = 0
    failed_count = 0

    conn = get_db_connection()
    try:
        phone_col = _get_customer_phone_column(conn)
        if not phone_col:
            return {"sent": 0, "failed": 0, "checked": 0}

        job_cols = set(table_columns(conn, "jobs") or [])
        if "scheduled_date" not in job_cols:
            return {"sent": 0, "failed": 0, "checked": 0}

        title_expr = "j.title" if "title" in job_cols else "NULL"
        status_expr = "j.status" if "status" in job_cols else "NULL"

        rows = conn.execute(f"""
            SELECT
                j.id,
                j.company_id,
                j.customer_id,
                j.scheduled_date,
                {title_expr} AS job_title,
                {status_expr} AS job_status,
                c.name AS customer_name,
                c.{phone_col} AS customer_phone,
                c.sms_opt_in,
                c.sms_opt_out_at,
                cp.name AS company_name,
                ms.messaging_enabled,
                ms.send_job_updates,
                ms.enable_job_reminders,
                ms.job_reminder_hours,
                ms.default_job_reminder_template
            FROM jobs j
            JOIN customers c
              ON j.customer_id = c.id
            LEFT JOIN companies cp
              ON j.company_id = cp.id
            JOIN messaging_settings ms
              ON j.company_id = ms.company_id
            WHERE ms.messaging_enabled = TRUE
              AND ms.send_job_updates = TRUE
              AND ms.enable_job_reminders = TRUE
              AND j.customer_id IS NOT NULL
              AND j.scheduled_date IS NOT NULL
              AND c.sms_opt_in = TRUE
              AND c.sms_opt_out_at IS NULL
            ORDER BY j.scheduled_date ASC
        """).fetchall()
    finally:
        conn.close()

    checked = 0

    for row in rows:
        checked += 1

        status_text = _safe_text(row.get("job_status")).lower()
        if status_text in {"completed", "cancelled", "canceled"}:
            continue

        scheduled_dt = _coerce_datetime(row.get("scheduled_date"))
        if not scheduled_dt:
            continue

        reminder_hours = _safe_int(row.get("job_reminder_hours"), 24)
        reminder_time = scheduled_dt - timedelta(hours=reminder_hours)

        if now < reminder_time:
            continue

        phone_number = _normalize_phone(row.get("customer_phone"))
        if not _is_reasonable_phone(phone_number):
            continue

        automation_key = f"job_reminder:{row['id']}:{scheduled_dt.strftime('%Y%m%d%H%M')}"
        if has_automation_message(row["company_id"], automation_key):
            continue

        message_body = _render_job_reminder_message(
            row.get("default_job_reminder_template"),
            row,
        )

        success, provider_message_id, error_message = send_text_message(
            to_number=phone_number,
            message_body=message_body,
            settings_row=row,
        )

        if success:
            sent_count += 1
            insert_message_log(
                company_id=row["company_id"],
                customer_id=row["customer_id"],
                job_id=row["id"],
                phone_number=phone_number,
                message_body=message_body,
                direction="outbound",
                status="sent",
                provider="twilio",
                provider_message_id=provider_message_id,
                automation_key=automation_key,
                conversation_key=_thread_key(row["company_id"], phone_number),
            )

            create_notification(
                company_id=row["company_id"],
                user_id=None,
                notif_type="message",
                title="Job reminder sent",
                message=f"Reminder sent to {phone_number} for {row.get('job_title') or 'scheduled job'}.",
                link=url_for("messages.messages_page"),
            )
        else:
            failed_count += 1
            insert_message_log(
                company_id=row["company_id"],
                customer_id=row["customer_id"],
                job_id=row["id"],
                phone_number=phone_number,
                message_body=message_body,
                direction="outbound",
                status="failed",
                provider="twilio",
                automation_key=automation_key,
                conversation_key=_thread_key(row["company_id"], phone_number),
                error_message=error_message,
            )

            create_notification(
                company_id=row["company_id"],
                user_id=None,
                notif_type="message",
                title="Job reminder failed",
                message=f"Reminder to {phone_number} failed: {error_message}",
                link=url_for("messages.messages_page"),
            )

    return {"sent": sent_count, "failed": failed_count, "checked": checked}


def process_late_invoice_reminders():
    ensure_messaging_tables()

    today = _utcnow()
    sent_count = 0
    failed_count = 0

    conn = get_db_connection()
    try:
        phone_col = _get_customer_phone_column(conn)
        if not phone_col:
            return {"sent": 0, "failed": 0, "checked": 0}

        invoice_cols = set(table_columns(conn, "invoices") or [])
        if "due_date" not in invoice_cols:
            return {"sent": 0, "failed": 0, "checked": 0}

        invoice_number_expr = "i.invoice_number" if "invoice_number" in invoice_cols else "NULL"
        balance_due_expr = "i.balance_due" if "balance_due" in invoice_cols else "NULL"
        status_expr = "i.status" if "status" in invoice_cols else "NULL"

        rows = conn.execute(f"""
            SELECT
                i.id,
                i.company_id,
                i.customer_id,
                i.due_date,
                {invoice_number_expr} AS invoice_number,
                {balance_due_expr} AS balance_due,
                {status_expr} AS invoice_status,
                c.name AS customer_name,
                c.{phone_col} AS customer_phone,
                c.sms_opt_in,
                c.sms_opt_out_at,
                cp.name AS company_name,
                ms.messaging_enabled,
                ms.send_invoice_reminders,
                ms.enable_late_invoice_reminders,
                ms.late_invoice_days,
                ms.default_late_invoice_reminder_template
            FROM invoices i
            JOIN customers c
              ON i.customer_id = c.id
            LEFT JOIN companies cp
              ON i.company_id = cp.id
            JOIN messaging_settings ms
              ON i.company_id = ms.company_id
            WHERE ms.messaging_enabled = TRUE
              AND ms.send_invoice_reminders = TRUE
              AND ms.enable_late_invoice_reminders = TRUE
              AND i.customer_id IS NOT NULL
              AND i.due_date IS NOT NULL
              AND c.sms_opt_in = TRUE
              AND c.sms_opt_out_at IS NULL
            ORDER BY i.due_date ASC
        """).fetchall()
    finally:
        conn.close()

    checked = 0

    for row in rows:
        checked += 1

        invoice_status = _safe_text(row.get("invoice_status")).lower()
        if invoice_status in {"paid", "void", "cancelled", "canceled"}:
            continue

        balance_due = _safe_float(row.get("balance_due"), 0.0)
        if balance_due <= 0:
            continue

        due_dt = _coerce_datetime(row.get("due_date"))
        if not due_dt:
            continue

        late_invoice_days = _safe_int(row.get("late_invoice_days"), 30)
        due_plus_days = due_dt + timedelta(days=late_invoice_days)

        if today < due_plus_days:
            continue

        phone_number = _normalize_phone(row.get("customer_phone"))
        if not _is_reasonable_phone(phone_number):
            continue

        automation_key = f"late_invoice:{row['id']}:{late_invoice_days}"
        if has_automation_message(row["company_id"], automation_key):
            continue

        message_body = _render_invoice_late_message(
            row.get("default_late_invoice_reminder_template"),
            row,
        )

        success, provider_message_id, error_message = send_text_message(
            to_number=phone_number,
            message_body=message_body,
            settings_row=row,
        )

        if success:
            sent_count += 1
            insert_message_log(
                company_id=row["company_id"],
                customer_id=row["customer_id"],
                invoice_id=row["id"],
                phone_number=phone_number,
                message_body=message_body,
                direction="outbound",
                status="sent",
                provider="twilio",
                provider_message_id=provider_message_id,
                automation_key=automation_key,
                conversation_key=_thread_key(row["company_id"], phone_number),
            )

            create_notification(
                company_id=row["company_id"],
                user_id=None,
                notif_type="message",
                title="Late invoice reminder sent",
                message=f"Reminder sent to {phone_number} for invoice {row.get('invoice_number') or ''}.",
                link=url_for("messages.messages_page"),
            )
        else:
            failed_count += 1
            insert_message_log(
                company_id=row["company_id"],
                customer_id=row["customer_id"],
                invoice_id=row["id"],
                phone_number=phone_number,
                message_body=message_body,
                direction="outbound",
                status="failed",
                provider="twilio",
                automation_key=automation_key,
                conversation_key=_thread_key(row["company_id"], phone_number),
                error_message=error_message,
            )

            create_notification(
                company_id=row["company_id"],
                user_id=None,
                notif_type="message",
                title="Late invoice reminder failed",
                message=f"Reminder to {phone_number} failed: {error_message}",
                link=url_for("messages.messages_page"),
            )

    return {"sent": sent_count, "failed": failed_count, "checked": checked}


@messages_bp.route("/messages")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def messages_page():
    ensure_messaging_tables()

    company_id = session.get("company_id")
    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("dashboard.dashboard"))

    settings = get_messaging_settings(company_id)
    history = get_message_history(company_id, limit=100)
    threads = get_message_threads(company_id, limit=50)
    customers = get_customers_for_messages(company_id)
    from_number = _get_from_number()

    page_html = """
    <style>
        .messages-page {
            display:grid;
            gap:18px;
        }

        .messages-top-grid {
            display:grid;
            grid-template-columns:1.1fr 0.9fr;
            gap:18px;
            align-items:start;
        }

        .desktop-only { display:block; }
        .mobile-only { display:none; }

        .table-wrap {
            width:100%;
            overflow-x:auto;
            -webkit-overflow-scrolling:touch;
        }

        .message-history-table th,
        .message-history-table td {
            vertical-align:top;
        }

        .message-body-cell {
            max-width:420px;
            white-space:normal;
            word-break:break-word;
            line-height:1.35;
        }

        .thread-list {
            display:grid;
            gap:10px;
        }

        .thread-row {
            display:flex;
            justify-content:space-between;
            align-items:flex-start;
            gap:12px;
            padding:12px;
            border:1px solid rgba(15,23,42,.08);
            border-radius:12px;
            background:#fff;
        }

        .thread-row:hover {
            background:#fafafa;
        }

        .thread-main {
            min-width:0;
            flex:1;
        }

        .thread-title {
            font-weight:700;
            color:#0f172a;
            line-height:1.2;
            margin-bottom:4px;
        }

        .thread-meta {
            font-size:.85rem;
            color:#64748b;
            margin-bottom:6px;
            line-height:1.3;
        }

        .thread-preview {
            color:#334155;
            line-height:1.35;
            word-break:break-word;
        }

        .mobile-list {
            display:grid;
            gap:12px;
        }

        .mobile-list-card {
            border:1px solid rgba(15,23,42,.08);
            border-radius:14px;
            padding:14px;
            background:#fff;
            box-shadow:0 1px 2px rgba(15,23,42,.04);
        }

        .mobile-list-top {
            display:flex;
            justify-content:space-between;
            align-items:flex-start;
            gap:10px;
            margin-bottom:10px;
        }

        .mobile-list-title {
            font-weight:700;
            color:#0f172a;
            line-height:1.25;
            word-break:break-word;
        }

        .mobile-list-subtitle {
            margin-top:4px;
            font-size:.9rem;
            color:#64748b;
            line-height:1.25;
            word-break:break-word;
        }

        .mobile-badge {
            font-size:.85rem;
            font-weight:700;
            color:#334155;
            background:#f1f5f9;
            padding:6px 10px;
            border-radius:999px;
            white-space:nowrap;
        }

        .mobile-list-grid {
            display:grid;
            grid-template-columns:1fr 1fr;
            gap:10px 12px;
            margin-bottom:12px;
        }

        .mobile-list-grid span {
            display:block;
            font-size:.78rem;
            color:#64748b;
            margin-bottom:3px;
        }

        .mobile-list-grid strong {
            display:block;
            color:#0f172a;
            font-size:.95rem;
            line-height:1.25;
            word-break:break-word;
        }

        .mobile-message-body {
            margin-top:4px;
            border-top:1px solid rgba(15,23,42,.08);
            padding-top:12px;
        }

        .mobile-message-body span {
            display:block;
            font-size:.78rem;
            color:#64748b;
            margin-bottom:5px;
        }

        .mobile-message-body div {
            color:#0f172a;
            line-height:1.4;
            word-break:break-word;
            white-space:normal;
        }

        .status-pill {
            display:inline-flex;
            align-items:center;
            justify-content:center;
            padding:6px 10px;
            border-radius:999px;
            font-size:.85rem;
            font-weight:700;
            white-space:nowrap;
        }

        .status-pill.good { background:#dff3d2; color:#254314; }
        .status-pill.bad { background:#f6d5d2; color:#7a1f17; }
        .status-pill.neutral { background:#f3d77b; color:#4a3720; }

        .direction-pill.inbound { background:#d7ebff; color:#15406b; }
        .direction-pill.outbound { background:#ece8ff; color:#40307a; }

        .consent-pill {
            display:inline-flex;
            align-items:center;
            justify-content:center;
            padding:4px 9px;
            border-radius:999px;
            font-size:.78rem;
            font-weight:700;
            white-space:nowrap;
        }

        .consent-pill.yes { background:#dff3d2; color:#254314; }
        .consent-pill.no { background:#f6d5d2; color:#7a1f17; }

        @media (max-width: 900px) {
            .messages-top-grid {
                grid-template-columns:1fr;
            }
        }

        @media (max-width: 640px) {
            .desktop-only { display:none !important; }
            .mobile-only { display:block !important; }
            .mobile-list-grid { grid-template-columns:1fr; }
        }
    </style>

    <div class="messages-page">
        <div class="card">
            <div class="section-head">
                <div>
                    <h1 style="margin-bottom:6px;">Messages</h1>
                    <div class="muted">Send manual customer texts, receive replies, and reply from TerraLedger threads.</div>
                </div>
                <div class="row-actions">
                    <a class="btn secondary" href="{{ url_for('messages.messaging_configuration') }}">Messaging Configuration</a>
                </div>
            </div>
        </div>

        <div class="messages-top-grid">
            <div class="card">
                <h3>Send Message</h3>
                <form method="post" action="{{ url_for('messages.send_message') }}">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">

                    <div style="margin-bottom:14px;">
                        <label>Customer</label>
                        <select id="customerSelect" onchange="fillCustomerPhoneFromDropdown()">
                            <option value="">Select customer (required for consent-safe sending)</option>
                            {% for customer in customers %}
                                <option
                                    value="{{ customer['id'] }}"
                                    data-phone="{{ customer['phone'] or '' }}"
                                    data-opt-in="{{ 'yes' if customer['sms_opt_in'] else 'no' }}"
                                >
                                    {{ customer['name'] }}
                                    {% if customer['phone'] %} — {{ customer['phone'] }}{% endif %}
                                    {% if customer['sms_opt_in'] %}
                                        — SMS Opted In
                                    {% else %}
                                        — No SMS Consent
                                    {% endif %}
                                </option>
                            {% endfor %}
                        </select>
                    </div>

                    <input type="hidden" name="customer_id" id="customerIdField">

                    <div style="margin-bottom:10px;">
                        <label>Phone Number</label>
                        <input type="text" name="phone_number" id="phoneNumberField" placeholder="Enter mobile number" required>
                    </div>

                    <div id="customerConsentStatus" class="muted" style="margin-bottom:14px;">
                        Select a customer to verify SMS consent.
                    </div>

                    <div style="margin-bottom:14px;">
                        <label>Template</label>
                        <select id="templateSelect" onchange="applyMessageTemplate()">
                            <option value="">Choose a template (optional)</option>
                            <option value="{{ settings['default_on_the_way_template'] if settings and settings['default_on_the_way_template'] else 'Hello from TerraLedger — we are on the way to your job site. Reply STOP to opt out, HELP for help. Msg&data rates may apply.' }}">On The Way</option>
                            <option value="{{ settings['default_job_started_template'] if settings and settings['default_job_started_template'] else 'Hello from TerraLedger — we have started your scheduled job. Reply STOP to opt out, HELP for help. Msg&data rates may apply.' }}">Job Started</option>
                            <option value="{{ settings['default_job_completed_template'] if settings and settings['default_job_completed_template'] else 'Hello from TerraLedger — your job has been completed. Thank you. Reply STOP to opt out, HELP for help. Msg&data rates may apply.' }}">Job Completed</option>
                            <option value="{{ settings['default_invoice_reminder_template'] if settings and settings['default_invoice_reminder_template'] else 'Hello from TerraLedger — this is a reminder that your invoice is still outstanding. Reply STOP to opt out, HELP for help. Msg&data rates may apply.' }}">Invoice Reminder</option>
                            <option value="{{ settings['default_job_reminder_template'] if settings and settings['default_job_reminder_template'] else 'Hello {{customer_name}} — this is a reminder from {{company_name}} about {{job_title}} scheduled for {{scheduled_date}}. Reply STOP to opt out, HELP for help. Msg&data rates may apply.' }}">Job Reminder</option>
                            <option value="{{ settings['default_late_invoice_reminder_template'] if settings and settings['default_late_invoice_reminder_template'] else 'Hello {{customer_name}} — this is a reminder from {{company_name}} that invoice {{invoice_number}} is now past due. Remaining balance: {{balance_due}}. Due date: {{due_date}}. Reply STOP to opt out, HELP for help. Msg&data rates may apply.' }}">Late Invoice Reminder</option>
                        </select>
                    </div>

                    <div style="margin-bottom:14px;">
                        <label>Message</label>
                        <textarea name="message_body" id="messageBodyField" placeholder="Type your message here..." required></textarea>
                    </div>

                    <div class="muted small" style="margin-bottom:14px;">
                        Manual messages only send to customers with recorded SMS consent.
                    </div>

                    <div class="row-actions">
                        <button type="submit" class="btn">Send Message</button>
                    </div>
                </form>
            </div>

            <div class="card">
                <h3>Messaging Status</h3>

                <div style="margin-bottom:12px;">
                    <strong>Enabled:</strong>
                    {% if settings and settings['messaging_enabled'] %}
                        <span class="pill" style="background:#dff3d2;color:#254314;">Enabled</span>
                    {% else %}
                        <span class="pill warning">Not Enabled</span>
                    {% endif %}
                </div>

                <div style="margin-bottom:12px;">
                    <strong>Provider:</strong>
                    <span class="muted">TerraLedger Messaging (Twilio)</span>
                </div>

                <div style="margin-bottom:12px;">
                    <strong>From Number:</strong>
                    <span class="muted">{{ from_number if from_number else 'Platform number not configured' }}</span>
                </div>

                <div style="margin-bottom:12px;">
                    <strong>Owner Relay Alerts:</strong>
                    <span class="muted">
                        {% if settings and settings['forward_inbound_to_owner'] %}On{% else %}Off{% endif %}
                        {% if settings and settings['owner_forward_phone'] %} — {{ settings['owner_forward_phone'] }}{% endif %}
                    </span>
                </div>

                <div style="margin-bottom:12px;">
                    <strong>Manual Messages:</strong>
                    <span class="muted">{% if settings and settings['send_manual_messages'] %}On{% else %}Off{% endif %}</span>
                </div>

                <div style="margin-bottom:12px;">
                    <strong>Job Updates:</strong>
                    <span class="muted">{% if settings and settings['send_job_updates'] %}On{% else %}Off{% endif %}</span>
                </div>

                <div style="margin-bottom:12px;">
                    <strong>Job Reminders:</strong>
                    <span class="muted">
                        {% if settings and settings['enable_job_reminders'] %}On{% else %}Off{% endif %}
                        {% if settings %}( {{ settings['job_reminder_hours'] or 24 }} hrs before ){% endif %}
                    </span>
                </div>

                <div style="margin-bottom:12px;">
                    <strong>Invoice Reminders:</strong>
                    <span class="muted">{% if settings and settings['send_invoice_reminders'] %}On{% else %}Off{% endif %}</span>
                </div>

                <div style="margin-bottom:12px;">
                    <strong>Late Invoice Reminders:</strong>
                    <span class="muted">
                        {% if settings and settings['enable_late_invoice_reminders'] %}On{% else %}Off{% endif %}
                        {% if settings %}( {{ settings['late_invoice_days'] or 30 }} days late ){% endif %}
                    </span>
                </div>

                <div class="row-actions" style="margin-top:16px;">
                    <a class="btn secondary" href="{{ url_for('messages.messaging_configuration') }}">Open Configuration</a>
                </div>
            </div>
        </div>

        <div class="card">
            <div class="section-head">
                <div>
                    <h3 style="margin-bottom:6px;">Conversations</h3>
                    <div class="muted">Reply from these threads so the customer always sees your TerraLedger number.</div>
                </div>
            </div>

            {% if threads %}
                <div class="thread-list">
                    {% for thread in threads %}
                        <div class="thread-row">
                            <div class="thread-main">
                                <div class="thread-title">{{ thread['customer_name'] or thread['phone_number'] or 'Unknown' }}</div>
                                <div class="thread-meta">
                                    {{ thread['phone_number'] or '—' }} •
                                    {{ thread['message_count'] or 0 }} message(s) •
                                    {{ thread['last_created_at'] or '' }}
                                </div>
                                <div class="thread-preview">
                                    {% if thread['last_direction'] == 'inbound' %}
                                        <strong>Customer:</strong>
                                    {% else %}
                                        <strong>You:</strong>
                                    {% endif %}
                                    {{ thread['last_message_body'] or '' }}
                                </div>
                            </div>

                            <div class="row-actions">
                                <a class="btn secondary small" href="{{ url_for('messages.view_thread', thread_key=thread['conversation_key']) }}">Open Thread</a>
                            </div>
                        </div>
                    {% endfor %}
                </div>
            {% else %}
                <div class="muted">No conversations yet.</div>
            {% endif %}
        </div>

        <div class="card">
            <div class="section-head">
                <div>
                    <h3 style="margin-bottom:6px;">Message History</h3>
                    <div class="muted">Latest inbound and outbound messages for your company.</div>
                </div>
            </div>

            {% if history %}
                <div class="table-wrap desktop-only">
                    <table class="message-history-table">
                        <thead>
                            <tr>
                                <th>Date</th>
                                <th>Direction</th>
                                <th>Customer</th>
                                <th>Phone</th>
                                <th>Message</th>
                                <th>Status</th>
                                <th>Related</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for row in history %}
                            <tr>
                                <td>{{ row['created_at'] or '' }}</td>
                                <td>
                                    {% if row['direction'] == 'inbound' %}
                                        <span class="pill direction-pill inbound">Inbound</span>
                                    {% else %}
                                        <span class="pill direction-pill outbound">Outbound</span>
                                    {% endif %}
                                </td>
                                <td>{{ row['customer_name'] or '—' }}</td>
                                <td>{{ row['phone_number'] or '—' }}</td>
                                <td class="message-body-cell">{{ row['message_body'] or '' }}</td>
                                <td>
                                    {% if row['status'] in ['sent', 'received', 'delivered'] %}
                                        <span class="status-pill good">{{ row['status']|title }}</span>
                                    {% elif row['status'] == 'failed' %}
                                        <span class="status-pill bad">Failed</span>
                                    {% else %}
                                        <span class="status-pill neutral">{{ row['status'] }}</span>
                                    {% endif %}
                                </td>
                                <td>
                                    {% if row['job_title'] %}
                                        Job: {{ row['job_title'] }}<br>
                                    {% endif %}
                                    {% if row['invoice_number'] %}
                                        Invoice: {{ row['invoice_number'] }}
                                    {% endif %}
                                    {% if not row['job_title'] and not row['invoice_number'] %}
                                        —
                                    {% endif %}
                                </td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>

                <div class="mobile-only">
                    <div class="mobile-list">
                        {% for row in history %}
                            <div class="mobile-list-card">
                                <div class="mobile-list-top">
                                    <div>
                                        <div class="mobile-list-title">{{ row['customer_name'] or 'Unknown Customer' }}</div>
                                        <div class="mobile-list-subtitle">{{ row['created_at'] or '' }}</div>
                                    </div>

                                    {% if row['direction'] == 'inbound' %}
                                        <div class="mobile-badge direction-pill inbound">Inbound</div>
                                    {% else %}
                                        <div class="mobile-badge direction-pill outbound">Outbound</div>
                                    {% endif %}
                                </div>

                                <div class="mobile-list-grid">
                                    <div>
                                        <span>Phone</span>
                                        <strong>{{ row['phone_number'] or '—' }}</strong>
                                    </div>

                                    <div>
                                        <span>Status</span>
                                        <strong>
                                            {% if row['status'] in ['sent', 'received', 'delivered'] %}
                                                <span class="status-pill good">{{ row['status']|title }}</span>
                                            {% elif row['status'] == 'failed' %}
                                                <span class="status-pill bad">Failed</span>
                                            {% else %}
                                                <span class="status-pill neutral">{{ row['status'] }}</span>
                                            {% endif %}
                                        </strong>
                                    </div>

                                    <div>
                                        <span>Related</span>
                                        <strong>
                                            {% if row['job_title'] %}
                                                Job: {{ row['job_title'] }}
                                            {% elif row['invoice_number'] %}
                                                Invoice: {{ row['invoice_number'] }}
                                            {% else %}
                                                —
                                            {% endif %}
                                        </strong>
                                    </div>
                                </div>

                                <div class="mobile-message-body">
                                    <span>Message</span>
                                    <div>{{ row['message_body'] or '' }}</div>
                                </div>
                            </div>
                        {% endfor %}
                    </div>
                </div>
            {% else %}
                <div class="muted">No messages have been logged yet.</div>
            {% endif %}
        </div>
    </div>

    <script>
    function fillCustomerPhoneFromDropdown() {
        const select = document.getElementById("customerSelect");
        const phoneField = document.getElementById("phoneNumberField");
        const customerIdField = document.getElementById("customerIdField");
        const consentStatus = document.getElementById("customerConsentStatus");

        if (!select || !phoneField || !customerIdField || !consentStatus) return;

        const selected = select.options[select.selectedIndex];
        const phone = selected.getAttribute("data-phone") || "";
        const customerId = selected.value || "";
        const optIn = selected.getAttribute("data-opt-in") || "no";

        customerIdField.value = customerId;

        if (phone) {
            phoneField.value = phone;
        }

        if (!customerId) {
            consentStatus.innerHTML = "Select a customer to verify SMS consent.";
            return;
        }

        if (optIn === "yes") {
            consentStatus.innerHTML = '<span class="consent-pill yes">SMS Opted In</span>';
        } else {
            consentStatus.innerHTML = '<span class="consent-pill no">No SMS Consent On File</span>';
        }
    }

    function applyMessageTemplate() {
        const templateSelect = document.getElementById("templateSelect");
        const messageField = document.getElementById("messageBodyField");

        if (!templateSelect || !messageField) return;

        const selectedValue = templateSelect.value || "";
        if (selectedValue) {
            messageField.value = selectedValue;
        }
    }
    </script>
    """

    return render_page(
        render_template_string(
            page_html,
            settings=settings,
            history=history,
            threads=threads,
            customers=customers,
            from_number=from_number,
        ),
        "Messages",
    )


@messages_bp.route("/messages/thread/<thread_key>")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def view_thread(thread_key):
    ensure_messaging_tables()

    company_id = session.get("company_id")
    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("dashboard.dashboard"))

    if not thread_key.startswith(f"{int(company_id)}__"):
        abort(404)

    thread = get_thread_context(company_id, thread_key)
    if not thread:
        flash("Conversation not found.")
        return redirect(url_for("messages.messages_page"))

    messages = get_thread_messages(company_id, thread_key)
    reply_csrf = generate_csrf()

    page_html = """
    <style>
        .thread-page {
            display:grid;
            gap:18px;
        }

        .thread-shell {
            display:grid;
            gap:14px;
        }

        .thread-message {
            max-width:760px;
            border:1px solid rgba(15,23,42,.08);
            border-radius:14px;
            padding:12px 14px;
            background:#fff;
        }

        .thread-message.inbound {
            border-left:4px solid #60a5fa;
        }

        .thread-message.outbound {
            border-left:4px solid #8b5cf6;
        }

        .thread-message-head {
            display:flex;
            justify-content:space-between;
            gap:12px;
            flex-wrap:wrap;
            margin-bottom:6px;
        }

        .thread-message-title {
            font-weight:700;
            color:#0f172a;
        }

        .thread-message-meta {
            font-size:.84rem;
            color:#64748b;
        }

        .thread-message-body {
            color:#0f172a;
            line-height:1.45;
            white-space:pre-wrap;
            word-break:break-word;
        }
    </style>

    <div class="thread-page">
        <div class="card">
            <div class="section-head">
                <div>
                    <h1 style="margin-bottom:6px;">Conversation</h1>
                    <div class="muted">
                        {{ thread['customer_name'] or thread['phone_number'] }}{% if thread['phone_number'] %} — {{ thread['phone_number'] }}{% endif %}
                    </div>
                </div>
                <div class="row-actions">
                    <a class="btn secondary" href="{{ url_for('messages.messages_page') }}">Back to Messages</a>
                </div>
            </div>
        </div>

        <div class="card">
            <div class="thread-shell">
                {% for row in messages %}
                    <div class="thread-message {{ row['direction'] }}">
                        <div class="thread-message-head">
                            <div class="thread-message-title">
                                {% if row['direction'] == 'inbound' %}
                                    Customer
                                {% else %}
                                    TerraLedger
                                {% endif %}
                            </div>
                            <div class="thread-message-meta">
                                {{ row['created_at'] or '' }}
                                {% if row['status'] and row['direction'] == 'outbound' %}
                                    • {{ row['status'] }}
                                {% endif %}
                            </div>
                        </div>

                        <div class="thread-message-body">{{ row['message_body'] or '' }}</div>
                    </div>
                {% endfor %}
            </div>
        </div>

        <div class="card">
            <h3>Reply</h3>
            <form method="post" action="{{ url_for('messages.reply_to_thread', thread_key=thread['thread_key']) }}">
                <input type="hidden" name="csrf_token" value="{{ reply_csrf }}">
                <div style="margin-bottom:14px;">
                    <label>Message</label>
                    <textarea name="message_body" maxlength="{{ max_len }}" required placeholder="Type your reply..."></textarea>
                </div>

                <div class="row-actions">
                    <button type="submit" class="btn">Send Reply</button>
                    <a class="btn secondary" href="{{ url_for('messages.messages_page') }}">Cancel</a>
                </div>
            </form>
        </div>
    </div>
    """

    return render_page(
        render_template_string(
            page_html,
            thread=thread,
            messages=messages,
            reply_csrf=reply_csrf,
            max_len=MAX_MESSAGE_LENGTH,
        ),
        f"Conversation - {thread.get('customer_name') or thread.get('phone_number') or 'Messages'}",
    )


@messages_bp.route("/messages/thread/<thread_key>/reply", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def reply_to_thread(thread_key):
    ensure_messaging_tables()

    company_id = session.get("company_id")
    user_id = session.get("user_id")

    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("messages.messages_page"))

    if not thread_key.startswith(f"{int(company_id)}__"):
        abort(404)

    thread = get_thread_context(company_id, thread_key)
    if not thread:
        flash("Conversation not found.")
        return redirect(url_for("messages.messages_page"))

    message_body = _safe_text(request.form.get("message_body"))
    if not message_body:
        flash("Message is required.")
        return redirect(url_for("messages.view_thread", thread_key=thread_key))

    if len(message_body) > MAX_MESSAGE_LENGTH:
        flash(f"Message is too long. Keep it under {MAX_MESSAGE_LENGTH} characters.")
        return redirect(url_for("messages.view_thread", thread_key=thread_key))

    phone_number = _normalize_phone(thread.get("phone_number"))
    if not _is_reasonable_phone(phone_number):
        flash("This thread does not have a valid phone number.")
        return redirect(url_for("messages.view_thread", thread_key=thread_key))

    customer = get_customer_by_id(company_id, thread.get("customer_id")) if thread.get("customer_id") else None
    if customer and not customer_has_sms_consent(customer):
        flash("This customer has not opted in to SMS notifications.")
        return redirect(url_for("messages.view_thread", thread_key=thread_key))

    settings = get_messaging_settings(company_id)
    if settings and not settings["messaging_enabled"]:
        flash("Messaging is not enabled for this company.")
        return redirect(url_for("messages.view_thread", thread_key=thread_key))

    success, provider_message_id, error_message = send_text_message(
        to_number=phone_number,
        message_body=message_body,
        settings_row=settings,
    )

    if success:
        insert_message_log(
            company_id=company_id,
            customer_id=thread.get("customer_id"),
            phone_number=phone_number,
            message_body=message_body,
            direction="outbound",
            status="sent",
            provider="twilio",
            provider_message_id=provider_message_id,
            conversation_key=thread_key,
            sent_by_user_id=user_id,
        )

        create_notification(
            company_id=company_id,
            user_id=None,
            notif_type="message",
            title="Reply sent",
            message=f"Reply sent to {phone_number}.",
            link=url_for("messages.view_thread", thread_key=thread_key),
        )

        flash("Reply sent.")
    else:
        insert_message_log(
            company_id=company_id,
            customer_id=thread.get("customer_id"),
            phone_number=phone_number,
            message_body=message_body,
            direction="outbound",
            status="failed",
            provider="twilio",
            conversation_key=thread_key,
            sent_by_user_id=user_id,
            error_message=error_message,
        )

        create_notification(
            company_id=company_id,
            user_id=None,
            notif_type="message",
            title="Reply failed",
            message=f"Reply to {phone_number} failed: {error_message}",
            link=url_for("messages.view_thread", thread_key=thread_key),
        )

        flash(f"Reply failed: {error_message}")

    return redirect(url_for("messages.view_thread", thread_key=thread_key))


@messages_bp.route("/messages/send", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def send_message():
    ensure_messaging_tables()

    company_id = session.get("company_id")
    user_id = session.get("user_id")

    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("messages.messages_page"))

    customer_id = _safe_int(request.form.get("customer_id"), None)
    phone_number = _normalize_phone(request.form.get("phone_number"))
    message_body = _safe_text(request.form.get("message_body"))

    if not customer_id:
        flash("Please select a customer so SMS consent can be verified.")
        return redirect(url_for("messages.messages_page"))

    customer = get_customer_by_id(company_id, customer_id)
    if not customer:
        flash("Customer not found.")
        return redirect(url_for("messages.messages_page"))

    if not phone_number:
        flash("Phone number is required.")
        return redirect(url_for("messages.messages_page"))

    if not _is_reasonable_phone(phone_number):
        flash("Please enter a valid phone number.")
        return redirect(url_for("messages.messages_page"))

    customer_phone = _normalize_phone(customer.get("phone"))
    if customer_phone != phone_number:
        flash("The phone number must match the selected customer's saved phone number.")
        return redirect(url_for("messages.messages_page"))

    if not customer_has_sms_consent(customer):
        flash("This customer has not opted in to SMS notifications.")
        return redirect(url_for("messages.messages_page"))

    if not message_body:
        flash("Message is required.")
        return redirect(url_for("messages.messages_page"))

    if len(message_body) > MAX_MESSAGE_LENGTH:
        flash(f"Message is too long. Keep it under {MAX_MESSAGE_LENGTH} characters.")
        return redirect(url_for("messages.messages_page"))

    settings = get_messaging_settings(company_id)

    if settings and not settings["messaging_enabled"]:
        flash("Messaging is not enabled for this company.")
        return redirect(url_for("messages.messages_page"))

    if settings and not settings["send_manual_messages"]:
        flash("Manual messaging is disabled in messaging settings.")
        return redirect(url_for("messages.messages_page"))

    success, provider_message_id, error_message = send_text_message(
        to_number=phone_number,
        message_body=message_body,
        settings_row=settings,
    )

    thread_key = _thread_key(company_id, phone_number)

    if success:
        insert_message_log(
            company_id=company_id,
            customer_id=customer_id,
            phone_number=phone_number,
            message_body=message_body,
            direction="outbound",
            status="sent",
            provider="twilio",
            provider_message_id=provider_message_id,
            conversation_key=thread_key,
            sent_by_user_id=user_id,
        )

        create_notification(
            company_id=company_id,
            user_id=None,
            notif_type="message",
            title="Message sent",
            message=f"Message sent to {phone_number}.",
            link=url_for("messages.view_thread", thread_key=thread_key),
        )

        flash("Message sent successfully.")
        return redirect(url_for("messages.view_thread", thread_key=thread_key))

    insert_message_log(
        company_id=company_id,
        customer_id=customer_id,
        phone_number=phone_number,
        message_body=message_body,
        direction="outbound",
        status="failed",
        provider="twilio",
        conversation_key=thread_key,
        sent_by_user_id=user_id,
        error_message=error_message,
    )

    create_notification(
        company_id=company_id,
        user_id=None,
        notif_type="message",
        title="Message failed",
        message=f"Message to {phone_number} failed: {error_message}",
        link=url_for("messages.messages_page"),
    )

    flash(f"Message failed: {error_message}")
    return redirect(url_for("messages.messages_page"))


@messages_bp.route("/messages/run-automations", methods=["POST"])
@csrf.exempt
def run_message_automations():
    ensure_messaging_tables()

    if not _validate_automation_request(request):
        return Response("Forbidden", status=403)

    job_results = process_job_reminders()
    invoice_results = process_late_invoice_reminders()

    summary = (
        f"Job reminders: sent={job_results['sent']}, failed={job_results['failed']}, checked={job_results['checked']} | "
        f"Late invoices: sent={invoice_results['sent']}, failed={invoice_results['failed']}, checked={invoice_results['checked']}"
    )
    return Response(summary, status=200, mimetype="text/plain")


@messages_bp.route("/messages/webhook", methods=["GET", "POST"])
@csrf.exempt
def incoming_message_webhook():
    ensure_messaging_tables()

    if request.method == "GET":
        return Response("Twilio SMS webhook is live.", status=200, mimetype="text/plain")

    if not _validate_twilio_request(request):
        return Response("Forbidden", status=403)

    from_number = _normalize_phone(request.form.get("From"))
    to_number = _normalize_phone(request.form.get("To"))
    body = _safe_text(request.form.get("Body"))
    provider_message_id = _safe_text(request.form.get("MessageSid"))

    resp = MessagingResponse()

    if not from_number:
        return Response(str(resp), mimetype="application/xml")

    platform_number = _get_from_number()
    if platform_number and to_number and to_number != platform_number:
        return Response(str(resp), mimetype="application/xml")

    matched_company_id = None
    matched_customer_id = None
    conversation_key = None

    last_conversation = get_last_conversation_for_phone(from_number)
    if last_conversation:
        matched_company_id = last_conversation["company_id"]
        matched_customer_id = last_conversation["customer_id"]
        conversation_key = last_conversation.get("conversation_key")

    if not matched_company_id:
        conn = get_db_connection()
        try:
            enabled_rows = conn.execute("""
                SELECT company_id
                FROM messaging_settings
                WHERE messaging_enabled = TRUE
                ORDER BY company_id ASC
            """).fetchall()
        finally:
            conn.close()

        for row in enabled_rows:
            company_id = row["company_id"]
            customer = find_customer_by_phone(company_id, from_number)
            if customer:
                matched_company_id = company_id
                matched_customer_id = customer["id"]
                conversation_key = _thread_key(company_id, from_number)
                break

    normalized_body = _safe_text(body).strip().lower()
    first_word = normalized_body.split()[0] if normalized_body else ""

    if matched_company_id:
        if not conversation_key:
            conversation_key = _thread_key(matched_company_id, from_number)

        insert_message_log(
            company_id=matched_company_id,
            customer_id=matched_customer_id,
            phone_number=from_number,
            message_body=body or "",
            direction="inbound",
            status="received",
            provider="twilio",
            provider_message_id=provider_message_id,
            conversation_key=conversation_key,
        )

        if first_word in STOP_WORDS:
            set_customer_sms_consent(
                company_id=matched_company_id,
                phone_number=from_number,
                is_opted_in=False,
                method="sms_reply_stop",
            )
        elif first_word in START_WORDS:
            set_customer_sms_consent(
                company_id=matched_company_id,
                phone_number=from_number,
                is_opted_in=True,
                method="sms_reply_start",
            )

        customer_name = ""
        if matched_customer_id:
            customer = find_customer_by_phone(matched_company_id, from_number)
            if customer:
                customer_name = _safe_text(customer.get("name"))

        preview = _preview_text(body, 120)

        create_notification(
            company_id=matched_company_id,
            user_id=None,
            notif_type="message",
            title="New inbound message",
            message=f"{customer_name or from_number}: {preview}",
            link=url_for("messages.view_thread", thread_key=conversation_key),
        )

        if body:
            _send_owner_relay_alert(
                company_id=matched_company_id,
                customer_id=matched_customer_id,
                phone_number=from_number,
                inbound_body=body,
                thread_key=conversation_key,
            )

    if first_word in STOP_WORDS:
        resp.message("You have been opted out of SMS messages from TerraLedger. Reply START to opt back in.")
    elif first_word in HELP_WORDS:
        resp.message("TerraLedger support: Reply STOP to opt out. Reply START to opt back in.")
    elif first_word in START_WORDS:
        resp.message("You have been opted back in to SMS messages from TerraLedger.")
    else:
        # no auto-reply for normal inbound messages
        pass

    return Response(str(resp), mimetype="application/xml")


@messages_bp.route("/messages/status-callback", methods=["GET", "POST"])
@csrf.exempt
def message_status_callback():
    if request.method == "GET":
        return Response("Twilio status callback is live.", status=200, mimetype="text/plain")

    if not _validate_twilio_request(request):
        return Response("Forbidden", status=403)

    provider_message_id = _safe_text(request.form.get("MessageSid"))
    message_status = _safe_text(request.form.get("MessageStatus")).lower()
    error_code = _safe_text(request.form.get("ErrorCode"))
    error_message = _safe_text(request.form.get("ErrorMessage"))

    normalized_status = "queued"
    if message_status in {"sent", "accepted", "queued", "sending"}:
        normalized_status = "sent"
    elif message_status in {"delivered"}:
        normalized_status = "delivered"
    elif message_status in {"failed", "undelivered"}:
        normalized_status = "failed"

    final_error = ""
    if normalized_status == "failed":
        if error_message:
            final_error = error_message
        elif error_code:
            final_error = f"Twilio error code: {error_code}"
        else:
            final_error = "Message delivery failed."

    update_message_status_by_provider_id(
        provider_message_id=provider_message_id,
        status=normalized_status,
        error_message=final_error or None,
    )

    return Response("OK", status=200)


@messages_bp.route("/messaging/configuration", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def messaging_configuration():
    ensure_messaging_tables()

    company_id = session.get("company_id")
    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("dashboard.dashboard"))

    conn = get_db_connection()
    try:
        if request.method == "POST":
            messaging_enabled = request.form.get("messaging_enabled") == "on"
            send_job_updates = request.form.get("send_job_updates") == "on"
            send_invoice_reminders = request.form.get("send_invoice_reminders") == "on"
            send_manual_messages = request.form.get("send_manual_messages") == "on"
            enable_job_reminders = request.form.get("enable_job_reminders") == "on"
            job_reminder_hours = _safe_int(request.form.get("job_reminder_hours"), 24)
            enable_late_invoice_reminders = request.form.get("enable_late_invoice_reminders") == "on"
            late_invoice_days = _safe_int(request.form.get("late_invoice_days"), 30)
            forward_inbound_to_owner = request.form.get("forward_inbound_to_owner") == "on"
            owner_forward_phone = _normalize_phone(request.form.get("owner_forward_phone"))

            if job_reminder_hours is None or job_reminder_hours < 1:
                job_reminder_hours = 24

            if late_invoice_days is None or late_invoice_days < 1:
                late_invoice_days = 30

            default_on_the_way_template = _safe_text(request.form.get("default_on_the_way_template"))
            default_job_started_template = _safe_text(request.form.get("default_job_started_template"))
            default_job_completed_template = _safe_text(request.form.get("default_job_completed_template"))
            default_invoice_reminder_template = _safe_text(request.form.get("default_invoice_reminder_template"))
            default_job_reminder_template = _safe_text(request.form.get("default_job_reminder_template"))
            default_late_invoice_reminder_template = _safe_text(request.form.get("default_late_invoice_reminder_template"))

            existing = get_messaging_settings(company_id)

            if existing:
                conn.execute("""
                    UPDATE messaging_settings
                    SET messaging_enabled = %s,
                        send_job_updates = %s,
                        send_invoice_reminders = %s,
                        send_manual_messages = %s,
                        enable_job_reminders = %s,
                        job_reminder_hours = %s,
                        enable_late_invoice_reminders = %s,
                        late_invoice_days = %s,
                        forward_inbound_to_owner = %s,
                        owner_forward_phone = %s,
                        default_on_the_way_template = %s,
                        default_job_started_template = %s,
                        default_job_completed_template = %s,
                        default_invoice_reminder_template = %s,
                        default_job_reminder_template = %s,
                        default_late_invoice_reminder_template = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE company_id = %s
                """, (
                    messaging_enabled,
                    send_job_updates,
                    send_invoice_reminders,
                    send_manual_messages,
                    enable_job_reminders,
                    job_reminder_hours,
                    enable_late_invoice_reminders,
                    late_invoice_days,
                    forward_inbound_to_owner,
                    owner_forward_phone or None,
                    default_on_the_way_template,
                    default_job_started_template,
                    default_job_completed_template,
                    default_invoice_reminder_template,
                    default_job_reminder_template,
                    default_late_invoice_reminder_template,
                    company_id,
                ))
            else:
                conn.execute("""
                    INSERT INTO messaging_settings (
                        company_id,
                        messaging_enabled,
                        send_job_updates,
                        send_invoice_reminders,
                        send_manual_messages,
                        enable_job_reminders,
                        job_reminder_hours,
                        enable_late_invoice_reminders,
                        late_invoice_days,
                        forward_inbound_to_owner,
                        owner_forward_phone,
                        default_on_the_way_template,
                        default_job_started_template,
                        default_job_completed_template,
                        default_invoice_reminder_template,
                        default_job_reminder_template,
                        default_late_invoice_reminder_template
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    company_id,
                    messaging_enabled,
                    send_job_updates,
                    send_invoice_reminders,
                    send_manual_messages,
                    enable_job_reminders,
                    job_reminder_hours,
                    enable_late_invoice_reminders,
                    late_invoice_days,
                    forward_inbound_to_owner,
                    owner_forward_phone or None,
                    default_on_the_way_template,
                    default_job_started_template,
                    default_job_completed_template,
                    default_invoice_reminder_template,
                    default_job_reminder_template,
                    default_late_invoice_reminder_template,
                ))

            conn.commit()
            flash("Messaging configuration saved.")
            return redirect(url_for("messages.messaging_configuration"))
    finally:
        conn.close()

    settings = get_messaging_settings(company_id)
    from_number = _get_from_number()
    inbound_webhook_url = _get_full_inbound_webhook_url()
    status_callback_url = _get_full_status_callback_url() or "/messages/status-callback"

    page_html = """
    <div class="card">
        <div class="section-head">
            <div>
                <h1 style="margin-bottom:6px;">Messaging Configuration</h1>
                <div class="muted">Control messaging preferences, owner relay alerts, automation, and default templates.</div>
            </div>
            <div class="row-actions">
                <a class="btn secondary" href="{{ url_for('messages.messages_page') }}">Back to Messages</a>
            </div>
        </div>
    </div>

    <div class="card">
        <form method="post">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">

            <div class="card" style="margin-top:0; margin-bottom:18px;">
                <h3>Platform Messaging</h3>

                <div style="margin-bottom:10px;">
                    <strong>Provider:</strong>
                    <span class="muted">TerraLedger Messaging (Twilio)</span>
                </div>

                <div style="margin-bottom:10px;">
                    <strong>Sending Number:</strong>
                    <span class="muted">{{ from_number if from_number else 'Platform number not configured yet' }}</span>
                </div>

                <div style="margin-bottom:10px;">
                    <strong>Inbound Webhook:</strong>
                    <span class="muted">{{ inbound_webhook_url }}</span>
                </div>

                <div style="margin-bottom:0;">
                    <strong>Status Callback:</strong>
                    <span class="muted">{{ status_callback_url }}</span>
                </div>

                <div class="muted small" style="margin-top:12px;">
                    Messaging is provided by TerraLedger. Customers do not need to connect their own Twilio account.
                    Only customers with recorded SMS consent will receive outbound messages.
                </div>
            </div>

            <div class="card" style="margin-top:0; margin-bottom:18px;">
                <h3>Reply Relay Settings</h3>

                <div style="display:grid; gap:10px;">
                    <label style="display:flex; align-items:center; gap:10px; font-weight:600;">
                        <input type="checkbox" name="forward_inbound_to_owner" {% if not settings or settings['forward_inbound_to_owner'] %}checked{% endif %}>
                        Forward inbound customer replies to owner phone
                    </label>

                    <div style="margin-left:28px;">
                        <label>Owner Forward Phone</label>
                        <input type="text" name="owner_forward_phone" value="{{ settings['owner_forward_phone'] if settings and settings['owner_forward_phone'] else '' }}" placeholder="+1XXXXXXXXXX">
                        <div class="muted small" style="margin-top:6px;">
                            When a customer replies, TerraLedger sends an alert text to this phone with a link to the TerraLedger conversation thread.
                        </div>
                    </div>
                </div>
            </div>

            <div class="card" style="margin-top:0; margin-bottom:18px;">
                <h3>Messaging Options</h3>

                <div style="display:grid; gap:10px;">
                    <label style="display:flex; align-items:center; gap:10px; font-weight:600;">
                        <input type="checkbox" name="messaging_enabled" {% if settings and settings['messaging_enabled'] %}checked{% endif %}>
                        Enable Messaging
                    </label>

                    <label style="display:flex; align-items:center; gap:10px; font-weight:600;">
                        <input type="checkbox" name="send_manual_messages" {% if not settings or settings['send_manual_messages'] %}checked{% endif %}>
                        Allow Manual Messages
                    </label>

                    <label style="display:flex; align-items:center; gap:10px; font-weight:600;">
                        <input type="checkbox" name="send_job_updates" {% if not settings or settings['send_job_updates'] %}checked{% endif %}>
                        Enable Job Update Messages
                    </label>

                    <label style="display:flex; align-items:center; gap:10px; font-weight:600;">
                        <input type="checkbox" name="enable_job_reminders" {% if not settings or settings['enable_job_reminders'] %}checked{% endif %}>
                        Enable Automated Job Reminders
                    </label>

                    <div style="margin-left:28px;">
                        <label>Hours Before Job</label>
                        <input type="number" min="1" name="job_reminder_hours" value="{{ settings['job_reminder_hours'] if settings else 24 }}">
                    </div>

                    <label style="display:flex; align-items:center; gap:10px; font-weight:600;">
                        <input type="checkbox" name="send_invoice_reminders" {% if settings and settings['send_invoice_reminders'] %}checked{% endif %}>
                        Enable Invoice Reminder Messages
                    </label>

                    <label style="display:flex; align-items:center; gap:10px; font-weight:600;">
                        <input type="checkbox" name="enable_late_invoice_reminders" {% if settings and settings['enable_late_invoice_reminders'] %}checked{% endif %}>
                        Enable Automated Late Invoice Reminders
                    </label>

                    <div style="margin-left:28px;">
                        <label>Days Late Before Sending</label>
                        <input type="number" min="1" name="late_invoice_days" value="{{ settings['late_invoice_days'] if settings else 30 }}">
                    </div>
                </div>
            </div>

            <div class="card" style="margin-top:0;">
                <h3>Default Templates</h3>

                <div style="margin-bottom:14px;">
                    <label>On The Way Template</label>
                    <textarea name="default_on_the_way_template">{{ settings['default_on_the_way_template'] if settings and settings['default_on_the_way_template'] else 'Hello from TerraLedger — we are on the way to your job site. Reply STOP to opt out, HELP for help. Msg&data rates may apply.' }}</textarea>
                </div>

                <div style="margin-bottom:14px;">
                    <label>Job Started Template</label>
                    <textarea name="default_job_started_template">{{ settings['default_job_started_template'] if settings and settings['default_job_started_template'] else 'Hello from TerraLedger — we have started your scheduled job. Reply STOP to opt out, HELP for help. Msg&data rates may apply.' }}</textarea>
                </div>

                <div style="margin-bottom:14px;">
                    <label>Job Completed Template</label>
                    <textarea name="default_job_completed_template">{{ settings['default_job_completed_template'] if settings and settings['default_job_completed_template'] else 'Hello from TerraLedger — your job has been completed. Thank you. Reply STOP to opt out, HELP for help. Msg&data rates may apply.' }}</textarea>
                </div>

                <div style="margin-bottom:14px;">
                    <label>Invoice Reminder Template</label>
                    <textarea name="default_invoice_reminder_template">{{ settings['default_invoice_reminder_template'] if settings and settings['default_invoice_reminder_template'] else 'Hello from TerraLedger — this is a reminder that your invoice is still outstanding. Reply STOP to opt out, HELP for help. Msg&data rates may apply.' }}</textarea>
                </div>

                <div style="margin-bottom:14px;">
                    <label>Job Reminder Template</label>
                    <textarea name="default_job_reminder_template">{{ settings['default_job_reminder_template'] if settings and settings['default_job_reminder_template'] else 'Hello {{customer_name}} — this is a reminder from {{company_name}} about {{job_title}} scheduled for {{scheduled_date}}. Reply STOP to opt out, HELP for help. Msg&data rates may apply.' }}</textarea>
                </div>

                <div style="margin-bottom:0;">
                    <label>Late Invoice Reminder Template</label>
                    <textarea name="default_late_invoice_reminder_template">{{ settings['default_late_invoice_reminder_template'] if settings and settings['default_late_invoice_reminder_template'] else 'Hello {{customer_name}} — this is a reminder from {{company_name}} that invoice {{invoice_number}} is now past due. Remaining balance: {{balance_due}}. Due date: {{due_date}}. Reply STOP to opt out, HELP for help. Msg&data rates may apply.' }}</textarea>
                </div>
            </div>

            <div class="row-actions" style="margin-top:18px;">
                <button type="submit" class="btn">Save Configuration</button>
                <a class="btn secondary" href="{{ url_for('messages.messages_page') }}">Cancel</a>
            </div>
        </form>
    </div>
    """

    return render_page(
        render_template_string(
            page_html,
            settings=settings,
            from_number=from_number,
            inbound_webhook_url=inbound_webhook_url,
            status_callback_url=status_callback_url,
        ),
        "Messaging Configuration",
    )