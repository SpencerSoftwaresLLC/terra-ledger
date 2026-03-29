import os
import re

from flask import (
    Blueprint,
    request,
    redirect,
    url_for,
    flash,
    session,
    render_template_string,
    Response,
)
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
from twilio.twiml.messaging_response import MessagingResponse
from twilio.request_validator import RequestValidator

from db import get_db_connection, table_columns
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page
from extensions import csrf


messages_bp = Blueprint("messages", __name__)

MAX_MESSAGE_LENGTH = 1600


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


def _normalize_phone(value):
    """
    Normalize to E.164 when possible.
    Assumes US/Canada for 10-digit local numbers.
    """
    text = _safe_text(value)
    if not text:
        return ""

    digits = re.sub(r"\D", "", text)

    if not digits:
        return ""

    if len(digits) == 10:
        return f"+1{digits}"

    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"

    if 10 <= len(digits) <= 15:
        return f"+{digits}"

    return ""


def _digits_only(value):
    return re.sub(r"\D", "", _safe_text(value))


def _is_reasonable_phone(value):
    if not value:
        return False
    digits = _digits_only(value)
    return 10 <= len(digits) <= 15


def _to_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False

    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y", "on"}


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


def _validate_twilio_request(req):
    auth_token = _safe_text(os.environ.get("TWILIO_AUTH_TOKEN"))
    if not auth_token:
        return False

    signature = req.headers.get("X-Twilio-Signature", "")
    if not signature:
        return False

    validator = RequestValidator(auth_token)

    try:
        return validator.validate(
            req.url,
            req.form,
            signature,
        )
    except Exception:
        return False


def ensure_messaging_tables():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # ---------- messaging_settings ----------
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'messaging_settings'
            ) AS exists_flag
        """)
        settings_exists = cur.fetchone()["exists_flag"]

        if not settings_exists:
            cur.execute("""
                CREATE TABLE messaging_settings (
                    id SERIAL PRIMARY KEY,
                    company_id INTEGER NOT NULL UNIQUE,
                    messaging_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    send_job_updates BOOLEAN NOT NULL DEFAULT TRUE,
                    send_invoice_reminders BOOLEAN NOT NULL DEFAULT FALSE,
                    send_manual_messages BOOLEAN NOT NULL DEFAULT TRUE,
                    default_on_the_way_template TEXT,
                    default_job_started_template TEXT,
                    default_job_completed_template TEXT,
                    default_invoice_reminder_template TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'messaging_settings'
            """)
            existing_columns = {row["column_name"]: row["data_type"] for row in cur.fetchall()}

            required_columns = {
                "id": "SERIAL PRIMARY KEY",
                "company_id": "INTEGER NOT NULL UNIQUE",
                "messaging_enabled": "BOOLEAN NOT NULL DEFAULT FALSE",
                "send_job_updates": "BOOLEAN NOT NULL DEFAULT TRUE",
                "send_invoice_reminders": "BOOLEAN NOT NULL DEFAULT FALSE",
                "send_manual_messages": "BOOLEAN NOT NULL DEFAULT TRUE",
                "default_on_the_way_template": "TEXT",
                "default_job_started_template": "TEXT",
                "default_job_completed_template": "TEXT",
                "default_invoice_reminder_template": "TEXT",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            }

            # If any of the boolean columns are not actually boolean, rebuild the table cleanly.
            bool_columns = [
                "messaging_enabled",
                "send_job_updates",
                "send_invoice_reminders",
                "send_manual_messages",
            ]

            needs_rebuild = False
            for col in bool_columns:
                if col in existing_columns and existing_columns[col] != "boolean":
                    needs_rebuild = True
                    break

            if needs_rebuild:
                cur.execute("""
                    CREATE TABLE messaging_settings_new (
                        id SERIAL PRIMARY KEY,
                        company_id INTEGER NOT NULL UNIQUE,
                        messaging_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                        send_job_updates BOOLEAN NOT NULL DEFAULT TRUE,
                        send_invoice_reminders BOOLEAN NOT NULL DEFAULT FALSE,
                        send_manual_messages BOOLEAN NOT NULL DEFAULT TRUE,
                        default_on_the_way_template TEXT,
                        default_job_started_template TEXT,
                        default_job_completed_template TEXT,
                        default_invoice_reminder_template TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                cur.execute("""
                    INSERT INTO messaging_settings_new (
                        company_id,
                        messaging_enabled,
                        send_job_updates,
                        send_invoice_reminders,
                        send_manual_messages,
                        default_on_the_way_template,
                        default_job_started_template,
                        default_job_completed_template,
                        default_invoice_reminder_template,
                        created_at,
                        updated_at
                    )
                    SELECT
                        company_id,
                        CASE
                            WHEN messaging_enabled IS NULL THEN FALSE
                            WHEN LOWER(BTRIM(messaging_enabled::text)) IN ('1', 'true', 't', 'yes', 'y', 'on') THEN TRUE
                            ELSE FALSE
                        END,
                        CASE
                            WHEN send_job_updates IS NULL THEN TRUE
                            WHEN LOWER(BTRIM(send_job_updates::text)) IN ('1', 'true', 't', 'yes', 'y', 'on') THEN TRUE
                            ELSE FALSE
                        END,
                        CASE
                            WHEN send_invoice_reminders IS NULL THEN FALSE
                            WHEN LOWER(BTRIM(send_invoice_reminders::text)) IN ('1', 'true', 't', 'yes', 'y', 'on') THEN TRUE
                            ELSE FALSE
                        END,
                        CASE
                            WHEN send_manual_messages IS NULL THEN TRUE
                            WHEN LOWER(BTRIM(send_manual_messages::text)) IN ('1', 'true', 't', 'yes', 'y', 'on') THEN TRUE
                            ELSE FALSE
                        END,
                        default_on_the_way_template,
                        default_job_started_template,
                        default_job_completed_template,
                        default_invoice_reminder_template,
                        COALESCE(created_at, CURRENT_TIMESTAMP),
                        COALESCE(updated_at, CURRENT_TIMESTAMP)
                    FROM messaging_settings
                    ON CONFLICT (company_id) DO NOTHING
                """)

                cur.execute("DROP TABLE messaging_settings")
                cur.execute("ALTER TABLE messaging_settings_new RENAME TO messaging_settings")
            else:
                # Add any missing columns without rebuilding.
                for col_name, col_def in required_columns.items():
                    if col_name not in existing_columns and col_name != "id":
                        cur.execute(f"ALTER TABLE messaging_settings ADD COLUMN {col_name} {col_def}")

        # ---------- message_log ----------
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

        message_required_columns = {
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
            "sent_by_user_id": "INTEGER",
            "error_message": "TEXT",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "sent_at": "TIMESTAMP",
        }

        for col_name, col_def in message_required_columns.items():
            if col_name not in message_cols:
                cur.execute(f"ALTER TABLE message_log ADD COLUMN {col_name} {col_def}")

        conn.commit()
    finally:
        conn.close()


def get_messaging_settings(company_id):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT *
            FROM messaging_settings
            WHERE company_id = %s
        """, (company_id,))
        row = cur.fetchone()
        if row:
            row["messaging_enabled"] = _to_bool(row.get("messaging_enabled"))
            row["send_job_updates"] = _to_bool(row.get("send_job_updates"))
            row["send_invoice_reminders"] = _to_bool(row.get("send_invoice_reminders"))
            row["send_manual_messages"] = _to_bool(row.get("send_manual_messages"))
        return row
    finally:
        conn.close()


def get_message_history(company_id, limit=100):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
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
        """, (company_id, limit))
        return cur.fetchall()
    finally:
        conn.close()


def get_customers_for_messages(company_id):
    conn = get_db_connection()

    try:
        cols = table_columns(conn, "customers")
        if not cols:
            return []

        phone_col = None
        if "phone" in cols:
            phone_col = "phone"
        elif "phone_number" in cols:
            phone_col = "phone_number"

        if not phone_col:
            return conn.execute("""
                SELECT id, name, NULL AS phone
                FROM customers
                WHERE company_id = %s
                ORDER BY name ASC
            """, (company_id,)).fetchall()

        return conn.execute(f"""
            SELECT id, name, {phone_col} AS phone
            FROM customers
            WHERE company_id = %s
            ORDER BY name ASC
        """, (company_id,)).fetchall()
    except Exception:
        return []
    finally:
        conn.close()


def find_customer_by_phone(company_id, phone_number):
    conn = get_db_connection()
    try:
        cols = table_columns(conn, "customers")
        if not cols:
            return None

        phone_col = None
        if "phone" in cols:
            phone_col = "phone"
        elif "phone_number" in cols:
            phone_col = "phone_number"

        if not phone_col:
            return None

        search_digits = _digits_only(phone_number)
        if not search_digits:
            return None

        cur = conn.cursor()
        cur.execute(f"""
            SELECT id, name, {phone_col} AS phone
            FROM customers
            WHERE company_id = %s
            ORDER BY id DESC
        """, (company_id,))
        rows = cur.fetchall()

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
    sent_by_user_id=None,
    error_message=None,
):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
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
                sent_by_user_id,
                error_message,
                sent_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                CASE WHEN %s IN ('sent', 'received', 'delivered') THEN CURRENT_TIMESTAMP ELSE NULL END
            )
        """, (
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
        cur = conn.cursor()
        cur.execute("""
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

    status_callback_url = _safe_text(os.environ.get("TWILIO_STATUS_CALLBACK_URL"))

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
    customers = get_customers_for_messages(company_id)
    from_number = _get_from_number()

    page_html = """
    <div class="card">
        <div class="section-head">
            <div>
                <h1 style="margin-bottom:6px;">Messages</h1>
                <div class="muted">Send manual customer texts, receive replies, and review message history.</div>
            </div>
            <div class="row-actions">
                <a class="btn secondary" href="{{ url_for('messages.messaging_configuration') }}">
                    Messaging Configuration
                </a>
            </div>
        </div>
    </div>

    <div class="grid" style="align-items:start;">
        <div class="card">
            <h3>Send Message</h3>
            <form method="post" action="{{ url_for('messages.send_message') }}">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">

                <div style="margin-bottom:14px;">
                    <label>Customer</label>
                    <select id="customerSelect" onchange="fillCustomerPhoneFromDropdown()">
                        <option value="">Select customer (optional)</option>
                        {% for customer in customers %}
                            <option
                                value="{{ customer['id'] }}"
                                data-phone="{{ customer['phone'] or '' }}"
                            >
                                {{ customer['name'] }}{% if customer['phone'] %} — {{ customer['phone'] }}{% endif %}
                            </option>
                        {% endfor %}
                    </select>
                </div>

                <input type="hidden" name="customer_id" id="customerIdField">

                <div style="margin-bottom:14px;">
                    <label>Phone Number</label>
                    <input
                        type="text"
                        name="phone_number"
                        id="phoneNumberField"
                        placeholder="Enter mobile number"
                        required
                    >
                </div>

                <div style="margin-bottom:14px;">
                    <label>Template</label>
                    <select id="templateSelect" onchange="applyMessageTemplate()">
                        <option value="">Choose a template (optional)</option>
                        <option value="{{ settings['default_on_the_way_template'] if settings and settings['default_on_the_way_template'] else 'Hello from TerraLedger — we are on the way to your job site.' }}">
                            On The Way
                        </option>
                        <option value="{{ settings['default_job_started_template'] if settings and settings['default_job_started_template'] else 'Hello from TerraLedger — we have started your scheduled job.' }}">
                            Job Started
                        </option>
                        <option value="{{ settings['default_job_completed_template'] if settings and settings['default_job_completed_template'] else 'Hello from TerraLedger — your job has been completed. Thank you.' }}">
                            Job Completed
                        </option>
                        <option value="{{ settings['default_invoice_reminder_template'] if settings and settings['default_invoice_reminder_template'] else 'Hello from TerraLedger — this is a reminder that your invoice is still outstanding.' }}">
                            Invoice Reminder
                        </option>
                    </select>
                </div>

                <div style="margin-bottom:14px;">
                    <label>Message</label>
                    <textarea
                        name="message_body"
                        id="messageBodyField"
                        placeholder="Type your message here..."
                        required
                    ></textarea>
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
                <strong>Manual Messages:</strong>
                <span class="muted">
                    {% if settings and settings['send_manual_messages'] %}On{% else %}Off{% endif %}
                </span>
            </div>

            <div style="margin-bottom:12px;">
                <strong>Job Updates:</strong>
                <span class="muted">
                    {% if settings and settings['send_job_updates'] %}On{% else %}Off{% endif %}
                </span>
            </div>

            <div style="margin-bottom:12px;">
                <strong>Invoice Reminders:</strong>
                <span class="muted">
                    {% if settings and settings['send_invoice_reminders'] %}On{% else %}Off{% endif %}
                </span>
            </div>

            <div class="row-actions" style="margin-top:16px;">
                <a class="btn secondary" href="{{ url_for('messages.messaging_configuration') }}">
                    Open Configuration
                </a>
            </div>
        </div>
    </div>

    <div class="card">
        <div class="section-head">
            <div>
                <h3 style="margin-bottom:6px;">Message History</h3>
                <div class="muted">Latest inbound and outbound messages for your company.</div>
            </div>
        </div>

        {% if history %}
            <div class="table-wrap">
                <table>
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
                                    <span class="pill" style="background:#d7ebff;color:#15406b;">Inbound</span>
                                {% else %}
                                    <span class="pill" style="background:#ece8ff;color:#40307a;">Outbound</span>
                                {% endif %}
                            </td>
                            <td>{{ row['customer_name'] or '—' }}</td>
                            <td>{{ row['phone_number'] or '—' }}</td>
                            <td style="max-width:420px; white-space:normal;">{{ row['message_body'] or '' }}</td>
                            <td>
                                {% if row['status'] in ['sent', 'received', 'delivered'] %}
                                    <span class="pill" style="background:#dff3d2;color:#254314;">{{ row['status']|title }}</span>
                                {% elif row['status'] == 'failed' %}
                                    <span class="pill" style="background:#f6d5d2;color:#7a1f17;">Failed</span>
                                {% else %}
                                    <span class="pill warning">{{ row['status'] }}</span>
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
        {% else %}
            <div class="muted">No messages have been logged yet.</div>
        {% endif %}
    </div>

    <script>
    function fillCustomerPhoneFromDropdown() {
        const select = document.getElementById("customerSelect");
        const phoneField = document.getElementById("phoneNumberField");
        const customerIdField = document.getElementById("customerIdField");

        if (!select || !phoneField || !customerIdField) return;

        const selected = select.options[select.selectedIndex];
        const phone = selected.getAttribute("data-phone") || "";
        const customerId = selected.value || "";

        customerIdField.value = customerId;

        if (phone) {
            phoneField.value = phone;
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
            customers=customers,
            from_number=from_number,
        ),
        "Messages"
    )


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

    if not phone_number:
        flash("Phone number is required.")
        return redirect(url_for("messages.messages_page"))

    if not _is_reasonable_phone(phone_number):
        flash("Please enter a valid phone number.")
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
            sent_by_user_id=user_id,
        )
        flash("Message sent successfully.")
    else:
        insert_message_log(
            company_id=company_id,
            customer_id=customer_id,
            phone_number=phone_number,
            message_body=message_body,
            direction="outbound",
            status="failed",
            provider="twilio",
            sent_by_user_id=user_id,
            error_message=error_message,
        )
        flash(f"Message failed: {error_message}")

    return redirect(url_for("messages.messages_page"))


@messages_bp.route("/messages/webhook", methods=["POST"])
@csrf.exempt
def incoming_message_webhook():
    ensure_messaging_tables()

    if not _validate_twilio_request(request):
        return Response("Forbidden", status=403)

    from_number = _normalize_phone(request.form.get("From"))
    to_number = _normalize_phone(request.form.get("To"))
    body = _safe_text(request.form.get("Body"))
    provider_message_id = _safe_text(request.form.get("MessageSid"))

    if not from_number or not body:
        resp = MessagingResponse()
        return Response(str(resp), mimetype="application/xml")

    platform_number = _get_from_number()
    if not platform_number or to_number != platform_number:
        resp = MessagingResponse()
        return Response(str(resp), mimetype="application/xml")

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT company_id
            FROM messaging_settings
            WHERE messaging_enabled = TRUE
            ORDER BY company_id ASC
        """)
        enabled_rows = cur.fetchall()
    finally:
        conn.close()

    matched_company_id = None
    matched_customer = None

    for row in enabled_rows:
        company_id = row["company_id"]
        customer = find_customer_by_phone(company_id, from_number)
        if customer:
            matched_company_id = company_id
            matched_customer = customer
            break

    if matched_company_id:
        insert_message_log(
            company_id=matched_company_id,
            customer_id=matched_customer["id"] if matched_customer else None,
            phone_number=from_number,
            message_body=body,
            direction="inbound",
            status="received",
            provider="twilio",
            provider_message_id=provider_message_id,
        )

    resp = MessagingResponse()
    return Response(str(resp), mimetype="application/xml")


@messages_bp.route("/messages/status-callback", methods=["POST"])
@csrf.exempt
def message_status_callback():
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
    cur = conn.cursor()

    try:
        if request.method == "POST":
            messaging_enabled = request.form.get("messaging_enabled") == "on"
            send_job_updates = request.form.get("send_job_updates") == "on"
            send_invoice_reminders = request.form.get("send_invoice_reminders") == "on"
            send_manual_messages = request.form.get("send_manual_messages") == "on"

            default_on_the_way_template = _safe_text(request.form.get("default_on_the_way_template"))
            default_job_started_template = _safe_text(request.form.get("default_job_started_template"))
            default_job_completed_template = _safe_text(request.form.get("default_job_completed_template"))
            default_invoice_reminder_template = _safe_text(request.form.get("default_invoice_reminder_template"))

            existing = get_messaging_settings(company_id)

            if existing:
                cur.execute("""
                    UPDATE messaging_settings
                    SET messaging_enabled = %s,
                        send_job_updates = %s,
                        send_invoice_reminders = %s,
                        send_manual_messages = %s,
                        default_on_the_way_template = %s,
                        default_job_started_template = %s,
                        default_job_completed_template = %s,
                        default_invoice_reminder_template = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE company_id = %s
                """, (
                    messaging_enabled,
                    send_job_updates,
                    send_invoice_reminders,
                    send_manual_messages,
                    default_on_the_way_template,
                    default_job_started_template,
                    default_job_completed_template,
                    default_invoice_reminder_template,
                    company_id,
                ))
            else:
                cur.execute("""
                    INSERT INTO messaging_settings (
                        company_id,
                        messaging_enabled,
                        send_job_updates,
                        send_invoice_reminders,
                        send_manual_messages,
                        default_on_the_way_template,
                        default_job_started_template,
                        default_job_completed_template,
                        default_invoice_reminder_template
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    company_id,
                    messaging_enabled,
                    send_job_updates,
                    send_invoice_reminders,
                    send_manual_messages,
                    default_on_the_way_template,
                    default_job_started_template,
                    default_job_completed_template,
                    default_invoice_reminder_template,
                ))

            conn.commit()
            flash("Messaging configuration saved.")
            return redirect(url_for("messages.messaging_configuration"))
    finally:
        conn.close()

    settings = get_messaging_settings(company_id)
    from_number = _get_from_number()

    page_html = """
    <div class="card">
        <div class="section-head">
            <div>
                <h1 style="margin-bottom:6px;">Messaging Configuration</h1>
                <div class="muted">Control messaging preferences, automation, and default templates.</div>
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

                <div style="margin-bottom:0;">
                    <strong>Inbound Webhook:</strong>
                    <span class="muted">/messages/webhook</span>
                </div>

                <div class="muted small" style="margin-top:12px;">
                    Messaging is provided by TerraLedger. Customers do not need to connect their own Twilio account.
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
                        <input type="checkbox" name="send_invoice_reminders" {% if settings and settings['send_invoice_reminders'] %}checked{% endif %}>
                        Enable Invoice Reminder Messages
                    </label>
                </div>
            </div>

            <div class="card" style="margin-top:0;">
                <h3>Default Templates</h3>

                <div style="margin-bottom:14px;">
                    <label>On The Way Template</label>
                    <textarea name="default_on_the_way_template">{{ settings['default_on_the_way_template'] if settings and settings['default_on_the_way_template'] else 'Hello from TerraLedger — we are on the way to your job site.' }}</textarea>
                </div>

                <div style="margin-bottom:14px;">
                    <label>Job Started Template</label>
                    <textarea name="default_job_started_template">{{ settings['default_job_started_template'] if settings and settings['default_job_started_template'] else 'Hello from TerraLedger — we have started your scheduled job.' }}</textarea>
                </div>

                <div style="margin-bottom:14px;">
                    <label>Job Completed Template</label>
                    <textarea name="default_job_completed_template">{{ settings['default_job_completed_template'] if settings and settings['default_job_completed_template'] else 'Hello from TerraLedger — your job has been completed. Thank you.' }}</textarea>
                </div>

                <div style="margin-bottom:0;">
                    <label>Invoice Reminder Template</label>
                    <textarea name="default_invoice_reminder_template">{{ settings['default_invoice_reminder_template'] if settings and settings['default_invoice_reminder_template'] else 'Hello from TerraLedger — this is a reminder that your invoice is still outstanding.' }}</textarea>
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
        ),
        "Messaging Configuration"
    )