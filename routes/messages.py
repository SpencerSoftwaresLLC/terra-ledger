import os
import re

from flask import Blueprint, request, redirect, url_for, flash, session, render_template_string

from db import get_db_connection
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page


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
    text = _safe_text(value)
    if not text:
        return ""

    allowed = re.sub(r"[^0-9+()\-.\s]", "", text).strip()
    return allowed


def _is_reasonable_phone(value):
    if not value:
        return False
    digits = re.sub(r"\D", "", value)
    return 10 <= len(digits) <= 15


def ensure_messaging_tables():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messaging_settings (
                id SERIAL PRIMARY KEY,
                company_id INTEGER NOT NULL UNIQUE,
                messaging_enabled INTEGER NOT NULL DEFAULT 0,
                send_job_updates INTEGER NOT NULL DEFAULT 1,
                send_invoice_reminders INTEGER NOT NULL DEFAULT 0,
                send_manual_messages INTEGER NOT NULL DEFAULT 1,
                default_on_the_way_template TEXT,
                default_job_started_template TEXT,
                default_job_completed_template TEXT,
                default_invoice_reminder_template TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

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
                provider TEXT DEFAULT 'email',
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
            WHERE table_name = 'messaging_settings'
        """)
        existing_cols = [row["column_name"] for row in cur.fetchall()]

        required_columns = {
            "messaging_enabled": "INTEGER NOT NULL DEFAULT 0",
            "send_job_updates": "INTEGER NOT NULL DEFAULT 1",
            "send_invoice_reminders": "INTEGER NOT NULL DEFAULT 0",
            "send_manual_messages": "INTEGER NOT NULL DEFAULT 1",
            "default_on_the_way_template": "TEXT",
            "default_job_started_template": "TEXT",
            "default_job_completed_template": "TEXT",
            "default_invoice_reminder_template": "TEXT",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }

        for col_name, col_def in required_columns.items():
            if col_name not in existing_cols:
                cur.execute(f"ALTER TABLE messaging_settings ADD COLUMN {col_name} {col_def}")

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
        return cur.fetchone()
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
    cur = conn.cursor()

    try:
        try:
            cur.execute("""
                SELECT id, name, phone
                FROM customers
                WHERE company_id = %s
                ORDER BY name ASC
            """, (company_id,))
            return cur.fetchall()
        except Exception:
            cur.execute("""
                SELECT id, name, phone_number AS phone
                FROM customers
                WHERE company_id = %s
                ORDER BY name ASC
            """, (company_id,))
            return cur.fetchall()
    except Exception:
        return []
    finally:
        conn.close()


def insert_message_log(
    company_id,
    phone_number,
    message_body,
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
            VALUES (%s, %s, %s, %s, %s, 'outbound', %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, (
            company_id,
            customer_id,
            job_id,
            invoice_id,
            phone_number,
            message_body,
            status,
            provider,
            provider_message_id,
            sent_by_user_id,
            error_message,
        ))
        conn.commit()
    finally:
        conn.close()


def send_text_message(to_number, message_body, settings_row=None):
    return False, None, "SMS sending is temporarily disabled."


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
    from_number = _safe_text(os.environ.get("TWILIO_FROM_NUMBER"))

    page_html = """
    <div class="card">
        <div class="section-head">
            <div>
                <h1 style="margin-bottom:6px;">Messages</h1>
                <div class="muted">Send manual customer texts and review message history.</div>
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
                {{ csrf_input() }}
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
                <div class="muted">Latest outbound messages for your company.</div>
            </div>
        </div>

        {% if history %}
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>
                            <th>Date</th>
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
                            <td>{{ row['customer_name'] or '—' }}</td>
                            <td>{{ row['phone_number'] or '—' }}</td>
                            <td>{{ row['message_body'] or '' }}</td>
                            <td>
                                {% if row['status'] == 'sent' %}
                                    <span class="pill" style="background:#dff3d2;color:#254314;">Sent</span>
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
            status="failed",
            provider="twilio",
            sent_by_user_id=user_id,
            error_message=error_message,
        )
        flash(f"Message failed: {error_message}")

    return redirect(url_for("messages.messages_page"))


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
            messaging_enabled = 1 if request.form.get("messaging_enabled") == "on" else 0
            send_job_updates = 1 if request.form.get("send_job_updates") == "on" else 0
            send_invoice_reminders = 1 if request.form.get("send_invoice_reminders") == "on" else 0
            send_manual_messages = 1 if request.form.get("send_manual_messages") == "on" else 0

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
    from_number = _safe_text(os.environ.get("TWILIO_FROM_NUMBER"))

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
            {{ csrf_input() }}
            <div class="card" style="margin-top:0; margin-bottom:18px;">
                <h3>Platform Messaging</h3>

                <div style="margin-bottom:10px;">
                    <strong>Provider:</strong>
                    <span class="muted">TerraLedger Messaging (Twilio)</span>
                </div>

                <div style="margin-bottom:0;">
                    <strong>Sending Number:</strong>
                    <span class="muted">{{ from_number if from_number else 'Platform number not configured yet' }}</span>
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