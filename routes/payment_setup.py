import os
from datetime import datetime
from html import escape

from flask import (
    Blueprint,
    request,
    redirect,
    url_for,
    session,
    flash,
    render_template_string,
    current_app,
)

from db import get_db_connection
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page

try:
    import stripe
except ImportError:
    stripe = None


payment_setup_bp = Blueprint("payment_setup", __name__)


# =========================================================
# Helpers
# =========================================================

def _stripe_secret_key():
    return (
        os.environ.get("STRIPE_SECRET_KEY")
        or os.environ.get("STRIPE_API_KEY")
        or ""
    ).strip()


def _stripe_publishable_key():
    return (
        os.environ.get("STRIPE_PUBLISHABLE_KEY")
        or os.environ.get("STRIPE_PUBLIC_KEY")
        or ""
    ).strip()


def stripe_is_ready():
    return bool(stripe is not None and _stripe_secret_key())


def get_now_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def ensure_company_payment_settings_table():
    """
    PostgreSQL version.
    Stores one payment settings row per company.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS company_payment_settings (
                id SERIAL PRIMARY KEY,
                company_id INTEGER NOT NULL UNIQUE,
                provider VARCHAR(50) NOT NULL DEFAULT 'stripe',
                stripe_account_id VARCHAR(255),
                stripe_account_email VARCHAR(255),
                charges_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                payouts_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                details_submitted BOOLEAN NOT NULL DEFAULT FALSE,
                payments_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                allow_partial_online_payments BOOLEAN NOT NULL DEFAULT FALSE,
                default_payment_message TEXT,
                onboarding_complete BOOLEAN NOT NULL DEFAULT FALSE,
                last_status_sync_at TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def get_company(company_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT id, name, email
            FROM companies
            WHERE id = %s
            """,
            (company_id,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def get_payment_settings(company_id):
    ensure_company_payment_settings_table()

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT *
            FROM company_payment_settings
            WHERE company_id = %s
            """,
            (company_id,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def upsert_payment_settings(
    company_id,
    provider="stripe",
    stripe_account_id=None,
    stripe_account_email=None,
    charges_enabled=None,
    payouts_enabled=None,
    details_submitted=None,
    payments_enabled=None,
    allow_partial_online_payments=None,
    default_payment_message=None,
    onboarding_complete=None,
    last_status_sync_at=None,
):
    ensure_company_payment_settings_table()

    existing = get_payment_settings(company_id)

    def pick(new_val, old_key, fallback=None):
        if new_val is not None:
            return new_val
        if existing:
            try:
                return existing[old_key]
            except Exception:
                pass
        return fallback

    provider_val = pick(provider, "provider", "stripe")
    stripe_account_id_val = pick(stripe_account_id, "stripe_account_id")
    stripe_account_email_val = pick(stripe_account_email, "stripe_account_email")
    charges_enabled_val = bool(pick(charges_enabled, "charges_enabled", False))
    payouts_enabled_val = bool(pick(payouts_enabled, "payouts_enabled", False))
    details_submitted_val = bool(pick(details_submitted, "details_submitted", False))
    payments_enabled_val = bool(pick(payments_enabled, "payments_enabled", False))
    allow_partial_online_payments_val = bool(
        pick(allow_partial_online_payments, "allow_partial_online_payments", False)
    )
    default_payment_message_val = pick(
        default_payment_message,
        "default_payment_message",
        "",
    )
    onboarding_complete_val = bool(
        pick(onboarding_complete, "onboarding_complete", False)
    )
    last_status_sync_at_val = pick(
        last_status_sync_at,
        "last_status_sync_at",
        None,
    )

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO company_payment_settings (
                company_id,
                provider,
                stripe_account_id,
                stripe_account_email,
                charges_enabled,
                payouts_enabled,
                details_submitted,
                payments_enabled,
                allow_partial_online_payments,
                default_payment_message,
                onboarding_complete,
                last_status_sync_at,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            ON CONFLICT (company_id)
            DO UPDATE SET
                provider = EXCLUDED.provider,
                stripe_account_id = EXCLUDED.stripe_account_id,
                stripe_account_email = EXCLUDED.stripe_account_email,
                charges_enabled = EXCLUDED.charges_enabled,
                payouts_enabled = EXCLUDED.payouts_enabled,
                details_submitted = EXCLUDED.details_submitted,
                payments_enabled = EXCLUDED.payments_enabled,
                allow_partial_online_payments = EXCLUDED.allow_partial_online_payments,
                default_payment_message = EXCLUDED.default_payment_message,
                onboarding_complete = EXCLUDED.onboarding_complete,
                last_status_sync_at = EXCLUDED.last_status_sync_at,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                company_id,
                provider_val,
                stripe_account_id_val,
                stripe_account_email_val,
                charges_enabled_val,
                payouts_enabled_val,
                details_submitted_val,
                payments_enabled_val,
                allow_partial_online_payments_val,
                default_payment_message_val,
                onboarding_complete_val,
                last_status_sync_at_val,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _normalize_external_base_url(raw_url):
    """
    Makes sure APP_BASE_URL / request.url_root becomes a valid absolute URL.
    Examples:
    - terraledger.net -> https://terraledger.net
    - http://127.0.0.1:5000 -> http://127.0.0.1:5000
    - https://www.terraledger.net/ -> https://www.terraledger.net
    """
    base_url = (raw_url or "").strip()

    if not base_url:
        raise ValueError("A base URL could not be determined.")

    if not base_url.startswith(("http://", "https://")):
        base_url = f"https://{base_url}"

    return base_url.rstrip("/")


def get_base_external_url():
    configured = (os.environ.get("APP_BASE_URL") or "").strip()
    if configured:
        return _normalize_external_base_url(configured)

    return _normalize_external_base_url(request.url_root)


def get_connect_return_url():
    return f"{get_base_external_url()}{url_for('payment_setup.payment_setup_return')}"


def get_connect_refresh_url():
    return f"{get_base_external_url()}{url_for('payment_setup.payment_setup_refresh')}"


def _row_value(row, key, default=None):
    if not row:
        return default
    try:
        value = row[key]
        return default if value is None else value
    except Exception:
        return default


def create_connected_account_for_company(company_row):
    if not stripe_is_ready():
        raise RuntimeError("Stripe is not configured.")

    stripe.api_key = _stripe_secret_key()

    company_name = (_row_value(company_row, "name", "") or "").strip()
    company_email = (_row_value(company_row, "email", "") or "").strip()

    account = stripe.Account.create(
        type="express",
        country="US",
        email=company_email or None,
        business_type="company",
        business_profile={
            "name": company_name[:250] if company_name else None,
        },
        metadata={
            "company_id": str(_row_value(company_row, "id", "")),
            "source": "terraledger",
        },
    )
    return account


def create_account_link(stripe_account_id):
    if not stripe_is_ready():
        raise RuntimeError("Stripe is not configured.")

    stripe.api_key = _stripe_secret_key()

    account_link = stripe.AccountLink.create(
        account=stripe_account_id,
        refresh_url=get_connect_refresh_url(),
        return_url=get_connect_return_url(),
        type="account_onboarding",
    )
    return account_link


def sync_stripe_status(company_id, stripe_account_id=None):
    settings_row = get_payment_settings(company_id)
    acct_id = stripe_account_id or _row_value(settings_row, "stripe_account_id")

    if not acct_id or not stripe_is_ready():
        return

    stripe.api_key = _stripe_secret_key()
    account = stripe.Account.retrieve(acct_id)

    charges_enabled = bool(getattr(account, "charges_enabled", False))
    payouts_enabled = bool(getattr(account, "payouts_enabled", False))
    details_submitted = bool(getattr(account, "details_submitted", False))
    onboarding_complete = bool(charges_enabled and payouts_enabled and details_submitted)

    acct_email = None
    try:
        acct_email = getattr(account, "email", None)
    except Exception:
        acct_email = None

    existing = get_payment_settings(company_id)
    current_payments_enabled = bool(_row_value(existing, "payments_enabled", False))

    if not onboarding_complete:
        current_payments_enabled = False

    upsert_payment_settings(
        company_id=company_id,
        stripe_account_id=acct_id,
        stripe_account_email=acct_email,
        charges_enabled=charges_enabled,
        payouts_enabled=payouts_enabled,
        details_submitted=details_submitted,
        onboarding_complete=onboarding_complete,
        payments_enabled=current_payments_enabled,
        last_status_sync_at=datetime.utcnow(),
    )


# =========================================================
# Routes
# =========================================================

@payment_setup_bp.route("/payment-setup", methods=["GET"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def payment_setup():
    ensure_company_payment_settings_table()

    company_id = session.get("company_id")
    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("dashboard.dashboard"))

    company = get_company(company_id)

    if not company:
        flash("Company not found.")
        return redirect(url_for("dashboard.dashboard"))

    settings_row = get_payment_settings(company_id)

    stripe_connected = bool(_row_value(settings_row, "stripe_account_id"))
    charges_enabled = bool(_row_value(settings_row, "charges_enabled", False))
    payouts_enabled = bool(_row_value(settings_row, "payouts_enabled", False))
    details_submitted = bool(_row_value(settings_row, "details_submitted", False))
    payments_enabled = bool(_row_value(settings_row, "payments_enabled", False))
    allow_partial = bool(_row_value(settings_row, "allow_partial_online_payments", False))
    onboarding_complete = bool(_row_value(settings_row, "onboarding_complete", False))

    if onboarding_complete:
        connection_status = "Connected and ready"
        status_badge = "ready"
    elif stripe_connected:
        connection_status = "Connected, but setup still needs attention"
        status_badge = "warning"
    else:
        connection_status = "Not connected"
        status_badge = "not-connected"

    default_message = _row_value(
        settings_row,
        "default_payment_message",
        "Click the button below to pay this invoice securely online.",
    ) or "Click the button below to pay this invoice securely online."

    stripe_account_id = _row_value(settings_row, "stripe_account_id", "")
    stripe_account_email = _row_value(settings_row, "stripe_account_email", "")
    last_sync = _row_value(settings_row, "last_status_sync_at", None)

    html = render_template_string(
        """
        <style>
            .payment-wrap {
                max-width: 1100px;
                margin: 0 auto;
                padding: 18px;
            }
            .payment-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 16px;
                flex-wrap: wrap;
                margin-bottom: 18px;
            }
            .payment-title {
                font-size: 2rem;
                font-weight: 800;
                color: #111827;
                margin: 0;
            }
            .payment-subtitle {
                color: #374151;
                margin-top: 6px;
                font-size: 0.98rem;
            }
            .payment-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
                gap: 18px;
            }
            .payment-card {
                background: #1f2937;
                border: 1px solid rgba(255,255,255,0.08);
                border-radius: 18px;
                padding: 18px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.18);
            }
            .payment-card h3 {
                margin: 0 0 12px 0;
                color: #f7f3ea;
                font-size: 1.1rem;
                font-weight: 800;
            }
            .status-pill {
                display: inline-flex;
                align-items: center;
                padding: 7px 12px;
                border-radius: 999px;
                font-size: 0.9rem;
                font-weight: 700;
            }
            .status-pill.ready {
                background: rgba(22,163,74,0.16);
                color: #86efac;
                border: 1px solid rgba(22,163,74,0.35);
            }
            .status-pill.warning {
                background: rgba(245,158,11,0.16);
                color: #fcd34d;
                border: 1px solid rgba(245,158,11,0.35);
            }
            .status-pill.not-connected {
                background: rgba(239,68,68,0.16);
                color: #fca5a5;
                border: 1px solid rgba(239,68,68,0.35);
            }
            .payment-meta {
                margin-top: 14px;
                display: grid;
                gap: 10px;
            }
            .payment-meta-row {
                display: flex;
                justify-content: space-between;
                gap: 12px;
                padding: 10px 12px;
                border-radius: 12px;
                background: rgba(255,255,255,0.03);
                color: #e5e7eb;
                font-size: 0.95rem;
            }
            .payment-meta-label {
                color: #9ca3af;
                font-weight: 700;
            }
            .btn-row {
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
                margin-top: 16px;
            }
            .btn-tl {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                border: none;
                border-radius: 12px;
                padding: 10px 16px;
                font-weight: 800;
                text-decoration: none;
                cursor: pointer;
            }
            .btn-primary-tl {
                background: linear-gradient(135deg, #16a34a, #15803d);
                color: white;
            }
            .btn-secondary-tl {
                background: #374151;
                color: #f3f4f6;
                border: 1px solid rgba(255,255,255,0.08);
            }
            .btn-warning-tl {
                background: linear-gradient(135deg, #f59e0b, #d97706);
                color: white;
            }
            .form-section {
                margin-top: 8px;
            }
            .form-label {
                display: block;
                margin-bottom: 6px;
                color: #e5e7eb;
                font-weight: 700;
                font-size: 0.95rem;
            }
            .form-help {
                color: #9ca3af;
                font-size: 0.9rem;
                margin-top: 4px;
                margin-bottom: 12px;
            }
            .form-control-tl, .form-textarea-tl {
                width: 100%;
                border-radius: 12px;
                border: 1px solid rgba(255,255,255,0.10);
                background: #111827;
                color: #f9fafb;
                padding: 12px 14px;
                outline: none;
            }
            .form-textarea-tl {
                min-height: 120px;
                resize: vertical;
            }
            .check-row {
                display: flex;
                align-items: flex-start;
                gap: 10px;
                margin-bottom: 14px;
                background: rgba(255,255,255,0.03);
                border-radius: 12px;
                padding: 12px;
            }
            .check-row input[type="checkbox"] {
                margin-top: 4px;
                transform: scale(1.15);
            }
            .check-title {
                color: #f3f4f6;
                font-weight: 700;
                margin-bottom: 3px;
            }
            .check-subtitle {
                color: #9ca3af;
                font-size: 0.9rem;
            }
            .callout {
                margin-top: 16px;
                padding: 14px;
                border-radius: 14px;
                background: rgba(59,130,246,0.12);
                border: 1px solid rgba(59,130,246,0.20);
                color: #dbeafe;
                font-size: 0.95rem;
                line-height: 1.5;
            }
            .small-muted {
                color: #9ca3af;
                font-size: 0.9rem;
            }
        </style>

        <div class="payment-wrap">
            <div class="payment-header">
                <div>
                    <h1 class="payment-title">Payment Setup</h1>
                    <div class="payment-subtitle">
                        Connect Stripe so your customers can pay invoices online and funds go to your business.
                    </div>
                </div>
                <div>
                    <a class="btn-tl btn-secondary-tl" href="{{ url_for('settings.settings_home') if false else '#' }}" onclick="history.back(); return false;">Back</a>
                </div>
            </div>

            <div class="payment-grid">

                <div class="payment-card">
                    <h3>Stripe Connection</h3>

                    <span class="status-pill {{ status_badge }}">{{ connection_status }}</span>

                    <div class="payment-meta">
                        <div class="payment-meta-row">
                            <span class="payment-meta-label">Provider</span>
                            <span>Stripe Connect</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">Stripe Account ID</span>
                            <span>{{ stripe_account_id or 'Not connected yet' }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">Stripe Email</span>
                            <span>{{ stripe_account_email or '—' }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">Charges Enabled</span>
                            <span>{{ 'Yes' if charges_enabled else 'No' }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">Payouts Enabled</span>
                            <span>{{ 'Yes' if payouts_enabled else 'No' }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">Details Submitted</span>
                            <span>{{ 'Yes' if details_submitted else 'No' }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">Last Status Sync</span>
                            <span>{{ last_sync if last_sync else 'Not synced yet' }}</span>
                        </div>
                    </div>

                    <div class="btn-row">
                        {% if not stripe_connected %}
                            <form method="POST" action="{{ url_for('payment_setup.connect_stripe_account') }}" style="display:inline;">
                                {{ csrf_input() }}
                                <button type="submit" class="btn-tl btn-primary-tl">Connect Stripe</button>
                            </form>
                        {% elif not onboarding_complete %}
                            <form method="POST" action="{{ url_for('payment_setup.connect_stripe_account') }}" style="display:inline;">
                                {{ csrf_input() }}
                                <button type="submit" class="btn-tl btn-warning-tl">Continue Setup</button>
                            </form>
                        {% else %}
                            <form method="POST" action="{{ url_for('payment_setup.connect_stripe_account') }}" style="display:inline;">
                                {{ csrf_input() }}
                                <button type="submit" class="btn-tl btn-secondary-tl">Reconnect Stripe</button>
                            </form>
                        {% endif %}

                        <form method="POST" action="{{ url_for('payment_setup.payment_setup_refresh') }}" style="display:inline;">
                            {{ csrf_input() }}
                            <button type="submit" class="btn-tl btn-secondary-tl">Refresh Status</button>
                        </form>
                    </div>

                    <div class="callout">
                        TerraLedger does not need to hold your invoice funds.
                        Your customers pay through Stripe, and payouts go to the connected business account.
                    </div>
                </div>

                <div class="payment-card">
                    <h3>Invoice Payment Settings</h3>

                    <form method="POST" action="{{ url_for('payment_setup.save_payment_preferences') }}">
                        {{ csrf_input() }}
                        <div class="form-section">
                            <label class="form-label">Default payment message for invoice emails</label>
                            <textarea
                                class="form-textarea-tl"
                                name="default_payment_message"
                                placeholder="Enter the message you want shown above the Pay Invoice button..."
                            >{{ default_message }}</textarea>
                            <div class="form-help">
                                This message can be shown in invoice emails when online payments are enabled.
                            </div>
                        </div>

                        <div class="check-row">
                            <input
                                type="checkbox"
                                id="payments_enabled"
                                name="payments_enabled"
                                value="1"
                                {% if payments_enabled %}checked{% endif %}
                            >
                            <div>
                                <div class="check-title">Enable online invoice payments</div>
                                <div class="check-subtitle">
                                    Lets TerraLedger include a Pay Invoice link in invoice emails.
                                    {% if not onboarding_complete %}
                                        Stripe must be fully connected before this can stay enabled.
                                    {% endif %}
                                </div>
                            </div>
                        </div>

                        <div class="check-row">
                            <input
                                type="checkbox"
                                id="allow_partial_online_payments"
                                name="allow_partial_online_payments"
                                value="1"
                                {% if allow_partial %}checked{% endif %}
                            >
                            <div>
                                <div class="check-title">Allow partial online payments</div>
                                <div class="check-subtitle">
                                    Leave this off for now if you want the first version to only collect the full remaining balance.
                                </div>
                            </div>
                        </div>

                        <div class="btn-row">
                            <button type="submit" class="btn-tl btn-primary-tl">Save Payment Settings</button>
                        </div>
                    </form>
                </div>

                <div class="payment-card">
                    <h3>Setup Notes</h3>

                    <div class="payment-meta">
                        <div class="payment-meta-row">
                            <span class="payment-meta-label">Company</span>
                            <span>{{ company_name }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">Platform Key Loaded</span>
                            <span>{{ 'Yes' if stripe_ready else 'No' }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">Publishable Key Loaded</span>
                            <span>{{ 'Yes' if publishable_ready else 'No' }}</span>
                        </div>
                    </div>

                    <div class="callout">
                        Next step after this page is connected:
                        TerraLedger will create a Stripe Checkout session for each invoice email,
                        then a Stripe webhook will mark the invoice paid automatically after a successful payment.
                    </div>

                    <p class="small-muted" style="margin-top:16px;">
                        Recommended first version: full remaining balance only.
                    </p>
                </div>

            </div>
        </div>
        """,
        company_name=_row_value(company, "name", "Your Company"),
        status_badge=status_badge,
        connection_status=connection_status,
        stripe_account_id=stripe_account_id,
        stripe_account_email=stripe_account_email,
        charges_enabled=charges_enabled,
        payouts_enabled=payouts_enabled,
        details_submitted=details_submitted,
        payments_enabled=payments_enabled,
        allow_partial=allow_partial,
        onboarding_complete=onboarding_complete,
        default_message=default_message,
        last_sync=last_sync,
        stripe_ready=stripe_is_ready(),
        publishable_ready=bool(_stripe_publishable_key()),
    )

    return render_page(html)


@payment_setup_bp.route("/payment-setup/save", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def save_payment_preferences():
    ensure_company_payment_settings_table()

    company_id = session.get("company_id")
    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("dashboard.dashboard"))

    settings_row = get_payment_settings(company_id)

    onboarding_complete = bool(_row_value(settings_row, "onboarding_complete", False))

    requested_payments_enabled = request.form.get("payments_enabled") == "1"
    allow_partial = request.form.get("allow_partial_online_payments") == "1"
    default_message = (request.form.get("default_payment_message") or "").strip()

    if requested_payments_enabled and not onboarding_complete:
        flash("Finish Stripe setup before enabling online invoice payments.")
        requested_payments_enabled = False

    upsert_payment_settings(
        company_id=company_id,
        payments_enabled=requested_payments_enabled,
        allow_partial_online_payments=allow_partial,
        default_payment_message=default_message,
    )

    flash("Payment settings saved.")
    return redirect(url_for("payment_setup.payment_setup"))


@payment_setup_bp.route("/payment-setup/connect", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def connect_stripe_account():
    ensure_company_payment_settings_table()

    company_id = session.get("company_id")
    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("dashboard.dashboard"))

    company = get_company(company_id)

    if not company:
        flash("Company not found.")
        return redirect(url_for("dashboard.dashboard"))

    if stripe is None:
        flash("Stripe is not installed. Run: pip install stripe")
        return redirect(url_for("payment_setup.payment_setup"))

    if not _stripe_secret_key():
        flash("Missing STRIPE_SECRET_KEY in environment variables.")
        return redirect(url_for("payment_setup.payment_setup"))

    settings_row = get_payment_settings(company_id)
    stripe_account_id = _row_value(settings_row, "stripe_account_id")

    try:
        if not stripe_account_id:
            account = create_connected_account_for_company(company)
            stripe_account_id = account.id

            upsert_payment_settings(
                company_id=company_id,
                stripe_account_id=stripe_account_id,
                stripe_account_email=_row_value(company, "email", ""),
                charges_enabled=False,
                payouts_enabled=False,
                details_submitted=False,
                onboarding_complete=False,
                payments_enabled=False,
                last_status_sync_at=datetime.utcnow(),
            )

        account_link = create_account_link(stripe_account_id)
        return redirect(account_link.url)

    except Exception as e:
        current_app.logger.exception("Stripe connect setup failed.")
        flash(f"Stripe connection failed: {escape(str(e))}")
        return redirect(url_for("payment_setup.payment_setup"))


@payment_setup_bp.route("/payment-setup/return", methods=["GET"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def payment_setup_return():
    company_id = session.get("company_id")
    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("dashboard.dashboard"))

    try:
        settings_row = get_payment_settings(company_id)
        stripe_account_id = _row_value(settings_row, "stripe_account_id")

        if stripe_account_id:
            sync_stripe_status(company_id, stripe_account_id)
            refreshed = get_payment_settings(company_id)
            onboarding_complete = bool(_row_value(refreshed, "onboarding_complete", False))

            if onboarding_complete:
                flash("Stripe setup complete. Online payments can now be enabled.")
            else:
                flash("Stripe setup was updated, but more information may still be required.")
        else:
            flash("Stripe account was not found for this company.")
    except Exception as e:
        current_app.logger.exception("Stripe return sync failed.")
        flash(f"Could not refresh Stripe status: {escape(str(e))}")

    return redirect(url_for("payment_setup.payment_setup"))


@payment_setup_bp.route("/payment-setup/refresh", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def payment_setup_refresh():
    company_id = session.get("company_id")
    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("dashboard.dashboard"))

    try:
        settings_row = get_payment_settings(company_id)
        stripe_account_id = _row_value(settings_row, "stripe_account_id")

        if not stripe_account_id:
            flash("No Stripe account is connected yet.")
            return redirect(url_for("payment_setup.payment_setup"))

        sync_stripe_status(company_id, stripe_account_id)
        flash("Stripe status refreshed.")
    except Exception as e:
        current_app.logger.exception("Stripe status refresh failed.")
        flash(f"Could not refresh Stripe status: {escape(str(e))}")

    return redirect(url_for("payment_setup.payment_setup"))