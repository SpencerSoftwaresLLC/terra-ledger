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
# Language Helpers
# =========================================================

def _lang():
    return "es" if session.get("language") == "es" else "en"


def _is_es():
    return _lang() == "es"


def _t(en, es):
    return es if _is_es() else en


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
        raise ValueError(_t(
            "A base URL could not be determined.",
            "No se pudo determinar una URL base.",
        ))

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
        raise RuntimeError(_t("Stripe is not configured.", "Stripe no está configurado."))

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
        raise RuntimeError(_t("Stripe is not configured.", "Stripe no está configurado."))

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
        flash(_t("Company session not found.", "No se encontró la sesión de la empresa."))
        return redirect(url_for("dashboard.dashboard"))

    company = get_company(company_id)

    if not company:
        flash(_t("Company not found.", "Empresa no encontrada."))
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
        connection_status = _t("Connected and ready", "Conectado y listo")
        status_badge = "ready"
    elif stripe_connected:
        connection_status = _t(
            "Connected, but setup still needs attention",
            "Conectado, pero la configuración todavía necesita atención",
        )
        status_badge = "warning"
    else:
        connection_status = _t("Not connected", "No conectado")
        status_badge = "not-connected"

    default_message = _row_value(
        settings_row,
        "default_payment_message",
        _t(
            "Click the button below to pay this invoice securely online.",
            "Haz clic en el botón de abajo para pagar esta factura de forma segura en línea.",
        ),
    ) or _t(
        "Click the button below to pay this invoice securely online.",
        "Haz clic en el botón de abajo para pagar esta factura de forma segura en línea.",
    )

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
                    <h1 class="payment-title">{{ page_title }}</h1>
                    <div class="payment-subtitle">
                        {{ page_subtitle }}
                    </div>
                </div>
                <div>
                    <a class="btn-tl btn-secondary-tl" href="{{ url_for('settings.settings') }}">{{ back_to_settings_label }}</a>
                </div>
            </div>

            <div class="payment-grid">

                <div class="payment-card">
                    <h3>{{ stripe_connection_label }}</h3>

                    <span class="status-pill {{ status_badge }}">{{ connection_status }}</span>

                    <div class="payment-meta">
                        <div class="payment-meta-row">
                            <span class="payment-meta-label">{{ provider_label }}</span>
                            <span>Stripe Connect</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">{{ stripe_account_id_label }}</span>
                            <span>{{ stripe_account_id or not_connected_yet_label }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">{{ stripe_email_label }}</span>
                            <span>{{ stripe_account_email or dash_label }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">{{ charges_enabled_label }}</span>
                            <span>{{ yes_label if charges_enabled else no_label }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">{{ payouts_enabled_label }}</span>
                            <span>{{ yes_label if payouts_enabled else no_label }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">{{ details_submitted_label }}</span>
                            <span>{{ yes_label if details_submitted else no_label }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">{{ last_status_sync_label }}</span>
                            <span>{{ last_sync if last_sync else not_synced_yet_label }}</span>
                        </div>
                    </div>

                    <div class="btn-row">
                        {% if not stripe_connected %}
                            <form method="POST" action="{{ url_for('payment_setup.connect_stripe_account') }}" style="display:inline;">
                                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                                <button type="submit" class="btn-tl btn-primary-tl">{{ connect_stripe_label }}</button>
                            </form>
                        {% elif not onboarding_complete %}
                            <form method="POST" action="{{ url_for('payment_setup.connect_stripe_account') }}" style="display:inline;">
                                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                                <button type="submit" class="btn-tl btn-warning-tl">{{ continue_setup_label }}</button>
                            </form>
                        {% else %}
                            <form method="POST" action="{{ url_for('payment_setup.connect_stripe_account') }}" style="display:inline;">
                                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                                <button type="submit" class="btn-tl btn-secondary-tl">{{ reconnect_stripe_label }}</button>
                            </form>
                        {% endif %}

                        <form method="POST" action="{{ url_for('payment_setup.payment_setup_refresh') }}" style="display:inline;">
                            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                            <button type="submit" class="btn-tl btn-secondary-tl">{{ refresh_status_label }}</button>
                        </form>
                    </div>

                    <div class="callout">
                        {{ stripe_callout }}
                    </div>
                </div>

                <div class="payment-card">
                    <h3>{{ invoice_payment_settings_label }}</h3>

                    <form method="POST" action="{{ url_for('payment_setup.save_payment_preferences') }}">
                        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                        <div class="form-section">
                            <label class="form-label">{{ default_payment_message_label }}</label>
                            <textarea
                                class="form-textarea-tl"
                                name="default_payment_message"
                                placeholder="{{ default_payment_placeholder }}"
                            >{{ default_message }}</textarea>
                            <div class="form-help">
                                {{ default_payment_help }}
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
                                <div class="check-title">{{ enable_online_payments_title }}</div>
                                <div class="check-subtitle">
                                    {{ enable_online_payments_subtitle }}
                                    {% if not onboarding_complete %}
                                        {{ onboarding_required_note }}
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
                                <div class="check-title">{{ allow_partial_title }}</div>
                                <div class="check-subtitle">
                                    {{ allow_partial_subtitle }}
                                </div>
                            </div>
                        </div>

                        <div class="btn-row">
                            <button type="submit" class="btn-tl btn-primary-tl">{{ save_payment_settings_label }}</button>
                        </div>
                    </form>
                </div>

                <div class="payment-card">
                    <h3>{{ setup_notes_label }}</h3>

                    <div class="payment-meta">
                        <div class="payment-meta-row">
                            <span class="payment-meta-label">{{ company_label }}</span>
                            <span>{{ company_name }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">{{ platform_key_loaded_label }}</span>
                            <span>{{ yes_label if stripe_ready else no_label }}</span>
                        </div>

                        <div class="payment-meta-row">
                            <span class="payment-meta-label">{{ publishable_key_loaded_label }}</span>
                            <span>{{ yes_label if publishable_ready else no_label }}</span>
                        </div>
                    </div>

                    <div class="callout">
                        {{ next_step_callout }}
                    </div>

                    <p class="small-muted" style="margin-top:16px;">
                        {{ recommended_first_version }}
                    </p>
                </div>

            </div>
        </div>
        """,
        company_name=_row_value(company, "name", _t("Your Company", "Su empresa")),
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
        page_title=_t("Payment Setup", "Configuración de pagos"),
        page_subtitle=_t(
            "Connect Stripe so your customers can pay invoices online and funds go to your business.",
            "Conecta Stripe para que tus clientes puedan pagar facturas en línea y los fondos lleguen a tu negocio.",
        ),
        back_to_settings_label=_t("Back to Settings", "Volver a configuración"),
        stripe_connection_label=_t("Stripe Connection", "Conexión de Stripe"),
        provider_label=_t("Provider", "Proveedor"),
        stripe_account_id_label=_t("Stripe Account ID", "ID de cuenta de Stripe"),
        stripe_email_label=_t("Stripe Email", "Correo de Stripe"),
        charges_enabled_label=_t("Charges Enabled", "Cobros habilitados"),
        payouts_enabled_label=_t("Payouts Enabled", "Depósitos habilitados"),
        details_submitted_label=_t("Details Submitted", "Detalles enviados"),
        last_status_sync_label=_t("Last Status Sync", "Última sincronización de estado"),
        not_connected_yet_label=_t("Not connected yet", "Todavía no conectado"),
        dash_label="—",
        yes_label=_t("Yes", "Sí"),
        no_label=_t("No", "No"),
        not_synced_yet_label=_t("Not synced yet", "Aún no sincronizado"),
        connect_stripe_label=_t("Connect Stripe", "Conectar Stripe"),
        continue_setup_label=_t("Continue Setup", "Continuar configuración"),
        reconnect_stripe_label=_t("Reconnect Stripe", "Reconectar Stripe"),
        refresh_status_label=_t("Refresh Status", "Actualizar estado"),
        stripe_callout=_t(
            "TerraLedger does not need to hold your invoice funds. Your customers pay through Stripe, and payouts go to the connected business account.",
            "TerraLedger no necesita retener los fondos de tus facturas. Tus clientes pagan a través de Stripe y los depósitos van a la cuenta comercial conectada.",
        ),
        invoice_payment_settings_label=_t("Invoice Payment Settings", "Configuración de pago de facturas"),
        default_payment_message_label=_t(
            "Default payment message for invoice emails",
            "Mensaje de pago predeterminado para correos de facturas",
        ),
        default_payment_placeholder=_t(
            "Enter the message you want shown above the Pay Invoice button...",
            "Escribe el mensaje que quieres mostrar encima del botón Pagar factura...",
        ),
        default_payment_help=_t(
            "This message can be shown in invoice emails when online payments are enabled.",
            "Este mensaje puede mostrarse en los correos de facturas cuando los pagos en línea estén habilitados.",
        ),
        enable_online_payments_title=_t(
            "Enable online invoice payments",
            "Habilitar pagos de facturas en línea",
        ),
        enable_online_payments_subtitle=_t(
            "Lets TerraLedger include a Pay Invoice link in invoice emails.",
            "Permite que TerraLedger incluya un enlace para pagar la factura en los correos de facturas.",
        ),
        onboarding_required_note=_t(
            "Stripe must be fully connected before this can stay enabled.",
            "Stripe debe estar completamente conectado antes de que esto pueda permanecer habilitado.",
        ),
        allow_partial_title=_t(
            "Allow partial online payments",
            "Permitir pagos parciales en línea",
        ),
        allow_partial_subtitle=_t(
            "Leave this off for now if you want the first version to only collect the full remaining balance.",
            "Déjalo desactivado por ahora si quieres que la primera versión solo cobre el saldo restante completo.",
        ),
        save_payment_settings_label=_t(
            "Save Payment Settings",
            "Guardar configuración de pagos",
        ),
        setup_notes_label=_t("Setup Notes", "Notas de configuración"),
        company_label=_t("Company", "Empresa"),
        platform_key_loaded_label=_t(
            "Platform Key Loaded",
            "Clave de plataforma cargada",
        ),
        publishable_key_loaded_label=_t(
            "Publishable Key Loaded",
            "Clave pública cargada",
        ),
        next_step_callout=_t(
            "Next step after this page is connected: TerraLedger will create a Stripe Checkout session for each invoice email, then a Stripe webhook will mark the invoice paid automatically after a successful payment.",
            "Siguiente paso después de conectar esta página: TerraLedger creará una sesión de Stripe Checkout para cada correo de factura, y luego un webhook de Stripe marcará la factura como pagada automáticamente después de un pago exitoso.",
        ),
        recommended_first_version=_t(
            "Recommended first version: full remaining balance only.",
            "Primera versión recomendada: solo el saldo restante completo.",
        ),
    )

    return render_page(html, _t("Payment Setup", "Configuración de pagos"))


@payment_setup_bp.route("/payment-setup/save", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def save_payment_preferences():
    ensure_company_payment_settings_table()

    company_id = session.get("company_id")
    if not company_id:
        flash(_t("Company session not found.", "No se encontró la sesión de la empresa."))
        return redirect(url_for("dashboard.dashboard"))

    settings_row = get_payment_settings(company_id)

    onboarding_complete = bool(_row_value(settings_row, "onboarding_complete", False))

    requested_payments_enabled = request.form.get("payments_enabled") == "1"
    allow_partial = request.form.get("allow_partial_online_payments") == "1"
    default_message = (request.form.get("default_payment_message") or "").strip()

    if requested_payments_enabled and not onboarding_complete:
        flash(_t(
            "Finish Stripe setup before enabling online invoice payments.",
            "Termina la configuración de Stripe antes de habilitar pagos de facturas en línea.",
        ))
        requested_payments_enabled = False

    upsert_payment_settings(
        company_id=company_id,
        payments_enabled=requested_payments_enabled,
        allow_partial_online_payments=allow_partial,
        default_payment_message=default_message,
    )

    flash(_t("Payment settings saved.", "Configuración de pagos guardada."))
    return redirect(url_for("payment_setup.payment_setup"))


@payment_setup_bp.route("/payment-setup/connect", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def connect_stripe_account():
    ensure_company_payment_settings_table()

    company_id = session.get("company_id")
    if not company_id:
        flash(_t("Company session not found.", "No se encontró la sesión de la empresa."))
        return redirect(url_for("dashboard.dashboard"))

    company = get_company(company_id)

    if not company:
        flash(_t("Company not found.", "Empresa no encontrada."))
        return redirect(url_for("dashboard.dashboard"))

    if stripe is None:
        flash(_t(
            "Stripe is not installed. Run: pip install stripe",
            "Stripe no está instalado. Ejecuta: pip install stripe",
        ))
        return redirect(url_for("payment_setup.payment_setup"))

    if not _stripe_secret_key():
        flash(_t(
            "Missing STRIPE_SECRET_KEY in environment variables.",
            "Falta STRIPE_SECRET_KEY en las variables de entorno.",
        ))
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
        flash(_t(
            f"Stripe connection failed: {escape(str(e))}",
            f"La conexión con Stripe falló: {escape(str(e))}",
        ))
        return redirect(url_for("payment_setup.payment_setup"))


@payment_setup_bp.route("/payment-setup/return", methods=["GET"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def payment_setup_return():
    company_id = session.get("company_id")
    if not company_id:
        flash(_t("Company session not found.", "No se encontró la sesión de la empresa."))
        return redirect(url_for("dashboard.dashboard"))

    try:
        settings_row = get_payment_settings(company_id)
        stripe_account_id = _row_value(settings_row, "stripe_account_id")

        if stripe_account_id:
            sync_stripe_status(company_id, stripe_account_id)
            refreshed = get_payment_settings(company_id)
            onboarding_complete = bool(_row_value(refreshed, "onboarding_complete", False))

            if onboarding_complete:
                flash(_t(
                    "Stripe setup complete. Online payments can now be enabled.",
                    "La configuración de Stripe está completa. Ahora se pueden habilitar los pagos en línea.",
                ))
            else:
                flash(_t(
                    "Stripe setup was updated, but more information may still be required.",
                    "La configuración de Stripe se actualizó, pero todavía puede requerirse más información.",
                ))
        else:
            flash(_t(
                "Stripe account was not found for this company.",
                "No se encontró una cuenta de Stripe para esta empresa.",
            ))
    except Exception as e:
        current_app.logger.exception("Stripe return sync failed.")
        flash(_t(
            f"Could not refresh Stripe status: {escape(str(e))}",
            f"No se pudo actualizar el estado de Stripe: {escape(str(e))}",
        ))

    return redirect(url_for("payment_setup.payment_setup"))


@payment_setup_bp.route("/payment-setup/refresh", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def payment_setup_refresh():
    company_id = session.get("company_id")
    if not company_id:
        flash(_t("Company session not found.", "No se encontró la sesión de la empresa."))
        return redirect(url_for("dashboard.dashboard"))

    try:
        settings_row = get_payment_settings(company_id)
        stripe_account_id = _row_value(settings_row, "stripe_account_id")

        if not stripe_account_id:
            flash(_t(
                "No Stripe account is connected yet.",
                "Todavía no hay una cuenta de Stripe conectada.",
            ))
            return redirect(url_for("payment_setup.payment_setup"))

        sync_stripe_status(company_id, stripe_account_id)
        flash(_t("Stripe status refreshed.", "Estado de Stripe actualizado."))
    except Exception as e:
        current_app.logger.exception("Stripe status refresh failed.")
        flash(_t(
            f"Could not refresh Stripe status: {escape(str(e))}",
            f"No se pudo actualizar el estado de Stripe: {escape(str(e))}",
        ))

    return redirect(url_for("payment_setup.payment_setup"))