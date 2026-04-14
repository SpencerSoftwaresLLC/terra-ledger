import os
from datetime import datetime
from urllib.parse import urlencode

from flask import Blueprint, request, redirect, url_for, session, flash, render_template_string

from db import (
    get_db_connection,
    get_company_subscription,
    upsert_company_subscription,
    insert_billing_event,
    get_billing_history,
)
from decorators import login_required
from page_helpers import render_page

print("LOADED BILLING FILE:", __file__)

billing_bp = Blueprint("billing", __name__)

try:
    import stripe
    STRIPE_IMPORT_OK = True
except Exception:
    stripe = None
    STRIPE_IMPORT_OK = False


def _lang():
    value = str(session.get("language") or "en").strip().lower()
    return "es" if value == "es" else "en"


def _is_es():
    return _lang() == "es"


def _t(en, es):
    return es if _is_es() else en


def _env_flag(name, default=False):
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def get_stripe_config():
    stripe_secret_key = os.environ.get("STRIPE_SECRET_KEY", "").strip()
    stripe_publishable_key = os.environ.get("STRIPE_PUBLISHABLE_KEY", "").strip()
    stripe_billing_webhook_secret = os.environ.get("STRIPE_BILLING_WEBHOOK_SECRET", "").strip()
    app_base_url = os.environ.get("APP_BASE_URL", "http://127.0.0.1:5000").strip().rstrip("/")

    stripe_price_monthly = os.environ.get("STRIPE_PRICE_MONTHLY", "").strip()
    stripe_price_yearly = os.environ.get("STRIPE_PRICE_YEARLY", "").strip()

    stripe_owner_email = os.environ.get("STRIPE_OWNER_EMAIL", "").strip().lower()
    stripe_owner_coupon_id = os.environ.get("STRIPE_OWNER_COUPON_ID", "").strip()
    stripe_owner_promo_code_id = os.environ.get("STRIPE_OWNER_PROMO_CODE_ID", "").strip()

    spencer_site_base_url = os.environ.get(
        "SPENCER_SITE_BASE_URL",
        "https://spencersoftwaresllc.com"
    ).strip().rstrip("/")

    spencer_terraledger_pricing_url = os.environ.get(
        "SPENCER_TERRALEDGER_PRICING_URL",
        f"{spencer_site_base_url}/terraledger#pricing"
    ).strip()

    redirect_new_companies_to_parent_site = _env_flag(
        "REDIRECT_NEW_COMPANIES_TO_PARENT_SITE",
        default=True,
    )

    stripe_enabled = bool(STRIPE_IMPORT_OK and stripe_secret_key)

    if stripe_enabled:
        stripe.api_key = stripe_secret_key

    return {
        "secret_key": stripe_secret_key,
        "publishable_key": stripe_publishable_key,
        "webhook_secret": stripe_billing_webhook_secret,
        "app_base_url": app_base_url,
        "price_monthly": stripe_price_monthly,
        "price_yearly": stripe_price_yearly,
        "owner_email": stripe_owner_email,
        "owner_coupon_id": stripe_owner_coupon_id,
        "owner_promo_code_id": stripe_owner_promo_code_id,
        "enabled": stripe_enabled,
        "spencer_site_base_url": spencer_site_base_url,
        "spencer_terraledger_pricing_url": spencer_terraledger_pricing_url,
        "redirect_new_companies_to_parent_site": redirect_new_companies_to_parent_site,
    }


def _get_company(company_id):
    conn = get_db_connection()
    row = conn.execute(
        "SELECT * FROM companies WHERE id = %s",
        (company_id,),
    ).fetchone()
    conn.close()
    return row


def _get_user_email():
    return (session.get("user_email") or "").strip().lower()


def _normalize_status(status):
    value = (status or "").strip().lower()

    if value in ("active", "trialing"):
        return _t("Active", "Activa")
    if value == "past_due":
        return _t("Past Due", "Vencida")
    if value in ("canceled", "cancelled"):
        return _t("Canceled", "Cancelada")
    if value == "unpaid":
        return _t("Unpaid", "No pagada")
    if value == "incomplete":
        return _t("Incomplete", "Incompleta")
    if value == "incomplete_expired":
        return _t("Incomplete Expired", "Incompleta vencida")
    if value == "paused":
        return _t("Paused", "Pausada")
    if value == "expired":
        return _t("Expired", "Expirada")
    if value == "trial":
        return _t("Trial", "Prueba")
    if not value:
        return _t("Inactive", "Inactiva")

    return value.replace("_", " ").title()


def _display_interval(interval):
    value = (interval or "").strip().lower()
    if value == "month":
        return _t("Month", "Mes")
    if value == "year":
        return _t("Year", "Año")
    if not value:
        return "-"
    return value.title()


def _display_access_text(sub):
    return _t("Unlocked", "Desbloqueado") if _has_active_access(sub) else _t("Locked", "Bloqueado")


def _display_auto_renew(value):
    return _t("Enabled", "Activada") if value else _t("Disabled", "Desactivada")


def _display_billing_event_type(event_type):
    value = (event_type or "").strip().lower()

    mapping = {
        "invoice.paid": _t("Invoice Paid", "Factura pagada"),
        "invoice.payment_succeeded": _t("Payment Succeeded", "Pago exitoso"),
        "invoice.payment_failed": _t("Payment Failed", "Pago fallido"),
        "customer.subscription.created": _t("Subscription Created", "Suscripción creada"),
        "customer.subscription.updated": _t("Subscription Updated", "Suscripción actualizada"),
        "customer.subscription.deleted": _t("Subscription Deleted", "Suscripción eliminada"),
        "checkout.session.completed": _t("Checkout Completed", "Compra completada"),
    }
    return mapping.get(value, event_type or "-")


def _has_active_access(sub):
    if not sub:
        return False
    status = (sub["status"] or "").strip().lower()
    return status in ("active", "trialing", "trial")


def _get_subscription_css_class(status):
    raw = (status or "").strip().lower()
    if raw in ("active", "trialing", "trial"):
        return "status-good"
    if raw in ("past_due", "unpaid", "incomplete", "incomplete_expired"):
        return "status-warn"
    return "status-bad"


def _find_best_subscription_for_customer(customer_id):
    subscriptions = stripe.Subscription.list(customer=customer_id, status="all", limit=20)

    items = subscriptions.get("data", []) if subscriptions else []
    if not items:
        return None

    priority = {
        "active": 1,
        "trialing": 2,
        "past_due": 3,
        "unpaid": 4,
        "incomplete": 5,
        "canceled": 6,
        "incomplete_expired": 7,
        "paused": 8,
    }

    def sort_key(sub):
        status = (sub.get("status") or "").lower()
        created = sub.get("created") or 0
        return (priority.get(status, 99), -created)

    items = sorted(items, key=sort_key)
    return items[0]


def _build_parent_pricing_url(plan):
    cfg = get_stripe_config()
    base_url = cfg["spencer_terraledger_pricing_url"] or f"{cfg['spencer_site_base_url']}/terraledger#pricing"

    company_id = session.get("company_id")
    user_email = _get_user_email()

    params = {
        "plan": plan,
        "source": "terraledger_app",
    }

    if company_id:
        params["company_id"] = str(company_id)
    if user_email:
        params["email"] = user_email

    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode(params)}"


def _should_redirect_to_parent_site(sub):
    cfg = get_stripe_config()

    if not cfg["redirect_new_companies_to_parent_site"]:
        return False

    if not sub:
        return True

    status = (sub["status"] or "").strip().lower()
    stripe_customer_id = (sub["stripe_customer_id"] or "").strip() if "stripe_customer_id" in sub.keys() else ""
    stripe_subscription_id = (sub["stripe_subscription_id"] or "").strip() if "stripe_subscription_id" in sub.keys() else ""

    if status in ("active", "trialing", "trial", "past_due", "unpaid", "incomplete"):
        return False

    if stripe_customer_id or stripe_subscription_id:
        return False

    return True


def _sync_subscription_from_stripe(company_id, stripe_subscription_id):
    sub = stripe.Subscription.retrieve(
        stripe_subscription_id,
        expand=[
            "default_payment_method",
            "items.data.price",
        ],
    )

    price = None
    interval = None
    amount_cents = None

    if sub.get("items") and sub["items"].get("data"):
        item = sub["items"]["data"][0]
        price = item.get("price")
        if price:
            amount_cents = price.get("unit_amount")
            recurring = price.get("recurring") or {}
            interval = recurring.get("interval")

    plan_name = "Subscription"
    if interval == "month":
        plan_name = "Pro Monthly"
    elif interval == "year":
        plan_name = "Pro Yearly"

    pm = sub.get("default_payment_method")
    pm_type = None
    pm_last4 = None
    pm_label = None

    if pm:
        pm_type = pm.get("type")

        if pm_type == "card" and pm.get("card"):
            pm_last4 = pm["card"].get("last4")
            pm_label = _t(f"Card ending in {pm_last4}", f"Tarjeta terminada en {pm_last4}")

        elif pm_type == "us_bank_account" and pm.get("us_bank_account"):
            pm_last4 = pm["us_bank_account"].get("last4")
            bank_name = pm["us_bank_account"].get("bank_name") or _t("Bank account", "Cuenta bancaria")
            pm_label = _t(
                f"{bank_name} ending in {pm_last4}",
                f"{bank_name} terminada en {pm_last4}"
            )

    current_period_start = None
    current_period_end = None

    if sub.get("current_period_start"):
        current_period_start = datetime.fromtimestamp(
            sub["current_period_start"]
        ).strftime("%Y-%m-%d")

    if sub.get("current_period_end"):
        current_period_end = datetime.fromtimestamp(
            sub["current_period_end"]
        ).strftime("%Y-%m-%d")

    upsert_company_subscription(
        company_id=company_id,
        stripe_customer_id=sub.get("customer"),
        stripe_subscription_id=sub.get("id"),
        stripe_price_id=price.get("id") if price else None,
        plan_name=plan_name,
        billing_interval=interval,
        amount_cents=amount_cents,
        status=sub.get("status"),
        auto_renew=0 if sub.get("cancel_at_period_end") else 1,
        cancel_at_period_end=1 if sub.get("cancel_at_period_end") else 0,
        current_period_start=current_period_start,
        current_period_end=current_period_end,
        payment_method_type=pm_type,
        payment_method_last4=pm_last4,
        payment_method_label=pm_label,
    )


def _refresh_company_subscription_from_stripe(company_id):
    cfg = get_stripe_config()
    if not cfg["enabled"]:
        return False, _t("Stripe is not configured.", "Stripe no está configurado.")

    sub = get_company_subscription(company_id)

    if not sub:
        return False, _t("No subscription record found yet.", "Todavía no se encontró un registro de suscripción.")

    stripe_subscription_id = sub["stripe_subscription_id"] if "stripe_subscription_id" in sub.keys() else None
    stripe_customer_id = sub["stripe_customer_id"] if "stripe_customer_id" in sub.keys() else None

    try:
        if stripe_subscription_id:
            _sync_subscription_from_stripe(company_id, stripe_subscription_id)
            return True, _t("Subscription status refreshed.", "El estado de la suscripción fue actualizado.")

        if stripe_customer_id:
            best = _find_best_subscription_for_customer(stripe_customer_id)
            if best and best.get("id"):
                _sync_subscription_from_stripe(company_id, best["id"])
                return True, _t("Subscription status refreshed.", "El estado de la suscripción fue actualizado.")

        return False, _t(
            "No Stripe subscription was found for this account.",
            "No se encontró una suscripción de Stripe para esta cuenta."
        )
    except Exception as e:
        return False, _t(
            f"Could not refresh billing status: {e}",
            f"No se pudo actualizar el estado de facturación: {e}"
        )


@billing_bp.route("/subscription-required")
@login_required
def subscription_required_page():
    cid = session["company_id"]
    sub = get_company_subscription(cid)

    status_text = _normalize_status(sub["status"]) if sub else _t("Inactive", "Inactiva")
    plan_name = sub["plan_name"] if sub and sub["plan_name"] else _t("No active plan", "Sin plan activo")
    renewal = sub["current_period_end"] if sub and sub["current_period_end"] else "-"

    monthly_redirect_url = _build_parent_pricing_url("monthly")
    yearly_redirect_url = _build_parent_pricing_url("yearly")

    content = render_template_string(
        """
        <div class="card" style="max-width:900px;margin:0 auto;">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
                <div>
                    <h1 style="margin-bottom:6px;">{{ t_subscription_required }}</h1>
                    <p class="muted" style="margin:0;">
                        {{ t_locked_message }}
                    </p>
                </div>
                <div class="row-actions">
                    <a class="btn secondary" href="{{ settings_url }}">{{ t_back_to_settings }}</a>
                </div>
            </div>

            <div style="margin-top:20px;">
                <div class="card" style="margin:0;">
                    <h2>{{ t_current_status }}</h2>
                    <p><strong>{{ t_plan }}:</strong> {{ plan_name }}</p>
                    <p><strong>{{ t_status }}:</strong> {{ status_text }}</p>
                    <p><strong>{{ t_renewal_date }}:</strong> {{ renewal }}</p>
                </div>

                <div class="card" style="margin-top:20px;">
                    <h2>{{ t_what_to_do }}</h2>
                    <p>
                        {{ t_what_to_do_text }}
                    </p>

                    <div style="display:flex;gap:10px;flex-wrap:wrap;">
                        <a class="btn" href="{{ monthly_redirect_url }}">{{ t_start_monthly }}</a>
                        <a class="btn secondary" href="{{ yearly_redirect_url }}">{{ t_start_yearly }}</a>

                        <form method="post" action="{{ refresh_url }}" style="display:inline;">
                            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                            <button class="btn secondary" type="submit">{{ t_refresh_access }}</button>
                        </form>
                        <a class="btn secondary" href="{{ billing_url }}">{{ t_view_billing_page }}</a>
                    </div>
                </div>
            </div>
        </div>
        """,
        plan_name=plan_name,
        status_text=status_text,
        renewal=renewal,
        refresh_url=url_for("billing.refresh_billing_status"),
        billing_url=url_for("billing.billing_page"),
        settings_url=url_for("settings.settings"),
        monthly_redirect_url=monthly_redirect_url,
        yearly_redirect_url=yearly_redirect_url,
        t_subscription_required=_t("Subscription Required", "Suscripción requerida"),
        t_locked_message=_t(
            "TerraLedger access is currently locked for this account.",
            "El acceso a TerraLedger está bloqueado actualmente para esta cuenta."
        ),
        t_back_to_settings=_t("Back to Settings", "Volver a Configuración"),
        t_current_status=_t("Current Status", "Estado actual"),
        t_plan=_t("Plan", "Plan"),
        t_status=_t("Status", "Estado"),
        t_renewal_date=_t("Renewal Date", "Fecha de renovación"),
        t_what_to_do=_t("What To Do", "Qué hacer"),
        t_what_to_do_text=_t(
            "Your account needs an active subscription to continue using TerraLedger.",
            "Tu cuenta necesita una suscripción activa para seguir usando TerraLedger."
        ),
        t_start_monthly=_t("Start Monthly", "Iniciar mensual"),
        t_start_yearly=_t("Start Yearly", "Iniciar anual"),
        t_refresh_access=_t("Refresh Access", "Actualizar acceso"),
        t_view_billing_page=_t("View Billing Page", "Ver página de facturación"),
    )

    return render_page(content, title=_t("Subscription Required", "Suscripción requerida"))


@billing_bp.route("/settings/billing")
@login_required
def billing_page():
    cid = session["company_id"]
    sub = get_company_subscription(cid)
    history = get_billing_history(cid, 20)
    cfg = get_stripe_config()

    billing_notice = ""
    if not STRIPE_IMPORT_OK:
        billing_notice = _t("Stripe is not installed yet.", "Stripe todavía no está instalado.")
    elif not cfg["secret_key"]:
        billing_notice = _t("Stripe secret key is missing.", "Falta la clave secreta de Stripe.")

    checkout_status = (request.args.get("checkout") or "").strip().lower()
    if checkout_status == "success":
        flash(_t(
            "Checkout completed. Billing status may take a moment to update. Use Refresh Status if needed.",
            "La compra se completó. El estado de facturación puede tardar un momento en actualizarse. Usa Actualizar estado si es necesario."
        ))
    elif checkout_status == "cancelled":
        flash(_t("Checkout was cancelled.", "La compra fue cancelada."))

    status_text = _normalize_status(sub["status"]) if sub else _t("Inactive", "Inactiva")
    status_css = _get_subscription_css_class(sub["status"] if sub else "inactive")
    access_text = _display_access_text(sub)

    use_parent_redirects = _should_redirect_to_parent_site(sub)
    monthly_action_url = _build_parent_pricing_url("monthly") if use_parent_redirects else url_for("billing.create_checkout_session", plan="monthly")
    yearly_action_url = _build_parent_pricing_url("yearly") if use_parent_redirects else url_for("billing.create_checkout_session", plan="yearly")

    content = render_template_string("""
    <style>
        .billing-pill{
            display:inline-flex;
            align-items:center;
            padding:6px 12px;
            border-radius:999px;
            font-weight:700;
            font-size:.92rem;
        }
        .status-good{background:#eaf8ef;color:#0a7a33;}
        .status-warn{background:#fff6e6;color:#9a6700;}
        .status-bad{background:#fdecec;color:#b42318;}
        .billing-grid{
            display:grid;
            grid-template-columns:1fr 1fr;
            gap:20px;
            margin-top:20px;
        }
        @media (max-width: 860px){
            .billing-grid{grid-template-columns:1fr;}
        }
    </style>

    <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
            <div>
                <h1 style="margin-bottom:6px;">{{ t_billing }}</h1>
                <p class="muted" style="margin:0;">
                    {{ t_manage_billing }}
                </p>
            </div>
            <div class="row-actions">
                <a class="btn secondary" href="{{ url_for('settings.settings') }}">{{ t_back_to_settings }}</a>
            </div>
        </div>
    </div>

    {% if billing_notice %}
    <div class="card" style="margin-top:20px; border:1px solid #f0c36d; background:#fff8e8;">
        <h2>{{ t_billing_setup_notice }}</h2>
        <p style="margin:0;">{{ billing_notice }}</p>
    </div>
    {% endif %}

    <div class="billing-grid">
        <div class="card">
            <h2>{{ t_current_subscription }}</h2>

            {% if sub %}
                <p><strong>{{ t_plan }}:</strong> {{ sub['plan_name'] or t_not_set }}</p>
                <p>
                    <strong>{{ t_status }}:</strong>
                    <span class="billing-pill {{ status_css }}">{{ status_text }}</span>
                </p>
                <p><strong>{{ t_access }}:</strong> {{ access_text }}</p>
                <p><strong>{{ t_billing_interval }}:</strong> {{ billing_interval }}</p>
                <p><strong>{{ t_auto_renew }}:</strong> {{ auto_renew_text }}</p>
                <p><strong>{{ t_current_period_start }}:</strong> {{ sub['current_period_start'] or '-' }}</p>
                <p><strong>{{ t_renewal_date }}:</strong> {{ sub['current_period_end'] or '-' }}</p>
                <p><strong>{{ t_payment_method }}:</strong> {{ sub['payment_method_label'] or t_not_added_yet }}</p>
            {% else %}
                <p><strong>{{ t_status }}:</strong> <span class="billing-pill status-bad">{{ t_inactive }}</span></p>
                <p><strong>{{ t_access }}:</strong> {{ t_locked }}</p>
                <p class="muted">{{ t_no_subscription_linked }}</p>
            {% endif %}

            <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:16px;">
                <a class="btn" href="{{ monthly_action_url }}">{{ t_start_monthly }}</a>
                <a class="btn secondary" href="{{ yearly_action_url }}">{{ t_start_yearly }}</a>

                {% if stripe_enabled %}
                    <form method="post" action="{{ url_for('billing.refresh_billing_status') }}" style="display:inline;">
                        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                        <button class="btn secondary" type="submit">{{ t_refresh_status }}</button>
                    </form>
                {% endif %}

                {% if stripe_enabled and sub and sub['stripe_customer_id'] %}
                    <a class="btn secondary" href="{{ url_for('billing.customer_portal') }}">{{ t_customer_portal }}</a>
                {% endif %}
            </div>

            {% if use_parent_redirects %}
            <p class="muted" style="margin-top:14px;">
                {{ t_parent_site_redirect_note }}
            </p>
            {% endif %}
        </div>

        <div class="card">
            <h2>{{ t_access_rules }}</h2>

            <div style="border:1px solid #d9e1ea;border-radius:14px;padding:16px;margin-bottom:12px;">
                <div style="font-weight:700;">{{ t_app_access }}</div>
                <div class="muted" style="margin-top:6px;">
                    {{ t_app_access_text_1 }}
                    <strong>{{ t_active }}</strong>
                    {{ t_or }}
                    <strong>{{ t_trialing }}</strong>.
                </div>
            </div>

            <div style="border:1px solid #d9e1ea;border-radius:14px;padding:16px;">
                <div style="font-weight:700;">{{ t_billing_visibility }}</div>
                <div class="muted" style="margin-top:6px;">
                    {{ t_billing_visibility_text }}
                </div>
            </div>
        </div>
    </div>

    <div class="card" style="margin-top:20px;">
        <h2>{{ t_billing_history }}</h2>
        <div class="table-wrap">
            <table>
                <tr>
                    <th>{{ t_date }}</th>
                    <th>{{ t_event }}</th>
                    <th>{{ t_amount }}</th>
                    <th>{{ t_status }}</th>
                    <th>{{ t_invoice }}</th>
                </tr>
                {% for row in history %}
                <tr>
                    <td>{{ row['event_date'] or row['created_at'] or '-' }}</td>
                    <td>{{ display_billing_event_type(row['event_type']) }}</td>
                    <td>
                        {% if row['amount_cents'] is not none %}
                            ${{ '%.2f'|format((row['amount_cents'] or 0) / 100) }}
                        {% else %}
                            -
                        {% endif %}
                    </td>
                    <td>{{ normalize_status(row['status']) if row['status'] else '-' }}</td>
                    <td>
                        {% if row['hosted_invoice_url'] %}
                            <a href="{{ row['hosted_invoice_url'] }}" target="_blank" rel="noopener noreferrer">{{ t_view }}</a>
                        {% else %}
                            -
                        {% endif %}
                    </td>
                </tr>
                {% else %}
                <tr>
                    <td colspan="5" class="muted">{{ t_no_billing_history }}</td>
                </tr>
                {% endfor %}
            </table>
        </div>
    </div>
    """,
    sub=sub,
    history=history,
    stripe_enabled=cfg["enabled"],
    billing_notice=billing_notice,
    status_text=status_text,
    status_css=status_css,
    access_text=access_text,
    use_parent_redirects=use_parent_redirects,
    monthly_action_url=monthly_action_url,
    yearly_action_url=yearly_action_url,
    billing_interval=_display_interval(sub["billing_interval"]) if sub and "billing_interval" in sub.keys() else "-",
    auto_renew_text=_display_auto_renew(sub["auto_renew"]) if sub and "auto_renew" in sub.keys() else _t("Disabled", "Desactivada"),
    normalize_status=_normalize_status,
    display_billing_event_type=_display_billing_event_type,
    t_billing=_t("Billing", "Facturación"),
    t_manage_billing=_t(
        "Manage your subscription status, account access, and billing history here.",
        "Administra aquí el estado de tu suscripción, el acceso de la cuenta y el historial de facturación."
    ),
    t_back_to_settings=_t("Back to Settings", "Volver a Configuración"),
    t_billing_setup_notice=_t("Billing Setup Notice", "Aviso de configuración de facturación"),
    t_current_subscription=_t("Current Subscription", "Suscripción actual"),
    t_plan=_t("Plan", "Plan"),
    t_not_set=_t("Not set", "No establecido"),
    t_status=_t("Status", "Estado"),
    t_access=_t("Access", "Acceso"),
    t_billing_interval=_t("Billing Interval", "Intervalo de facturación"),
    t_auto_renew=_t("Auto Renew", "Renovación automática"),
    t_current_period_start=_t("Current Period Start", "Inicio del período actual"),
    t_renewal_date=_t("Renewal Date", "Fecha de renovación"),
    t_payment_method=_t("Payment Method", "Método de pago"),
    t_not_added_yet=_t("Not added yet", "Todavía no agregado"),
    t_inactive=_t("Inactive", "Inactiva"),
    t_locked=_t("Locked", "Bloqueado"),
    t_no_subscription_linked=_t(
        "No subscription has been linked to this company yet.",
        "Todavía no se ha vinculado una suscripción a esta empresa."
    ),
    t_start_monthly=_t("Start Monthly", "Iniciar mensual"),
    t_start_yearly=_t("Start Yearly", "Iniciar anual"),
    t_refresh_status=_t("Refresh Status", "Actualizar estado"),
    t_customer_portal=_t("Customer Portal", "Portal del cliente"),
    t_access_rules=_t("Access Rules", "Reglas de acceso"),
    t_app_access=_t("App Access", "Acceso a la app"),
    t_app_access_text_1=_t(
        "TerraLedger access should only be available when the subscription status is ",
        "El acceso a TerraLedger solo debe estar disponible cuando el estado de la suscripción sea "
    ),
    t_active=_t("Active", "Activa"),
    t_or=_t(" or ", " o "),
    t_trialing=_t("Trialing", "En prueba"),
    t_billing_visibility=_t("Billing Visibility", "Visibilidad de facturación"),
    t_billing_visibility_text=_t(
        "This page is for subscription status, billing history, refresh actions, and customer billing visibility.",
        "Esta página es para el estado de la suscripción, historial de facturación, acciones de actualización y visibilidad de facturación del cliente."
    ),
    t_parent_site_redirect_note=_t(
        "New or unlinked companies are sent to the Spencer Softwares pricing page first so billing starts from the main site.",
        "Las empresas nuevas o no vinculadas se envían primero a la página de precios de Spencer Softwares para que la facturación comience desde el sitio principal."
    ),
    t_billing_history=_t("Billing History", "Historial de facturación"),
    t_date=_t("Date", "Fecha"),
    t_event=_t("Event", "Evento"),
    t_amount=_t("Amount", "Cantidad"),
    t_invoice=_t("Invoice", "Factura"),
    t_view=_t("View", "Ver"),
    t_no_billing_history=_t("No billing history yet.", "Todavía no hay historial de facturación."))

    return render_page(content, title=_t("Billing", "Facturación"))


@billing_bp.route("/settings/billing/refresh", methods=["POST"])
@login_required
def refresh_billing_status():
    cid = session["company_id"]
    ok, message = _refresh_company_subscription_from_stripe(cid)
    flash(message)
    return redirect(url_for("billing.billing_page"))


@billing_bp.route("/settings/billing/portal")
@login_required
def customer_portal():
    cfg = get_stripe_config()

    if not cfg["enabled"]:
        flash(_t("Billing is not configured yet.", "La facturación todavía no está configurada."))
        return redirect(url_for("billing.billing_page"))

    cid = session["company_id"]
    sub = get_company_subscription(cid)

    if not sub or not sub["stripe_customer_id"]:
        flash(_t(
            "No Stripe customer is linked to this account yet.",
            "Todavía no hay un cliente de Stripe vinculado a esta cuenta."
        ))
        return redirect(url_for("billing.billing_page"))

    try:
        portal_session = stripe.billing_portal.Session.create(
            customer=sub["stripe_customer_id"],
            return_url=f"{cfg['app_base_url']}/settings/billing",
        )
        return redirect(portal_session.url)
    except Exception as e:
        flash(_t(
            f"Could not open customer portal: {e}",
            f"No se pudo abrir el portal del cliente: {e}"
        ))
        return redirect(url_for("billing.billing_page"))


@billing_bp.route("/settings/billing/checkout")
@login_required
def create_checkout_session():
    cfg = get_stripe_config()

    if not cfg["enabled"]:
        flash(_t("Stripe billing is not configured yet.", "La facturación de Stripe todavía no está configurada."))
        return redirect(url_for("billing.billing_page"))

    plan = (request.args.get("plan") or "monthly").strip().lower()

    cid = session["company_id"]
    sub = get_company_subscription(cid)

    if _should_redirect_to_parent_site(sub):
        redirect_url = _build_parent_pricing_url(plan)
        print("REDIRECTING NEW/UNLINKED COMPANY TO PARENT SITE:", redirect_url)
        return redirect(redirect_url)

    if plan == "yearly":
        price_id = cfg["price_yearly"]
    else:
        plan = "monthly"
        price_id = cfg["price_monthly"]

    if not price_id:
        flash(_t("The selected billing plan is not configured yet.", "El plan de facturación seleccionado todavía no está configurado."))
        return redirect(url_for("billing.billing_page"))

    company = _get_company(cid)
    user_email = _get_user_email()

    stripe_customer_id = None
    if sub and sub["stripe_customer_id"]:
        stripe_customer_id = sub["stripe_customer_id"]

    discounts = []

    if user_email and user_email == cfg["owner_email"]:
        if cfg["owner_promo_code_id"]:
            discounts.append({"promotion_code": cfg["owner_promo_code_id"]})
        elif cfg["owner_coupon_id"]:
            discounts.append({"coupon": cfg["owner_coupon_id"]})

    metadata = {
        "company_id": str(cid),
        "company_name": company["name"] if company and "name" in company.keys() else "",
        "user_email": user_email,
        "selected_plan": plan,
    }

    session_kwargs = {
        "mode": "subscription",
        "line_items": [
            {
                "price": price_id,
                "quantity": 1,
            }
        ],
        "success_url": f"{cfg['app_base_url']}/settings/billing?checkout=success",
        "cancel_url": f"{cfg['app_base_url']}/settings/billing?checkout=cancelled",
        "allow_promotion_codes": True,
        "client_reference_id": str(cid),
        "metadata": metadata,
        "subscription_data": {
            "metadata": metadata,
        },
    }

    if stripe_customer_id:
        session_kwargs["customer"] = stripe_customer_id
    elif user_email:
        session_kwargs["customer_email"] = user_email

    if discounts:
        session_kwargs["discounts"] = discounts

    print("CREATE CHECKOUT SESSION company_id:", cid)
    print("CREATE CHECKOUT SESSION user_email:", user_email)
    print("CREATE CHECKOUT SESSION metadata:", metadata)
    print("CREATE CHECKOUT SESSION client_reference_id:", session_kwargs.get("client_reference_id"))

    try:
        checkout_session = stripe.checkout.Session.create(**session_kwargs)

        verified_session = stripe.checkout.Session.retrieve(checkout_session.id)

        print("CHECKOUT SESSION CREATED ID:", checkout_session.id)
        print("CHECKOUT SESSION CREATED URL:", checkout_session.url)
        print("CHECKOUT SESSION VERIFIED METADATA:", verified_session.get("metadata"))
        print("CHECKOUT SESSION VERIFIED CLIENT_REFERENCE_ID:", verified_session.get("client_reference_id"))
        print("CHECKOUT SESSION VERIFIED MODE:", verified_session.get("mode"))

        return redirect(checkout_session.url)
    except Exception as e:
        flash(_t(
            f"Could not start checkout: {e}",
            f"No se pudo iniciar el pago: {e}"
        ))
        print("CHECKOUT SESSION CREATE FAILED:", e)
        return redirect(url_for("billing.billing_page"))


@billing_bp.route("/stripe-webhook", methods=["POST"])
def stripe_webhook():
    cfg = get_stripe_config()

    if not cfg["enabled"] or not cfg["webhook_secret"]:
        return {"ok": False, "message": "Stripe webhook not configured."}, 400

    payload = request.data
    sig_header = request.headers.get("Stripe-Signature", "")

    try:
        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig_header,
            secret=cfg["webhook_secret"]
        )
    except Exception as e:
        print("STRIPE WEBHOOK SIGNATURE ERROR:", e)
        return {"ok": False}, 400

    event_type = event.get("type")
    obj = event["data"]["object"]

    print("STRIPE WEBHOOK EVENT:", event_type)
    print("WEBHOOK OBJECT ID:", obj.get("id"))
    print("WEBHOOK CUSTOMER:", obj.get("customer"))
    print("WEBHOOK SUBSCRIPTION:", obj.get("subscription"))
    print("WEBHOOK METADATA:", obj.get("metadata"))
    print("WEBHOOK CLIENT_REFERENCE_ID:", obj.get("client_reference_id"))

    def company_id_from_customer(customer_id):
        conn = get_db_connection()
        row = conn.execute(
            "SELECT company_id FROM subscriptions WHERE stripe_customer_id = %s",
            (customer_id,)
        ).fetchone()
        conn.close()
        return row["company_id"] if row else None

    def company_id_from_user_email(email):
        if not email:
            return None

        conn = get_db_connection()
        row = conn.execute(
            "SELECT company_id FROM users WHERE LOWER(email) = %s ORDER BY id LIMIT 1",
            ((email or "").strip().lower(),)
        ).fetchone()
        conn.close()
        return row["company_id"] if row else None

    if event_type == "checkout.session.completed":
        customer_id = obj.get("customer")
        subscription_id = obj.get("subscription")

        company_id = None
        metadata = obj.get("metadata") or {}

        if metadata.get("company_id"):
            try:
                company_id = int(metadata.get("company_id"))
            except Exception:
                company_id = None

        if not company_id and obj.get("client_reference_id"):
            try:
                company_id = int(obj.get("client_reference_id"))
            except Exception:
                company_id = None

        checkout_email = None

        customer_details = obj.get("customer_details") or {}
        if customer_details.get("email"):
            checkout_email = (customer_details.get("email") or "").strip().lower()

        if not checkout_email and obj.get("customer_email"):
            checkout_email = (obj.get("customer_email") or "").strip().lower()

        if not checkout_email and customer_id:
            try:
                stripe_customer = stripe.Customer.retrieve(customer_id)
                if stripe_customer and stripe_customer.get("email"):
                    checkout_email = (stripe_customer.get("email") or "").strip().lower()
            except Exception as e:
                print("Could not retrieve Stripe customer email:", e)

        if not company_id and checkout_email:
            company_id = company_id_from_user_email(checkout_email)

        print("checkout.session.completed resolved company_id:", company_id)
        print("checkout.session.completed resolved email:", checkout_email)

        if company_id:
            try:
                if subscription_id:
                    _sync_subscription_from_stripe(company_id, subscription_id)
                    print("checkout.session.completed sync success:", company_id, subscription_id)
                else:
                    upsert_company_subscription(
                        company_id=company_id,
                        stripe_customer_id=customer_id,
                        stripe_subscription_id=None,
                        stripe_price_id=None,
                        plan_name="Subscription",
                        billing_interval=None,
                        amount_cents=None,
                        status="active",
                        auto_renew=1,
                        cancel_at_period_end=0,
                        current_period_start=None,
                        current_period_end=None,
                        payment_method_type=None,
                        payment_method_last4=None,
                        payment_method_label=None,
                    )
                    print("checkout.session.completed linked customer only:", company_id, customer_id)
            except Exception as e:
                print("checkout.session.completed sync failed:", e)
        else:
            print("checkout.session.completed could not resolve company_id")

    elif event_type in (
        "customer.subscription.created",
        "customer.subscription.updated",
        "customer.subscription.deleted",
    ):
        company_id = company_id_from_customer(obj.get("customer"))
        if company_id and obj.get("id"):
            try:
                _sync_subscription_from_stripe(company_id, obj["id"])
                print("subscription sync success:", company_id, obj["id"])
            except Exception as e:
                print("subscription sync failed:", e)

    elif event_type in (
        "invoice.paid",
        "invoice.payment_succeeded",
        "invoice.payment_failed",
    ):
        company_id = company_id_from_customer(obj.get("customer"))
        if company_id:
            event_date = None
            if obj.get("created"):
                event_date = datetime.fromtimestamp(obj["created"]).strftime("%Y-%m-%d")

            try:
                insert_billing_event(
                    company_id=company_id,
                    stripe_invoice_id=obj.get("id"),
                    stripe_event_id=event.get("id"),
                    event_type=event_type,
                    amount_cents=obj.get("amount_paid") or obj.get("amount_due") or 0,
                    currency=obj.get("currency"),
                    status=obj.get("status"),
                    hosted_invoice_url=obj.get("hosted_invoice_url"),
                    invoice_pdf=obj.get("invoice_pdf"),
                    event_date=event_date,
                    notes=obj.get("number"),
                )
                print("invoice event saved:", event_type, company_id)
            except Exception as e:
                print("invoice event save failed:", e)

    return {"ok": True}