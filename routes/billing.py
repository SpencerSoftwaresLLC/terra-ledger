import os
from datetime import datetime
from flask import Blueprint, request, redirect, url_for, session, flash, render_template_string

from ..db import (
    get_db_connection,
    get_company_subscription,
    upsert_company_subscription,
    insert_billing_event,
    get_billing_history,
)
from ..decorators import login_required
from ..page_helpers import render_page

billing_bp = Blueprint("billing", __name__)

try:
    import stripe
    STRIPE_IMPORT_OK = True
except Exception:
    stripe = None
    STRIPE_IMPORT_OK = False


def get_stripe_config():
    stripe_secret_key = os.environ.get("STRIPE_SECRET_KEY", "").strip()
    stripe_publishable_key = os.environ.get("STRIPE_PUBLISHABLE_KEY", "").strip()
    stripe_webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
    app_base_url = os.environ.get("APP_BASE_URL", "http://127.0.0.1:5000").strip()

    parent_website_url = os.environ.get("PARENT_WEBSITE_URL", "").strip()
    parent_billing_url = os.environ.get("PARENT_BILLING_URL", "").strip()

    stripe_enabled = bool(STRIPE_IMPORT_OK and stripe_secret_key)

    if STRIPE_IMPORT_OK and stripe_secret_key:
        stripe.api_key = stripe_secret_key

    return {
        "secret_key": stripe_secret_key,
        "publishable_key": stripe_publishable_key,
        "webhook_secret": stripe_webhook_secret,
        "app_base_url": app_base_url,
        "parent_website_url": parent_website_url,
        "parent_billing_url": parent_billing_url or parent_website_url,
        "enabled": stripe_enabled,
    }


def _get_company(company_id):
    conn = get_db_connection()
    row = conn.execute(
        "SELECT * FROM companies WHERE id = ?",
        (company_id,)
    ).fetchone()
    conn.close()
    return row


def _get_user_email():
    return session.get("user_email") or ""


def _normalize_status(status):
    value = (status or "").strip().lower()

    if value in ("active", "trialing"):
        return "Active"
    if value == "past_due":
        return "Past Due"
    if value == "canceled":
        return "Canceled"
    if value == "cancelled":
        return "Canceled"
    if value == "unpaid":
        return "Unpaid"
    if value == "incomplete":
        return "Incomplete"
    if value == "incomplete_expired":
        return "Incomplete Expired"
    if value == "paused":
        return "Paused"
    if value == "expired":
        return "Expired"
    if value == "trial":
        return "Trial"
    if not value:
        return "Inactive"

    return value.replace("_", " ").title()


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
    """
    Find the best/current Stripe subscription for a customer.
    Preference order:
    active -> trialing -> past_due -> unpaid -> incomplete -> canceled -> anything newest
    """
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
            pm_label = f"Card ending in {pm_last4}"

        elif pm_type == "us_bank_account" and pm.get("us_bank_account"):
            pm_last4 = pm["us_bank_account"].get("last4")
            bank_name = pm["us_bank_account"].get("bank_name") or "Bank account"
            pm_label = f"{bank_name} ending in {pm_last4}"

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
        return False, "Stripe is not configured."

    sub = get_company_subscription(company_id)

    if not sub:
        return False, "No subscription record found yet."

    stripe_subscription_id = sub["stripe_subscription_id"] if "stripe_subscription_id" in sub.keys() else None
    stripe_customer_id = sub["stripe_customer_id"] if "stripe_customer_id" in sub.keys() else None

    try:
        if stripe_subscription_id:
            _sync_subscription_from_stripe(company_id, stripe_subscription_id)
            return True, "Subscription status refreshed."

        if stripe_customer_id:
            best = _find_best_subscription_for_customer(stripe_customer_id)
            if best and best.get("id"):
                _sync_subscription_from_stripe(company_id, best["id"])
                return True, "Subscription status refreshed."

        return False, "No Stripe subscription found for this account."
    except Exception as e:
        return False, f"Could not refresh billing status: {e}"


@billing_bp.route("/subscription-required")
@login_required
def subscription_required_page():
    cid = session["company_id"]
    sub = get_company_subscription(cid)
    cfg = get_stripe_config()

    status_text = _normalize_status(sub["status"]) if sub else "Inactive"
    plan_name = sub["plan_name"] if sub and sub["plan_name"] else "No active plan"

    content = render_template_string("""
    <div class="card" style="max-width:900px;margin:0 auto;">
        <h1>Subscription Required</h1>
        <p class="muted">
            TerraLedger access is currently locked for this account. A subscription must be purchased
            on the parent website before the app can be used.
        </p>

        <div style="margin-top:20px;display:grid;grid-template-columns:1fr 1fr;gap:20px;">
            <div class="card" style="margin:0;">
                <h2>Current Status</h2>
                <p><strong>Plan:</strong> {{ plan_name }}</p>
                <p><strong>Status:</strong> {{ status_text }}</p>
                <p><strong>Renewal Date:</strong> {{ sub['current_period_end'] if sub and sub['current_period_end'] else '-' }}</p>
            </div>

            <div class="card" style="margin:0;">
                <h2>What To Do</h2>
                <p style="margin:0 0 12px 0;">
                    Purchase or manage your TerraLedger subscription on the parent website first.
                    Then come back here and refresh access.
                </p>

                <div style="display:flex;gap:10px;flex-wrap:wrap;">
                    {% if parent_billing_url %}
                        <a class="btn" href="{{ parent_billing_url }}" target="_blank">Go To Billing Website</a>
                    {% endif %}
                    <a class="btn secondary" href="{{ url_for('billing.refresh_billing_status') }}">Refresh Access</a>
                    <a class="btn secondary" href="{{ url_for('billing.billing_page') }}">View Billing Page</a>
                </div>
            </div>
        </div>
    </div>
    """, sub=sub, plan_name=plan_name, status_text=status_text, parent_billing_url=cfg["parent_billing_url"])

    return render_page(content, title="Subscription Required")


@billing_bp.route("/settings/billing")
@login_required
def billing_page():
    cid = session["company_id"]
    sub = get_company_subscription(cid)
    history = get_billing_history(cid, 20)
    cfg = get_stripe_config()

    billing_notice = ""
    if not STRIPE_IMPORT_OK:
        billing_notice = "Stripe is not installed yet."
    elif not cfg["secret_key"]:
        billing_notice = "Stripe secret key is missing."

    status_text = _normalize_status(sub["status"]) if sub else "Inactive"
    status_css = _get_subscription_css_class(sub["status"] if sub else "inactive")
    access_text = "Unlocked" if _has_active_access(sub) else "Locked"

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
        <h1>Billing</h1>
        <p class="muted">View your current subscription and billing history. New purchases are handled on the parent website.</p>
    </div>

    {% if billing_notice %}
    <div class="card" style="margin-top:20px; border:1px solid #f0c36d; background:#fff8e8;">
        <h2>Billing Setup Notice</h2>
        <p style="margin:0;">{{ billing_notice }}</p>
    </div>
    {% endif %}

    <div class="billing-grid">
        <div class="card">
            <h2>Current Subscription</h2>

            {% if sub %}
                <p><strong>Plan:</strong> {{ sub['plan_name'] or 'Not set' }}</p>
                <p>
                    <strong>Status:</strong>
                    <span class="billing-pill {{ status_css }}">{{ status_text }}</span>
                </p>
                <p><strong>Access:</strong> {{ access_text }}</p>
                <p><strong>Billing Interval:</strong> {{ (sub['billing_interval'] or '-')|title }}</p>
                <p><strong>Auto Renew:</strong> {{ 'Enabled' if sub['auto_renew'] else 'Disabled' }}</p>
                <p><strong>Current Period Start:</strong> {{ sub['current_period_start'] or '-' }}</p>
                <p><strong>Renewal Date:</strong> {{ sub['current_period_end'] or '-' }}</p>
                <p><strong>Payment Method:</strong> {{ sub['payment_method_label'] or 'Not added yet' }}</p>
            {% else %}
                <p><strong>Status:</strong> <span class="billing-pill status-bad">Inactive</span></p>
                <p><strong>Access:</strong> Locked</p>
                <p class="muted">No subscription has been linked to this company yet.</p>
            {% endif %}

            <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:16px;">
                {% if parent_billing_url %}
                    <a class="btn" href="{{ parent_billing_url }}" target="_blank">Subscribe / Manage On Website</a>
                {% endif %}
                {% if stripe_enabled %}
                    <a class="btn secondary" href="{{ url_for('billing.refresh_billing_status') }}">Refresh Status</a>
                {% endif %}
                {% if stripe_enabled and sub and sub['stripe_customer_id'] %}
                    <a class="btn secondary" href="{{ url_for('billing.customer_portal') }}">Customer Portal</a>
                {% endif %}
            </div>
        </div>

        <div class="card">
            <h2>Subscription Access Rules</h2>

            <div style="border:1px solid #d9e1ea;border-radius:14px;padding:16px;margin-bottom:12px;">
                <div style="font-weight:700;">App Access</div>
                <div class="muted" style="margin-top:6px;">
                    TerraLedger should only be accessible when the subscription status is
                    <strong>Active</strong> or <strong>Trialing</strong>.
                </div>
            </div>

            <div style="border:1px solid #d9e1ea;border-radius:14px;padding:16px;">
                <div style="font-weight:700;">Purchase Flow</div>
                <div class="muted" style="margin-top:6px;">
                    Customers must subscribe on the parent website first. This page is now read-only
                    for status, history, and account management.
                </div>
            </div>
        </div>
    </div>

    <div class="card" style="margin-top:20px;">
        <h2>Billing History</h2>
        <div class="table-wrap">
            <table>
                <tr>
                    <th>Date</th>
                    <th>Event</th>
                    <th>Amount</th>
                    <th>Status</th>
                    <th>Invoice</th>
                </tr>
                {% for row in history %}
                <tr>
                    <td>{{ row['event_date'] or row['created_at'] or '-' }}</td>
                    <td>{{ row['event_type'] or '-' }}</td>
                    <td>
                        {% if row['amount_cents'] is not none %}
                            ${{ '%.2f'|format((row['amount_cents'] or 0) / 100) }}
                        {% else %}
                            -
                        {% endif %}
                    </td>
                    <td>{{ row['status'] or '-' }}</td>
                    <td>
                        {% if row['hosted_invoice_url'] %}
                            <a href="{{ row['hosted_invoice_url'] }}" target="_blank">View</a>
                        {% else %}
                            -
                        {% endif %}
                    </td>
                </tr>
                {% else %}
                <tr>
                    <td colspan="5" class="muted">No billing history yet.</td>
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
    parent_billing_url=cfg["parent_billing_url"],
    status_text=status_text,
    status_css=status_css,
    access_text=access_text)

    return render_page(content, title="Billing")


@billing_bp.route("/settings/billing/refresh")
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
        flash("Billing is not configured yet.")
        return redirect(url_for("billing.billing_page"))

    cid = session["company_id"]
    sub = get_company_subscription(cid)

    if not sub or not sub["stripe_customer_id"]:
        flash("No Stripe customer is linked to this account yet.")
        return redirect(url_for("billing.billing_page"))

    portal_session = stripe.billing_portal.Session.create(
        customer=sub["stripe_customer_id"],
        return_url=f"{cfg['app_base_url']}/settings/billing",
    )

    return redirect(portal_session.url)


@billing_bp.route("/stripe/webhook", methods=["POST"])
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
    except Exception:
        return {"ok": False}, 400

    event_type = event.get("type")
    obj = event["data"]["object"]

    def company_id_from_customer(customer_id):
        conn = get_db_connection()
        row = conn.execute(
            "SELECT company_id FROM subscriptions WHERE stripe_customer_id = ?",
            (customer_id,)
        ).fetchone()
        conn.close()
        return row["company_id"] if row else None

    if event_type in (
        "customer.subscription.created",
        "customer.subscription.updated",
        "customer.subscription.deleted",
    ):
        company_id = company_id_from_customer(obj.get("customer"))
        if company_id and obj.get("id"):
            try:
                _sync_subscription_from_stripe(company_id, obj["id"])
            except Exception:
                pass

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

    return {"ok": True}