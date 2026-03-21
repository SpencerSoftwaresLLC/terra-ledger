import os
from datetime import datetime
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


def get_stripe_config():
    stripe_secret_key = os.environ.get("STRIPE_SECRET_KEY", "").strip()
    stripe_publishable_key = os.environ.get("STRIPE_PUBLISHABLE_KEY", "").strip()
    stripe_webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
    app_base_url = os.environ.get("APP_BASE_URL", "http://127.0.0.1:5000").strip().rstrip("/")

    stripe_price_monthly = os.environ.get("STRIPE_PRICE_MONTHLY", "").strip()
    stripe_price_yearly = os.environ.get("STRIPE_PRICE_YEARLY", "").strip()

    stripe_owner_email = os.environ.get("STRIPE_OWNER_EMAIL", "").strip().lower()
    stripe_owner_coupon_id = os.environ.get("STRIPE_OWNER_COUPON_ID", "").strip()
    stripe_owner_promo_code_id = os.environ.get("STRIPE_OWNER_PROMO_CODE_ID", "").strip()

    stripe_enabled = bool(STRIPE_IMPORT_OK and stripe_secret_key)

    if stripe_enabled:
        stripe.api_key = stripe_secret_key

    return {
        "secret_key": stripe_secret_key,
        "publishable_key": stripe_publishable_key,
        "webhook_secret": stripe_webhook_secret,
        "app_base_url": app_base_url,
        "price_monthly": stripe_price_monthly,
        "price_yearly": stripe_price_yearly,
        "owner_email": stripe_owner_email,
        "owner_coupon_id": stripe_owner_coupon_id,
        "owner_promo_code_id": stripe_owner_promo_code_id,
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
    return (session.get("user_email") or "").strip().lower()


def _normalize_status(status):
    value = (status or "").strip().lower()

    if value in ("active", "trialing"):
        return "Active"
    if value == "past_due":
        return "Past Due"
    if value in ("canceled", "cancelled"):
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

        return False, "No Stripe subscription was found for this account."
    except Exception as e:
        return False, f"Could not refresh billing status: {e}"


@billing_bp.route("/subscription-required")
@login_required
def subscription_required_page():
    cid = session["company_id"]
    sub = get_company_subscription(cid)

    status_text = _normalize_status(sub["status"]) if sub else "Inactive"
    plan_name = sub["plan_name"] if sub and sub["plan_name"] else "No active plan"

    content = """
    <div class="card" style="max-width:900px;margin:0 auto;">
        <h1>Subscription Required</h1>
        <p class="muted">
            TerraLedger access is currently locked for this account.
        </p>

        <div style="margin-top:20px;">
            <div class="card" style="margin:0;">
                <h2>Current Status</h2>
                <p><strong>Plan:</strong> {plan_name}</p>
                <p><strong>Status:</strong> {status_text}</p>
                <p><strong>Renewal Date:</strong> {renewal}</p>
            </div>

            <div class="card" style="margin-top:20px;">
                <h2>What To Do</h2>
                <p>
                    Your account needs an active subscription to continue using TerraLedger.
                </p>

                <div style="display:flex;gap:10px;flex-wrap:wrap;">
                    <a class="btn secondary" href="{refresh_url}">Refresh Access</a>
                    <a class="btn secondary" href="{billing_url}">View Billing Page</a>
                </div>
            </div>
        </div>
    </div>
    """.format(
        plan_name=plan_name,
        status_text=status_text,
        renewal=sub["current_period_end"] if sub and sub["current_period_end"] else "-",
        refresh_url=url_for("billing.refresh_billing_status"),
        billing_url=url_for("billing.billing_page"),
    )

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
        <p class="muted">
            Manage your subscription status, account access, and billing history here.
        </p>
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
                {% if stripe_enabled %}
                    <a class="btn" href="{{ url_for('billing.create_checkout_session', plan='monthly') }}">Start Monthly</a>
                    <a class="btn secondary" href="{{ url_for('billing.create_checkout_session', plan='yearly') }}">Start Yearly</a>
                    <a class="btn secondary" href="{{ url_for('billing.refresh_billing_status') }}">Refresh Status</a>
                {% endif %}
                {% if stripe_enabled and sub and sub['stripe_customer_id'] %}
                    <a class="btn secondary" href="{{ url_for('billing.customer_portal') }}">Customer Portal</a>
                {% endif %}
            </div>
        </div>

        <div class="card">
            <h2>Access Rules</h2>

            <div style="border:1px solid #d9e1ea;border-radius:14px;padding:16px;margin-bottom:12px;">
                <div style="font-weight:700;">App Access</div>
                <div class="muted" style="margin-top:6px;">
                    TerraLedger access should only be available when the subscription status is
                    <strong>Active</strong> or <strong>Trialing</strong>.
                </div>
            </div>

            <div style="border:1px solid #d9e1ea;border-radius:14px;padding:16px;">
                <div style="font-weight:700;">Billing Visibility</div>
                <div class="muted" style="margin-top:6px;">
                    This page is for subscription status, billing history, refresh actions,
                    and customer billing visibility.
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
                            <a href="{{ row['hosted_invoice_url'] }}" target="_blank" rel="noopener noreferrer">View</a>
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


@billing_bp.route("/settings/billing/checkout")
@login_required
def create_checkout_session():
    cfg = get_stripe_config()

    if not cfg["enabled"]:
        flash("Stripe billing is not configured yet.")
        return redirect(url_for("billing.billing_page"))

    plan = (request.args.get("plan") or "monthly").strip().lower()

    if plan == "yearly":
        price_id = cfg["price_yearly"]
    else:
        plan = "monthly"
        price_id = cfg["price_monthly"]

    if not price_id:
        flash("The selected billing plan is not configured yet.")
        return redirect(url_for("billing.billing_page"))

    cid = session["company_id"]
    company = _get_company(cid)
    user_email = _get_user_email()

    sub = get_company_subscription(cid)
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
        flash(f"Could not start checkout: {e}")
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
            "SELECT company_id FROM subscriptions WHERE stripe_customer_id = ?",
            (customer_id,)
        ).fetchone()
        conn.close()
        return row["company_id"] if row else None

    def company_id_from_user_email(email):
        if not email:
            return None

        conn = get_db_connection()
        row = conn.execute(
            "SELECT company_id FROM users WHERE LOWER(email) = ? ORDER BY id LIMIT 1",
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