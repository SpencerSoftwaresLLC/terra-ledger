import os
from functools import wraps
from flask import session, redirect, url_for, flash
from db import get_db_connection, ensure_user_permission_columns, get_company_subscription


def _row_value(row, key, default=None):
    if not row:
        return default
    try:
        value = row[key]
        return default if value is None else value
    except Exception:
        return default


def get_current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None

    conn = get_db_connection()
    try:
        user = conn.execute(
            "SELECT * FROM users WHERE id = %s",
            (user_id,)
        ).fetchone()
        return user
    finally:
        conn.close()


def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("auth.login"))
        return f(*args, **kwargs)
    return wrapped


def require_permission(permission_name):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            ensure_user_permission_columns()

            user = get_current_user()

            if not user:
                flash("Please log in.")
                return redirect(url_for("auth.login"))

            if not bool(_row_value(user, "is_active", True)):
                flash("Your account is inactive.")
                return redirect(url_for("auth.logout"))

            role = str(_row_value(user, "role", "") or "").strip().lower()
            if role == "owner":
                return fn(*args, **kwargs)

            has_permission = bool(_row_value(user, permission_name, False))
            if not has_permission:
                flash("You do not have permission to access that page.")
                return redirect(url_for("dashboard.dashboard"))

            return fn(*args, **kwargs)
        return wrapper
    return decorator


def subscription_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        company_id = session.get("company_id")
        if not company_id:
            return redirect(url_for("auth.login"))

        owner_email = (os.environ.get("STRIPE_OWNER_EMAIL") or "").strip().lower()
        session_email = (session.get("user_email") or "").strip().lower()

        db_email = ""
        if not session_email:
            user = get_current_user()
            if user:
                db_email = (str(_row_value(user, "email", "")) or "").strip().lower()

        effective_email = session_email or db_email

        # Owner bypass
        if owner_email and effective_email == owner_email:
            return view(*args, **kwargs)

        sub = get_company_subscription(company_id)
        status = (str(_row_value(sub, "status", "")) or "").strip().lower()

        if status not in ("active", "trialing", "trial"):
            flash("An active subscription is required to use TerraLedger.")
            return redirect(url_for("billing.subscription_required_page"))

        return view(*args, **kwargs)

    return wrapped