from functools import wraps
from flask import session, redirect, url_for, flash
from db import get_db_connection, ensure_user_permission_columns, get_company_subscription


def get_current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None

    conn = get_db_connection()
    user = conn.execute(
        "SELECT * FROM users WHERE id = ?",
        (user_id,)
    ).fetchone()
    conn.close()
    return user


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

            if not user["is_active"]:
                flash("Your account is inactive.")
                return redirect(url_for("auth.logout"))

            if (user["role"] or "").strip().lower() == "owner":
                return fn(*args, **kwargs)

            if not user[permission_name]:
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

        sub = get_company_subscription(company_id)
        status = (sub["status"] or "").strip().lower() if sub else ""

        if status not in ("active", "trialing", "trial"):
            flash("An active subscription is required to use TerraLedger.")
            return redirect(url_for("billing.subscription_required_page"))

        return view(*args, **kwargs)
    return wrapped