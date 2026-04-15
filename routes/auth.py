from flask import Blueprint, request, redirect, url_for, session, flash, render_template_string
from werkzeug.security import generate_password_hash, check_password_hash
import secrets
from datetime import datetime, timedelta

from db import get_db_connection, create_owner_user, ensure_password_reset_table
from page_helpers import render_public_page, csrf_input
from utils.emailing import send_company_email
from permissions import get_role_defaults

auth_bp = Blueprint("auth", __name__)

MAX_LOGIN_ATTEMPTS = 5


def _check_password_strength(password: str) -> bool:
    return (
        len(password) >= 8
        and any(c.isupper() for c in password)
        and any(c.islower() for c in password)
        and any(c.isdigit() for c in password)
    )


def _reset_login_attempts():
    session["login_attempts"] = 0


def _increment_login_attempts():
    session["login_attempts"] = session.get("login_attempts", 0) + 1


def _too_many_attempts():
    return session.get("login_attempts", 0) >= MAX_LOGIN_ATTEMPTS


def _get_company_language(company_id):
    conn = get_db_connection()
    try:
        try:
            conn.execute(
                """
                ALTER TABLE company_profile
                ADD COLUMN IF NOT EXISTS language_preference TEXT DEFAULT 'en'
                """
            )
            conn.commit()
        except Exception:
            conn.rollback()

        profile = conn.execute(
            """
            SELECT language_preference
            FROM company_profile
            WHERE company_id = %s
            """,
            (company_id,),
        ).fetchone()

        language = "en"
        if profile and "language_preference" in profile.keys() and profile["language_preference"]:
            language = str(profile["language_preference"]).strip().lower()
            if language not in {"en", "es"}:
                language = "en"

        return language
    finally:
        conn.close()


def _safe_int(value, default=None):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def _session_role_defaults(role):
    defaults = get_role_defaults(role or "")
    return {k: int(v or 0) for k, v in defaults.items()}


def _load_user_permission_map(conn, user_id, role):
    permission_map = _session_role_defaults(role)

    try:
        rows = conn.execute(
            """
            SELECT permission_name, allowed
            FROM user_permissions
            WHERE user_id = %s
            """,
            (user_id,),
        ).fetchall()
    except Exception:
        rows = []

    for row in rows:
        name = str(row["permission_name"] or "").strip()
        if not name:
            continue
        permission_map[name] = int(row["allowed"] or 0)

    return permission_map


def _load_linked_employee_id(conn, company_id, user_id, email):
    email = (email or "").strip().lower()
    employee_id = None

    try:
        cols = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'employees'
            """
        ).fetchall()

        employee_keys = {
            str(row["column_name"]).strip()
            for row in cols
            if row and row["column_name"]
        }
    except Exception:
        employee_keys = set()

    if not employee_keys:
        try:
            sample = conn.execute(
                "SELECT * FROM employees LIMIT 1"
            ).fetchone()
            if sample:
                employee_keys = set(sample.keys())
        except Exception:
            employee_keys = set()

    link_user_id = "user_id" in employee_keys
    link_email = "email" in employee_keys

    if link_user_id:
        try:
            row = conn.execute(
                """
                SELECT id
                FROM employees
                WHERE company_id = %s
                  AND user_id = %s
                ORDER BY id
                LIMIT 1
                """,
                (company_id, user_id),
            ).fetchone()
            if row:
                employee_id = _safe_int(row["id"])
        except Exception:
            pass

    if employee_id is None and link_email and email:
        try:
            row = conn.execute(
                """
                SELECT id
                FROM employees
                WHERE company_id = %s
                  AND LOWER(COALESCE(email, '')) = %s
                ORDER BY id
                LIMIT 1
                """,
                (company_id, email),
            ).fetchone()
            if row:
                employee_id = _safe_int(row["id"])
        except Exception:
            pass

    return employee_id


def _set_logged_in_session(user, email):
    conn = get_db_connection()
    try:
        role = str(
            user["role"]
            if "role" in user.keys() and user["role"] is not None
            else "owner"
        ).strip().lower() or "owner"

        permission_map = _load_user_permission_map(conn, user["id"], role)
        linked_employee_id = _load_linked_employee_id(
            conn=conn,
            company_id=user["company_id"],
            user_id=user["id"],
            email=email,
        )
    finally:
        conn.close()

    session.clear()
    session["user_id"] = user["id"]
    session["user_name"] = user["name"]
    session["user_email"] = email
    session["company_id"] = user["company_id"]
    session["company_name"] = user["company_name"]
    session["language"] = _get_company_language(user["company_id"])

    session["role"] = role
    session["user_role"] = role
    session["permissions"] = permission_map
    session["user_permissions"] = permission_map

    session["is_owner"] = 1 if role == "owner" else 0
    session["is_admin"] = 1 if role in {"owner", "admin"} else 0

    if linked_employee_id:
        session["employee_id"] = linked_employee_id
        session["linked_employee_id"] = linked_employee_id


def _render_auth_page(title: str, heading: str, subtitle: str, body_html: str):
    content = render_template_string("""
    <div class="tl-auth-page">
        <div class="tl-auth-card">
            <div class="tl-auth-brand">
                <img
                    src="{{ url_for('static', filename='images/logo.png') }}"
                    alt="TerraLedger Logo"
                    class="tl-auth-logo"
                >
                <div>
                    <div class="tl-auth-brand-title">TerraLedger<sup style="font-size:11px;">™</sup></div>
                    <div class="tl-auth-brand-subtitle">Landscaper-focused business software</div>
                </div>
            </div>

            <h1 class="tl-auth-title">{{ heading }}</h1>
            <p class="tl-auth-subtitle">{{ subtitle }}</p>

            {{ body_html|safe }}
        </div>
    </div>
    """, heading=heading, subtitle=subtitle, body_html=body_html)
    return render_public_page(content, title)


@auth_bp.route("/")
def home():
    if session.get("user_id"):
        return redirect(url_for("dashboard.dashboard"))
    return redirect(url_for("auth.login"))


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if session.get("user_id"):
        return redirect(url_for("dashboard.dashboard"))

    if request.method == "POST":
        company_name = (request.form.get("company_name") or "").strip()
        user_name = (request.form.get("user_name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        if not company_name or not user_name or not email or not password:
            flash("All fields are required.")
            return redirect(url_for("auth.register"))

        if not _check_password_strength(password):
            flash("Password must be 8+ chars, include upper, lower, and number.")
            return redirect(url_for("auth.register"))

        conn = get_db_connection()
        company_id = None

        try:
            exists = conn.execute(
                "SELECT id FROM users WHERE email = %s",
                (email,),
            ).fetchone()

            if exists:
                flash("That email is already in use.")
                return redirect(url_for("auth.register"))

            cur = conn.cursor()
            cur.execute(
                "INSERT INTO companies (name) VALUES (%s) RETURNING id",
                (company_name,),
            )
            row = cur.fetchone()
            company_id = row["id"] if row else None
            conn.commit()
        finally:
            conn.close()

        if not company_id:
            flash("Could not create company.")
            return redirect(url_for("auth.register"))

        user_id = create_owner_user(
            company_id=company_id,
            name=user_name,
            email=email,
            password_hash=generate_password_hash(password),
        )

        conn = get_db_connection()
        try:
            user = conn.execute(
                """
                SELECT u.*, c.name AS company_name
                FROM users u
                JOIN companies c ON u.company_id = c.id
                WHERE u.id = %s
                """,
                (user_id,),
            ).fetchone()
        finally:
            conn.close()

        if not user:
            flash("Account created, but login could not be completed.")
            return redirect(url_for("auth.login"))

        _set_logged_in_session(user, email)
        _reset_login_attempts()

        flash("Account created.")
        return redirect(url_for("dashboard.dashboard"))

    body_html = render_template_string("""
    <form method="post" class="tl-auth-form">
        {{ csrf_input() }}

        <div class="tl-auth-grid">
            <div class="tl-auth-field">
                <label class="tl-auth-label">Company Name</label>
                <input class="tl-auth-input" name="company_name" required>
            </div>

            <div class="tl-auth-field">
                <label class="tl-auth-label">Your Name</label>
                <input class="tl-auth-input" name="user_name" required>
            </div>

            <div class="tl-auth-field">
                <label class="tl-auth-label">Email</label>
                <input class="tl-auth-input" type="email" name="email" required>
            </div>

            <div class="tl-auth-field">
                <label class="tl-auth-label">Password</label>
                <input class="tl-auth-input" type="password" name="password" required>
            </div>
        </div>

        <div class="tl-auth-actions">
            <button class="tl-auth-btn tl-auth-btn-primary" type="submit">Create Account</button>
            <a class="tl-auth-btn tl-auth-btn-secondary" href="{{ url_for('auth.login') }}">Back to Login</a>
        </div>
    </form>
    """, csrf_input=csrf_input)
    return _render_auth_page(
        title="Register",
        heading="Create Account",
        subtitle="Create your TerraLedger workspace and owner account.",
        body_html=body_html,
    )


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if session.get("user_id"):
        return redirect(url_for("dashboard.dashboard"))

    if request.method == "POST":
        if _too_many_attempts():
            flash("Too many failed attempts. Try again later.")
            return redirect(url_for("auth.login"))

        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        conn = get_db_connection()
        try:
            user = conn.execute(
                """
                SELECT u.*, c.name AS company_name
                FROM users u
                JOIN companies c ON u.company_id = c.id
                WHERE u.email = %s
                """,
                (email,),
            ).fetchone()
        finally:
            conn.close()

        if not user or not check_password_hash(user["password_hash"], password):
            _increment_login_attempts()
            flash("Invalid email or password.")
            return redirect(url_for("auth.login"))

        if "is_active" in user.keys() and int(user["is_active"] or 1) != 1:
            flash("This user account is inactive.")
            return redirect(url_for("auth.login"))

        _set_logged_in_session(user, email)

        _reset_login_attempts()
        return redirect(url_for("dashboard.dashboard"))

    body_html = render_template_string("""
    <form method="post" class="tl-auth-form">
        {{ csrf_input() }}

        <div class="tl-auth-grid tl-auth-grid-single">
            <div class="tl-auth-field">
                <label class="tl-auth-label">Email</label>
                <input class="tl-auth-input" type="email" name="email" required>
            </div>

            <div class="tl-auth-field">
                <label class="tl-auth-label">Password</label>
                <input class="tl-auth-input" type="password" name="password" required>
            </div>
        </div>

        <div class="tl-auth-actions">
            <button class="tl-auth-btn tl-auth-btn-primary" type="submit">Login</button>
            <a class="tl-auth-btn tl-auth-btn-secondary" href="{{ url_for('auth.register') }}">Create Account</a>
        </div>

        <div class="tl-auth-links">
            <a href="{{ url_for('auth.forgot_password') }}">Forgot Password?</a>
        </div>
    </form>
    """, csrf_input=csrf_input)
    return _render_auth_page(
        title="Login",
        heading="Login",
        subtitle="Sign in to access your TerraLedger workspace.",
        body_html=body_html,
    )


@auth_bp.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    ensure_password_reset_table()
    conn = get_db_connection()

    try:
        if request.method == "POST":
            email = (request.form.get("email") or "").strip().lower()

            if not email:
                flash("Enter your email.")
                return redirect(url_for("auth.forgot_password"))

            user = conn.execute(
                "SELECT id, company_id FROM users WHERE email = %s",
                (email,),
            ).fetchone()

            if user:
                conn.execute(
                    "DELETE FROM password_resets WHERE email = %s",
                    (email,),
                )

                token = secrets.token_urlsafe(32)
                expires = datetime.utcnow() + timedelta(hours=1)

                conn.execute(
                    "INSERT INTO password_resets (email, token, expires_at) VALUES (%s, %s, %s)",
                    (email, token, expires),
                )
                conn.commit()

                reset_link = url_for("auth.reset_password", token=token, _external=True)

                send_company_email(
                    to_email=email,
                    subject="Reset Your TerraLedger Password",
                    html=f"<a href='{reset_link}'>Reset Password</a>",
                    body=reset_link,
                    company_id=user["company_id"],
                )

            flash("If that email exists, a reset link has been sent.")
            return redirect(url_for("auth.login"))
    finally:
        conn.close()

    body_html = render_template_string("""
    <form method="post" class="tl-auth-form">
        {{ csrf_input() }}

        <div class="tl-auth-grid tl-auth-grid-single">
            <div class="tl-auth-field">
                <label class="tl-auth-label">Email</label>
                <input class="tl-auth-input" type="email" name="email" required>
            </div>
        </div>

        <div class="tl-auth-actions">
            <button class="tl-auth-btn tl-auth-btn-primary" type="submit">Send Reset Link</button>
            <a class="tl-auth-btn tl-auth-btn-secondary" href="{{ url_for('auth.login') }}">Back to Login</a>
        </div>
    </form>
    """, csrf_input=csrf_input)
    return _render_auth_page(
        title="Forgot Password",
        heading="Forgot Password",
        subtitle="Enter your email and we’ll send you a password reset link.",
        body_html=body_html,
    )


@auth_bp.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    ensure_password_reset_table()
    conn = get_db_connection()

    try:
        row = conn.execute(
            "SELECT * FROM password_resets WHERE token = %s",
            (token,),
        ).fetchone()

        if not row:
            flash("Invalid or expired link.")
            return redirect(url_for("auth.login"))

        if row["expires_at"] < datetime.utcnow():
            conn.execute(
                "DELETE FROM password_resets WHERE token = %s",
                (token,),
            )
            conn.commit()
            flash("Expired link.")
            return redirect(url_for("auth.login"))

        if request.method == "POST":
            password = request.form.get("password") or ""

            if not _check_password_strength(password):
                flash("Password must be 8+ chars, include upper, lower, and number.")
                return redirect(url_for("auth.reset_password", token=token))

            conn.execute(
                "UPDATE users SET password_hash = %s WHERE email = %s",
                (generate_password_hash(password), row["email"]),
            )

            conn.execute(
                "DELETE FROM password_resets WHERE email = %s",
                (row["email"],),
            )

            conn.commit()

            flash("Password reset successful.")
            return redirect(url_for("auth.login"))
    finally:
        conn.close()

    body_html = render_template_string("""
    <form method="post" class="tl-auth-form">
        {{ csrf_input() }}

        <div class="tl-auth-grid tl-auth-grid-single">
            <div class="tl-auth-field">
                <label class="tl-auth-label">New Password</label>
                <input class="tl-auth-input" type="password" name="password" required>
            </div>
        </div>

        <div class="tl-auth-actions">
            <button class="tl-auth-btn tl-auth-btn-primary" type="submit">Reset Password</button>
            <a class="tl-auth-btn tl-auth-btn-secondary" href="{{ url_for('auth.login') }}">Back to Login</a>
        </div>
    </form>
    """, csrf_input=csrf_input)
    return _render_auth_page(
        title="Reset Password",
        heading="Reset Password",
        subtitle="Choose a new password for your TerraLedger account.",
        body_html=body_html,
    )


@auth_bp.route("/logout")
def logout():
    session.clear()
    flash("Logged out.")
    return redirect(url_for("auth.login"))