from flask import Blueprint, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash, check_password_hash
import secrets
from datetime import datetime, timedelta

from db import get_db_connection, create_owner_user, ensure_password_reset_table
from page_helpers import render_public_page
from utils.emailing import send_company_email

auth_bp = Blueprint("auth", __name__)

# 🔒 SIMPLE LOGIN ATTEMPT TRACKER (per session)
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


# =========================
# HOME
# =========================
@auth_bp.route("/")
def home():
    if session.get("user_id"):
        return redirect(url_for("dashboard.dashboard"))
    return redirect(url_for("auth.login"))


# =========================
# REGISTER
# =========================
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

        exists = conn.execute(
            "SELECT id FROM users WHERE email = %s",
            (email,),
        ).fetchone()

        if exists:
            conn.close()
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

        # 🔒 SESSION FIXATION PROTECTION
        session.clear()

        session["user_id"] = user_id
        session["user_name"] = user_name
        session["user_email"] = email
        session["company_id"] = company_id
        session["company_name"] = company_name

        _reset_login_attempts()

        flash("Account created.")
        return redirect(url_for("dashboard.dashboard"))

    content = f"""
    <form method="post">
        <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}">
        <input name="company_name" required>
        <input name="user_name" required>
        <input type="email" name="email" required>
        <input type="password" name="password" required>
        <button>Create</button>
    </form>
    """
    return render_public_page(content, "Register")


# =========================
# LOGIN
# =========================
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

        user = conn.execute(
            """
            SELECT u.*, c.name AS company_name
            FROM users u
            JOIN companies c ON u.company_id = c.id
            WHERE u.email = %s
            """,
            (email,),
        ).fetchone()

        conn.close()

        if not user or not check_password_hash(user["password_hash"], password):
            _increment_login_attempts()
            flash("Invalid email or password.")
            return redirect(url_for("auth.login"))

        if "is_active" in user.keys() and int(user["is_active"] or 1) != 1:
            flash("This user account is inactive.")
            return redirect(url_for("auth.login"))

        # 🔒 SESSION FIXATION PROTECTION
        session.clear()

        session["user_id"] = user["id"]
        session["user_name"] = user["name"]
        session["user_email"] = email
        session["company_id"] = user["company_id"]
        session["company_name"] = user["company_name"]

        _reset_login_attempts()

        return redirect(url_for("dashboard.dashboard"))

    content = """
    <form method="post">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
        <input type="email" name="email" required>
        <input type="password" name="password" required>
        <button>Login</button>
    </form>
    """
    return render_public_page(content, "Login")


# =========================
# FORGOT PASSWORD
# =========================
@auth_bp.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    ensure_password_reset_table()

    conn = get_db_connection()

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()

        if not email:
            conn.close()
            flash("Enter your email.")
            return redirect(url_for("auth.forgot_password"))

        user = conn.execute(
            "SELECT id, company_id FROM users WHERE email = %s",
            (email,),
        ).fetchone()

        if user:
            # 🔒 DELETE OLD TOKENS FIRST
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

        conn.close()

        flash("If that email exists, a reset link has been sent.")
        return redirect(url_for("auth.login"))

    content = """
    <form method="post">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
        <input type="email" name="email" required>
        <button>Send</button>
    </form>
    """
    return render_public_page(content, "Forgot Password")


# =========================
# RESET PASSWORD
# =========================
@auth_bp.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    ensure_password_reset_table()

    conn = get_db_connection()

    row = conn.execute(
        "SELECT * FROM password_resets WHERE token = %s",
        (token,),
    ).fetchone()

    if not row:
        conn.close()
        flash("Invalid or expired link.")
        return redirect(url_for("auth.login"))

    if row["expires_at"] < datetime.utcnow():
        conn.execute(
            "DELETE FROM password_resets WHERE token = %s",
            (token,),
        )
        conn.commit()
        conn.close()
        flash("Expired link.")
        return redirect(url_for("auth.login"))

    if request.method == "POST":
        password = request.form.get("password") or ""

        if not _check_password_strength(password):
            flash("Password must be strong.")
            return redirect(url_for("auth.reset_password", token=token))

        conn.execute(
            "UPDATE users SET password_hash = %s WHERE email = %s",
            (generate_password_hash(password), row["email"]),
        )

        # 🔒 DELETE ALL TOKENS FOR THIS USER
        conn.execute(
            "DELETE FROM password_resets WHERE email = %s",
            (row["email"],),
        )

        conn.commit()
        conn.close()

        flash("Password reset successful.")
        return redirect(url_for("auth.login"))

    conn.close()

    content = """
    <form method="post">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
        <input type="password" name="password" required>
        <button>Reset</button>
    </form>
    """
    return render_public_page(content, "Reset Password")


# =========================
# LOGOUT
# =========================
@auth_bp.route("/logout")
def logout():
    session.clear()
    flash("Logged out.")
    return redirect(url_for("auth.login"))