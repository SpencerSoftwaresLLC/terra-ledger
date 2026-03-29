from flask import Blueprint, request, redirect, url_for, session, flash, render_template_string
from werkzeug.security import generate_password_hash, check_password_hash
import secrets
from datetime import datetime, timedelta

from db import get_db_connection, create_owner_user, ensure_password_reset_table
from page_helpers import render_public_page
from utils.emailing import send_company_email

auth_bp = Blueprint("auth", __name__)

MAX_LOGIN_ATTEMPTS = 5

def _get_csrf_token():
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_hex(32)
        session["_csrf_token"] = token
    return token


def csrf_input():
    return f'<input type="hidden" name="csrf_token" value="{_get_csrf_token()}">'

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

        session.clear()
        session["user_id"] = user_id
        session["user_name"] = user_name
        session["user_email"] = email
        session["company_id"] = company_id
        session["company_name"] = company_name

        _reset_login_attempts()

        flash("Account created.")
        return redirect(url_for("dashboard.dashboard"))

    content = render_template_string("""
    <div class="card">
        <h1>Create Account</h1>
        <form method="post">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">

            <div class="grid">
                <div>
                    <label>Company Name</label>
                    <input name="company_name" required>
                </div>

                <div>
                    <label>Your Name</label>
                    <input name="user_name" required>
                </div>

                <div>
                    <label>Email</label>
                    <input type="email" name="email" required>
                </div>

                <div>
                    <label>Password</label>
                    <input type="password" name="password" required>
                </div>
            </div>

            <div class="row-actions" style="margin-top:16px;">
                <button class="btn" type="submit">Create Account</button>
                <a class="btn secondary" href="{{ url_for('auth.login') }}">Back to Login</a>
            </div>
        </form>
    </div>
    """)
    return render_public_page(content, "Register")


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

        session.clear()
        session["user_id"] = user["id"]
        session["user_name"] = user["name"]
        session["user_email"] = email
        session["company_id"] = user["company_id"]
        session["company_name"] = user["company_name"]

        _reset_login_attempts()

        return redirect(url_for("dashboard.dashboard"))

    content = render_template_string("""
    <div class="card">
        <h1>Login</h1>
        <form method="post">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">

            <div class="grid">
                <div>
                    <label>Email</label>
                    <input type="email" name="email" required>
                </div>

                <div>
                    <label>Password</label>
                    <input type="password" name="password" required>
                </div>
            </div>

            <div class="row-actions" style="margin-top:16px;">
                <button class="btn" type="submit">Login</button>
                <a class="btn secondary" href="{{ url_for('auth.forgot_password') }}">Forgot Password</a>
            </div>

            <div style="margin-top:12px;">
                <a href="{{ url_for('auth.register') }}">Create an account</a>
            </div>
        </form>
    </div>
    """)
    return render_public_page(content, "Login")


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

    content = render_template_string("""
    <div class="card">
        <h1>Forgot Password</h1>
        <form method="post">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">

            <div class="grid">
                <div>
                    <label>Email</label>
                    <input type="email" name="email" required>
                </div>
            </div>

            <div class="row-actions" style="margin-top:16px;">
                <button class="btn" type="submit">Send Reset Link</button>
                <a class="btn secondary" href="{{ url_for('auth.login') }}">Back to Login</a>
            </div>
        </form>
    </div>
    """)
    return render_public_page(content, "Forgot Password")


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

    content = render_template_string("""
    <div class="card">
        <h1>Reset Password</h1>
        <form method="post">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">

            <div class="grid">
                <div>
                    <label>New Password</label>
                    <input type="password" name="password" required>
                </div>
            </div>

            <div class="row-actions" style="margin-top:16px;">
                <button class="btn" type="submit">Reset Password</button>
                <a class="btn secondary" href="{{ url_for('auth.login') }}">Back to Login</a>
            </div>
        </form>
    </div>
    """)
    return render_public_page(content, "Reset Password")


@auth_bp.route("/logout")
def logout():
    session.clear()
    flash("Logged out.")
    return redirect(url_for("auth.login"))