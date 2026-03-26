from flask import Blueprint, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash, check_password_hash
import secrets
from datetime import datetime, timedelta

from db import get_db_connection, create_owner_user
from page_helpers import render_public_page
from utils.emailing import send_company_email

auth_bp = Blueprint("auth", __name__)


def _auth_styles():
    return """
    <style>
    .auth-panel{
        width:100%;
        max-width:560px;
        margin:0 auto;
    }

    .auth-card{
        width:100%;
        background: rgba(255,255,255,0.96);
        border:1px solid #d8e2d0;
        border-radius:18px;
        box-shadow: 0 16px 42px rgba(47, 79, 31, 0.12);
        padding:28px;
        position:relative;
        overflow:hidden;
    }

    .auth-card::before{
        content:"";
        position:absolute;
        inset:0 0 auto 0;
        height:6px;
        background: linear-gradient(90deg, #2f4f1f, #4f7f2b, #f08c4a);
    }

    .auth-logo-wrap{
        display:flex;
        align-items:center;
        gap:16px;
        margin-bottom:20px;
    }

    .auth-logo{
        width:64px;
        height:64px;
        object-fit:contain;
        border-radius:14px;
        background: rgba(255,255,255,0.9);
        padding:6px;
        border:1px solid rgba(47,79,31,0.08);
        flex:0 0 auto;
    }

    .auth-logo-text{
        font-size:24px;
        font-weight:900;
        color:#2f4f1f;
    }

    .auth-panel h1{
        margin:0 0 8px 0;
        font-size:34px;
        color:#2f4f1f;
        line-height:1.1;
    }

    .auth-subtext{
        margin:0 0 24px 0;
        color:#5b6470;
        line-height:1.6;
    }

    .auth-grid{
        display:grid;
        gap:16px;
    }

    .auth-grid label{
        display:block;
        font-size:14px;
        font-weight:700;
        color:#2f4f1f;
        margin-bottom:6px;
    }

    .auth-grid input{
        width:100%;
        box-sizing:border-box;
        border:1px solid #c9d5c0;
        background:#ffffff;
        color:#1f2933;
        border-radius:12px;
        padding:12px 14px;
        font-size:15px;
    }

    .auth-grid input:focus{
        outline:none;
        border-color:#4f7f2b;
        box-shadow:0 0 0 3px rgba(79,127,43,0.15);
    }

    .auth-actions{
        display:flex;
        gap:12px;
        flex-wrap:wrap;
        margin-top:22px;
    }

    .auth-actions .btn{
        min-width:140px;
        justify-content:center;
    }

    .btn{
        display:inline-flex;
        align-items:center;
        justify-content:center;
        background:#4f7f2b;
        color:#ffffff;
        border:none;
        padding:12px 16px;
        border-radius:12px;
        font-weight:800;
        cursor:pointer;
        box-shadow: 0 4px 10px rgba(79,127,43,0.18);
    }

    .btn:hover{
        background:#2f4f1f;
    }

    .btn.secondary{
        background:#6b4f2a;
        color:#ffffff;
    }

    .btn.secondary:hover{
        background:#4a3720;
    }

    .btn.secondary{
        background:#6b4f2a;
        color:#ffffff;
        border:1px solid #6b4f2a;
    }

    .btn.secondary:hover{
        background:#4a3720;
        border-color:#4a3720;
    }

    @media (max-width: 640px){
        .auth-card{
            padding:22px;
        }

        .auth-logo-wrap{
            align-items:flex-start;
        }

        .auth-actions{
            flex-direction:column;
        }

        .auth-actions .btn,
        .auth-actions .btn.secondary{
            width:100%;
        }

        .auth-panel h1{
            font-size:30px;
        }
    }
    </style>
    """


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

        conn = get_db_connection()

        exists = conn.execute(
            "SELECT id FROM users WHERE email = ?",
            (email,),
        ).fetchone()

        if exists:
            conn.close()
            flash("That email is already in use.")
            return redirect(url_for("auth.register"))

        cur = conn.cursor()
        cur.execute(
            "INSERT INTO companies (name) VALUES (?)",
            (company_name,),
        )
        company_id = cur.lastrowid

        conn.commit()
        conn.close()

        user_id = create_owner_user(
            company_id=company_id,
            name=user_name,
            email=email,
            password_hash=generate_password_hash(password),
        )

        session["user_id"] = user_id
        session["user_name"] = user_name
        session["user_email"] = email
        session["company_id"] = company_id
        session["company_name"] = company_name

        flash("Account created.")
        return redirect(url_for("dashboard.dashboard"))

    logo_src = "/static/images/logo.png"

    content = f"""
    <div class="auth-panel">
        <div class="auth-card">
            <div class="auth-logo-wrap">
                <img src="{logo_src}" class="auth-logo" alt="TerraLedger Logo">
                <div class="auth-logo-text">TerraLedger<sup style="font-size:10px;">™</sup></div>
            </div>

            <h1>Create Account</h1>
            <p class="auth-subtext">
                Create your company account to start using TerraLedger.
            </p>

            <form method="post">
                <div class="auth-grid">
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

                <div class="auth-actions">
                    <button class="btn" type="submit">Create Account</button>
                    <a class="btn secondary" href="/login">Login</a>
                </div>
            </form>
        </div>
    </div>

    {_auth_styles()}
    """
    return render_public_page(content, "Register")


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if session.get("user_id"):
        return redirect(url_for("dashboard.dashboard"))

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        conn = get_db_connection()

        user = conn.execute(
            """
            SELECT u.*, c.name AS company_name
            FROM users u
            JOIN companies c ON u.company_id = c.id
            WHERE u.email = ?
            """,
            (email,),
        ).fetchone()

        conn.close()

        if not user or not check_password_hash(user["password_hash"], password):
            flash("Invalid email or password.")
            return redirect(url_for("auth.login"))

        if "is_active" in user.keys() and int(user["is_active"] or 1) != 1:
            flash("This user account is inactive.")
            return redirect(url_for("auth.login"))

        session["user_id"] = user["id"]
        session["user_name"] = user["name"]
        session["user_email"] = email
        session["company_id"] = user["company_id"]
        session["company_name"] = user["company_name"]

        return redirect(url_for("dashboard.dashboard"))

    logo_src = url_for("static", filename="images/logo.png")

    content = f"""
    <div class="auth-panel">
        <div class="auth-card">
            <div class="auth-logo-wrap">
                <img src="{logo_src}" class="auth-logo" alt="TerraLedger Logo">
                <div class="auth-logo-text">TerraLedger<sup style="font-size:10px;">™</sup></div>
            </div>

            <h1>Login</h1>
            <p class="auth-subtext">
                Sign in to access your TerraLedger workspace.
            </p>

            <form method="post">
                <div class="auth-grid">
                    <div>
                        <label>Email</label>
                        <input type="email" name="email" required>
                    </div>
                    <div>
                        <label>Password</label>
                        <input type="password" name="password" required>
                    </div>
                </div>

                <div class="auth-actions">
                    <button class="btn" type="submit">Login</button>
                    <a class="btn secondary" href="/register">Create Account</a>
                </div>
            </form>
        </div>
    </div>

    {_auth_styles()}
    """
    return render_public_page(content, "Login")

@auth_bp.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()

        if not email:
            flash("Enter your email.")
            return redirect(url_for("auth.forgot_password"))

        conn = get_db_connection()

        user = conn.execute(
            "SELECT id FROM users WHERE email = %s",
            (email,),
        ).fetchone()

        if user:
            token = secrets.token_urlsafe(32)
            expires = datetime.utcnow() + timedelta(hours=1)

            conn.execute(
                "INSERT INTO password_resets (email, token, expires_at) VALUES (%s, %s, %s)",
                (email, token, expires),
            )
            conn.commit()

            reset_link = url_for("auth.reset_password", token=token, _external=True)

            try:
                send_company_email(
                    to_email=email,
                    subject="Reset Your TerraLedger Password",
                    html_body=f"""
                        Click below to reset your password:<br><br>
                        <a href="{reset_link}">{reset_link}</a><br><br>
                        This link expires in 1 hour.
                    """,
                    text_body=f"Reset your password: {reset_link}",
                )
            except Exception as e:
                flash(f"Email failed: {e}")

        conn.close()

        flash("If that email exists, a reset link has been sent.")
        return redirect(url_for("auth.login"))

    content = """
    <div class="auth-panel">
        <div class="auth-card">
            <h1>Forgot Password</h1>
            <p class="auth-subtext">Enter your email to reset your password.</p>

            <form method="post">
                <div class="auth-grid">
                    <div>
                        <label>Email</label>
                        <input type="email" name="email" required>
                    </div>
                </div>

                <div class="auth-actions">
                    <button class="btn">Send Reset Link</button>
                    <a class="btn secondary" href="/login">Back to Login</a>
                </div>
            </form>
        </div>
    </div>
    """
    return render_public_page(content, "Forgot Password")

@auth_bp.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    conn = get_db_connection()

    row = conn.execute(
        "SELECT * FROM password_resets WHERE token = %s",
        (token,),
    ).fetchone()

    if not row or row["expires_at"] < datetime.utcnow():
        conn.close()
        flash("Invalid or expired reset link.")
        return redirect(url_for("auth.login"))

    if request.method == "POST":
        password = request.form.get("password") or ""

        if not password:
            flash("Password required.")
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
        conn.close()

        flash("Password reset successful. Please login.")
        return redirect(url_for("auth.login"))

    conn.close()

    content = """
    <div class="auth-panel">
        <div class="auth-card">
            <h1>Reset Password</h1>

            <form method="post">
                <div class="auth-grid">
                    <div>
                        <label>New Password</label>
                        <input type="password" name="password" required>
                    </div>
                </div>

                <div class="auth-actions">
                    <button class="btn">Reset Password</button>
                </div>
            </form>
        </div>
    </div>
    """
    return render_public_page(content, "Reset Password")


@auth_bp.route("/logout")
def logout():
    session.clear()
    flash("Logged out.")
    return redirect(url_for("auth.login"))