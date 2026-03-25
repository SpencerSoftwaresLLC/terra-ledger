from flask import Blueprint, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash, check_password_hash

from db import get_db_connection, create_owner_user
from page_helpers import render_public_page

auth_bp = Blueprint("auth", __name__)


def _auth_styles():
    return """
    <style>
    .auth-panel{
        width:100%;
        max-width:520px;
        margin:0 auto;
    }

    .auth-logo-wrap{
        display:flex;
        align-items:center;
        gap:12px;
        margin-bottom:18px;
    }

    .auth-logo{
        width:48px;
        height:48px;
        object-fit:contain;
        border-radius:12px;
        background: rgba(255,255,255,0.10);
        padding:4px;
        flex:0 0 auto;
    }

    .auth-logo-text{
        font-size:22px;
        font-weight:800;
        color:#f7f3ea;
        line-height:1.1;
    }

    .auth-panel h1{
        margin:0 0 8px 0;
        font-size:34px;
        color:#f7f3ea;
        line-height:1.1;
    }

    .auth-subtext{
        margin:0 0 24px 0;
        color:#d6c2a8;
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
        color:#f7f3ea;
        margin-bottom:6px;
    }

    .auth-grid input{
        width:100%;
        box-sizing:border-box;
        border:1px solid #314756;
        background:#182833;
        color:#f7f3ea;
        border-radius:12px;
        padding:12px 14px;
        font-size:15px;
    }

    .auth-grid input:focus{
        outline:none;
        border-color:#6bbf72;
        box-shadow:0 0 0 3px rgba(107,191,114,0.20);
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

    .auth-actions .btn.secondary{
        background:#223746;
        color:#f7f3ea;
        border:1px solid #314756;
    }

    .auth-actions .btn.secondary:hover{
        background:#2a4253;
    }

    @media (max-width: 640px){
        .auth-actions{
            flex-direction:column;
        }

        .auth-actions .btn,
        .auth-actions .btn.secondary{
            width:100%;
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

    logo_src = url_for("static", filename="images/logo.png")

    content = f"""
    <div class="auth-panel">
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

    {_auth_styles()}
    """
    return render_public_page(content, "Login")


@auth_bp.route("/logout")
def logout():
    session.clear()
    flash("Logged out.")
    return redirect(url_for("auth.login"))