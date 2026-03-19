from flask import Blueprint, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash, check_password_hash

from ..db import get_db_connection, create_owner_user
from ..page_helpers import render_page

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/")
def home():
    if session.get("user_id"):
        return redirect(url_for("dashboard.dashboard"))
    return redirect(url_for("auth.login"))


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
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
        session["company_id"] = company_id
        session["company_name"] = company_name

        flash("Account created.")
        return redirect(url_for("dashboard.dashboard"))

    content = """
    <div class='card' style='max-width:520px; margin:40px auto;'>
        <h1>Create Account</h1>
        <form method='post'>
            <div class='grid'>
                <div><label>Company Name</label><input name='company_name' required></div>
                <div><label>Your Name</label><input name='user_name' required></div>
                <div><label>Email</label><input type='email' name='email' required></div>
                <div><label>Password</label><input type='password' name='password' required></div>
            </div>
            <br><button class='btn'>Create Account</button>
            <a class='btn secondary' href='/login'>Login</a>
        </form>
    </div>
    """
    return render_page(content, "Register")


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
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
        session["company_id"] = user["company_id"]
        session["company_name"] = user["company_name"]

        return redirect(url_for("dashboard.dashboard"))

    content = """
    <div class='card' style='max-width:520px; margin:40px auto;'>
        <h1>Login</h1>
        <form method='post'>
            <div class='grid'>
                <div><label>Email</label><input type='email' name='email' required></div>
                <div><label>Password</label><input type='password' name='password' required></div>
            </div>
            <br><button class='btn'>Login</button>
            <a class='btn secondary' href='/register'>Create Account</a>
        </form>
    </div>
    """
    return render_page(content, "Login")


@auth_bp.route("/logout")
def logout():
    session.clear()
    flash("Logged out.")
    return redirect(url_for("auth.login"))