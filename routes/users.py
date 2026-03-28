from flask import Blueprint, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash
from html import escape

from db import get_db_connection, ensure_user_permission_columns
from decorators import login_required, require_permission, subscription_required
from permissions import get_role_defaults
from page_helpers import render_page

users_bp = Blueprint("users", __name__)


@users_bp.route("/users", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_users")
def users():
    ensure_user_permission_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()
        role = (request.form.get("role") or "Employee").strip().title()

        if not name or not email or not password:
            conn.close()
            flash("Name, email, and password are required.")
            return redirect(url_for("users.users"))

        existing = conn.execute(
            """
            SELECT id
            FROM users
            WHERE email = ?
            """,
            (email,),
        ).fetchone()

        if existing:
            conn.close()
            flash("A user with that email already exists.")
            return redirect(url_for("users.users"))

        defaults = get_role_defaults(role)
        password_hash = generate_password_hash(password)

        conn.execute(
            """
            INSERT INTO users (
                company_id,
                name,
                email,
                password_hash,
                role,
                is_active,
                can_manage_users,
                can_view_payroll,
                can_manage_payroll,
                can_view_bookkeeping,
                can_manage_bookkeeping,
                can_manage_jobs,
                can_manage_customers,
                can_manage_invoices,
                can_manage_settings
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cid,
                name,
                email,
                password_hash,
                role,
                1,
                defaults["can_manage_users"],
                defaults["can_view_payroll"],
                defaults["can_manage_payroll"],
                defaults["can_view_bookkeeping"],
                defaults["can_manage_bookkeeping"],
                defaults["can_manage_jobs"],
                defaults["can_manage_customers"],
                defaults["can_manage_invoices"],
                defaults["can_manage_settings"],
            ),
        )

        conn.commit()
        conn.close()

        flash("User added.")
        return redirect(url_for("users.users"))

    rows = conn.execute(
        """
        SELECT *
        FROM users
        WHERE company_id = ?
        ORDER BY
            CASE
                WHEN role = 'Owner' THEN 1
                WHEN role = 'Manager' THEN 2
                ELSE 3
            END,
            is_active DESC,
            name ASC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    user_rows = "".join(
        f"""
        <tr>
            <td>{escape(r['name'] or '-')}</td>
            <td>{escape(r['email'] or '-')}</td>
            <td>{escape(r['role'] or '-')}</td>
            <td>{"Yes" if r["is_active"] else "No"}</td>
            <td style="white-space:nowrap;">
                <a class='btn secondary small' href='{url_for("users.edit_user_permissions", user_id=r["id"])}'>Permissions</a>
                {
                    f'''
                    <form method='post'
                          action='{url_for("users.toggle_user_active", user_id=r["id"])}'
                          style='display:inline;'
                          onsubmit="return confirm('Change this user\\'s active status?');">
                        {{{{ csrf_input() }}}}
                        <button class='btn warning small' type='submit'>{"Deactivate" if r["is_active"] else "Activate"}</button>
                    </form>
                    '''
                    if r["id"] != session.get("user_id")
                    else "<span class='muted small'>Current User</span>"
                }
                {
                    f'''
                    <form method='post'
                          action='{url_for("users.delete_user", user_id=r["id"])}'
                          style='display:inline;'
                          onsubmit="return confirm('Delete this user?');">
                        {{{{ csrf_input() }}}}
                        <button class='btn danger small' type='submit'>Delete</button>
                    </form>
                    '''
                    if r["id"] != session.get("user_id")
                    else ""
                }
            </td>
        </tr>
        """
        for r in rows
    )

    content = f"""
    <div class='card'>
        <h1>Users & Permissions</h1>
        <p class='muted'>Manage company users, roles, and access levels.</p>
    </div>

    <div class='card'>
        <h2>Add User</h2>
        <form method='post'>
            {{{{ csrf_input() }}}}
            <div class='grid'>
                <div>
                    <label>Name</label>
                    <input name='name' required>
                </div>
                <div>
                    <label>Email</label>
                    <input name='email' type='email' required>
                </div>
                <div>
                    <label>Password</label>
                    <input name='password' type='password' required>
                </div>
                <div>
                    <label>Role</label>
                    <select name='role'>
                        <option>Owner</option>
                        <option>Manager</option>
                        <option selected>Employee</option>
                    </select>
                </div>
            </div>
            <br>
            <button class='btn success' type='submit'>Add User</button>
            <a class='btn secondary' href='{url_for("settings.settings")}'>Back to Settings</a>
        </form>
    </div>

    <div class='card'>
        <h2>Company Users</h2>
        <table>
            <tr>
                <th>Name</th>
                <th>Email</th>
                <th>Role</th>
                <th>Active</th>
                <th>Actions</th>
            </tr>
            {user_rows or '<tr><td colspan="5" class="muted">No users found.</td></tr>'}
        </table>
    </div>
    """

    return render_page(content, "Users & Permissions")


@users_bp.route("/users/<int:user_id>/permissions", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_users")
def edit_user_permissions(user_id):
    ensure_user_permission_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    user = conn.execute(
        """
        SELECT *
        FROM users
        WHERE id = ? AND company_id = ?
        """,
        (user_id, cid),
    ).fetchone()

    if not user:
        conn.close()
        flash("User not found.")
        return redirect(url_for("users.users"))

    if request.method == "POST":
        role = (request.form.get("role") or user["role"] or "Employee").strip().title()

        can_manage_users = 1 if request.form.get("can_manage_users") else 0
        can_view_payroll = 1 if request.form.get("can_view_payroll") else 0
        can_manage_payroll = 1 if request.form.get("can_manage_payroll") else 0
        can_view_bookkeeping = 1 if request.form.get("can_view_bookkeeping") else 0
        can_manage_bookkeeping = 1 if request.form.get("can_manage_bookkeeping") else 0
        can_manage_jobs = 1 if request.form.get("can_manage_jobs") else 0
        can_manage_customers = 1 if request.form.get("can_manage_customers") else 0
        can_manage_invoices = 1 if request.form.get("can_manage_invoices") else 0
        can_manage_settings = 1 if request.form.get("can_manage_settings") else 0

        conn.execute(
            """
            UPDATE users
            SET role = ?,
                can_manage_users = ?,
                can_view_payroll = ?,
                can_manage_payroll = ?,
                can_view_bookkeeping = ?,
                can_manage_bookkeeping = ?,
                can_manage_jobs = ?,
                can_manage_customers = ?,
                can_manage_invoices = ?,
                can_manage_settings = ?
            WHERE id = ? AND company_id = ?
            """,
            (
                role,
                can_manage_users,
                can_view_payroll,
                can_manage_payroll,
                can_view_bookkeeping,
                can_manage_bookkeeping,
                can_manage_jobs,
                can_manage_customers,
                can_manage_invoices,
                can_manage_settings,
                user_id,
                cid,
            ),
        )

        conn.commit()
        conn.close()

        if user_id == session.get("user_id"):
            flash("Your permissions were updated. Log out and back in if anything looks out of sync.")
        else:
            flash("User permissions updated.")

        return redirect(url_for("users.users"))

    def checked(val):
        return "checked" if val else ""

    content = f"""
    <div class='card'>
        <h1>Edit Permissions</h1>
        <p class='muted'><strong>User:</strong> {escape(user['name'] or '-')} ({escape(user['email'] or '-')})</p>

        <form method='post'>
            {{{{ csrf_input() }}}}
            <div class='grid'>
                <div>
                    <label>Role</label>
                    <select name='role'>
                        <option {'selected' if (user['role'] or '') == 'Owner' else ''}>Owner</option>
                        <option {'selected' if (user['role'] or '') == 'Manager' else ''}>Manager</option>
                        <option {'selected' if (user['role'] or '') == 'Employee' else ''}>Employee</option>
                    </select>
                </div>
            </div>

            <br>

            <div class='card' style='padding:16px;'>
                <h3 style='margin-top:0;'>Permissions</h3>

                <div class='grid'>
                    <label><input type='checkbox' name='can_manage_users' {checked(user['can_manage_users'])}> Manage Users</label>
                    <label><input type='checkbox' name='can_view_payroll' {checked(user['can_view_payroll'])}> View Payroll</label>
                    <label><input type='checkbox' name='can_manage_payroll' {checked(user['can_manage_payroll'])}> Manage Payroll</label>
                    <label><input type='checkbox' name='can_view_bookkeeping' {checked(user['can_view_bookkeeping'])}> View Bookkeeping</label>
                    <label><input type='checkbox' name='can_manage_bookkeeping' {checked(user['can_manage_bookkeeping'])}> Manage Bookkeeping</label>
                    <label><input type='checkbox' name='can_manage_jobs' {checked(user['can_manage_jobs'])}> Manage Jobs</label>
                    <label><input type='checkbox' name='can_manage_customers' {checked(user['can_manage_customers'])}> Manage Customers</label>
                    <label><input type='checkbox' name='can_manage_invoices' {checked(user['can_manage_invoices'])}> Manage Invoices</label>
                    <label><input type='checkbox' name='can_manage_settings' {checked(user['can_manage_settings'])}> Manage Settings</label>
                </div>
            </div>

            <br>
            <button class='btn success' type='submit'>Save Permissions</button>
            <a class='btn secondary' href='{url_for("users.users")}'>Cancel</a>
        </form>
    </div>
    """

    conn.close()
    return render_page(content, f"Permissions - {user['name']}")


@users_bp.route("/users/<int:user_id>/toggle-active", methods=["POST"])
@login_required
@require_permission("can_manage_users")
def toggle_user_active(user_id):
    if user_id == session.get("user_id"):
        flash("You cannot deactivate your own account.")
        return redirect(url_for("users.users"))

    conn = get_db_connection()
    cid = session["company_id"]

    user = conn.execute(
        """
        SELECT id, is_active
        FROM users
        WHERE id = ? AND company_id = ?
        """,
        (user_id, cid),
    ).fetchone()

    if not user:
        conn.close()
        flash("User not found.")
        return redirect(url_for("users.users"))

    new_status = 0 if user["is_active"] else 1

    conn.execute(
        """
        UPDATE users
        SET is_active = ?
        WHERE id = ? AND company_id = ?
        """,
        (new_status, user_id, cid),
    )

    conn.commit()
    conn.close()

    flash("User status updated.")
    return redirect(url_for("users.users"))


@users_bp.route("/users/delete/<int:user_id>", methods=["POST"])
@login_required
@require_permission("can_manage_users")
def delete_user(user_id):
    if user_id == session["user_id"]:
        flash("You cannot delete your own account.")
        return redirect(url_for("users.users"))

    conn = get_db_connection()
    cid = session["company_id"]

    user = conn.execute(
        """
        SELECT id
        FROM users
        WHERE id = ? AND company_id = ?
        """,
        (user_id, cid),
    ).fetchone()

    if not user:
        conn.close()
        flash("User not found.")
        return redirect(url_for("users.users"))

    conn.execute(
        """
        DELETE FROM users
        WHERE id = ? AND company_id = ?
        """,
        (user_id, cid),
    )

    conn.commit()
    conn.close()

    flash("User deleted.")
    return redirect(url_for("users.users"))