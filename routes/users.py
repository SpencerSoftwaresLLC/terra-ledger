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
            WHERE email = %s
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        WHERE company_id = %s
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

    user_rows = ""
    mobile_cards = ""

    for r in rows:
        is_current_user = r["id"] == session.get("user_id")
        status_text = "Yes" if r["is_active"] else "No"
        status_badge_class = "active" if r["is_active"] else "inactive"

        permissions_button = (
            f"<a class='btn secondary small' href='{url_for('users.edit_user_permissions', user_id=r['id'])}'>Permissions</a>"
        )

        if not is_current_user:
            toggle_button = f"""
            <form method='post'
                  action='{url_for("users.toggle_user_active", user_id=r["id"])}'
                  class='inline-form'
                  onsubmit="return confirm('Change this user\\'s active status?');">
                <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}">
                <button class='btn warning small' type='submit'>{"Deactivate" if r["is_active"] else "Activate"}</button>
            </form>
            """
            delete_button = f"""
            <form method='post'
                  action='{url_for("users.delete_user", user_id=r["id"])}'
                  class='inline-form'
                  onsubmit="return confirm('Delete this user?');">
                <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}">
                <button class='btn danger small' type='submit'>Delete</button>
            </form>
            """
            current_user_text = ""
        else:
            toggle_button = ""
            delete_button = ""
            current_user_text = "<span class='muted small'>Current User</span>"

        user_rows += f"""
        <tr>
            <td>{escape(r['name'] or '-')}</td>
            <td>{escape(r['email'] or '-')}</td>
            <td>{escape(r['role'] or '-')}</td>
            <td><span class='status-pill {status_badge_class}'>{status_text}</span></td>
            <td>
                <div class='row-actions'>
                    {permissions_button}
                    {toggle_button}
                    {delete_button}
                    {current_user_text}
                </div>
            </td>
        </tr>
        """

        mobile_cards += f"""
        <div class='mobile-list-card'>
            <div class='mobile-list-top'>
                <div>
                    <div class='mobile-list-title'>{escape(r['name'] or '-')}</div>
                    <div class='mobile-list-subtitle'>{escape(r['email'] or '-')}</div>
                </div>
                <div class='mobile-badge {status_badge_class}'>{status_text}</div>
            </div>

            <div class='mobile-list-grid'>
                <div>
                    <span>Role</span>
                    <strong>{escape(r['role'] or '-')}</strong>
                </div>
                <div>
                    <span>Status</span>
                    <strong>{status_text}</strong>
                </div>
            </div>

            <div class='mobile-list-actions'>
                {permissions_button}
                {toggle_button}
                {delete_button}
                {current_user_text}
            </div>
        </div>
        """

    content = f"""
    <style>
        .users-page {{
            display: grid;
            gap: 18px;
        }}

        .desktop-only {{
            display: block;
        }}

        .mobile-only {{
            display: none;
        }}

        .table-wrap {{
            width: 100%;
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
        }}

        .status-pill {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: .85rem;
            font-weight: 700;
            white-space: nowrap;
            border: 1px solid rgba(15, 23, 42, 0.08);
            background: #f1f5f9;
            color: #334155;
        }}

        .status-pill.active {{
            background: #ecfdf3;
            color: #166534;
            border-color: rgba(22, 101, 52, 0.14);
        }}

        .status-pill.inactive {{
            background: #fef2f2;
            color: #991b1b;
            border-color: rgba(153, 27, 27, 0.14);
        }}

        .mobile-list {{
            display: grid;
            gap: 12px;
        }}

        .mobile-list-card {{
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 14px;
            padding: 14px;
            background: #fff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }}

        .mobile-list-top {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 10px;
            margin-bottom: 10px;
        }}

        .mobile-list-title {{
            font-weight: 700;
            color: #0f172a;
            line-height: 1.25;
            word-break: break-word;
            font-size: 1rem;
        }}

        .mobile-list-subtitle {{
            margin-top: 4px;
            font-size: .9rem;
            color: #64748b;
            line-height: 1.25;
            word-break: break-word;
        }}

        .mobile-badge {{
            font-size: .85rem;
            font-weight: 700;
            padding: 6px 10px;
            border-radius: 999px;
            white-space: nowrap;
            border: 1px solid rgba(15, 23, 42, 0.08);
            background: #f1f5f9;
            color: #334155;
        }}

        .mobile-badge.active {{
            background: #ecfdf3;
            color: #166534;
            border-color: rgba(22, 101, 52, 0.14);
        }}

        .mobile-badge.inactive {{
            background: #fef2f2;
            color: #991b1b;
            border-color: rgba(153, 27, 27, 0.14);
        }}

        .mobile-list-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px 12px;
            margin-bottom: 12px;
        }}

        .mobile-list-grid span {{
            display: block;
            font-size: .78rem;
            color: #64748b;
            margin-bottom: 3px;
        }}

        .mobile-list-grid strong {{
            display: block;
            color: #0f172a;
            font-size: .95rem;
            line-height: 1.25;
            word-break: break-word;
        }}

        .mobile-list-actions {{
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            align-items: center;
        }}

        @media (max-width: 640px) {{
            .desktop-only {{
                display: none !important;
            }}

            .mobile-only {{
                display: block !important;
            }}

            .mobile-list-grid {{
                grid-template-columns: 1fr;
            }}

            .mobile-list-actions .btn,
            .mobile-list-actions a,
            .mobile-list-actions button,
            .mobile-list-actions form {{
                flex: 1 1 auto;
            }}

            .mobile-list-actions .btn,
            .mobile-list-actions a.btn,
            .mobile-list-actions button {{
                width: 100%;
                text-align: center;
            }}

            .row-actions .btn,
            .row-actions a.btn,
            .row-actions button.btn {{
                width: auto;
            }}
        }}
    </style>

    <div class='users-page'>
        <div class='card'>
            <h1>Users & Permissions</h1>
            <p class='muted'>Manage company users, roles, and access levels.</p>
        </div>

        <div class='card'>
            <h2>Add User</h2>
            <form method='post'>
                <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}">
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

                <div class='row-actions' style='margin-top:20px;'>
                    <button class='btn success' type='submit'>Add User</button>
                    <a class='btn secondary' href='{url_for("settings.settings")}'>Back to Settings</a>
                </div>
            </form>
        </div>

        <div class='card'>
            <h2>Company Users</h2>

            <div class='table-wrap desktop-only'>
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

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {mobile_cards or "<div class='mobile-list-card muted'>No users found.</div>"}
                </div>
            </div>
        </div>
    </div>
    """

    return render_page(content, "Users & Permissions")


@users_bp.route("/users/<int:user_id>/permissions", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_users")
def edit_user_permissions(user_id):
    ensure_user_permission_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    user = conn.execute(
        """
        SELECT *
        FROM users
        WHERE id = %s AND company_id = %s
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
            SET role = %s,
                can_manage_users = %s,
                can_view_payroll = %s,
                can_manage_payroll = %s,
                can_view_bookkeeping = %s,
                can_manage_bookkeeping = %s,
                can_manage_jobs = %s,
                can_manage_customers = %s,
                can_manage_invoices = %s,
                can_manage_settings = %s
            WHERE id = %s AND company_id = %s
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
            <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}">
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
@subscription_required
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
        WHERE id = %s AND company_id = %s
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
        SET is_active = %s
        WHERE id = %s AND company_id = %s
        """,
        (new_status, user_id, cid),
    )

    conn.commit()
    conn.close()

    flash("User status updated.")
    return redirect(url_for("users.users"))


@users_bp.route("/users/delete/<int:user_id>", methods=["POST"])
@login_required
@subscription_required
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
        WHERE id = %s AND company_id = %s
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
        WHERE id = %s AND company_id = %s
        """,
        (user_id, cid),
    )

    conn.commit()
    conn.close()

    flash("User deleted.")
    return redirect(url_for("users.users"))