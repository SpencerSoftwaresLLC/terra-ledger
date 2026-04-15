from flask import Blueprint, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash
from html import escape

from db import get_db_connection, ensure_user_permission_columns
from decorators import login_required, require_permission, subscription_required
from permissions import get_role_defaults
from page_helpers import render_page

users_bp = Blueprint("users", __name__)


# =========================================================
# Language Helpers
# =========================================================

def _lang():
    return "es" if session.get("language") == "es" else "en"


def _is_es():
    return _lang() == "es"


def _t(en, es):
    return es if _is_es() else en


def _role_label(role):
    raw = (role or "").strip()
    if _is_es():
        if raw == "Owner":
            return "Propietario"
        if raw == "Manager":
            return "Gerente"
        if raw == "Employee":
            return "Empleado"
    return raw or "-"


def sync_missing_user_permissions_for_company(company_id):
    ensure_user_permission_columns()

    conn = get_db_connection()
    updated = 0

    permission_fields = [
        "can_manage_users",
        "can_view_payroll",
        "can_manage_payroll",
        "can_view_bookkeeping",
        "can_manage_bookkeeping",
        "can_manage_jobs",
        "can_manage_customers",
        "can_manage_invoices",
        "can_manage_settings",
        "can_manage_employees",
        "can_manage_messages",
        "can_manage_payments",
        "can_view_calendar",
    ]

    try:
        users = conn.execute(
            "SELECT * FROM users WHERE company_id = %s",
            (company_id,),
        ).fetchall()

        for user in users:
            role = (user["role"] or "Employee").strip().title()
            defaults = get_role_defaults(role)

            update_parts = []
            params = []

            user_keys = set(user.keys()) if hasattr(user, "keys") else set()

            for field in permission_fields:
                current_value = user[field] if field in user_keys else None
                if current_value is None:
                    update_parts.append(f"{field} = %s")
                    params.append(int(defaults.get(field, 0)))

            if update_parts:
                params.append(user["id"])
                conn.execute(
                    f"""
                    UPDATE users
                    SET {", ".join(update_parts)}
                    WHERE id = %s
                    """,
                    tuple(params),
                )
                updated += 1

        conn.commit()
        return updated
    finally:
        conn.close()


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
            flash(_t(
                "Name, email, and password are required.",
                "Se requieren nombre, correo electrónico y contraseña.",
            ))
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
            flash(_t(
                "A user with that email already exists.",
                "Ya existe un usuario con ese correo electrónico.",
            ))
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
                can_manage_settings,
                can_manage_employees,
                can_manage_messages,
                can_manage_payments,
                can_view_calendar
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                defaults["can_manage_employees"],
                defaults["can_manage_messages"],
                defaults["can_manage_payments"],
                defaults["can_view_calendar"],
            ),
        )

        conn.commit()
        conn.close()

        flash(_t("User added.", "Usuario agregado."))
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

    permissions_label = _t("Permissions", "Permisos")
    current_user_label = _t("Current User", "Usuario actual")
    deactivate_label = _t("Deactivate", "Desactivar")
    activate_label = _t("Activate", "Activar")
    delete_label = _t("Delete", "Eliminar")
    role_label = _t("Role", "Rol")
    status_label = _t("Status", "Estado")
    yes_label = _t("Yes", "Sí")
    no_label = _t("No", "No")

    confirm_toggle = _t(
        "Change this user's active status?",
        "¿Cambiar el estado activo de este usuario?",
    )
    confirm_delete = _t(
        "Delete this user?",
        "¿Eliminar este usuario?",
    )

    for r in rows:
        is_current_user = r["id"] == session.get("user_id")
        status_text = yes_label if r["is_active"] else no_label
        status_badge_class = "active" if r["is_active"] else "inactive"

        permissions_button = (
            f"<a class='btn secondary small' href='{url_for('users.edit_user_permissions', user_id=r['id'])}'>{permissions_label}</a>"
        )

        if not is_current_user:
            toggle_button = f"""
            <form method='post'
                  action='{url_for("users.toggle_user_active", user_id=r["id"])}'
                  class='inline-form'
                  onsubmit="return confirm({confirm_toggle!r});">
                <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}">
                <button class='btn warning small' type='submit'>{deactivate_label if r["is_active"] else activate_label}</button>
            </form>
            """
            delete_button = f"""
            <form method='post'
                  action='{url_for("users.delete_user", user_id=r["id"])}'
                  class='inline-form'
                  onsubmit="return confirm({confirm_delete!r});">
                <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}">
                <button class='btn danger small' type='submit'>{delete_label}</button>
            </form>
            """
            current_user_text = ""
        else:
            toggle_button = ""
            delete_button = ""
            current_user_text = f"<span class='muted small'>{current_user_label}</span>"

        user_rows += f"""
        <tr>
            <td>{escape(r['name'] or '-')}</td>
            <td>{escape(r['email'] or '-')}</td>
            <td>{escape(_role_label(r['role']))}</td>
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
                    <span>{role_label}</span>
                    <strong>{escape(_role_label(r['role']))}</strong>
                </div>
                <div>
                    <span>{status_label}</span>
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

    page_title = _t("Users & Permissions", "Usuarios y Permisos")
    page_subtitle = _t(
        "Manage company users, roles, and access levels.",
        "Administra usuarios, roles y niveles de acceso de la empresa.",
    )
    add_user_title = _t("Add User", "Agregar Usuario")
    company_users_title = _t("Company Users", "Usuarios de la Empresa")
    name_label = _t("Name", "Nombre")
    email_label = _t("Email", "Correo Electrónico")
    password_label = _t("Password", "Contraseña")
    active_label = _t("Active", "Activo")
    actions_label = _t("Actions", "Acciones")
    add_user_btn = _t("Add User", "Agregar Usuario")
    back_to_settings_btn = _t("Back to Settings", "Volver a Configuración")
    sync_permissions_btn = _t("Sync Permissions", "Sincronizar Permisos")
    no_users_found = _t("No users found.", "No se encontraron usuarios.")

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
            <h1>{page_title}</h1>
            <p class='muted'>{page_subtitle}</p>

            <form method="post" action="{url_for('users.sync_user_permissions')}" style="margin-top:14px;">
                <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}">
                <button class="btn secondary" type="submit">{sync_permissions_btn}</button>
            </form>
        </div>

        <div class='card'>
            <h2>{add_user_title}</h2>
            <form method='post'>
                <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}">
                <div class='grid'>
                    <div>
                        <label>{name_label}</label>
                        <input name='name' required>
                    </div>
                    <div>
                        <label>{email_label}</label>
                        <input name='email' type='email' required>
                    </div>
                    <div>
                        <label>{password_label}</label>
                        <input name='password' type='password' required>
                    </div>
                    <div>
                        <label>{role_label}</label>
                        <select name='role'>
                            <option value='Owner'>{_t("Owner", "Propietario")}</option>
                            <option value='Manager'>{_t("Manager", "Gerente")}</option>
                            <option value='Employee' selected>{_t("Employee", "Empleado")}</option>
                        </select>
                    </div>
                </div>

                <div class='row-actions' style='margin-top:20px;'>
                    <button class='btn success' type='submit'>{add_user_btn}</button>
                    <a class='btn secondary' href='{url_for("settings.settings")}'>{back_to_settings_btn}</a>
                </div>
            </form>
        </div>

        <div class='card'>
            <h2>{company_users_title}</h2>

            <div class='table-wrap desktop-only'>
                <table>
                    <tr>
                        <th>{name_label}</th>
                        <th>{email_label}</th>
                        <th>{role_label}</th>
                        <th>{active_label}</th>
                        <th>{actions_label}</th>
                    </tr>
                    {user_rows or f'<tr><td colspan="5" class="muted">{no_users_found}</td></tr>'}
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {mobile_cards or f"<div class='mobile-list-card muted'>{no_users_found}</div>"}
                </div>
            </div>
        </div>
    </div>
    """

    return render_page(content, page_title)


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
        flash(_t("User not found.", "Usuario no encontrado."))
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
        can_manage_employees = 1 if request.form.get("can_manage_employees") else 0
        can_manage_messages = 1 if request.form.get("can_manage_messages") else 0
        can_manage_payments = 1 if request.form.get("can_manage_payments") else 0
        can_view_calendar = 1 if request.form.get("can_view_calendar") else 0

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
                can_manage_settings = %s,
                can_manage_employees = %s,
                can_manage_messages = %s,
                can_manage_payments = %s,
                can_view_calendar = %s
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
                can_manage_employees,
                can_manage_messages,
                can_manage_payments,
                can_view_calendar,
                user_id,
                cid,
            ),
        )

        conn.commit()
        conn.close()

        if user_id == session.get("user_id"):
            flash(_t(
                "Your permissions were updated. Log out and back in if anything looks out of sync.",
                "Tus permisos fueron actualizados. Cierra sesión y vuelve a entrar si algo parece desincronizado.",
            ))
        else:
            flash(_t("User permissions updated.", "Permisos del usuario actualizados."))

        return redirect(url_for("users.users"))

    def checked(val):
        return "checked" if val else ""

    def user_perm(name, default=0):
        try:
            if name in user.keys():
                value = user[name]
                return default if value is None else value
        except Exception:
            pass
        return default

    page_title = _t("Edit Permissions", "Editar Permisos")
    user_label = _t("User", "Usuario")
    role_label = _t("Role", "Rol")
    permissions_title = _t("Permissions", "Permisos")
    save_btn = _t("Save Permissions", "Guardar Permisos")
    cancel_btn = _t("Cancel", "Cancelar")

    content = f"""
    <div class='card'>
        <h1>{page_title}</h1>
        <p class='muted'><strong>{user_label}:</strong> {escape(user['name'] or '-')} ({escape(user['email'] or '-')})</p>

        <form method='post'>
            <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}">
            <div class='grid'>
                <div>
                    <label>{role_label}</label>
                    <select name='role'>
                        <option value='Owner' {'selected' if (user['role'] or '') == 'Owner' else ''}>{_t("Owner", "Propietario")}</option>
                        <option value='Manager' {'selected' if (user['role'] or '') == 'Manager' else ''}>{_t("Manager", "Gerente")}</option>
                        <option value='Employee' {'selected' if (user['role'] or '') == 'Employee' else ''}>{_t("Employee", "Empleado")}</option>
                    </select>
                </div>
            </div>

            <br>

            <div class='card' style='padding:16px;'>
                <h3 style='margin-top:0;'>{permissions_title}</h3>

                <div class='grid'>
                    <label><input type='checkbox' name='can_manage_users' {checked(user_perm('can_manage_users'))}> {_t("Manage Users", "Administrar Usuarios")}</label>
                    <label><input type='checkbox' name='can_view_payroll' {checked(user_perm('can_view_payroll'))}> {_t("View Payroll", "Ver Nómina")}</label>
                    <label><input type='checkbox' name='can_manage_payroll' {checked(user_perm('can_manage_payroll'))}> {_t("Manage Payroll", "Administrar Nómina")}</label>
                    <label><input type='checkbox' name='can_view_bookkeeping' {checked(user_perm('can_view_bookkeeping'))}> {_t("View Bookkeeping", "Ver Contabilidad")}</label>
                    <label><input type='checkbox' name='can_manage_bookkeeping' {checked(user_perm('can_manage_bookkeeping'))}> {_t("Manage Bookkeeping", "Administrar Contabilidad")}</label>
                    <label><input type='checkbox' name='can_manage_jobs' {checked(user_perm('can_manage_jobs'))}> {_t("Manage Jobs", "Administrar Trabajos")}</label>
                    <label><input type='checkbox' name='can_manage_customers' {checked(user_perm('can_manage_customers'))}> {_t("Manage Customers", "Administrar Clientes")}</label>
                    <label><input type='checkbox' name='can_manage_invoices' {checked(user_perm('can_manage_invoices'))}> {_t("Manage Invoices", "Administrar Facturas")}</label>
                    <label><input type='checkbox' name='can_manage_settings' {checked(user_perm('can_manage_settings'))}> {_t("Manage Settings", "Administrar Configuración")}</label>
                    <label><input type='checkbox' name='can_manage_employees' {checked(user_perm('can_manage_employees'))}> {_t("Manage Employees", "Administrar Empleados")}</label>
                    <label><input type='checkbox' name='can_manage_messages' {checked(user_perm('can_manage_messages'))}> {_t("Manage Messages", "Administrar Mensajes")}</label>
                    <label><input type='checkbox' name='can_manage_payments' {checked(user_perm('can_manage_payments'))}> {_t("Manage Payments", "Administrar Pagos")}</label>
                    <label><input type='checkbox' name='can_view_calendar' {checked(user_perm('can_view_calendar', 1))}> {_t("View Calendar", "Ver Calendario")}</label>
                </div>
            </div>

            <br>
            <button class='btn success' type='submit'>{save_btn}</button>
            <a class='btn secondary' href='{url_for("users.users")}'>{cancel_btn}</a>
        </form>
    </div>
    """

    conn.close()
    return render_page(content, f"{page_title} - {user['name']}")


@users_bp.route("/users/<int:user_id>/toggle-active", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_users")
def toggle_user_active(user_id):
    if user_id == session.get("user_id"):
        flash(_t(
            "You cannot deactivate your own account.",
            "No puedes desactivar tu propia cuenta.",
        ))
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
        flash(_t("User not found.", "Usuario no encontrado."))
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

    flash(_t("User status updated.", "Estado del usuario actualizado."))
    return redirect(url_for("users.users"))


@users_bp.route("/users/delete/<int:user_id>", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_users")
def delete_user(user_id):
    if user_id == session["user_id"]:
        flash(_t(
            "You cannot delete your own account.",
            "No puedes eliminar tu propia cuenta.",
        ))
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
        flash(_t("User not found.", "Usuario no encontrado."))
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

    flash(_t("User deleted.", "Usuario eliminado."))
    return redirect(url_for("users.users"))


@users_bp.route("/users/sync-permissions", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_users")
def sync_user_permissions():
    updated = sync_missing_user_permissions_for_company(session["company_id"])
    flash(_t(
        f"Permissions synced for {updated} user(s).",
        f"Permisos sincronizados para {updated} usuario(s).",
    ))
    return redirect(url_for("users.users"))