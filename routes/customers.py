from flask import Blueprint, request, redirect, url_for, session, flash, make_response
from flask_wtf.csrf import generate_csrf
from html import escape
from datetime import date, datetime
import csv
import io

from db import get_db_connection, ensure_customer_name_columns
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page

customers_bp = Blueprint("customers", __name__)


def _t(lang, en, es):
    return es if lang == "es" else en


def ensure_customer_sms_consent_columns():
    conn = get_db_connection()
    try:
        conn.execute(
            """
            ALTER TABLE customers
            ADD COLUMN IF NOT EXISTS sms_opt_in BOOLEAN NOT NULL DEFAULT FALSE
            """
        )
        conn.execute(
            """
            ALTER TABLE customers
            ADD COLUMN IF NOT EXISTS sms_opt_in_at TIMESTAMP NULL
            """
        )
        conn.execute(
            """
            ALTER TABLE customers
            ADD COLUMN IF NOT EXISTS sms_opt_in_method TEXT
            """
        )
        conn.execute(
            """
            ALTER TABLE customers
            ADD COLUMN IF NOT EXISTS sms_opt_in_ip TEXT
            """
        )
        conn.execute(
            """
            ALTER TABLE customers
            ADD COLUMN IF NOT EXISTS sms_opt_out_at TIMESTAMP NULL
            """
        )
        conn.commit()
    finally:
        conn.close()


def _get_client_ip():
    forwarded = request.headers.get("X-Forwarded-For", "") or ""
    if forwarded:
        return forwarded.split(",")[0].strip()
    return (request.remote_addr or "").strip()


def _fmt_dt(value):
    if not value:
        return "-"
    try:
        return value.strftime("%Y-%m-%d %I:%M %p")
    except Exception:
        return str(value)


@customers_bp.route("/customers")
@login_required
@subscription_required
@require_permission("can_manage_customers")
def customers():
    lang = session.get("language_preference", "en")

    ensure_customer_name_columns()
    ensure_customer_sms_consent_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    try:
        rows = conn.execute(
            """
            SELECT *
            FROM customers
            WHERE company_id = %s
            ORDER BY
                LOWER(COALESCE(last_name, '')),
                LOWER(COALESCE(first_name, '')),
                LOWER(COALESCE(name, '')),
                id
            """,
            (cid,),
        ).fetchall()
    finally:
        conn.close()

    customer_rows = ""
    mobile_cards = ""

    for r in rows:
        customer_id = r["id"]

        first = (r["first_name"] or "").strip() if "first_name" in r.keys() else ""
        last = (r["last_name"] or "").strip() if "last_name" in r.keys() else ""
        full_name = (r["name"] or "").strip() if "name" in r.keys() else ""

        if first or last:
            display_name = f"{first} {last}".strip()
        else:
            display_name = full_name or f"Customer #{customer_id}"

        company = escape((r["company"] or "").strip()) if "company" in r.keys() and r["company"] else "-"
        phone = escape((r["phone"] or "").strip()) if "phone" in r.keys() and r["phone"] else "-"
        email = escape((r["email"] or "").strip()) if "email" in r.keys() and r["email"] else "-"
        sms_opt_in = bool(r["sms_opt_in"]) if "sms_opt_in" in r.keys() else False
        sms_badge = (
            f'<span class="sms-badge sms-yes">{_t(lang, "Opted In", "Aceptó SMS")}</span>'
            if sms_opt_in
            else f'<span class="sms-badge sms-no">{_t(lang, "Not Opted In", "No Aceptó SMS")}</span>'
        )
        delete_csrf = generate_csrf()

        customer_rows += f"""
        <tr>
            <td>#{customer_id}</td>
            <td>{escape(display_name)}</td>
            <td>{company}</td>
            <td>{phone}</td>
            <td>{email}</td>
            <td>{sms_badge}</td>
            <td style="white-space:nowrap;">
                <div class="row-actions">
                    <a class="btn secondary small" href="{url_for('customers.edit_customer', customer_id=customer_id)}">{_t(lang, "Edit", "Editar")}</a>

                    <form method="post"
                          action="{url_for('customers.delete_customer', customer_id=customer_id)}"
                          style="display:inline;"
                          onsubmit="return confirm('{_t(lang, "Delete this customer?", "¿Eliminar este cliente?")}');">
                        <input type="hidden" name="csrf_token" value="{delete_csrf}">
                        <button class="btn danger small" type="submit">{_t(lang, "Delete", "Eliminar")}</button>
                    </form>
                </div>
            </td>
        </tr>
        """

        mobile_cards += f"""
        <div class="mobile-list-card">
            <div class="mobile-list-top">
                <div>
                    <div class="mobile-list-title">{escape(display_name)}</div>
                    <div class="mobile-list-subtitle">{phone}</div>
                    <div style="margin-top:8px;">{sms_badge}</div>
                </div>
                <div class="mobile-badge">#{customer_id}</div>
            </div>

            <div class="mobile-list-actions">
                <a class="btn secondary small" href="{url_for('customers.edit_customer', customer_id=customer_id)}">{_t(lang, "Edit", "Editar")}</a>

                <form method="post"
                      action="{url_for('customers.delete_customer', customer_id=customer_id)}"
                      style="display:inline;"
                      onsubmit="return confirm('{_t(lang, "Delete this customer?", "¿Eliminar este cliente?")}');">
                    <input type="hidden" name="csrf_token" value="{delete_csrf}">
                    <button class="btn danger small" type="submit">{_t(lang, "Delete", "Eliminar")}</button>
                </form>
            </div>
        </div>
        """

    content = f"""
    <style>
        .customers-page {{
            display: grid;
            gap: 18px;
        }}

        .customers-head {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            flex-wrap: wrap;
        }}

        .table-wrap {{
            width: 100%;
            overflow-x: auto;
        }}

        .mobile-only {{
            display: none;
        }}

        .desktop-only {{
            display: block;
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
        }}

        .mobile-list-subtitle {{
            margin-top: 4px;
            font-size: .92rem;
            color: #64748b;
            line-height: 1.25;
            word-break: break-word;
        }}

        .mobile-badge {{
            font-size: .85rem;
            font-weight: 700;
            color: #334155;
            background: #f1f5f9;
            padding: 6px 10px;
            border-radius: 999px;
            white-space: nowrap;
        }}

        .mobile-list-actions {{
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
        }}

        .sms-badge {{
            display: inline-flex;
            align-items: center;
            padding: 5px 10px;
            border-radius: 999px;
            font-size: .8rem;
            font-weight: 700;
            line-height: 1;
            white-space: nowrap;
        }}

        .sms-yes {{
            background: #dcfce7;
            color: #166534;
        }}

        .sms-no {{
            background: #f1f5f9;
            color: #475569;
        }}

        @media (max-width: 640px) {{
            .desktop-only {{
                display: none !important;
            }}

            .mobile-only {{
                display: block !important;
            }}

            .mobile-list-actions .btn,
            .mobile-list-actions a,
            .mobile-list-actions button {{
                width: 100%;
                text-align: center;
            }}
        }}
    </style>

    <div class="customers-page">
        <div class="card">
            <div class="customers-head">
                <h1 style="margin:0;">{_t(lang, "Customers", "Clientes")}</h1>
                <div style="display:flex; gap:10px; flex-wrap:wrap;">
                    <a class="btn secondary" href="{url_for('customers.export_customers')}">{_t(lang, "Export CSV", "Exportar CSV")}</a>
                    <a class="btn" href="{url_for('customers.add_customer')}">{_t(lang, "Add Customer", "Agregar Cliente")}</a>
                </div>
            </div>

            <p class="muted" style="margin-top:8px;">{_t(lang, "Sorted alphabetically by last name.", "Ordenado alfabéticamente por apellido.")}</p>
        </div>

        <div class="card">
            <div class="table-wrap desktop-only">
                <table>
                    <tr>
                        <th>ID</th>
                        <th>{_t(lang, "Name", "Nombre")}</th>
                        <th>{_t(lang, "Company", "Empresa")}</th>
                        <th>{_t(lang, "Phone", "Teléfono")}</th>
                        <th>{_t(lang, "Email", "Correo")}</th>
                        <th>{_t(lang, "SMS Consent", "Consentimiento SMS")}</th>
                        <th>{_t(lang, "Actions", "Acciones")}</th>
                    </tr>
                    {customer_rows or f'<tr><td colspan="7" class="muted">{_t(lang, "No customers found.", "No se encontraron clientes.")}</td></tr>'}
                </table>
            </div>

            <div class="mobile-only">
                <div class="mobile-list">
                    {mobile_cards or f'<div class="mobile-list-card muted">{_t(lang, "No customers found.", "No se encontraron clientes.")}</div>'}
                </div>
            </div>
        </div>
    </div>
    """

    return render_page(content, _t(lang, "Customers", "Clientes"))


@customers_bp.route("/customers/add", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_customers")
def add_customer():
    lang = session.get("language_preference", "en")

    ensure_customer_name_columns()
    ensure_customer_sms_consent_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        company = (request.form.get("company") or "").strip()
        email = (request.form.get("email") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        billing_address = (request.form.get("billing_address") or "").strip()
        service_address = (request.form.get("service_address") or "").strip()
        notes = (request.form.get("notes") or "").strip()
        sms_opt_in = request.form.get("sms_opt_in") == "yes"
        client_ip = _get_client_ip()

        if not name:
            conn.close()
            flash(_t(lang, "Customer name is required.", "El nombre del cliente es obligatorio."))
            return redirect(url_for("customers.add_customer"))

        parts = name.split()
        first_name = parts[0] if parts else ""
        last_name = parts[-1] if len(parts) > 1 else ""

        try:
            conn.execute(
                """
                INSERT INTO customers (
                    company_id,
                    name,
                    first_name,
                    last_name,
                    company,
                    email,
                    phone,
                    billing_address,
                    service_address,
                    notes,
                    sms_opt_in,
                    sms_opt_in_at,
                    sms_opt_in_method,
                    sms_opt_in_ip,
                    sms_opt_out_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    cid,
                    name,
                    first_name,
                    last_name,
                    company,
                    email,
                    phone,
                    billing_address,
                    service_address,
                    notes,
                    sms_opt_in,
                    datetime.utcnow() if sms_opt_in else None,
                    "web_form" if sms_opt_in else None,
                    client_ip if sms_opt_in else None,
                    None,
                ),
            )
            conn.commit()
        finally:
            conn.close()

        flash(_t(lang, "Customer added.", "Cliente agregado."))
        return redirect(url_for("customers.customers"))

    conn.close()

    content = f"""
    <div class="card">
        <h1>{_t(lang, "Add Customer", "Agregar Cliente")}</h1>
        <form method="post">
            <input type="hidden" name="csrf_token" value="{generate_csrf()}">

            <div class="grid">
                <div>
                    <label>{_t(lang, "Name", "Nombre")}</label>
                    <input name="name" required>
                </div>
                <div>
                    <label>{_t(lang, "Company", "Empresa")}</label>
                    <input name="company">
                </div>
                <div>
                    <label>{_t(lang, "Email", "Correo")}</label>
                    <input name="email">
                </div>
                <div>
                    <label>{_t(lang, "Phone", "Teléfono")}</label>
                    <input name="phone">
                </div>
                <div>
                    <label>{_t(lang, "Billing Address", "Dirección de Facturación")}</label>
                    <input name="billing_address">
                </div>
                <div>
                    <label>{_t(lang, "Service Address", "Dirección de Servicio")}</label>
                    <input name="service_address">
                </div>
            </div>

            <br>

            <div class="card" style="padding:16px; background:#f8fafc; border:1px solid #e2e8f0;">
                <div style="font-weight:700; margin-bottom:10px;">{_t(lang, "SMS Consent", "Consentimiento SMS")}</div>

                <label style="display:flex; gap:10px; align-items:flex-start; line-height:1.5;">
                    <input type="checkbox" name="sms_opt_in" value="yes" style="margin-top:4px;">
                    <span>
                        {_t(
                            lang,
                            "I agree to receive SMS notifications regarding scheduling updates, service alerts, job reminders, and invoice reminders. Message frequency may vary. Message and data rates may apply. Reply STOP to opt out and HELP for help.",
                            "Acepto recibir notificaciones por SMS sobre actualizaciones de programación, alertas de servicio, recordatorios de trabajo y recordatorios de facturas. La frecuencia de mensajes puede variar. Pueden aplicarse tarifas de mensajes y datos. Responde STOP para cancelar y HELP para obtener ayuda."
                        )}
                    </span>
                </label>

                <div class="muted" style="margin-top:8px; font-size:.92rem;">
                    {_t(lang, "Consent is optional and not a condition of purchase.", "El consentimiento es opcional y no es una condición de compra.")}
                    <a href="/privacy" target="_blank" rel="noopener">{_t(lang, "Privacy Policy", "Política de Privacidad")}</a>
                    ·
                    <a href="/terms" target="_blank" rel="noopener">{_t(lang, "Terms of Service", "Términos de Servicio")}</a>
                </div>
            </div>

            <br>

            <label>{_t(lang, "Notes", "Notas")}</label>
            <textarea name="notes"></textarea>

            <br>

            <button class="btn" type="submit">{_t(lang, "Save Customer", "Guardar Cliente")}</button>
            <a class="btn secondary" href="{url_for('customers.customers')}">{_t(lang, "Cancel", "Cancelar")}</a>
        </form>
    </div>
    """

    return render_page(content, _t(lang, "Add Customer", "Agregar Cliente"))


@customers_bp.route("/customers/<int:customer_id>/edit", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_customers")
def edit_customer(customer_id):
    lang = session.get("language_preference", "en")

    ensure_customer_name_columns()
    ensure_customer_sms_consent_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    customer = conn.execute(
        "SELECT * FROM customers WHERE id = %s AND company_id = %s",
        (customer_id, cid),
    ).fetchone()

    if not customer:
        conn.close()
        flash(_t(lang, "Customer not found.", "Cliente no encontrado."))
        return redirect(url_for("customers.customers"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        company = (request.form.get("company") or "").strip()
        email = (request.form.get("email") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        billing_address = (request.form.get("billing_address") or "").strip()
        service_address = (request.form.get("service_address") or "").strip()
        notes = (request.form.get("notes") or "").strip()
        new_sms_opt_in = request.form.get("sms_opt_in") == "yes"
        old_sms_opt_in = bool(customer["sms_opt_in"]) if "sms_opt_in" in customer.keys() else False
        client_ip = _get_client_ip()

        if not name:
            conn.close()
            flash(_t(lang, "Customer name is required.", "El nombre del cliente es obligatorio."))
            return redirect(url_for("customers.edit_customer", customer_id=customer_id))

        parts = name.split()
        first_name = parts[0] if parts else ""
        last_name = parts[-1] if len(parts) > 1 else ""

        sms_opt_in_at = customer["sms_opt_in_at"] if "sms_opt_in_at" in customer.keys() else None
        sms_opt_in_method = customer["sms_opt_in_method"] if "sms_opt_in_method" in customer.keys() else None
        sms_opt_in_ip = customer["sms_opt_in_ip"] if "sms_opt_in_ip" in customer.keys() else None
        sms_opt_out_at = customer["sms_opt_out_at"] if "sms_opt_out_at" in customer.keys() else None

        if new_sms_opt_in and not old_sms_opt_in:
            sms_opt_in_at = datetime.utcnow()
            sms_opt_in_method = "web_form"
            sms_opt_in_ip = client_ip
            sms_opt_out_at = None
        elif not new_sms_opt_in and old_sms_opt_in:
            sms_opt_out_at = datetime.utcnow()

        try:
            conn.execute(
                """
                UPDATE customers
                SET name = %s,
                    first_name = %s,
                    last_name = %s,
                    company = %s,
                    email = %s,
                    phone = %s,
                    billing_address = %s,
                    service_address = %s,
                    notes = %s,
                    sms_opt_in = %s,
                    sms_opt_in_at = %s,
                    sms_opt_in_method = %s,
                    sms_opt_in_ip = %s,
                    sms_opt_out_at = %s
                WHERE id = %s AND company_id = %s
                """,
                (
                    name,
                    first_name,
                    last_name,
                    company,
                    email,
                    phone,
                    billing_address,
                    service_address,
                    notes,
                    new_sms_opt_in,
                    sms_opt_in_at,
                    sms_opt_in_method,
                    sms_opt_in_ip,
                    sms_opt_out_at,
                    customer_id,
                    cid,
                ),
            )
            conn.commit()
        finally:
            conn.close()

        flash(_t(lang, "Customer updated.", "Cliente actualizado."))
        return redirect(url_for("customers.customers"))

    name = escape(customer["name"] or "")
    company = escape(customer["company"] or "")
    email = escape(customer["email"] or "")
    phone = escape(customer["phone"] or "")
    billing_address = escape(customer["billing_address"] or "")
    service_address = escape(customer["service_address"] or "")
    notes = escape(customer["notes"] or "")
    sms_opt_in_checked = "checked" if customer["sms_opt_in"] else ""
    sms_opt_in_at = escape(_fmt_dt(customer["sms_opt_in_at"]))
    sms_opt_in_method = escape((customer["sms_opt_in_method"] or "-"))
    sms_opt_in_ip = escape((customer["sms_opt_in_ip"] or "-"))
    sms_opt_out_at = escape(_fmt_dt(customer["sms_opt_out_at"]))

    conn.close()

    content = f"""
    <div class="card">
        <h1>{_t(lang, "Edit Customer", "Editar Cliente")} #{customer['id']}</h1>
        <form method="post">
            <input type="hidden" name="csrf_token" value="{generate_csrf()}">

            <div class="grid">
                <div>
                    <label>{_t(lang, "Name", "Nombre")}</label>
                    <input name="name" value="{name}" required>
                </div>
                <div>
                    <label>{_t(lang, "Company", "Empresa")}</label>
                    <input name="company" value="{company}">
                </div>
                <div>
                    <label>{_t(lang, "Email", "Correo")}</label>
                    <input name="email" value="{email}">
                </div>
                <div>
                    <label>{_t(lang, "Phone", "Teléfono")}</label>
                    <input name="phone" value="{phone}">
                </div>
                <div>
                    <label>{_t(lang, "Billing Address", "Dirección de Facturación")}</label>
                    <input name="billing_address" value="{billing_address}">
                </div>
                <div>
                    <label>{_t(lang, "Service Address", "Dirección de Servicio")}</label>
                    <input name="service_address" value="{service_address}">
                </div>
            </div>

            <br>

            <div class="card" style="padding:16px; background:#f8fafc; border:1px solid #e2e8f0;">
                <div style="font-weight:700; margin-bottom:10px;">{_t(lang, "SMS Consent", "Consentimiento SMS")}</div>

                <label style="display:flex; gap:10px; align-items:flex-start; line-height:1.5;">
                    <input type="checkbox" name="sms_opt_in" value="yes" {sms_opt_in_checked} style="margin-top:4px;">
                    <span>
                        {_t(
                            lang,
                            "I agree to receive SMS notifications regarding scheduling updates, service alerts, job reminders, and invoice reminders. Message frequency may vary. Message and data rates may apply. Reply STOP to opt out and HELP for help.",
                            "Acepto recibir notificaciones por SMS sobre actualizaciones de programación, alertas de servicio, recordatorios de trabajo y recordatorios de facturas. La frecuencia de mensajes puede variar. Pueden aplicarse tarifas de mensajes y datos. Responde STOP para cancelar y HELP para obtener ayuda."
                        )}
                    </span>
                </label>

                <div class="muted" style="margin-top:8px; font-size:.92rem;">
                    {_t(lang, "Consent is optional and not a condition of purchase.", "El consentimiento es opcional y no es una condición de compra.")}
                    <a href="/privacy" target="_blank" rel="noopener">{_t(lang, "Privacy Policy", "Política de Privacidad")}</a>
                    ·
                    <a href="/terms" target="_blank" rel="noopener">{_t(lang, "Terms of Service", "Términos de Servicio")}</a>
                </div>

                <div style="margin-top:14px; display:grid; gap:6px; font-size:.92rem;">
                    <div><strong>{_t(lang, "Opted in at:", "Aceptó en:")}</strong> {sms_opt_in_at}</div>
                    <div><strong>{_t(lang, "Opt-in method:", "Método de aceptación:")}</strong> {sms_opt_in_method}</div>
                    <div><strong>{_t(lang, "Opt-in IP:", "IP de aceptación:")}</strong> {sms_opt_in_ip}</div>
                    <div><strong>{_t(lang, "Opted out at:", "Canceló en:")}</strong> {sms_opt_out_at}</div>
                </div>
            </div>

            <br>

            <label>{_t(lang, "Notes", "Notas")}</label>
            <textarea name="notes">{notes}</textarea>

            <br>

            <button class="btn" type="submit">{_t(lang, "Save Changes", "Guardar Cambios")}</button>
            <a class="btn secondary" href="{url_for('customers.customers')}">{_t(lang, "Cancel", "Cancelar")}</a>
        </form>
    </div>
    """

    return render_page(content, f"{_t(lang, 'Edit Customer', 'Editar Cliente')} #{customer['id']}")


@customers_bp.route("/customers/<int:customer_id>/delete", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_customers")
def delete_customer(customer_id):
    lang = session.get("language_preference", "en")

    conn = get_db_connection()
    cid = session["company_id"]

    try:
        customer = conn.execute(
            "SELECT id FROM customers WHERE id = %s AND company_id = %s",
            (customer_id, cid),
        ).fetchone()

        if not customer:
            flash(_t(lang, "Customer not found.", "Cliente no encontrado."))
            return redirect(url_for("customers.customers"))

        conn.execute(
            "DELETE FROM customers WHERE id = %s AND company_id = %s",
            (customer_id, cid),
        )
        conn.commit()
        flash(_t(lang, "Customer deleted.", "Cliente eliminado."))
    except Exception:
        conn.rollback()
        flash(
            _t(
                lang,
                "Could not delete customer. They may be linked to jobs, quotes, or invoices.",
                "No se pudo eliminar el cliente. Puede estar vinculado a trabajos, cotizaciones o facturas.",
            )
        )
    finally:
        conn.close()

    return redirect(url_for("customers.customers"))


@customers_bp.route("/customers/export")
@login_required
@subscription_required
@require_permission("can_manage_customers")
def export_customers():
    ensure_customer_name_columns()
    ensure_customer_sms_consent_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    try:
        rows = conn.execute(
            """
            SELECT id, name, first_name, last_name, company, email, phone,
                   billing_address, service_address, notes,
                   sms_opt_in, sms_opt_in_at, sms_opt_in_method,
                   sms_opt_in_ip, sms_opt_out_at
            FROM customers
            WHERE company_id = %s
            ORDER BY
                LOWER(COALESCE(last_name, '')),
                LOWER(COALESCE(first_name, '')),
                LOWER(COALESCE(name, '')),
                id
            """,
            (cid,),
        ).fetchall()
    finally:
        conn.close()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(
        [
            "ID",
            "Name",
            "First Name",
            "Last Name",
            "Company",
            "Email",
            "Phone",
            "Billing Address",
            "Service Address",
            "Notes",
            "SMS Opt In",
            "SMS Opt In At",
            "SMS Opt In Method",
            "SMS Opt In IP",
            "SMS Opt Out At",
        ]
    )

    for row in rows:
        writer.writerow(
            [
                row["id"],
                row["name"] or "",
                row["first_name"] or "",
                row["last_name"] or "",
                row["company"] or "",
                row["email"] or "",
                row["phone"] or "",
                row["billing_address"] or "",
                row["service_address"] or "",
                row["notes"] or "",
                "Yes" if row["sms_opt_in"] else "No",
                row["sms_opt_in_at"] or "",
                row["sms_opt_in_method"] or "",
                row["sms_opt_in_ip"] or "",
                row["sms_opt_out_at"] or "",
            ]
        )

    response = make_response(output.getvalue())
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = (
        f'attachment; filename="customers_{date.today().isoformat()}.csv"'
    )
    return response