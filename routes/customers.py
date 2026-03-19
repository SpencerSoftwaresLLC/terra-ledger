from flask import Blueprint, request, redirect, url_for, session, flash, make_response
from html import escape
from datetime import date
import csv
import io

from ..db import get_db_connection, ensure_customer_name_columns
from ..decorators import login_required, require_permission, subscription_required
from ..page_helpers import render_page

customers_bp = Blueprint("customers", __name__)


@customers_bp.route("/customers")
@login_required
@subscription_required
@require_permission("can_manage_customers")
def customers():
    ensure_customer_name_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT *
        FROM customers
        WHERE company_id = ?
        ORDER BY
            LOWER(COALESCE(last_name, '')),
            LOWER(COALESCE(first_name, '')),
            LOWER(COALESCE(name, '')),
            id
        """,
        (cid,),
    ).fetchall()

    conn.close()

    customer_rows = ""
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

        customer_rows += f"""
        <tr>
            <td>#{customer_id}</td>
            <td>{escape(display_name)}</td>
            <td>{company}</td>
            <td>{phone}</td>
            <td>{email}</td>
            <td style="white-space:nowrap;">
                <a class='btn secondary' href='{url_for("customers.edit_customer", customer_id=customer_id)}'>Edit</a>
                <form method='post' action='{url_for("customers.delete_customer", customer_id=customer_id)}' style='display:inline;' onsubmit='return confirm("Delete this customer?");'>
                    <button class='btn danger' type='submit'>Delete</button>
                </form>
            </td>
        </tr>
        """

    content = f"""
    <div class='card'>
        <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
            <h1 style="margin:0;">Customers</h1>

            <div style="display:flex; gap:10px; flex-wrap:wrap;">
                <a class='btn secondary' href='{url_for("customers.export_customers")}'>Export CSV</a>
                <a class='btn' href='{url_for("customers.add_customer")}'>Add Customer</a>
            </div>
        </div>

        <p class='muted' style='margin-top:8px;'>Sorted alphabetically by last name.</p>

        <table>
            <tr>
                <th>ID</th>
                <th>Name</th>
                <th>Company</th>
                <th>Phone</th>
                <th>Email</th>
                <th>Actions</th>
            </tr>
            {customer_rows or '<tr><td colspan="6" class="muted">No customers found.</td></tr>'}
        </table>
    </div>
    """

    return render_page(content, "Customers")


@customers_bp.route("/customers/export")
@login_required
@require_permission("can_manage_customers")
def export_customers():
    ensure_customer_name_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT
            id,
            name,
            first_name,
            last_name,
            company,
            email,
            phone,
            billing_address,
            service_address,
            notes
        FROM customers
        WHERE company_id = ?
        ORDER BY
            LOWER(COALESCE(last_name, '')),
            LOWER(COALESCE(first_name, '')),
            LOWER(COALESCE(name, '')),
            id
        """,
        (cid,),
    ).fetchall()

    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Customer ID",
        "Name",
        "First Name",
        "Last Name",
        "Company",
        "Email",
        "Phone",
        "Billing Address",
        "Service Address",
        "Notes",
    ])

    for r in rows:
        writer.writerow([
            r["id"] or "",
            r["name"] or "",
            r["first_name"] or "",
            r["last_name"] or "",
            r["company"] or "",
            r["email"] or "",
            r["phone"] or "",
            r["billing_address"] or "",
            r["service_address"] or "",
            r["notes"] or "",
        ])

    csv_data = output.getvalue()
    output.close()

    filename = f"customers_export_{date.today().isoformat()}.csv"

    response = make_response(csv_data)
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response


@customers_bp.route("/customers/add", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_customers")
def add_customer():
    ensure_customer_name_columns()

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

        if not name:
            conn.close()
            flash("Customer name is required.")
            return redirect(url_for("customers.add_customer"))

        parts = name.split()
        first_name = parts[0] if parts else ""
        last_name = parts[-1] if len(parts) > 1 else ""

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
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ),
        )

        conn.commit()
        conn.close()

        flash("Customer added.")
        return redirect(url_for("customers.customers"))

    conn.close()

    content = f"""
    <div class='card'>
        <h1>Add Customer</h1>
        <form method='post'>
            <div class='grid'>
                <div>
                    <label>Name</label>
                    <input name='name' required>
                </div>
                <div>
                    <label>Company</label>
                    <input name='company'>
                </div>
                <div>
                    <label>Email</label>
                    <input name='email'>
                </div>
                <div>
                    <label>Phone</label>
                    <input name='phone'>
                </div>
                <div>
                    <label>Billing Address</label>
                    <input name='billing_address'>
                </div>
                <div>
                    <label>Service Address</label>
                    <input name='service_address'>
                </div>
            </div>
            <br>
            <label>Notes</label>
            <textarea name='notes'></textarea>
            <br>
            <button class='btn' type='submit'>Save Customer</button>
            <a class='btn secondary' href='{url_for("customers.customers")}'>Cancel</a>
        </form>
    </div>
    """

    return render_page(content, "Add Customer")


@customers_bp.route("/customers/<int:customer_id>/edit", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_customers")
def edit_customer(customer_id):
    ensure_customer_name_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    customer = conn.execute(
        "SELECT * FROM customers WHERE id = ? AND company_id = ?",
        (customer_id, cid),
    ).fetchone()

    if not customer:
        conn.close()
        flash("Customer not found.")
        return redirect(url_for("customers.customers"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        company = (request.form.get("company") or "").strip()
        email = (request.form.get("email") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        billing_address = (request.form.get("billing_address") or "").strip()
        service_address = (request.form.get("service_address") or "").strip()
        notes = (request.form.get("notes") or "").strip()

        if not name:
            conn.close()
            flash("Customer name is required.")
            return redirect(url_for("customers.edit_customer", customer_id=customer_id))

        parts = name.split()
        first_name = parts[0] if parts else ""
        last_name = parts[-1] if len(parts) > 1 else ""

        conn.execute(
            """
            UPDATE customers
            SET name = ?,
                first_name = ?,
                last_name = ?,
                company = ?,
                email = ?,
                phone = ?,
                billing_address = ?,
                service_address = ?,
                notes = ?
            WHERE id = ? AND company_id = ?
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
                customer_id,
                cid,
            ),
        )
        conn.commit()
        conn.close()

        flash("Customer updated.")
        return redirect(url_for("customers.customers"))

    name = escape(customer["name"] or "")
    company = escape(customer["company"] or "")
    email = escape(customer["email"] or "")
    phone = escape(customer["phone"] or "")
    billing_address = escape(customer["billing_address"] or "")
    service_address = escape(customer["service_address"] or "")
    notes = escape(customer["notes"] or "")

    content = f"""
    <div class='card'>
        <h1>Edit Customer #{customer['id']}</h1>
        <form method='post'>
            <div class='grid'>
                <div>
                    <label>Name</label>
                    <input name='name' value="{name}" required>
                </div>
                <div>
                    <label>Company</label>
                    <input name='company' value="{company}">
                </div>
                <div>
                    <label>Email</label>
                    <input name='email' value="{email}">
                </div>
                <div>
                    <label>Phone</label>
                    <input name='phone' value="{phone}">
                </div>
                <div>
                    <label>Billing Address</label>
                    <input name='billing_address' value="{billing_address}">
                </div>
                <div>
                    <label>Service Address</label>
                    <input name='service_address' value="{service_address}">
                </div>
            </div>
            <br>
            <label>Notes</label>
            <textarea name='notes'>{notes}</textarea>
            <br>
            <button class='btn' type='submit'>Save Changes</button>
            <a class='btn secondary' href='{url_for("customers.customers")}'>Cancel</a>
        </form>
    </div>
    """

    conn.close()
    return render_page(content, f"Edit Customer #{customer['id']}")


@customers_bp.route("/customers/<int:customer_id>/delete", methods=["POST"])
@login_required
@require_permission("can_manage_customers")
def delete_customer(customer_id):
    conn = get_db_connection()
    cid = session["company_id"]

    customer = conn.execute(
        "SELECT id, name FROM customers WHERE id = ? AND company_id = ?",
        (customer_id, cid),
    ).fetchone()

    if not customer:
        conn.close()
        flash("Customer not found.")
        return redirect(url_for("customers.customers"))

    conn.execute(
        "DELETE FROM customers WHERE id = ? AND company_id = ?",
        (customer_id, cid),
    )
    conn.commit()
    conn.close()

    flash("Customer deleted.")
    return redirect(url_for("customers.customers"))