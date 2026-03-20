from flask import Blueprint, request, redirect, url_for, session, flash, render_template, abort, make_response, current_app
from datetime import date, datetime
from html import escape
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
import os
import tempfile
import json
import io
import csv
from urllib.request import urlopen
from urllib.parse import urlparse

from db import get_db_connection, update_invoice_balance, get_next_invoice_number, ensure_document_number_columns
from decorators import login_required, require_permission, subscription_required
from page_helpers import *
from helpers import *
from calculations import *
from utils.emailing import send_company_email

invoices_bp = Blueprint("invoices", __name__)


def build_invoice_pdf(invoice, items, company, profile):
    pdf_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf_temp.close()

    try:
        invoice_number = invoice["invoice_number"] or invoice["id"]

        company_name = (
            profile["invoice_header_name"]
            if profile and profile["invoice_header_name"]
            else (
                profile["display_name"]
                if profile and profile["display_name"]
                else (company["name"] if company else "Your Company")
            )
        )

        footer_note = profile["invoice_footer_note"] if profile and profile["invoice_footer_note"] else ""
        logo_url = profile["logo_url"] if profile and profile["logo_url"] else ""

        address_parts = []
        if company:
            if company["address_line_1"] and str(company["address_line_1"]).strip().lower() != "none":
                address_parts.append(company["address_line_1"])
            if company["address_line_2"] and str(company["address_line_2"]).strip().lower() != "none":
                address_parts.append(company["address_line_2"])

            city_state_zip = " ".join(
                part for part in [
                    f"{company['city']}," if company["city"] else "",
                    company["state"] or "",
                    company["zip_code"] or "",
                ] if part
            ).strip()

            if city_state_zip:
                address_parts.append(city_state_zip)

        company_contact_lines = []
        if address_parts:
            company_contact_lines.extend(address_parts)
        if company and company["phone"] and str(company["phone"]).strip().lower() != "none":
            company_contact_lines.append(company["phone"])
        if company and company["email"] and str(company["email"]).strip().lower() != "none":
            company_contact_lines.append(company["email"])
        if company and company["website"] and str(company["website"]).strip().lower() != "none":
            company_contact_lines.append(company["website"])

        def load_logo_reader(logo_path_or_url):
            if not logo_path_or_url:
                return None

            try:
                parsed = urlparse(logo_path_or_url)

                if parsed.scheme in ("http", "https"):
                    with urlopen(logo_path_or_url, timeout=5) as resp:
                        return ImageReader(io.BytesIO(resp.read()))

                cleaned = str(logo_path_or_url).strip()

                if cleaned.startswith("/"):
                    full_path = os.path.join(current_app.root_path, cleaned.lstrip("/"))
                else:
                    full_path = os.path.join(current_app.root_path, cleaned)

                if os.path.exists(full_path):
                    return ImageReader(full_path)

            except Exception:
                return None

            return None

        logo_reader = load_logo_reader(logo_url)

        c = canvas.Canvas(pdf_temp.name, pagesize=letter)
        width, height = letter

        footer_chunks = [footer_note[i:i + 95] for i in range(0, len(footer_note), 95)] if footer_note else []

        def draw_footer():
            if not footer_chunks:
                return

            c.setFont("Helvetica-Oblique", 9)
            footer_y = 40
            for chunk in footer_chunks[:3]:
                c.drawCentredString(width / 2, footer_y, chunk)
                footer_y -= 11

        def draw_header():
            y_pos = height - 50
            text_x = 50

            if logo_reader:
                try:
                    max_width = 180
                    max_height = 70
                    logo_x = 50
                    logo_top_y = height - 50

                    img_width, img_height = logo_reader.getSize()

                    if img_width and img_height:
                        width_ratio = max_width / float(img_width)
                        height_ratio = max_height / float(img_height)
                        scale = min(width_ratio, height_ratio)
                        draw_width = img_width * scale
                        draw_height = img_height * scale
                    else:
                        draw_width = max_width
                        draw_height = max_height

                    logo_y = (logo_top_y - max_height) + ((max_height - draw_height) / 2)

                    c.drawImage(
                        logo_reader,
                        logo_x,
                        logo_y,
                        width=draw_width,
                        height=draw_height,
                        mask='auto'
                    )

                    text_x = 250
                except Exception:
                    text_x = 50

            c.setFont("Helvetica-Bold", 18)
            c.drawString(text_x, y_pos, str(company_name or "Your Company")[:45])

            c.setFont("Helvetica-Bold", 20)
            c.drawRightString(width - 50, height - 50, "INVOICE")

            info_y = y_pos - 22
            c.setFont("Helvetica", 10)
            for line in company_contact_lines:
                c.drawString(text_x, info_y, str(line)[:85])
                info_y -= 14

            draw_footer()

            return min(info_y - 10, height - 125 if logo_reader else info_y - 10)

        def new_page():
            c.showPage()
            return draw_header()

        y = draw_header()

        def ensure_space(required_height):
            nonlocal y
            if y - required_height < 85:
                y = new_page()

        ensure_space(90)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(50, y, f"Invoice #: {invoice_number}")
        y -= 16
        c.drawString(50, y, f"Customer: {invoice['customer_name'] or ''}")
        y -= 16
        c.drawString(50, y, f"Status: {invoice['status'] or ''}")
        y -= 16
        c.drawString(50, y, f"Date: {invoice['invoice_date'] or date.today().isoformat()}")
        y -= 24

        ensure_space(40)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(50, y, "Description")
        c.drawString(280, y, "Qty")
        c.drawString(330, y, "Unit")
        c.drawString(390, y, "Unit Price")
        c.drawString(480, y, "Line Total")
        y -= 10

        c.line(50, y, 560, y)
        y -= 18

        c.setFont("Helvetica", 10)

        if items:
            for i in items:
                ensure_space(24)

                description = str(i["description"] or "")[:38]
                qty = f"{float(i['quantity'] or 0):g}"
                unit = str(i["unit"] or "")[:8]
                unit_price = f"${float(i['unit_price'] or 0):.2f}"
                line_total = f"${float(i['line_total'] or 0):.2f}"

                c.drawString(50, y, description)
                c.drawString(280, y, qty)
                c.drawString(330, y, unit)
                c.drawRightString(460, y, unit_price)
                c.drawRightString(560, y, line_total)

                y -= 18
        else:
            c.drawString(50, y, "No items.")
            y -= 18

        ensure_space(55)
        y -= 8
        c.line(380, y, 560, y)
        y -= 18
        c.setFont("Helvetica-Bold", 12)
        c.drawRightString(560, y, f"Total: ${float(invoice['total'] or 0):.2f}")
        y -= 28

        if invoice["notes"]:
            ensure_space(50)
            c.setFont("Helvetica-Bold", 11)
            c.drawString(50, y, "Notes:")
            y -= 18

            c.setFont("Helvetica", 10)
            notes_text = str(invoice["notes"])
            note_chunks = [notes_text[i:i + 95] for i in range(0, len(notes_text), 95)]

            for chunk in note_chunks:
                ensure_space(18)
                c.drawString(50, y, chunk)
                y -= 15

        draw_footer()
        c.save()

        with open(pdf_temp.name, "rb") as f:
            return f.read()

    finally:
        if os.path.exists(pdf_temp.name):
            os.remove(pdf_temp.name)


@invoices_bp.route("/invoices", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def invoices():
    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT i.*, c.name AS customer_name
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.company_id = ?
          AND IFNULL(i.status, '') != 'Paid'
        ORDER BY c.name COLLATE NOCASE ASC,
                 COALESCE(i.invoice_date, '') DESC,
                 i.id DESC
        """,
        (cid,),
    ).fetchall()
    conn.close()

    invoice_rows = "".join(
        f"""<tr>
            <td>#{r['id']}</td>
            <td>{escape(r['invoice_number'] or '-')}</td>
            <td>{escape(r['customer_name'] or '-')}</td>
            <td>{escape(r['invoice_date'] or '-')}</td>
            <td>${float(r['total'] or 0):.2f}</td>
            <td>${float(r['balance_due'] or 0):.2f}</td>
            <td>{escape(r['status'] or '-')}</td>
            <td>
                <div class='row-actions'>
                    <a class='btn secondary small' href='{url_for("invoices.view_invoice", invoice_id=r["id"])}'>View</a>
                    <a class='btn small' href='{url_for("invoices.email_invoice_preview", invoice_id=r["id"])}'>Email</a>
                </div>
            </td>
        </tr>"""
        for r in rows
    )

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Invoices</h1>
                <p class='muted' style='margin:0;'>Showing open and unpaid invoices. Paid invoices are kept on a separate page.</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("invoices.export_invoices")}' class='btn secondary'>Export CSV</a>
                <a href='{url_for("invoices.paid_invoices")}' class='btn secondary'>Paid Invoices</a>
                <a href='{url_for("invoices.new_invoice")}' class='btn success'>+ New Invoice</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Open Invoice List</h2>
        <table>
            <tr>
                <th>ID</th>
                <th>Number</th>
                <th>Customer</th>
                <th>Date</th>
                <th>Total</th>
                <th>Balance Due</th>
                <th>Status</th>
                <th>Actions</th>
            </tr>
            {invoice_rows or '<tr><td colspan="8" class="muted">No open invoices yet.</td></tr>'}
        </table>
    </div>
    """
    return render_page(content, "Invoices")


@invoices_bp.route("/invoices/paid")
@login_required
@require_permission("can_manage_invoices")
def paid_invoices():
    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT i.*, c.name AS customer_name
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.company_id = ?
          AND i.status = 'Paid'
        ORDER BY c.name COLLATE NOCASE, i.invoice_date DESC, i.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    invoice_rows = "".join(
        f"""
        <tr>
            <td>#{r['id']}</td>
            <td>{escape(r['invoice_number'] or '-')}</td>
            <td>{escape(r['customer_name'] or '-')}</td>
            <td>{escape(r['invoice_date'] or '-')}</td>
            <td>${float(r['total'] or 0):.2f}</td>
            <td>
                {
                    f"<span class='pill warning'>Paid</span>" if (r['status'] or '').lower() == 'paid'
                    else f"<span class='pill success'>Sent</span>" if (r['status'] or '').lower() == 'sent'
                    else f"<span class='pill'>Draft</span>" if (r['status'] or '').lower() == 'draft'
                    else escape(r['status'] or '-')
                }
            </td>
            <td>${float(r['balance_due'] or 0):.2f}</td>
            <td>
                <a class='btn secondary small' href='{url_for("invoices.view_invoice", invoice_id=r["id"])}'>View</a>
            </td>
        </tr>
        """
        for r in rows
    )

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <h1 style='margin:0;'>Paid Invoices</h1>
            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("invoices.export_invoices")}'>Export CSV</a>
                <a class='btn secondary' href='{url_for("invoices.invoices")}'>Back to Open Invoices</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <table>
            <tr>
                <th>ID</th>
                <th>Invoice #</th>
                <th>Customer</th>
                <th>Date</th>
                <th>Total</th>
                <th>Status</th>
                <th>Balance</th>
                <th>Actions</th>
            </tr>
            {invoice_rows or '<tr><td colspan="8" class="muted">No paid invoices yet.</td></tr>'}
        </table>
    </div>
    """
    return render_page(content, "Paid Invoices")


@invoices_bp.route("/invoices/export")
@login_required
@require_permission("can_manage_invoices")
def export_invoices():
    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT
            i.id,
            i.invoice_number,
            i.invoice_date,
            i.due_date,
            i.status,
            i.total,
            i.balance_due,
            i.notes,
            c.name AS customer_name,
            c.email AS customer_email,
            (
                SELECT COALESCE(SUM(ip.amount), 0)
                FROM invoice_payments ip
                WHERE ip.invoice_id = i.id
            ) AS paid_total
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.company_id = ?
        ORDER BY
            LOWER(COALESCE(c.name, '')),
            COALESCE(i.invoice_date, '') DESC,
            i.id DESC
        """,
        (cid,),
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Invoice ID",
        "Invoice Number",
        "Customer",
        "Customer Email",
        "Invoice Date",
        "Due Date",
        "Status",
        "Total",
        "Paid",
        "Balance Due",
        "Notes",
    ])

    for r in rows:
        writer.writerow([
            r["id"] or "",
            r["invoice_number"] or "",
            r["customer_name"] or "",
            r["customer_email"] or "",
            r["invoice_date"] or "",
            r["due_date"] or "",
            r["status"] or "",
            float(r["total"] or 0),
            float(r["paid_total"] or 0),
            float(r["balance_due"] or 0),
            r["notes"] or "",
        ])

    conn.close()

    csv_data = output.getvalue()
    output.close()

    filename = f"invoices_export_{date.today().isoformat()}.csv"

    response = make_response(csv_data)
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response


@invoices_bp.route("/invoices/new", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_invoices")
def new_invoice():
    ensure_document_number_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    customers = conn.execute(
        """
        SELECT id, name, company, email
        FROM customers
        WHERE company_id = ?
        ORDER BY name
        """,
        (cid,),
    ).fetchall()

    company_row = conn.execute(
        """
        SELECT next_invoice_number
        FROM companies
        WHERE id = ?
        """,
        (cid,),
    ).fetchone()

    next_invoice_number_preview = "1001"
    if company_row and company_row["next_invoice_number"] is not None:
        next_invoice_number_preview = str(company_row["next_invoice_number"])

    customer_list = [
        {
            "id": c["id"],
            "name": c["name"] or "",
            "company": c["company"] or "",
            "email": c["email"] or "",
        }
        for c in customers
    ]

    if request.method == "POST":
        customer_id = request.form.get("customer_id", type=int)
        invoice_number = (request.form.get("invoice_number") or "").strip()
        invoice_date = (request.form.get("invoice_date") or "").strip()
        due_date = (request.form.get("due_date") or "").strip()
        description = (request.form.get("description") or "").strip()
        total = float(request.form.get("total") or 0)
        status = (request.form.get("status") or "Unpaid").strip()

        if not customer_id:
            conn.close()
            flash("Please select a customer.")
            return redirect(url_for("invoices.new_invoice"))

        conn.close()

        if not invoice_number:
            invoice_number = get_next_invoice_number(cid)

        conn = get_db_connection()
        conn.execute(
            """
            INSERT INTO invoices (
                company_id,
                customer_id,
                invoice_number,
                invoice_date,
                due_date,
                notes,
                total,
                balance_due,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cid,
                customer_id,
                invoice_number,
                invoice_date or None,
                due_date or None,
                description,
                total,
                0 if status == "Paid" else total,
                status,
            ),
        )
        conn.commit()
        conn.close()

        flash(f"Invoice #{invoice_number} created successfully.")
        return redirect(url_for("invoices.invoices"))

    conn.close()

    content = f"""
    <style>
        .customer-search-wrap {{
            position: relative;
        }}

        .customer-results {{
            position: absolute;
            top: 100%;
            left: 0;
            right: 0;
            background: #fff;
            border: 1px solid #dbe2ea;
            border-radius: 10px;
            margin-top: 6px;
            box-shadow: 0 8px 20px rgba(0,0,0,.08);
            z-index: 1000;
            max-height: 260px;
            overflow-y: auto;
        }}

        .customer-result-item {{
            padding: 10px 12px;
            cursor: pointer;
            border-bottom: 1px solid #eef2f7;
        }}

        .customer-result-item:last-child {{
            border-bottom: none;
        }}

        .customer-result-item:hover {{
            background: #f8fbff;
        }}
    </style>

    <div class='card'>
        <h1>Create Invoice</h1>
        <p class='muted'>Create an invoice manually without converting a job.</p>
    </div>

    <div class='card'>
        <form method='post'>
            <div class='grid'>
                <div class='customer-search-wrap'>
                    <label>Customer</label>
                    <input type='text'
                           id='customer_search'
                           placeholder='Search customer name, company, or email...'
                           autocomplete='off'
                           required>
                    <input type='hidden' name='customer_id' id='customer_id' required>
                    <div id='customer_results' class='customer-results'></div>
                </div>

                <div>
                    <label>Invoice Number</label>
                    <input type='text' name='invoice_number' placeholder='Auto: {escape(next_invoice_number_preview)}'>
                    <div class='muted' style='margin-top:6px;'>Leave blank to auto-assign the next invoice number.</div>
                </div>

                <div>
                    <label>Invoice Date</label>
                    <input type='date' name='invoice_date' value='{date.today().isoformat()}'>
                </div>

                <div>
                    <label>Due Date</label>
                    <input type='date' name='due_date'>
                </div>

                <div>
                    <label>Status</label>
                    <select name='status'>
                        <option value='Unpaid'>Unpaid</option>
                        <option value='Paid'>Paid</option>
                        <option value='Partial'>Partial</option>
                    </select>
                </div>

                <div>
                    <label>Total</label>
                    <input type='number' step='0.01' min='0' name='total' placeholder='0.00' required>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>Description / Notes</label>
                    <textarea name='description' placeholder='Material, labor, fuel, delivery, etc.'></textarea>
                </div>
            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>Create Invoice</button>
                <a class='btn secondary' href='{url_for("invoices.invoices")}'>Back to Invoices</a>
            </div>
        </form>
    </div>

    <script>
        const customers = {json.dumps(customer_list)};

        const searchInput = document.getElementById("customer_search");
        const customerIdInput = document.getElementById("customer_id");
        const resultsBox = document.getElementById("customer_results");

        function escapeHtml(text) {{
            return String(text || "")
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }}

        function renderCustomerResults(matches) {{
            if (!matches.length) {{
                resultsBox.innerHTML = "<div class='customer-result-item muted'>No customers found</div>";
                return;
            }}

            resultsBox.innerHTML = matches.map(c => `
                <div class="customer-result-item" data-id="${{c.id}}">
                    <strong>${{escapeHtml(c.name || "Unnamed Customer")}}</strong>
                    ${{c.company ? `<div class="muted small">${{escapeHtml(c.company)}}</div>` : ""}}
                    ${{c.email ? `<div class="muted small">${{escapeHtml(c.email)}}</div>` : ""}}
                </div>
            `).join("");

            document.querySelectorAll(".customer-result-item[data-id]").forEach(item => {{
                item.addEventListener("click", function () {{
                    const id = this.dataset.id;
                    const customer = customers.find(x => String(x.id) === String(id));
                    if (!customer) return;

                    customerIdInput.value = customer.id;
                    searchInput.value = customer.company
                        ? `${{customer.name}} - ${{customer.company}}`
                        : customer.name;

                    resultsBox.innerHTML = "";
                }});
            }});
        }}

        searchInput.addEventListener("input", function () {{
            const q = this.value.trim().toLowerCase();
            customerIdInput.value = "";

            if (!q) {{
                resultsBox.innerHTML = "";
                return;
            }}

            const matches = customers.filter(c =>
                (c.name && c.name.toLowerCase().includes(q)) ||
                (c.company && c.company.toLowerCase().includes(q)) ||
                (c.email && c.email.toLowerCase().includes(q))
            ).slice(0, 8);

            renderCustomerResults(matches);
        }});

        document.addEventListener("click", function (e) {{
            if (!e.target.closest(".customer-search-wrap")) {{
                resultsBox.innerHTML = "";
            }}
        }});
    </script>
    """
    return render_page(content, "Create Invoice")


@invoices_bp.route("/invoices/<int:invoice_id>/add_payment", methods=["POST"])
@login_required
@require_permission("can_manage_invoices")
def add_invoice_payment(invoice_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT id, total, balance_due, customer_id, invoice_number
        FROM invoices
        WHERE id = ? AND company_id = ?
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        flash("Invoice not found.")
        return redirect(url_for("invoices.invoices"))

    amount = request.form.get("amount", type=float) or 0
    payment_date = request.form.get("payment_date") or date.today().isoformat()
    payment_method = (request.form.get("payment_method") or "").strip()
    reference = (request.form.get("reference") or "").strip()
    notes = (request.form.get("notes") or "").strip()

    if amount <= 0:
        conn.close()
        flash("Payment amount must be greater than 0.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    if amount > float(invoice["balance_due"] or 0):
        conn.close()
        flash("Payment cannot exceed the remaining balance.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    conn.execute(
        """
        INSERT INTO invoice_payments
        (company_id, invoice_id, payment_date, amount, payment_method, reference, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (cid, invoice_id, payment_date, amount, payment_method, reference, notes),
    )

    conn.commit()
    conn.close()

    update_invoice_balance(invoice_id)

    flash("Payment recorded.")
    return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))


@invoices_bp.route("/invoices/<int:invoice_id>/payments/<int:payment_id>/edit", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_invoices")
def edit_invoice_payment(invoice_id, payment_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT *
        FROM invoices
        WHERE id = ? AND company_id = ?
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        flash("Invoice not found.")
        return redirect(url_for("invoices.invoices"))

    payment = conn.execute(
        """
        SELECT *
        FROM invoice_payments
        WHERE id = ? AND invoice_id = ? AND company_id = ?
        """,
        (payment_id, invoice_id, cid),
    ).fetchone()

    if not payment:
        conn.close()
        flash("Payment not found.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    if request.method == "POST":
        amount = request.form.get("amount", type=float) or 0
        payment_date = request.form.get("payment_date") or date.today().isoformat()
        payment_method = (request.form.get("payment_method") or "").strip()
        reference = (request.form.get("reference") or "").strip()
        notes = (request.form.get("notes") or "").strip()

        if amount <= 0:
            conn.close()
            flash("Payment amount must be greater than 0.")
            return redirect(url_for("invoices.edit_invoice_payment", invoice_id=invoice_id, payment_id=payment_id))

        other_paid_row = conn.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS other_paid_total
            FROM invoice_payments
            WHERE invoice_id = ? AND id != ?
            """,
            (invoice_id, payment_id),
        ).fetchone()

        other_paid_total = float(other_paid_row["other_paid_total"] or 0)
        invoice_total = float(invoice["total"] or 0)

        if other_paid_total + amount > invoice_total:
            conn.close()
            flash("Payment total cannot exceed the invoice total.")
            return redirect(url_for("invoices.edit_invoice_payment", invoice_id=invoice_id, payment_id=payment_id))

        conn.execute(
            """
            UPDATE invoice_payments
            SET payment_date = ?, amount = ?, payment_method = ?, reference = ?, notes = ?
            WHERE id = ? AND invoice_id = ? AND company_id = ?
            """,
            (payment_date, amount, payment_method, reference, notes, payment_id, invoice_id, cid),
        )
        conn.commit()
        conn.close()

        update_invoice_balance(invoice_id)

        flash("Payment updated.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    payment_method_val = escape(payment["payment_method"] or "", quote=True)
    reference_val = escape(payment["reference"] or "", quote=True)
    notes_val = escape(payment["notes"] or "")
    payment_date_val = payment["payment_date"] or date.today().isoformat()
    amount_val = payment["amount"] or 0

    conn.close()

    content = f"""
    <div class='card'>
        <h1>Edit Payment</h1>
        <form method='post'>
            <div class='grid'>
                <div>
                    <label>Amount</label>
                    <input type='number' step='0.01' min='0.01' name='amount' value='{amount_val}' required>
                </div>
                <div>
                    <label>Payment Date</label>
                    <input type='date' name='payment_date' value='{payment_date_val}'>
                </div>
                <div>
                    <label>Payment Method</label>
                    <input name='payment_method' value='{payment_method_val}' placeholder='Cash, Check, Card, ACH'>
                </div>
                <div>
                    <label>Reference</label>
                    <input name='reference' value='{reference_val}' placeholder='Check # or transaction ID'>
                </div>
            </div>

            <br>
            <label>Notes</label>
            <textarea name='notes'>{notes_val}</textarea>
            <br>

            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("invoices.invoices")}'>Back to Invoices</a>
                <button class='btn success' type='submit'>Save Changes</button>
                <a class='btn secondary' href='{url_for("invoices.view_invoice", invoice_id=invoice_id)}'>Cancel</a>
            </div>
        </form>
    </div>
    """
    return render_page(content, "Edit Payment")


@invoices_bp.route("/invoices/<int:invoice_id>/payments/<int:payment_id>/delete", methods=["POST"])
@login_required
@require_permission("can_manage_invoices")
def delete_invoice_payment(invoice_id, payment_id):
    conn = get_db_connection()
    cid = session["company_id"]

    payment = conn.execute(
        """
        SELECT *
        FROM invoice_payments
        WHERE id = ? AND invoice_id = ? AND company_id = ?
        """,
        (payment_id, invoice_id, cid),
    ).fetchone()

    if not payment:
        conn.close()
        flash("Payment not found.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    conn.execute(
        """
        DELETE FROM invoice_payments
        WHERE id = ? AND invoice_id = ? AND company_id = ?
        """,
        (payment_id, invoice_id, cid),
    )
    conn.commit()
    conn.close()

    update_invoice_balance(invoice_id)

    flash("Payment deleted.")
    return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))


@invoices_bp.route("/invoices/<int:invoice_id>/mark_paid", methods=["POST"])
@login_required
@require_permission("can_manage_invoices")
def mark_invoice_paid(invoice_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT *
        FROM invoices
        WHERE id = ? AND company_id = ?
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        flash("Invoice not found.")
        return redirect(url_for("invoices.invoices"))

    remaining_balance = float(invoice["balance_due"] or 0)

    if remaining_balance <= 0:
        conn.close()
        flash("Invoice is already fully paid.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    # ✅ Insert final payment
    conn.execute(
        """
        INSERT INTO invoice_payments
        (company_id, invoice_id, payment_date, amount, payment_method, reference, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            cid,
            invoice_id,
            date.today().isoformat(),
            remaining_balance,
            "Mark Paid",
            "",
            "Remaining balance marked paid",
        ),
    )

    conn.commit()

    # ✅ Update invoice balance/status
    update_invoice_balance(invoice_id)

    # ✅ REFETCH updated invoice (now should be Paid)
    invoice = conn.execute(
        """
        SELECT *
        FROM invoices
        WHERE id = ? AND company_id = ?
        """,
        (invoice_id, cid),
    ).fetchone()

    # ✅ AUTO FINISH JOB
    if "job_id" in invoice.keys() and invoice["job_id"]:
        conn.execute(
            """
            UPDATE jobs
            SET status = 'Finished'
            WHERE id = ? AND company_id = ?
            """,
            (invoice["job_id"], cid),
        )

    # ✅ AUTO FINISH QUOTE
    if "quote_id" in invoice.keys() and invoice["quote_id"]:
        conn.execute(
            """
            UPDATE quotes
            SET status = 'Finished'
            WHERE id = ? AND company_id = ?
            """,
            (invoice["quote_id"], cid),
        )

    conn.commit()
    conn.close()

    flash("Invoice marked paid. Job and Quote moved to Finished.")
    return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))


@invoices_bp.route("/invoices/<int:invoice_id>/mark_unpaid", methods=["POST"])
@login_required
@require_permission("can_manage_invoices")
def mark_invoice_unpaid(invoice_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT *
        FROM invoices
        WHERE id = ? AND company_id = ?
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        flash("Invoice not found.")
        return redirect(url_for("invoices.invoices"))

    conn.execute(
        """
        DELETE FROM invoice_payments
        WHERE invoice_id = ? AND company_id = ?
        """,
        (invoice_id, cid),
    )

    conn.commit()
    conn.close()

    update_invoice_balance(invoice_id)

    flash("Invoice marked unpaid and payment history cleared.")
    return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))


@invoices_bp.route("/invoices/<int:invoice_id>", methods=["GET"])
@login_required
@require_permission("can_manage_invoices")
def view_invoice(invoice_id):
    conn = get_db_connection()
    cid = session["company_id"]

    inv = conn.execute(
        """
        SELECT i.*, c.name AS customer_name, c.email AS customer_email
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.id = ? AND i.company_id = ?
        """,
        (invoice_id, cid),
    ).fetchone()

    if not inv:
        conn.close()
        abort(404)

    items = conn.execute(
        """
        SELECT *
        FROM invoice_items
        WHERE invoice_id = ?
        ORDER BY id
        """,
        (invoice_id,),
    ).fetchall()

    payments = conn.execute(
        """
        SELECT *
        FROM invoice_payments
        WHERE invoice_id = ?
        ORDER BY payment_date DESC, id DESC
        """,
        (invoice_id,),
    ).fetchall()

    paid_row = conn.execute(
        """
        SELECT COALESCE(SUM(amount), 0) AS paid_total
        FROM invoice_payments
        WHERE invoice_id = ?
        """,
        (invoice_id,),
    ).fetchone()

    paid_total = float(paid_row["paid_total"] or 0)

    conn.close()

    item_rows = "".join(
        f"<tr><td>{escape(i['description'] or '-')}</td><td>{float(i['quantity'] or 0):g}</td><td>{escape(i['unit'] or '-')}</td><td>${float(i['unit_price'] or 0):.2f}</td><td>${float(i['line_total'] or 0):.2f}</td></tr>"
        for i in items
    )

    payment_rows = "".join(
        f"""
        <tr>
            <td>{escape(p['payment_date'] or '-')}</td>
            <td>${float(p['amount'] or 0):.2f}</td>
            <td>{escape(p['payment_method'] or '-')}</td>
            <td>{escape(p['reference'] or '-')}</td>
            <td>{escape(p['notes'] or '-')}</td>
            <td style='white-space:nowrap;'>
                <a class='btn warning small' href='{url_for("invoices.edit_invoice_payment", invoice_id=invoice_id, payment_id=p["id"])}'>Edit</a>

                <form method='post'
                    action='{url_for("invoices.delete_invoice_payment", invoice_id=invoice_id, payment_id=p["id"])}'
                    style='display:inline;'
                    onsubmit="return confirm('Delete this payment?');">
                    <button class='btn danger small' type='submit'>Delete</button>
                </form>
            </td>
        </tr>
        """
        for p in payments
    )

    if float(inv["balance_due"] or 0) > 0:
        payment_button = f"""
        <form method='post' action='{url_for("invoices.mark_invoice_paid", invoice_id=invoice_id)}' style='display:inline;'>
            <button class='btn success' type='submit'>Mark Paid</button>
        </form>
        """
    else:
        payment_button = f"""
        <form method='post' action='{url_for("invoices.mark_invoice_unpaid", invoice_id=invoice_id)}' style='display:inline;'>
            <button class='btn warning' type='submit'>Mark Unpaid</button>
        </form>
        """

    content = f"""
    <div class='card'>
        <h1>Invoice #{inv['id']} <span class='pill'>{escape(inv['status'] or '-')}</span></h1>
        <p>
            <strong>Customer:</strong> {escape(inv['customer_name'] or '-')}<br>
            <strong>Total:</strong> ${float(inv['total'] or 0):.2f} |
            <strong>Paid:</strong> ${paid_total:.2f} |
            <strong>Balance:</strong> ${float(inv['balance_due'] or 0):.2f}
        </p>

        <div class='row-actions'>
            <a class='btn secondary' href='{url_for("invoices.invoices")}'>Back to Invoices</a>
            <a class='btn' href='{url_for("invoices.email_invoice_preview", invoice_id=invoice_id)}'>Email Invoice</a>
            {payment_button}
        </div>

        <form method='post' action='{url_for("invoices.delete_invoice", invoice_id=invoice_id)}' onsubmit="return confirm('Delete this invoice?');" style='margin-top:10px;'>
            <button class='btn danger' type='submit'>Delete Invoice</button>
        </form>
    </div>

    <div class='card'>
        <h2>Payment Summary</h2>
        <div class='grid'>
            <div><strong>Invoice Total:</strong><br>${float(inv['total'] or 0):.2f}</div>
            <div><strong>Paid So Far:</strong><br>${paid_total:.2f}</div>
            <div><strong>Balance Due:</strong><br>${float(inv['balance_due'] or 0):.2f}</div>
            <div><strong>Status:</strong><br>{escape(inv['status'] or '-')}</div>
        </div>
    </div>

    <div class='card'>
        <h2>Record Payment</h2>
        <form method='post' action='{url_for("invoices.add_invoice_payment", invoice_id=inv["id"])}'>
            <div class='grid'>
                <div>
                    <label>Amount</label>
                    <input type='number' step='0.01' min='0.01' name='amount' required>
                </div>
                <div>
                    <label>Payment Date</label>
                    <input type='date' name='payment_date' value='{date.today().isoformat()}'>
                </div>
                <div>
                    <label>Payment Method</label>
                    <input name='payment_method' placeholder='Cash, Check, Card, ACH'>
                </div>
                <div>
                    <label>Reference</label>
                    <input name='reference' placeholder='Check # or transaction ID'>
                </div>
            </div>
            <br>
            <label>Notes</label>
            <textarea name='notes'></textarea>
            <br>
            <button class='btn success' type='submit'>Add Payment</button>
        </form>
    </div>

    <div class='card'>
        <h2>Payment History</h2>
        <table>
            <tr>
                <th>Date</th>
                <th>Amount</th>
                <th>Method</th>
                <th>Reference</th>
                <th>Notes</th>
                <th>Actions</th>
            </tr>
            {payment_rows or '<tr><td colspan="6" class="muted">No payments recorded yet.</td></tr>'}
        </table>
    </div>

    <div class='card'>
        <h2>Invoice Items</h2>
        <table>
            <tr><th>Description</th><th>Qty</th><th>Unit</th><th>Unit Price</th><th>Line Total</th></tr>
            {item_rows or '<tr><td colspan="5" class="muted">No invoice items.</td></tr>'}
        </table>
    </div>
    """
    return render_page(content, f"Invoice #{invoice_id}")


@invoices_bp.route("/invoices/<int:invoice_id>/email")
@login_required
@require_permission("can_manage_invoices")
def email_invoice_preview(invoice_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT i.*, c.name AS customer_name, c.email AS customer_email
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.id = ? AND i.company_id = ?
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        abort(404)

    recipient = (invoice["customer_email"] or "").strip()

    conn.close()

    preview_url = url_for("invoices.preview_invoice_pdf", invoice_id=invoice_id)
    send_url = url_for("invoices.send_invoice_email", invoice_id=invoice_id)

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Email Invoice #{invoice['id']}</h1>
                <p style='margin:0;'>
                    <strong>Customer:</strong> {escape(invoice['customer_name'] or '-')}<br>
                    <strong>Email:</strong> {escape(recipient or 'No email on file')}<br>
                    <strong>Total:</strong> ${float(invoice['total'] or 0):.2f}
                </p>
            </div>
            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("invoices.view_invoice", invoice_id=invoice_id)}'>Back to Invoice</a>
                <a class='btn secondary' href='{preview_url}' target='_blank'>Open PDF Preview</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Preview</h2>
        <div style='margin-bottom:14px;'>
            <iframe src='{preview_url}' style='width:100%; height:820px; border:1px solid #dbe2ea; border-radius:12px; background:#fff;'></iframe>
        </div>

        {"<div class='notice warning'>This customer does not have an email address yet. Add one before sending.</div>" if not recipient else ""}

        <form method='post' action='{send_url}' onsubmit="return confirm('Send this invoice by email now?');">
            <button class='btn' type='submit' {"disabled" if not recipient else ""}>Send Email Now</button>
        </form>
    </div>
    """
    return render_page(content, f"Email Invoice #{invoice_id}")


@invoices_bp.route("/invoices/<int:invoice_id>/preview_pdf")
@login_required
@require_permission("can_manage_invoices")
def preview_invoice_pdf(invoice_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT i.*, c.name AS customer_name, c.email AS customer_email
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.id = ? AND i.company_id = ?
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        abort(404)

    items = conn.execute(
        "SELECT * FROM invoice_items WHERE invoice_id = ? ORDER BY id",
        (invoice_id,),
    ).fetchall()

    company = conn.execute(
        """
        SELECT name, email, phone, website, address_line_1, address_line_2, city, state, zip_code
        FROM companies
        WHERE id = ?
        """,
        (cid,),
    ).fetchone()

    profile = conn.execute(
        """
        SELECT display_name, legal_name, logo_url, invoice_header_name, invoice_footer_note, email
        FROM company_profile
        WHERE company_id = ?
        """,
        (cid,),
    ).fetchone()

    conn.close()

    pdf_data = build_invoice_pdf(invoice, items, company, profile)

    response = make_response(pdf_data)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f"inline; filename=Invoice_{invoice['invoice_number'] or invoice_id}.pdf"
    return response


@invoices_bp.route("/invoices/<int:invoice_id>/send_email", methods=["POST"])
@login_required
@require_permission("can_manage_invoices")
def send_invoice_email(invoice_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT i.*, c.name AS customer_name, c.email AS customer_email
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.id = ? AND i.company_id = ?
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        abort(404)

    items = conn.execute(
        "SELECT * FROM invoice_items WHERE invoice_id = ? ORDER BY id",
        (invoice_id,),
    ).fetchall()

    company = conn.execute(
        """
        SELECT name, email, phone, website, address_line_1, address_line_2, city, state, zip_code
        FROM companies
        WHERE id = ?
        """,
        (cid,),
    ).fetchone()

    profile = conn.execute(
        """
        SELECT display_name, legal_name, logo_url, invoice_header_name, invoice_footer_note, email
        FROM company_profile
        WHERE company_id = ?
        """,
        (cid,),
    ).fetchone()

    conn.close()

    recipient = (invoice["customer_email"] or "").strip()
    if not recipient:
        flash("This customer does not have an email address.")
        return redirect(url_for("invoices.email_invoice_preview", invoice_id=invoice_id))

    invoice_number = invoice["invoice_number"] or invoice["id"]

    try:
        pdf_data = build_invoice_pdf(invoice, items, company, profile)

        send_company_email(
            company_id=cid,
            to_email=recipient,
            subject=f"Invoice #{invoice_number}",
            body=(
                f"Hello {invoice['customer_name']},\n\n"
                f"Please find attached Invoice #{invoice_number}.\n\n"
                f"Total: ${float(invoice['total'] or 0):.2f}\n\n"
                f"Thank you."
            ),
            attachment_bytes=pdf_data,
            attachment_filename=f"Invoice_{invoice_number}.pdf",
            user_id=session.get("user_id"),
        )

        flash("Invoice emailed successfully as PDF.")

    except Exception as e:
        flash(f"Could not email invoice: {e}")

    return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))


@invoices_bp.route("/invoices/<int:invoice_id>/delete", methods=["POST"])
@login_required
@require_permission("can_manage_invoices")
def delete_invoice(invoice_id):
    conn = get_db_connection()
    invoice = conn.execute(
        "SELECT id FROM invoices WHERE id=? AND company_id=?",
        (invoice_id, session["company_id"]),
    ).fetchone()

    if not invoice:
        conn.close()
        abort(404)

    conn.execute(
        "DELETE FROM ledger_entries WHERE invoice_id=? AND company_id=?",
        (invoice_id, session["company_id"]),
    )
    conn.execute(
        "DELETE FROM invoice_payments WHERE invoice_id=? AND company_id=?",
        (invoice_id, session["company_id"]),
    )
    conn.execute(
        "DELETE FROM invoice_items WHERE invoice_id=?",
        (invoice_id,),
    )
    conn.execute(
        "DELETE FROM invoices WHERE id=? AND company_id=?",
        (invoice_id, session["company_id"]),
    )
    conn.commit()
    conn.close()

    flash("Invoice deleted.")
    return redirect(url_for("invoices.invoices"))