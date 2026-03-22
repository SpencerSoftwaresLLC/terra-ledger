from flask import Blueprint, request, redirect, url_for, session, flash, abort, make_response, current_app
from datetime import date, datetime
from html import escape
import json
import os
import tempfile
import io

from urllib.parse import urlparse
from urllib.request import urlopen

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader

from db import (
    get_db_connection,
    ensure_job_cost_ledger,
    get_next_quote_number,
    ensure_document_number_columns,
    table_columns,
)
from decorators import login_required, require_permission, subscription_required
from page_helpers import *
from helpers import *
from calculations import *
from utils.emailing import send_company_email

quotes_bp = Blueprint("quotes", __name__)


ITEM_TYPE_LABELS = {
    "mulch": "Mulch",
    "stone": "Stone",
    "dump_fee": "Dump Fee",
    "plants": "Plants",
    "trees": "Trees",
    "soil": "Soil",
    "fertilizer": "Fertilizer",
    "hardscape_material": "Hardscape Material",
    "labor": "Labor",
    "equipment": "Equipment",
    "delivery": "Delivery",
    "fuel": "Fuel",
    "misc": "Misc",
    "material": "Material",
}


def _safe_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _display_item_type(value):
    key = (value or "").strip().lower()
    if key in ITEM_TYPE_LABELS:
        return ITEM_TYPE_LABELS[key]
    return key.replace("_", " ").title() if key else "Material"


def ensure_quote_item_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cols = table_columns(conn, "quote_items")

    if "unit_cost" not in cols:
        cur.execute("ALTER TABLE quote_items ADD COLUMN unit_cost DOUBLE PRECISION NOT NULL DEFAULT 0")

    if "item_type" not in cols:
        cur.execute("ALTER TABLE quote_items ADD COLUMN item_type TEXT NOT NULL DEFAULT 'mulch'")

    if "unit" not in cols:
        cur.execute("ALTER TABLE quote_items ADD COLUMN unit TEXT DEFAULT ''")

    conn.commit()
    conn.close()


def build_quote_pdf(quote, items, company, profile):
    pdf_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf_temp.close()

    try:
        quote_number = quote["quote_number"] or quote["id"]

        company_name = (
            profile["quote_header_name"]
            if profile and profile["quote_header_name"]
            else (
                profile["display_name"]
                if profile and profile["display_name"]
                else (company["name"] if company else "Your Company")
            )
        )

        footer_note = profile["quote_footer_note"] if profile and profile["quote_footer_note"] else ""
        logo_url = profile["logo_url"] if profile and profile["logo_url"] else ""

        address_parts = []
        if company:
            if company["address_line_1"] and str(company["address_line_1"]).strip().lower() != "none":
                address_parts.append(company["address_line_1"])
            if company["address_line_2"] and str(company["address_line_2"]).strip().lower() != "none":
                address_parts.append(company["address_line_2"])

            city_state_zip = " ".join(
                part for part in [
                    f"{company['city']}," if company["city"] and str(company["city"]).strip().lower() != "none" else "",
                    company["state"] if company["state"] and str(company["state"]).strip().lower() != "none" else "",
                    company["zip_code"] if company["zip_code"] and str(company["zip_code"]).strip().lower() != "none" else "",
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
                        mask="auto"
                    )

                    text_x = 250
                except Exception:
                    text_x = 50

            c.setFont("Helvetica-Bold", 18)
            c.drawString(text_x, y_pos, str(company_name or "Your Company")[:45])

            c.setFont("Helvetica-Bold", 20)
            c.drawRightString(width - 50, height - 50, "QUOTE")

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
        c.drawString(50, y, f"Quote #: {quote_number}")
        y -= 16
        c.drawString(50, y, f"Customer: {quote['customer_name'] or ''}")
        y -= 16
        c.drawString(50, y, f"Status: {quote['status'] or ''}")
        y -= 16
        c.drawString(50, y, f"Date: {quote['quote_date'] or date.today().isoformat()}")
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
        c.drawRightString(560, y, f"Total: ${float(quote['total'] or 0):.2f}")
        y -= 28

        if quote["notes"]:
            ensure_space(50)
            c.setFont("Helvetica-Bold", 11)
            c.drawString(50, y, "Notes:")
            y -= 18

            c.setFont("Helvetica", 10)
            notes_text = str(quote["notes"])
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


@quotes_bp.route("/quotes", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def quotes():
    ensure_quote_item_columns()
    ensure_document_number_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    customers = conn.execute(
        """
        SELECT id, name, company, email
        FROM customers
        WHERE company_id = %s
        ORDER BY name
        """,
        (cid,),
    ).fetchall()

    company_row = conn.execute(
        """
        SELECT default_quote_notes, next_quote_number
        FROM companies
        WHERE id = %s
        """,
        (cid,),
    ).fetchone()

    default_quote_notes = ""
    next_quote_number_preview = "1001"

    if company_row:
        if "default_quote_notes" in company_row.keys():
            default_quote_notes = company_row["default_quote_notes"] or ""
        if "next_quote_number" in company_row.keys() and company_row["next_quote_number"] is not None:
            next_quote_number_preview = str(company_row["next_quote_number"])

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

        if not customer_id:
            conn.close()
            flash("Please select a customer from the search results.")
            return redirect(url_for("quotes.quotes"))

        quote_number = (request.form.get("quote_number") or "").strip()
        quote_date = (request.form.get("quote_date") or "").strip() or date.today().isoformat()
        expiration_date = (request.form.get("expiration_date") or "").strip()
        status = (request.form.get("status") or "Draft").strip()
        notes = (request.form.get("notes") or "").strip() or default_quote_notes

        if not quote_number:
            quote_number = get_next_quote_number(cid)

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO quotes (
                company_id, customer_id, quote_number, quote_date, expiration_date, status, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                cid,
                customer_id,
                quote_number,
                quote_date,
                expiration_date or None,
                status,
                notes,
            ),
        )

        row = cur.fetchone()
        if not row or "id" not in row:
            conn.rollback()
            conn.close()
            flash("Could not create quote.")
            return redirect(url_for("quotes.quotes"))

        quote_id = row["id"]
        conn.commit()
        conn.close()

        flash(f"Quote #{quote_number} created. Add items next.")
        return redirect(url_for("quotes.view_quote", quote_id=quote_id))

    rows = conn.execute(
        """
        SELECT q.*, c.name AS customer_name
        FROM quotes q
        JOIN customers c ON q.customer_id = c.id
        WHERE q.company_id = %s
          AND COALESCE(q.status, '') != 'Finished'
        ORDER BY q.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    quote_rows = "".join(
        f"""<tr>
            <td>#{r['id']}</td>
            <td>{escape(r['quote_number'] or '-')}</td>
            <td>{escape(r['customer_name'] or '-')}</td>
            <td>${float(r['total'] or 0):.2f}</td>
            <td>{escape(r['status'] or '-')}</td>
            <td>
                <div class='row-actions'>
                    <a class='btn secondary small' href='{url_for("quotes.view_quote", quote_id=r["id"])}'>View</a>
                    <a class='btn small' href='{url_for("quotes.email_quote_preview", quote_id=r["id"])}'>Email</a>
                    <a class='btn success small' href='{url_for("quotes.convert_quote_to_job", quote_id=r["id"])}'>Convert to Job</a>
                    <form method='post'
                          action='{url_for("quotes.delete_quote", quote_id=r["id"])}'
                          style='display:inline;'
                          onsubmit="return confirm('Delete this quote?');">
                        <button class='btn danger small' type='submit'>Delete</button>
                    </form>
                </div>
            </td>
        </tr>"""
        for r in rows
    )

    content = f"""
    <style>
        .customer-search-wrap {{
            position: relative;
        }}

        .customer-results {{
            display: none;
            position: absolute;
            top: calc(100% + 4px);
            left: 0;
            right: 0;
            background: #fff;
            border: 1px solid #dbe2ea;
            border-radius: 10px;
            margin-top: 0;
            box-shadow: 0 8px 20px rgba(0,0,0,.08);
            z-index: 1000;
            max-height: 260px;
            overflow-y: auto;
        }}

        .customer-results.show {{
            display: block;
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

        .grid {{
            align-items: start;
        }}
    </style>

    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <h1 style='margin:0;'>Quotes</h1>
            <div class='row-actions'>
                <a class='btn warning' href='{url_for("quotes.finished_quotes")}'>Finished Quotes</a>
            </div>
        </div>

        <form method='post'>
            <div class='grid'>
                <div class='customer-search-wrap'>
                    <label>Customer</label>
                    <input type='text' id='customer_search' placeholder='Search customer name, company, or email...' autocomplete='off' required>
                    <input type='hidden' name='customer_id' id='customer_id' required>
                    <div id='customer_results' class='customer-results'></div>
                </div>

                <div>
                    <label>Quote Number</label>
                    <input name='quote_number' placeholder='Auto-assigned if left blank'>
                </div>

                <div>
                    <label>Quote Date</label>
                    <input type='date' name='quote_date' value='{date.today().isoformat()}'>
                </div>

                <div>
                    <label>Expiration Date</label>
                    <input type='date' name='expiration_date'>
                </div>

                <div>
                    <label>Status</label>
                    <select name='status'>
                        <option>Draft</option>
                        <option>Sent</option>
                        <option>Approved</option>
                    </select>
                </div>
            </div>

            <br><label>Notes</label><textarea name='notes'>{escape(default_quote_notes)}</textarea><br>
            <button class='btn'>Create Quote</button>
        </form>
    </div>

    <div class='card'>
        <h2>Quote List</h2>
        <div class='table-wrap'>
            <table>
                <tr><th>ID</th><th>Number</th><th>Customer</th><th>Total</th><th>Status</th><th>Actions</th></tr>
                {quote_rows or '<tr><td colspan="6" class="muted">No quotes yet.</td></tr>'}
            </table>
        </div>
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

        function closeResults() {{
            resultsBox.innerHTML = "";
            resultsBox.classList.remove("show");
        }}

        function renderCustomerResults(matches) {{
            if (!matches.length) {{
                resultsBox.innerHTML = "<div class='customer-result-item muted'>No customers found</div>";
                resultsBox.classList.add("show");
                return;
            }}

            resultsBox.innerHTML = matches.map(c => `
                <div class="customer-result-item" data-id="${{c.id}}">
                    <strong>${{escapeHtml(c.name || "Unnamed Customer")}}</strong>
                    ${{c.company ? `<div class="muted small">${{escapeHtml(c.company)}}</div>` : ""}}
                    ${{c.email ? `<div class="muted small">${{escapeHtml(c.email)}}</div>` : ""}}
                </div>
            `).join("");

            resultsBox.classList.add("show");

            document.querySelectorAll(".customer-result-item[data-id]").forEach(item => {{
                item.addEventListener("click", function () {{
                    const id = this.dataset.id;
                    const customer = customers.find(x => String(x.id) === String(id));
                    if (!customer) return;

                    customerIdInput.value = customer.id;
                    searchInput.value = customer.company
                        ? `${{customer.name}} - ${{customer.company}}`
                        : customer.name;

                    closeResults();
                }});
            }});
        }}

        searchInput.addEventListener("input", function () {{
            const q = this.value.trim().toLowerCase();
            customerIdInput.value = "";

            if (!q) {{
                closeResults();
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
                closeResults();
            }}
        }});
    </script>
    """
    return render_page(content, "Quotes")


@quotes_bp.route("/quotes/<int:quote_id>", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_jobs")
def view_quote(quote_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    quote = conn.execute(
        """
        SELECT q.*, c.name AS customer_name, c.email AS customer_email
        FROM quotes q
        JOIN customers c ON q.customer_id = c.id
        WHERE q.id = %s AND q.company_id = %s
        """,
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        abort(404)

    if request.method == "POST":
        item_type = ((request.form.get("item_type") or "mulch").strip().lower() or "mulch")
        description = (request.form.get("description") or "").strip()
        quantity = _safe_float(request.form.get("quantity"))
        unit = (request.form.get("unit") or "").strip()
        unit_price = _safe_float(request.form.get("unit_price"))
        unit_cost = _safe_float(request.form.get("unit_cost"))

        if not description:
            conn.close()
            flash("Description is required.")
            return redirect(url_for("quotes.view_quote", quote_id=quote_id))

        if item_type == "mulch" and not unit:
            unit = "Yards"
        elif item_type == "stone" and not unit:
            unit = "Tons"
        elif item_type == "soil" and not unit:
            unit = "Yards"
        elif item_type == "fertilizer" and not unit:
            unit = "Bags"
        elif item_type == "hardscape_material" and not unit:
            unit = "Tons"
        elif item_type == "plants" and not unit:
            unit = "EA"
        elif item_type == "trees" and not unit:
            unit = "EA"
        elif item_type == "labor" and not unit:
            unit = "hr"
        elif item_type == "dump_fee" and not unit:
            unit = "fee"

        if item_type == "dump_fee":
            if quantity <= 0:
                quantity = 1
            unit_cost = 0.0

        line_total = quantity * unit_price

        conn.execute(
            """
            INSERT INTO quote_items (quote_id, item_type, description, quantity, unit, unit_price, unit_cost, line_total)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (quote_id, item_type, description, quantity, unit, unit_price, unit_cost, line_total),
        )
        recalc_quote(conn, quote_id)
        conn.commit()
        conn.close()

        flash("Quote item added.")
        return redirect(url_for("quotes.view_quote", quote_id=quote_id))

    items = conn.execute(
        "SELECT * FROM quote_items WHERE quote_id = %s ORDER BY id",
        (quote_id,),
    ).fetchall()
    conn.close()

    item_rows = "".join(
        f"""
        <tr>
            <td>{escape(_display_item_type(i['item_type']))}</td>
            <td>{escape(i['description'] or '')}</td>
            <td>{float(i['quantity'] or 0):g}</td>
            <td>{escape(i['unit'] or '-')}</td>
            <td>${float(i['unit_price'] or 0):.2f}</td>
            <td>{"-" if (i['item_type'] or '').strip().lower() == 'dump_fee' else f"${float(i['unit_cost'] or 0):.2f}"}</td>
            <td>${float(i['line_total'] or 0):.2f}</td>
            <td>
                <form method="post"
                      action="{url_for('quotes.delete_quote_item', quote_id=quote_id, item_id=i['id'])}"
                      style="display:inline;"
                      onsubmit="return confirm('Delete this line item?');">
                    <button class="btn danger small" type="submit">Delete</button>
                </form>
            </td>
        </tr>
        """
        for i in items
    )

    content = f"""
        <div class='card'>
            <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
                <div>
                    <h1 style='margin-bottom:6px;'>Quote #{quote['id']} <span class='pill'>{escape(quote['status'] or '-')}</span></h1>
                    <p style='margin:0;'>
                        <strong>Customer:</strong> {escape(quote['customer_name'] or '-')}<br>
                        <strong>Total:</strong> ${float(quote['total'] or 0):.2f}
                    </p>
                </div>
                <div class='row-actions'>
                    <a class='btn secondary' href='{url_for("quotes.quotes")}'>Back to Quotes</a>
                    <a class='btn' href='{url_for("quotes.email_quote_preview", quote_id=quote_id)}'>Email Quote</a>
                    <a class='btn success' href='{url_for("quotes.convert_quote_to_job", quote_id=quote_id)}'>Convert to Job</a>
                </div>
            </div>
        </div>

        <div class='card'>
            <div class='notice' style='margin-bottom:16px;'>
                <strong>Internal pricing note:</strong> “Your Cost (Internal)” is saved for your records and job profit tracking only.
                It is not shown on the customer PDF or email.
            </div>

            <h2>Add Quote Item</h2>
            <form method='post'>
                <div class='grid'>
                    <div>
                        <label>Item Type</label>
                        <select name='item_type' id='quote_item_type' onchange='toggleQuoteItemType()'>
                            <option value='mulch'>Mulch</option>
                            <option value='stone'>Stone</option>
                            <option value='dump_fee'>Dump Fee</option>
                            <option value='plants'>Plants</option>
                            <option value='trees'>Trees</option>
                            <option value='soil'>Soil</option>
                            <option value='fertilizer'>Fertilizer</option>
                            <option value='hardscape_material'>Hardscape Material</option>
                            <option value='labor'>Labor</option>
                            <option value='equipment'>Equipment</option>
                            <option value='delivery'>Delivery</option>
                            <option value='fuel'>Fuel</option>
                            <option value='misc'>Misc</option>
                        </select>
                    </div>
                    <div>
                        <label>Description</label>
                        <input name='description' required>
                    </div>
                    <div>
                        <label id='quantity_label'>Quantity</label>
                        <input name='quantity' id='quote_quantity' type='number' step='0.01' min='0' required>
                    </div>
                    <div>
                        <label>Unit</label>
                        <input name='unit' id='quote_unit' placeholder='Unit'>
                    </div>
                    <div>
                        <label id='unit_price_label'>Sale Price</label>
                        <input name='unit_price' id='quote_unit_price' type='number' step='0.01' min='0' required>
                    </div>
                    <div id='unit_cost_wrap'>
                        <label id='unit_cost_label'>Unit Cost (Internal)</label>
                        <input name='unit_cost' id='quote_unit_cost' type='number' step='0.01' min='0' value='0.00' required>
                    </div>
                </div>
                <br>
                <button class='btn'>Add Item</button>
            </form>
        </div>

        <div class='card'>
            <h2>Items</h2>
            <div class='table-wrap'>
                <table>
                    <tr>
                        <th>Type</th>
                        <th>Description</th>
                        <th>Qty</th>
                        <th>Unit</th>
                        <th>Sale Price / Rate / Fee</th>
                        <th>Unit Cost (Internal)</th>
                        <th>Line Total</th>
                        <th>Actions</th>
                    </tr>
                    {item_rows or '<tr><td colspan="8" class="muted">No items yet.</td></tr>'}
                </table>
            </div>
        </div>

        <script>
            function setFieldValue(id, value, overwriteBlankOnly=false) {{
                var el = document.getElementById(id);
                if (!el) return;
                if (overwriteBlankOnly) {{
                    if (!el.value || !el.value.trim()) {{
                        el.value = value;
                    }}
                }} else {{
                    el.value = value;
                }}
            }}

            function toggleQuoteItemType() {{
                var typeEl = document.getElementById("quote_item_type");
                var quantityLabel = document.getElementById("quantity_label");
                var unitField = document.getElementById("quote_unit");
                var unitPriceLabel = document.getElementById("unit_price_label");
                var unitCostWrap = document.getElementById("unit_cost_wrap");
                var unitCostLabel = document.getElementById("unit_cost_label");
                var quantityInput = document.getElementById("quote_quantity");
                var unitCostInput = document.getElementById("quote_unit_cost");

                if (!typeEl) return;

                var type = typeEl.value;

                if (unitCostWrap) unitCostWrap.style.display = "";
                if (quantityInput) {{
                    quantityInput.readOnly = false;
                    quantityInput.step = "0.01";
                }}

                if (type === "mulch") {{
                    if (quantityLabel) quantityLabel.textContent = "Amount";
                    if (unitPriceLabel) unitPriceLabel.textContent = "Sale Price";
                    if (unitCostLabel) unitCostLabel.textContent = "Unit Cost (Internal)";
                    setFieldValue("quote_unit", "Yards", true);
                }} else if (type === "stone") {{
                    if (quantityLabel) quantityLabel.textContent = "Amount";
                    if (unitPriceLabel) unitPriceLabel.textContent = "Sale Price";
                    if (unitCostLabel) unitCostLabel.textContent = "Unit Cost (Internal)";
                    setFieldValue("quote_unit", "Tons", true);
                }} else if (type === "soil") {{
                    if (quantityLabel) quantityLabel.textContent = "Amount";
                    if (unitPriceLabel) unitPriceLabel.textContent = "Sale Price";
                    if (unitCostLabel) unitCostLabel.textContent = "Unit Cost (Internal)";
                    setFieldValue("quote_unit", "Yards", true);
                }} else if (type === "fertilizer") {{
                    if (quantityLabel) quantityLabel.textContent = "Amount";
                    if (unitPriceLabel) unitPriceLabel.textContent = "Sale Price";
                    if (unitCostLabel) unitCostLabel.textContent = "Unit Cost (Internal)";
                    setFieldValue("quote_unit", "Bags", true);
                }} else if (type === "hardscape_material") {{
                    if (quantityLabel) quantityLabel.textContent = "Amount";
                    if (unitPriceLabel) unitPriceLabel.textContent = "Sale Price";
                    if (unitCostLabel) unitCostLabel.textContent = "Unit Cost (Internal)";
                    setFieldValue("quote_unit", "Tons", true);
                }} else if (type === "plants") {{
                    if (quantityLabel) quantityLabel.textContent = "Quantity";
                    if (unitPriceLabel) unitPriceLabel.textContent = "Sale Price";
                    if (unitCostLabel) unitCostLabel.textContent = "Unit Cost (Internal)";
                    setFieldValue("quote_unit", "EA", true);
                }} else if (type === "trees") {{
                    if (quantityLabel) quantityLabel.textContent = "Quantity";
                    if (unitPriceLabel) unitPriceLabel.textContent = "Sale Price";
                    if (unitCostLabel) unitCostLabel.textContent = "Unit Cost (Internal)";
                    setFieldValue("quote_unit", "EA", true);
                }} else if (type === "labor") {{
                    if (quantityLabel) quantityLabel.textContent = "Hours";
                    if (unitPriceLabel) unitPriceLabel.textContent = "Sale Rate / Hr";
                    if (unitCostLabel) unitCostLabel.textContent = "Unit Cost / Hr (Internal)";
                    setFieldValue("quote_unit", "hr", true);
                }} else if (type === "dump_fee") {{
                    if (quantityLabel) quantityLabel.textContent = "Fees";
                    if (unitPriceLabel) unitPriceLabel.textContent = "Fee Amount";
                    setFieldValue("quote_unit", "fee", true);
                    if (quantityInput) {{
                        quantityInput.value = "none";
                        quantityInput.readOnly = true;
                    }}
                    if (unitCostWrap) unitCostWrap.style.display = "none";
                    if (unitCostInput) unitCostInput.value = "none";
                }} else {{
                    if (quantityLabel) quantityLabel.textContent = "Quantity";
                    if (unitPriceLabel) unitPriceLabel.textContent = "Sale Price";
                    if (unitCostLabel) unitCostLabel.textContent = "Unit Cost (Internal)";
                }}
            }}

            document.addEventListener("DOMContentLoaded", function () {{
                toggleQuoteItemType();
            }});
        </script>
        """
    return render_page(content, f"Quote #{quote_id}")


@quotes_bp.route("/quotes/<int:quote_id>/email")
@login_required
@require_permission("can_manage_jobs")
def email_quote_preview(quote_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    quote = conn.execute(
        """
        SELECT q.*, c.name AS customer_name, c.email AS customer_email
        FROM quotes q
        JOIN customers c ON q.customer_id = c.id
        WHERE q.id = %s AND q.company_id = %s
        """,
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        abort(404)

    recipient = (quote["customer_email"] or "").strip()
    conn.close()

    preview_url = url_for("quotes.preview_quote_pdf", quote_id=quote_id)
    send_url = url_for("quotes.send_quote_email", quote_id=quote_id)

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Email Quote #{quote['id']}</h1>
                <p style='margin:0;'>
                    <strong>Customer:</strong> {escape(quote['customer_name'] or '-')}<br>
                    <strong>Email:</strong> {escape(recipient or 'No email on file')}<br>
                    <strong>Total:</strong> ${float(quote['total'] or 0):.2f}
                </p>
            </div>
            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("quotes.view_quote", quote_id=quote_id)}'>Back to Quote</a>
                <a class='btn secondary' href='{preview_url}' target='_blank'>Open PDF Preview</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <div class='notice' style='margin-bottom:16px;'>
            Customer-facing quote delivery hides all internal cost fields. Only description, quantity, unit, price, and totals are shown.
        </div>

        <h2>Preview</h2>
        <div style='margin-bottom:14px;'>
            <iframe src='{preview_url}' style='width:100%; height:820px; border:1px solid #dbe2ea; border-radius:12px; background:#fff;'></iframe>
        </div>

        {"<div class='notice warning'>This customer does not have an email address yet. Add one before sending.</div>" if not recipient else ""}

        <form method='post' action='{send_url}' onsubmit="return confirm('Send this quote by email now?');">
            <button class='btn' type='submit' {"disabled" if not recipient else ""}>Send Email Now</button>
        </form>
    </div>
    """
    return render_page(content, f"Email Quote #{quote_id}")


@quotes_bp.route("/quotes/<int:quote_id>/preview_pdf")
@login_required
@require_permission("can_manage_jobs")
def preview_quote_pdf(quote_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    quote = conn.execute(
        """
        SELECT q.*, c.name AS customer_name, c.email AS customer_email
        FROM quotes q
        JOIN customers c ON q.customer_id = c.id
        WHERE q.id = %s AND q.company_id = %s
        """,
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        abort(404)

    items = conn.execute(
        "SELECT * FROM quote_items WHERE quote_id = %s ORDER BY id",
        (quote_id,),
    ).fetchall()

    company = conn.execute(
        """
        SELECT name, email, phone, website, address_line_1, address_line_2, city, state, zip_code
        FROM companies
        WHERE id = %s
        """,
        (cid,),
    ).fetchone()

    profile = conn.execute(
        """
        SELECT display_name, legal_name, logo_url, quote_header_name, quote_footer_note, email
        FROM company_profile
        WHERE company_id = %s
        """,
        (cid,),
    ).fetchone()

    conn.close()

    pdf_data = build_quote_pdf(quote, items, company, profile)

    response = make_response(pdf_data)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f"inline; filename=Quote_{quote['quote_number'] or quote_id}.pdf"
    return response


@quotes_bp.route("/quotes/<int:quote_id>/send_email", methods=["POST"])
@login_required
@require_permission("can_manage_jobs")
def send_quote_email(quote_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    quote = conn.execute(
        """
        SELECT q.*, c.name AS customer_name, c.email AS customer_email
        FROM quotes q
        JOIN customers c ON q.customer_id = c.id
        WHERE q.id = %s AND q.company_id = %s
        """,
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        abort(404)

    items = conn.execute(
        "SELECT * FROM quote_items WHERE quote_id = %s ORDER BY id",
        (quote_id,),
    ).fetchall()

    company = conn.execute(
        """
        SELECT name, email, phone, website, address_line_1, address_line_2, city, state, zip_code
        FROM companies
        WHERE id = %s
        """,
        (cid,),
    ).fetchone()

    profile = conn.execute(
        """
        SELECT display_name, legal_name, logo_url, quote_header_name, quote_footer_note, email
        FROM company_profile
        WHERE company_id = %s
        """,
        (cid,),
    ).fetchone()

    conn.close()

    recipient = (quote["customer_email"] or "").strip()
    if not recipient:
        flash("This customer does not have an email address.")
        return redirect(url_for("quotes.email_quote_preview", quote_id=quote_id))

    quote_number = quote["quote_number"] or quote["id"]

    try:
        pdf_data = build_quote_pdf(quote, items, company, profile)

        send_company_email(
            company_id=cid,
            to_email=recipient,
            subject=f"Quote #{quote_number}",
            body=(
                f"Hello {quote['customer_name']},\n\n"
                f"Please find attached Quote #{quote_number}.\n\n"
                f"Total: ${float(quote['total'] or 0):.2f}\n\n"
                f"Thank you."
            ),
            attachment_bytes=pdf_data,
            attachment_filename=f"Quote_{quote_number}.pdf",
            user_id=session.get("user_id"),
        )

        flash("Quote emailed successfully as PDF.")

    except Exception as e:
        flash(f"Could not email quote: {e}")

    return redirect(url_for("quotes.view_quote", quote_id=quote_id))


@quotes_bp.route("/quotes/<int:quote_id>/convert_to_job")
@login_required
@require_permission("can_manage_jobs")
def convert_quote_to_job(quote_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    try:
        quote = conn.execute(
            """
            SELECT *
            FROM quotes
            WHERE id = %s AND company_id = %s
            """,
            (quote_id, cid),
        ).fetchone()

        if not quote:
            abort(404)

        existing_job = conn.execute(
            """
            SELECT id
            FROM jobs
            WHERE quote_id = %s AND company_id = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (quote_id, cid),
        ).fetchone()

        if existing_job:
            flash("This quote has already been converted to a job.")
            return redirect(url_for("jobs.view_job", job_id=existing_job["id"]))

        items = conn.execute(
            """
            SELECT *
            FROM quote_items
            WHERE quote_id = %s
            ORDER BY id
            """,
            (quote_id,),
        ).fetchall()

        if not items:
            flash("This quote has no items to convert.")
            return redirect(url_for("quotes.view_quote", quote_id=quote_id))

        quote_number = quote["quote_number"] or quote_id
        quote_title = (quote["title"] or "").strip() if "title" in quote.keys() and quote["title"] else ""
        quote_notes = (quote["notes"] or "").strip() if "notes" in quote.keys() and quote["notes"] else ""
        job_title = quote_title or f"Job from Quote {quote_number}"

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO jobs (
                company_id,
                customer_id,
                quote_id,
                title,
                scheduled_date,
                status,
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                quote["company_id"],
                quote["customer_id"],
                quote_id,
                job_title,
                date.today(),
                "Scheduled",
                quote_notes,
            ),
        )
        row = cur.fetchone()
        if not row or "id" not in row:
            raise Exception("Failed to create job record.")

        job_id = row["id"]

        for i in items:
            qty = float(i["quantity"] or 0)
            sale_price = float(i["unit_price"] or 0)
            unit_cost = float(i["unit_cost"] or 0)
            raw_item_type = ((i["item_type"] or "").strip().lower() if "item_type" in i.keys() and i["item_type"] else "")
            desc = (i["description"] or "").strip()
            unit = (i["unit"] or "").strip()

            item_type = raw_item_type or "mulch"

            if item_type == "mulch" and not unit:
                unit = "Yards"
            elif item_type == "stone" and not unit:
                unit = "Tons"
            elif item_type == "soil" and not unit:
                unit = "Yards"
            elif item_type == "fertilizer" and not unit:
                unit = "Bags"
            elif item_type == "hardscape_material" and not unit:
                unit = "Tons"
            elif item_type == "plants" and not unit:
                unit = "EA"
            elif item_type == "trees" and not unit:
                unit = "EA"
            elif item_type == "labor" and not unit:
                unit = "hr"
            elif item_type == "dump_fee" and not unit:
                unit = "fee"

            if item_type == "dump_fee" and qty <= 0:
                qty = 1

            line_total = qty * sale_price
            cost_amount = qty * unit_cost

            cur.execute(
                """
                INSERT INTO job_items (
                    job_id,
                    item_type,
                    description,
                    quantity,
                    unit,
                    unit_cost,
                    unit_price,
                    sale_price,
                    cost_amount,
                    line_total,
                    billable
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    job_id,
                    item_type,
                    desc,
                    qty,
                    unit,
                    unit_cost,
                    sale_price,
                    sale_price,
                    cost_amount,
                    line_total,
                    1,
                ),
            )

            item_row = cur.fetchone()
            if not item_row or "id" not in item_row:
                raise Exception(f"Failed to create job item for quote item {i['id']}.")

            job_item_id = item_row["id"]
            ensure_job_cost_ledger(conn, job_item_id)

        recalc_job(conn, job_id)

        cur.execute(
            """
            UPDATE quotes
            SET status = %s
            WHERE id = %s AND company_id = %s
            """,
            ("Converted", quote_id, cid),
        )

        conn.commit()
        flash("Quote converted to job.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    except Exception as e:
        conn.rollback()
        flash(f"Could not convert quote to job: {e}")
        return redirect(url_for("quotes.view_quote", quote_id=quote_id))

    finally:
        conn.close()


@quotes_bp.route("/quotes/<int:quote_id>/delete", methods=["POST"])
@login_required
@require_permission("can_manage_jobs")
def delete_quote(quote_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    quote = conn.execute(
        "SELECT id FROM quotes WHERE id = %s AND company_id = %s",
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        flash("Quote not found.")
        return redirect(url_for("quotes.quotes"))

    conn.execute("DELETE FROM quote_items WHERE quote_id = %s", (quote_id,))
    conn.execute(
        "DELETE FROM quotes WHERE id = %s AND company_id = %s",
        (quote_id, cid),
    )

    conn.commit()
    conn.close()

    flash("Quote deleted.")
    return redirect(url_for("quotes.quotes"))


@quotes_bp.route("/quotes/<int:quote_id>/items/<int:item_id>/delete", methods=["POST"])
@login_required
@require_permission("can_manage_jobs")
def delete_quote_item(quote_id, item_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    quote = conn.execute(
        "SELECT id FROM quotes WHERE id = %s AND company_id = %s",
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        flash("Quote not found.")
        return redirect(url_for("quotes.quotes"))

    item = conn.execute(
        """
        SELECT qi.id
        FROM quote_items qi
        JOIN quotes q ON qi.quote_id = q.id
        WHERE qi.id = %s AND qi.quote_id = %s AND q.company_id = %s
        """,
        (item_id, quote_id, cid),
    ).fetchone()

    if not item:
        conn.close()
        flash("Quote item not found.")
        return redirect(url_for("quotes.view_quote", quote_id=quote_id))

    conn.execute(
        "DELETE FROM quote_items WHERE id = %s AND quote_id = %s",
        (item_id, quote_id),
    )

    recalc_quote(conn, quote_id)
    conn.commit()
    conn.close()

    flash("Quote line item deleted.")
    return redirect(url_for("quotes.view_quote", quote_id=quote_id))


@quotes_bp.route("/quotes/finished")
@login_required
@require_permission("can_manage_jobs")
def finished_quotes():
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT q.*, c.name AS customer_name
        FROM quotes q
        JOIN customers c ON q.customer_id = c.id
        WHERE q.company_id = %s
          AND q.status = 'Finished'
        ORDER BY q.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    quote_rows = "".join(
        f"""
        <tr>
            <td>#{r['id']}</td>
            <td>{escape(r['quote_number'] or '-')}</td>
            <td>{escape(r['customer_name'] or '-')}</td>
            <td>${float(r['total'] or 0):.2f}</td>
            <td>{escape(r['status'] or '-')}</td>
            <td>
                <div class='row-actions'>
                    <a class='btn secondary small' href='{url_for("quotes.view_quote", quote_id=r["id"])}'>View</a>
                    <a class='btn warning small' href='{url_for("quotes.reopen_quote", quote_id=r["id"])}'>Reopen</a>
                </div>
            </td>
        </tr>
        """
        for r in rows
    )

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin:0;'>Finished Quotes</h1>
                <p class='muted' style='margin:6px 0 0 0;'>Quotes tied to fully paid work.</p>
            </div>
            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("quotes.quotes")}'>Back to Active Quotes</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <div class='table-wrap'>
            <table>
                <tr><th>ID</th><th>Number</th><th>Customer</th><th>Total</th><th>Status</th><th>Actions</th></tr>
                {quote_rows or '<tr><td colspan="6" class="muted">No finished quotes yet.</td></tr>'}
            </table>
        </div>
    </div>
    """
    return render_page(content, "Finished Quotes")


@quotes_bp.route("/quotes/<int:quote_id>/reopen")
@login_required
@require_permission("can_manage_jobs")
def reopen_quote(quote_id):
    conn = get_db_connection()
    cid = session["company_id"]

    quote = conn.execute(
        """
        SELECT id
        FROM quotes
        WHERE id = %s AND company_id = %s
        """,
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        flash("Quote not found.")
        return redirect(url_for("quotes.finished_quotes"))

    conn.execute(
        """
        UPDATE quotes
        SET status = 'Converted'
        WHERE id = %s AND company_id = %s
        """,
        (quote_id, cid),
    )

    conn.commit()
    conn.close()

    flash("Quote reopened.")
    return redirect(url_for("quotes.view_quote", quote_id=quote_id))