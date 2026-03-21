from flask import Blueprint, request, redirect, url_for, session, flash, make_response, abort
from datetime import date, datetime
from html import escape
import json
import io
import csv

from db import get_db_connection, ensure_job_cost_ledger
from decorators import login_required, require_permission, subscription_required
from page_helpers import *
from helpers import *
from calculations import recalc_job, recalc_invoice

jobs_bp = Blueprint("jobs", __name__)


def clean_text_input(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"none", "null", "n/a", "0", "0.0", "0.00"}:
        return ""
    return text


def clean_text_display(value, fallback="-"):
    text = clean_text_input(value)
    return text if text else fallback


def safe_float(value, default=0.0):
    try:
        return float(value or 0)
    except Exception:
        return default


@jobs_bp.route("/jobs", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def jobs():
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

    customer_list = [
        {
            "id": c["id"],
            "name": clean_text_input(c["name"]),
            "company": clean_text_input(c["company"]),
            "email": clean_text_input(c["email"]),
        }
        for c in customers
    ]

    if request.method == "POST":
        customer_id = request.form.get("customer_id", type=int)

        if not customer_id:
            conn.close()
            flash("Please select a customer from the search results.")
            return redirect(url_for("jobs.jobs"))

        title = clean_text_input(request.form.get("title", ""))
        scheduled_date = clean_text_input(request.form.get("scheduled_date", ""))
        status = clean_text_input(request.form.get("status", "Scheduled")) or "Scheduled"
        address = clean_text_input(request.form.get("address", ""))
        notes = clean_text_input(request.form.get("notes", ""))

        if not title:
            conn.close()
            flash("Job title is required.")
            return redirect(url_for("jobs.jobs"))

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO jobs (company_id, customer_id, title, scheduled_date, status, address, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (cid, customer_id, title, scheduled_date or None, status, address, notes),
        )
        job_id = cur.fetchone()[0]
        conn.commit()
        conn.close()

        flash("Job created.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    rows = conn.execute(
        """
        SELECT j.*, c.name AS customer_name
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.company_id = %s
          AND COALESCE(j.status, '') != 'Finished'
        ORDER BY j.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    job_rows = "".join(
        f"""
        <tr>
            <td>#{r['id']}</td>
            <td>{escape(clean_text_display(r['title']))}</td>
            <td>{escape(clean_text_display(r['customer_name']))}</td>
            <td>{escape(clean_text_display(r['status']))}</td>
            <td>${safe_float(r['revenue']):.2f}</td>
            <td>${safe_float(r['cost_total']):.2f}</td>
            <td>${safe_float(r['profit']):.2f}</td>
            <td>
                <div class='row-actions'>
                    <a class='btn secondary small' href='{url_for("jobs.view_job", job_id=r["id"])}'>View</a>
                    <a class='btn warning small' href='{url_for("jobs.edit_job", job_id=r["id"])}'>Edit Job</a>
                    <a class='btn success small' href='{url_for("jobs.convert_job_to_invoice", job_id=r["id"])}'>Convert to Invoice</a>
                    <form method='post'
                          action='{url_for("jobs.delete_job", job_id=r["id"])}'
                          style='display:inline;'
                          onsubmit="return confirm('Delete this job and all items?');">
                        <button class='btn danger small' type='submit'>Delete Job</button>
                    </form>
                </div>
            </td>
        </tr>
        """
        for r in rows
    )

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
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <h1 style='margin:0;'>Jobs</h1>
            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("jobs.export_jobs")}'>Export CSV</a>
                <a class='btn warning' href='{url_for("jobs.finished_jobs")}'>Finished Jobs</a>
            </div>
        </div>

        <form method='post' style='margin-top:18px;'>
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
                    <label>Title</label>
                    <input name='title' required>
                </div>

                <div>
                    <label>Scheduled Date</label>
                    <input type='date' name='scheduled_date'>
                </div>

                <div>
                    <label>Status</label>
                    <select name='status'>
                        <option>Scheduled</option>
                        <option>In Progress</option>
                        <option>Completed</option>
                    </select>
                </div>

                <div>
                    <label>Address</label>
                    <input name='address'>
                </div>
            </div>

            <br>
            <label>Notes</label>
            <textarea name='notes'></textarea>
            <br>
            <button class='btn'>Create Job</button>
        </form>
    </div>

    <div class='card'>
        <h2>Job List</h2>
        <div class='table-wrap'>
            <table>
                <tr>
                    <th>ID</th>
                    <th>Title</th>
                    <th>Customer</th>
                    <th>Status</th>
                    <th>Revenue</th>
                    <th>Costs</th>
                    <th>Profit/Loss</th>
                    <th>Actions</th>
                </tr>
                {job_rows or '<tr><td colspan="8" class="muted">No jobs yet.</td></tr>'}
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
                        : (customer.name || "Unnamed Customer");

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
    return render_page(content, "Jobs")


@jobs_bp.route("/jobs/export")
@login_required
@require_permission("can_manage_jobs")
def export_jobs():
    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT
            j.id,
            j.title,
            j.scheduled_date,
            j.status,
            j.address,
            j.notes,
            j.revenue,
            j.cost_total,
            j.profit,
            c.name AS customer_name,
            c.email AS customer_email
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.company_id = %s
        ORDER BY j.id DESC
        """,
        (cid,),
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Job ID",
        "Title",
        "Customer",
        "Customer Email",
        "Scheduled Date",
        "Status",
        "Address",
        "Revenue",
        "Costs",
        "Profit/Loss",
        "Notes",
    ])

    for r in rows:
        writer.writerow([
            r["id"] or "",
            clean_text_input(r["title"]),
            clean_text_input(r["customer_name"]),
            clean_text_input(r["customer_email"]),
            clean_text_input(r["scheduled_date"]),
            clean_text_input(r["status"]),
            clean_text_input(r["address"]),
            safe_float(r["revenue"]),
            safe_float(r["cost_total"]),
            safe_float(r["profit"]),
            clean_text_input(r["notes"]),
        ])

    conn.close()

    csv_data = output.getvalue()
    output.close()

    filename = f"jobs_export_{date.today().isoformat()}.csv"

    response = make_response(csv_data)
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response


@jobs_bp.route("/jobs/<int:job_id>", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_jobs")
def view_job(job_id):
    conn = get_db_connection()
    cid = session["company_id"]

    job = conn.execute(
        """
        SELECT j.*, c.name AS customer_name
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.id = %s AND j.company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    if not job:
        conn.close()
        abort(404)

    if request.method == "POST":
        item_type = clean_text_input(request.form.get("item_type", "")).lower()
        description = clean_text_input(request.form.get("description", ""))
        qty = safe_float(request.form.get("quantity"))
        unit = clean_text_input(request.form.get("unit", ""))
        sale_price = safe_float(request.form.get("sale_price"))
        unit_cost = safe_float(request.form.get("cost_amount"))
        billable = 1 if request.form.get("billable") == "1" else 0

        if not description:
            conn.close()
            flash("Description is required.")
            return redirect(url_for("jobs.view_job", job_id=job_id))

        if item_type == "labor":
            if not unit:
                unit = "hrs"
            sale_price = unit_cost
            line_total = qty * unit_cost
        else:
            line_total = qty * sale_price

        cost_amount = qty * unit_cost

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO job_items (
                job_id, item_type, description, quantity, unit,
                unit_price, sale_price, cost_amount, line_total, billable
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                job_id,
                item_type,
                description,
                qty,
                unit,
                sale_price,
                sale_price,
                cost_amount,
                line_total,
                billable,
            ),
        )
        job_item_id = cur.fetchone()[0]

        ensure_job_cost_ledger(conn, job_item_id)
        recalc_job(conn, job_id)
        conn.commit()
        conn.close()

        flash("Job item added and bookkeeping updated.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    items = conn.execute(
        "SELECT * FROM job_items WHERE job_id = %s ORDER BY id",
        (job_id,),
    ).fetchall()

    conn.close()

    item_rows = "".join(
        f"""
        <tr>
            <td>{escape(clean_text_display(i['item_type']))}</td>
            <td>{escape(clean_text_display(i['description']))}</td>
            <td>{safe_float(i['quantity']):g}</td>
            <td>{escape(clean_text_display(i['unit']))}</td>
            <td>{'-' if clean_text_input(i['item_type']).lower() == 'labor' else f"${safe_float(i['sale_price']):.2f}"}</td>
            <td>${((safe_float(i['cost_amount']) / safe_float(i['quantity'])) if safe_float(i['quantity']) else 0):.2f}</td>
            <td>${safe_float(i['cost_amount']):.2f}</td>
            <td>{'Yes' if i['billable'] else 'No'}</td>
            <td>${safe_float(i['line_total']):.2f}</td>
            <td>
                <div class='row-actions'>
                    <a class='btn secondary small' href='{url_for("jobs.edit_job_item", job_id=job_id, item_id=i["id"])}'>Edit</a>
                    <form method='post'
                          action='{url_for("jobs.delete_job_item", job_id=job_id, item_id=i["id"])}'
                          style='display:inline;'
                          onsubmit="return confirm('Delete this job item?');">
                        <button class='btn danger small' type='submit'>Delete</button>
                    </form>
                </div>
            </td>
        </tr>
        """
        for i in items
    )

    content = f"""
        <div class='card'>
            <h1>Job #{job['id']} - {escape(clean_text_display(job['title']))}</h1>
            <p>
                <strong>Customer:</strong> {escape(clean_text_display(job['customer_name']))}<br>
                <strong>Status:</strong> {escape(clean_text_display(job['status']))}<br>
                <strong>Revenue:</strong> ${safe_float(job['revenue']):.2f}
                |
                <strong>Costs:</strong> ${safe_float(job['cost_total']):.2f}
                |
                <strong>Profit/Loss:</strong> ${safe_float(job['profit']):.2f}
            </p>

            <div class="row-actions">
                <a class='btn secondary' href='{url_for("jobs.jobs")}'>Done Editing</a>
                <a class='btn warning' href='{url_for("jobs.edit_job", job_id=job_id)}'>Edit Job</a>
                <a class='btn success' href='{url_for("jobs.convert_job_to_invoice", job_id=job_id)}'>Convert to Invoice</a>
            </div>
        </div>

        <div class='card'>
            <h2>Add Job Item</h2>
            <p class='muted'>Any cost you enter here is automatically pushed into bookkeeping as an expense.</p>

            <form method='post'>
                <div class='grid'>

                    <div>
                        <label>Type</label>
                        <select name='item_type' id='item_type' onchange='toggleLaborMode()'>
                            <option value='material'>material</option>
                            <option value='labor'>labor</option>
                            <option value='fuel'>fuel</option>
                            <option value='equipment'>equipment</option>
                            <option value='delivery'>delivery</option>
                            <option value='misc'>misc</option>
                        </select>
                    </div>

                    <div>
                        <label>Description</label>
                        <input name='description' required>
                    </div>

                    <div>
                        <label id='quantity_label'>Quantity</label>
                        <input type='number' step='0.01' name='quantity' id='quantity' required>
                    </div>

                    <div>
                        <label>Unit</label>
                        <input name='unit' id='unit' placeholder='yards, hrs, ea'>
                    </div>

                    <div id='sale_price_wrap'>
                        <label>Sale Price</label>
                        <input type='number' step='0.01' name='sale_price' id='sale_price' value='0'>
                    </div>

                    <div>
                        <label id='cost_label'>Unit Cost</label>
                        <input type='number' step='0.01' name='cost_amount' id='cost_amount' value='0'>
                    </div>

                    <div>
                        <label>Billable?</label>
                        <select name='billable'>
                            <option value='1'>Yes</option>
                            <option value='0'>No</option>
                        </select>
                    </div>

                </div>

                <br>
                <button class='btn' type='submit'>Add Job Item</button>
            </form>
        </div>

        <script>
        function toggleLaborMode() {{
            const type = document.getElementById('item_type').value;

            const quantityLabel = document.getElementById('quantity_label');
            const costLabel = document.getElementById('cost_label');
            const salePriceWrap = document.getElementById('sale_price_wrap');
            const unitInput = document.getElementById('unit');

            if (type === 'labor') {{
                quantityLabel.innerText = 'Billable Hours';
                costLabel.innerText = 'Hourly Rate';
                salePriceWrap.style.display = 'none';
                if (unitInput && !unitInput.value) {{
                    unitInput.value = 'hrs';
                }}
            }} else {{
                quantityLabel.innerText = 'Quantity';
                costLabel.innerText = 'Unit Cost';
                salePriceWrap.style.display = 'block';
                if (unitInput && unitInput.value === 'hrs') {{
                    unitInput.value = '';
                }}
            }}
        }}

        document.addEventListener('DOMContentLoaded', function() {{
            toggleLaborMode();
        }});
        </script>

        <div class='card'>
            <h2>Job Items</h2>
            <div class='table-wrap'>
                <table>
                    <tr>
                        <th>Type</th>
                        <th>Description</th>
                        <th>Qty</th>
                        <th>Unit</th>
                        <th>Sale Price</th>
                        <th>Unit Cost</th>
                        <th>Total Cost</th>
                        <th>Billable</th>
                        <th>Revenue</th>
                        <th>Actions</th>
                    </tr>
                    {item_rows or '<tr><td colspan="10" class="muted">No job items yet.</td></tr>'}
                </table>
            </div>
        </div>
        """
    return render_page(content, f"Job #{job_id}")


@jobs_bp.route("/jobs/<int:job_id>/edit", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_jobs")
def edit_job(job_id):
    conn = get_db_connection()
    cid = session["company_id"]

    job = conn.execute(
        """
        SELECT *
        FROM jobs
        WHERE id = %s AND company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    if not job:
        conn.close()
        flash("Job not found.")
        return redirect(url_for("jobs.jobs"))

    customers = conn.execute(
        "SELECT id, name FROM customers WHERE company_id = %s ORDER BY name",
        (cid,),
    ).fetchall()

    if request.method == "POST":
        customer_id = request.form.get("customer_id", type=int)
        title = clean_text_input(request.form.get("title", ""))
        scheduled_date = clean_text_input(request.form.get("scheduled_date", ""))
        status = clean_text_input(request.form.get("status", ""))
        address = clean_text_input(request.form.get("address", ""))
        notes = clean_text_input(request.form.get("notes", ""))

        if not customer_id or not title:
            conn.close()
            flash("Customer and title are required.")
            return redirect(url_for("jobs.edit_job", job_id=job_id))

        conn.execute(
            """
            UPDATE jobs
            SET customer_id = %s, title = %s, scheduled_date = %s, status = %s, address = %s, notes = %s
            WHERE id = %s AND company_id = %s
            """,
            (customer_id, title, scheduled_date or None, status, address, notes, job_id, cid),
        )
        conn.commit()
        conn.close()

        flash("Job updated.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    customer_opts = "".join(
        f"<option value='{c['id']}' {'selected' if c['id'] == job['customer_id'] else ''}>{escape(clean_text_display(c['name'], 'Customer #' + str(c['id'])))}</option>"
        for c in customers
    )

    content = f"""
    <div class='card'>
        <h1>Edit Job #{job['id']}</h1>
        <form method='post'>
            <div class='grid'>
                <div>
                    <label>Customer</label>
                    <select name='customer_id' required>
                        <option value=''>Select customer</option>
                        {customer_opts}
                    </select>
                </div>
                <div>
                    <label>Title</label>
                    <input name='title' value="{escape(clean_text_input(job['title']))}" required>
                </div>
                <div>
                    <label>Scheduled Date</label>
                    <input type='date' name='scheduled_date' value="{escape(clean_text_input(job['scheduled_date']))}">
                </div>
                <div>
                    <label>Status</label>
                    <select name='status'>
                        <option {'selected' if job['status'] == 'Scheduled' else ''}>Scheduled</option>
                        <option {'selected' if job['status'] == 'In Progress' else ''}>In Progress</option>
                        <option {'selected' if job['status'] == 'Completed' else ''}>Completed</option>
                    </select>
                </div>
                <div>
                    <label>Address</label>
                    <input name='address' value="{escape(clean_text_input(job['address']))}">
                </div>
            </div>
            <br>
            <label>Notes</label>
            <textarea name='notes'>{escape(clean_text_input(job['notes']))}</textarea>
            <br>
            <button class='btn'>Save Changes</button>
            <a class='btn secondary' href='{url_for("jobs.view_job", job_id=job_id)}'>Cancel</a>
        </form>
    </div>
    """
    conn.close()
    return render_page(content, f"Edit Job #{job['id']}")


@jobs_bp.route("/jobs/<int:job_id>/items/<int:item_id>/edit", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_jobs")
def edit_job_item(job_id, item_id):
    conn = get_db_connection()
    cid = session["company_id"]

    job = conn.execute(
        """
        SELECT j.*, c.name AS customer_name
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.id = %s AND j.company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    if not job:
        conn.close()
        abort(404)

    item = conn.execute(
        """
        SELECT ji.*
        FROM job_items ji
        JOIN jobs j ON ji.job_id = j.id
        WHERE ji.id = %s AND ji.job_id = %s AND j.company_id = %s
        """,
        (item_id, job_id, cid),
    ).fetchone()

    if not item:
        conn.close()
        flash("Job item not found.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    if request.method == "POST":
        item_type = clean_text_input(request.form.get("item_type", "")).lower()
        description = clean_text_input(request.form.get("description", ""))
        unit = clean_text_input(request.form.get("unit", ""))
        qty = safe_float(request.form.get("quantity"))
        sale_price = safe_float(request.form.get("sale_price"))
        unit_cost = safe_float(request.form.get("cost_amount"))
        billable = 1 if request.form.get("billable") == "1" else 0

        if not description:
            conn.close()
            flash("Description is required.")
            return redirect(url_for("jobs.edit_job_item", job_id=job_id, item_id=item_id))

        if item_type == "labor":
            if not unit:
                unit = "hrs"
            sale_price = unit_cost
            line_total = qty * unit_cost
        else:
            line_total = qty * sale_price

        cost_amount = qty * unit_cost

        conn.execute(
            """
            UPDATE job_items
            SET item_type = %s,
                description = %s,
                quantity = %s,
                unit = %s,
                unit_price = %s,
                sale_price = %s,
                cost_amount = %s,
                line_total = %s,
                billable = %s
            WHERE id = %s AND job_id = %s
            """,
            (
                item_type,
                description,
                qty,
                unit,
                sale_price,
                sale_price,
                cost_amount,
                line_total,
                billable,
                item_id,
                job_id,
            ),
        )

        ensure_job_cost_ledger(conn, item_id)
        recalc_job(conn, job_id)
        conn.commit()
        conn.close()

        flash("Job item updated.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    item_type_val = clean_text_input(item["item_type"]).lower()
    qty_val = safe_float(item["quantity"])
    sale_price_val = safe_float(item["sale_price"])
    unit_cost_val = (safe_float(item["cost_amount"]) / safe_float(item["quantity"])) if safe_float(item["quantity"]) else 0
    is_labor = item_type_val == "labor"

    content = f"""
    <div class='card'>
        <h1>Edit Job Item</h1>
        <p>
            <strong>Job:</strong> #{job['id']} - {escape(clean_text_display(job['title']))}<br>
            <strong>Customer:</strong> {escape(clean_text_display(job['customer_name']))}
        </p>

        <form method='post'>
            <div class='grid'>
                <div>
                    <label>Type</label>
                    <select name='item_type' id='edit_item_type' onchange='toggleEditLaborMode()'>
                        <option value='material' {'selected' if item_type_val == 'material' else ''}>material</option>
                        <option value='labor' {'selected' if item_type_val == 'labor' else ''}>labor</option>
                        <option value='fuel' {'selected' if item_type_val == 'fuel' else ''}>fuel</option>
                        <option value='equipment' {'selected' if item_type_val == 'equipment' else ''}>equipment</option>
                        <option value='delivery' {'selected' if item_type_val == 'delivery' else ''}>delivery</option>
                        <option value='misc' {'selected' if item_type_val == 'misc' else ''}>misc</option>
                    </select>
                </div>

                <div>
                    <label>Description</label>
                    <input name='description' value="{escape(clean_text_input(item['description']))}" required>
                </div>

                <div>
                    <label id='edit_quantity_label'>{'Billable Hours' if is_labor else 'Quantity'}</label>
                    <input type='number' step='0.01' name='quantity' value="{qty_val:.2f}" required>
                </div>

                <div>
                    <label>Unit</label>
                    <input name='unit' id='edit_unit' value="{escape(clean_text_input(item['unit']))}">
                </div>

                <div id='edit_sale_price_wrap' style="display:{'none' if is_labor else 'block'};">
                    <label>Sale Price</label>
                    <input type='number' step='0.01' name='sale_price' id='edit_sale_price' value="{sale_price_val:.2f}">
                </div>

                <div>
                    <label id='edit_cost_label'>{'Hourly Rate' if is_labor else 'Unit Cost'}</label>
                    <input type='number' step='0.01' name='cost_amount' id='edit_cost_amount' value="{unit_cost_val:.2f}">
                </div>

                <div>
                    <label>Billable?</label>
                    <select name='billable'>
                        <option value='1' {'selected' if item['billable'] else ''}>Yes</option>
                        <option value='0' {'selected' if not item['billable'] else ''}>No</option>
                    </select>
                </div>
            </div>

            <br>
            <button class='btn'>Save Changes</button>
            <a class='btn secondary' href='{url_for("jobs.view_job", job_id=job_id)}'>Cancel</a>
        </form>
    </div>

    <script>
    function toggleEditLaborMode() {{
        const type = document.getElementById('edit_item_type').value;
        const quantityLabel = document.getElementById('edit_quantity_label');
        const costLabel = document.getElementById('edit_cost_label');
        const salePriceWrap = document.getElementById('edit_sale_price_wrap');
        const unitInput = document.getElementById('edit_unit');

        if (type === 'labor') {{
            quantityLabel.innerText = 'Billable Hours';
            costLabel.innerText = 'Hourly Rate';
            salePriceWrap.style.display = 'none';
            if (unitInput && !unitInput.value) {{
                unitInput.value = 'hrs';
            }}
        }} else {{
            quantityLabel.innerText = 'Quantity';
            costLabel.innerText = 'Unit Cost';
            salePriceWrap.style.display = 'block';
            if (unitInput && unitInput.value === 'hrs') {{
                unitInput.value = '';
            }}
        }}
    }}

    document.addEventListener('DOMContentLoaded', function() {{
        toggleEditLaborMode();
    }});
    </script>
    """

    conn.close()
    return render_page(content, f"Edit Job Item #{item_id}")


@jobs_bp.route("/jobs/<int:job_id>/items/<int:item_id>/delete", methods=["POST"])
@login_required
@require_permission("can_manage_jobs")
def delete_job_item(job_id, item_id):
    conn = get_db_connection()

    item = conn.execute(
        """
        SELECT ji.*, j.company_id
        FROM job_items ji
        JOIN jobs j ON ji.job_id = j.id
        WHERE ji.id = %s AND ji.job_id = %s AND j.company_id = %s
        """,
        (item_id, job_id, session["company_id"]),
    ).fetchone()

    if not item:
        conn.close()
        abort(404)

    if item["ledger_entry_id"]:
        conn.execute("DELETE FROM ledger_entries WHERE id = %s", (item["ledger_entry_id"],))

    conn.execute("DELETE FROM job_items WHERE id = %s", (item_id,))
    recalc_job(conn, job_id)
    conn.commit()
    conn.close()
    flash("Job item deleted.")
    return redirect(url_for("jobs.view_job", job_id=job_id))


@jobs_bp.route("/jobs/<int:job_id>/convert_to_invoice")
@login_required
@require_permission("can_manage_jobs")
def convert_job_to_invoice(job_id):
    conn = get_db_connection()
    cid = session["company_id"]

    job = conn.execute(
        """
        SELECT *
        FROM jobs
        WHERE id = %s AND company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    if not job:
        conn.close()
        abort(404)

    existing_invoice = conn.execute(
        """
        SELECT id
        FROM invoices
        WHERE job_id = %s AND company_id = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (job_id, cid),
    ).fetchone()

    if existing_invoice:
        conn.close()
        flash("This job has already been converted to an invoice.")
        return redirect(url_for("invoices.view_invoice", invoice_id=existing_invoice["id"]))

    items = conn.execute(
        """
        SELECT *
        FROM job_items
        WHERE job_id = %s AND billable = 1
        ORDER BY id
        """,
        (job_id,),
    ).fetchall()

    if not items:
        conn.close()
        flash("This job has no billable items to invoice.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    try:
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO invoices (
                company_id,
                customer_id,
                job_id,
                quote_id,
                invoice_number,
                invoice_date,
                due_date,
                status,
                notes,
                amount_paid,
                balance_due
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'Unpaid', %s, 0, 0)
            RETURNING id
            """,
            (
                job["company_id"],
                job["customer_id"],
                job_id,
                job["quote_id"],
                f"INV-{int(datetime.now().timestamp())}",
                date.today().isoformat(),
                date.today().isoformat(),
                clean_text_input(job["notes"]),
            ),
        )
        invoice_id = cur.fetchone()[0]

        for i in items:
            cur.execute(
                """
                INSERT INTO invoice_items (
                    invoice_id,
                    description,
                    quantity,
                    unit,
                    unit_price,
                    line_total
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    invoice_id,
                    clean_text_input(i["description"]),
                    safe_float(i["quantity"]),
                    clean_text_input(i["unit"]),
                    safe_float(i["sale_price"]),
                    safe_float(i["line_total"]),
                ),
            )

        recalc_invoice(conn, invoice_id)

        conn.execute(
            """
            UPDATE jobs
            SET status = 'Invoiced'
            WHERE id = %s AND company_id = %s
            """,
            (job_id, cid),
        )

        conn.commit()
        conn.close()

        flash("Job converted to invoice.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    except Exception:
        conn.rollback()
        conn.close()
        raise


@jobs_bp.route("/jobs/<int:job_id>/delete", methods=["POST"])
@login_required
@require_permission("can_manage_jobs")
def delete_job(job_id):
    conn = get_db_connection()
    job = conn.execute(
        "SELECT id FROM jobs WHERE id = %s AND company_id = %s",
        (job_id, session["company_id"]),
    ).fetchone()

    if not job:
        conn.close()
        abort(404)

    ledger_ids = conn.execute(
        "SELECT ledger_entry_id FROM job_items WHERE job_id = %s AND ledger_entry_id IS NOT NULL",
        (job_id,),
    ).fetchall()

    for row in ledger_ids:
        conn.execute("DELETE FROM ledger_entries WHERE id = %s", (row["ledger_entry_id"],))

    conn.execute("DELETE FROM job_items WHERE job_id = %s", (job_id,))
    conn.execute("DELETE FROM jobs WHERE id = %s AND company_id = %s", (job_id, session["company_id"]))
    conn.commit()
    conn.close()
    flash("Job deleted.")
    return redirect(url_for("jobs.jobs"))


@jobs_bp.route("/jobs/finished")
@login_required
@require_permission("can_manage_jobs")
def finished_jobs():
    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT j.*, c.name AS customer_name
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.company_id = %s
          AND j.status = 'Finished'
        ORDER BY j.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    job_rows = "".join(
        f"""
        <tr>
            <td>#{r['id']}</td>
            <td>{escape(clean_text_display(r['title']))}</td>
            <td>{escape(clean_text_display(r['customer_name']))}</td>
            <td>{escape(clean_text_display(r['status']))}</td>
            <td>${safe_float(r['revenue']):.2f}</td>
            <td>${safe_float(r['cost_total']):.2f}</td>
            <td>${safe_float(r['profit']):.2f}</td>
            <td>
                <div class='row-actions'>
                    <a class='btn secondary small' href='{url_for("jobs.view_job", job_id=r["id"])}'>View</a>
                    <a class='btn warning small' href='{url_for("jobs.reopen_job", job_id=r["id"])}'>Reopen</a>
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
                <h1 style='margin:0;'>Finished Jobs</h1>
                <p class='muted' style='margin:6px 0 0 0;'>Completed and fully paid jobs.</p>
            </div>
            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("jobs.jobs")}'>Back to Active Jobs</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <div class='table-wrap'>
            <table>
                <tr>
                    <th>ID</th>
                    <th>Title</th>
                    <th>Customer</th>
                    <th>Status</th>
                    <th>Revenue</th>
                    <th>Costs</th>
                    <th>Profit/Loss</th>
                    <th>Actions</th>
                </tr>
                {job_rows or '<tr><td colspan="8" class="muted">No finished jobs yet.</td></tr>'}
            </table>
        </div>
    </div>
    """
    return render_page(content, "Finished Jobs")


@jobs_bp.route("/jobs/<int:job_id>/reopen")
@login_required
@require_permission("can_manage_jobs")
def reopen_job(job_id):
    conn = get_db_connection()
    cid = session["company_id"]

    job = conn.execute(
        """
        SELECT id
        FROM jobs
        WHERE id = %s AND company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    if not job:
        conn.close()
        flash("Job not found.")
        return redirect(url_for("jobs.finished_jobs"))

    conn.execute(
        """
        UPDATE jobs
        SET status = 'Invoiced'
        WHERE id = %s AND company_id = %s
        """,
        (job_id, cid),
    )

    conn.commit()
    conn.close()

    flash("Job reopened.")
    return redirect(url_for("jobs.view_job", job_id=job_id))