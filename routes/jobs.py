from flask import Blueprint, request, redirect, url_for, session, flash, make_response, abort
from flask_wtf.csrf import generate_csrf
from datetime import date, datetime
from html import escape
import json
import io
import csv

from db import get_db_connection, ensure_job_cost_ledger
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page
from helpers import *
from calculations import recalc_job, recalc_invoice
from utils.emailing import send_company_email

jobs_bp = Blueprint("jobs", __name__)


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


def ensure_job_schedule_columns():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_start_time TIME")
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_end_time TIME")
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS assigned_to TEXT")
        conn.commit()
    finally:
        conn.close()


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


def _time_to_minutes(value):
    if not value:
        return None
    try:
        parts = str(value).split(":")
        return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        return None


def check_schedule_conflict(conn, company_id, scheduled_date, start_time, end_time, assigned_to, exclude_job_id=None):
    if not scheduled_date or not start_time or not assigned_to:
        return None

    new_start = _time_to_minutes(start_time)
    new_end = _time_to_minutes(end_time) if end_time else (new_start + 60)

    rows = conn.execute(
        """
        SELECT id, title, scheduled_start_time, scheduled_end_time
        FROM jobs
        WHERE company_id = %s
          AND scheduled_date = %s
          AND assigned_to = %s
          AND id != COALESCE(%s, -1)
        """,
        (company_id, scheduled_date, assigned_to, exclude_job_id),
    ).fetchall()

    for r in rows:
        existing_start = _time_to_minutes(r["scheduled_start_time"])
        existing_end = _time_to_minutes(r["scheduled_end_time"]) if r["scheduled_end_time"] else (existing_start + 60)

        if existing_start is None:
            continue

        if new_start < existing_end and new_end > existing_start:
            return {
                "id": r["id"],
                "title": r["title"],
                "start": r["scheduled_start_time"],
                "end": r["scheduled_end_time"],
            }

    return None


def display_item_type(value):
    key = clean_text_input(value).lower()
    if key in ITEM_TYPE_LABELS:
        return ITEM_TYPE_LABELS[key]
    return key.replace("_", " ").title() if key else "Material"


def default_unit_for_item_type(item_type):
    key = clean_text_input(item_type).lower()

    if key == "mulch":
        return "Yards"
    if key == "stone":
        return "Tons"
    if key == "soil":
        return "Yards"
    if key == "fertilizer":
        return "Bags"
    if key == "hardscape_material":
        return "Tons"
    if key == "plants":
        return "EA"
    if key == "trees":
        return "EA"
    if key == "labor":
        return "hr"
    if key == "dump_fee":
        return "fee"

    return ""


def build_job_update_email(job, update_type):
    company_name = clean_text_input(session.get("company_name")) or "TerraLedger"
    customer_name = clean_text_input(job.get("customer_name")) or "Customer"
    job_title = clean_text_input(job.get("title")) or "your scheduled job"
    scheduled_date = clean_text_input(job.get("scheduled_date"))
    start_time = clean_text_input(job.get("scheduled_start_time"))
    end_time = clean_text_input(job.get("scheduled_end_time"))
    address = clean_text_input(job.get("address"))
    assigned_to = clean_text_input(job.get("assigned_to"))

    schedule_line = ""
    if scheduled_date and start_time and end_time:
        schedule_line = f"{scheduled_date} from {start_time} to {end_time}"
    elif scheduled_date and start_time:
        schedule_line = f"{scheduled_date} at {start_time}"
    elif scheduled_date:
        schedule_line = scheduled_date

    if update_type == "on_the_way":
        subject = f"{company_name}: We are on the way"
        intro = f"Hello {customer_name},<br><br>We are on the way for <strong>{escape(job_title)}</strong>."
    elif update_type == "job_started":
        subject = f"{company_name}: Job started"
        intro = f"Hello {customer_name},<br><br>We have started <strong>{escape(job_title)}</strong>."
    elif update_type == "job_completed":
        subject = f"{company_name}: Job completed"
        intro = f"Hello {customer_name},<br><br>Your job <strong>{escape(job_title)}</strong> has been completed."
    else:
        subject = f"{company_name}: Job update"
        intro = f"Hello {customer_name},<br><br>Here is an update for <strong>{escape(job_title)}</strong>."

    details = []
    if schedule_line:
        details.append(f"<strong>Scheduled:</strong> {escape(schedule_line)}")
    if address:
        details.append(f"<strong>Address:</strong> {escape(address)}")
    if assigned_to:
        details.append(f"<strong>Assigned To:</strong> {escape(assigned_to)}")

    details_html = "<br>".join(details)

    html_body = f"""
    <div style="font-family: Arial, sans-serif; color: #1f2933; line-height: 1.5;">
        {intro}
        {'<br><br>' + details_html if details_html else ''}
        <br><br>
        Thank you,<br>
        {escape(company_name)}
    </div>
    """

    text_parts = [
        f"Hello {customer_name},",
        "",
    ]

    if update_type == "on_the_way":
        text_parts.append(f"We are on the way for {job_title}.")
    elif update_type == "job_started":
        text_parts.append(f"We have started {job_title}.")
    elif update_type == "job_completed":
        text_parts.append(f"Your job {job_title} has been completed.")
    else:
        text_parts.append(f"Here is an update for {job_title}.")

    if schedule_line:
        text_parts.append(f"Scheduled: {schedule_line}")
    if address:
        text_parts.append(f"Address: {address}")
    if assigned_to:
        text_parts.append(f"Assigned To: {assigned_to}")

    text_parts.extend([
        "",
        "Thank you,",
        company_name,
    ])

    text_body = "\n".join(text_parts)

    return subject, html_body, text_body


def send_job_update_email(company_id, customer_email, job, update_type, user_id=None):
    subject, html_body, text_body = build_job_update_email(job, update_type)

    try:
        send_company_email(
            company_id=company_id,
            user_id=user_id,
            to_email=customer_email,
            subject=subject,
            html=html_body,
            body=text_body,
        )
        return True, None
    except Exception as e:
        return False, str(e)


@jobs_bp.route("/jobs", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def jobs():
    ensure_job_schedule_columns()

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
        scheduled_start_time = clean_text_input(request.form.get("scheduled_start_time", ""))
        scheduled_end_time = clean_text_input(request.form.get("scheduled_end_time", ""))
        assigned_to = clean_text_input(request.form.get("assigned_to", ""))
        status = clean_text_input(request.form.get("status", "Scheduled")) or "Scheduled"
        address = clean_text_input(request.form.get("address", ""))
        notes = clean_text_input(request.form.get("notes", ""))

        if not title:
            conn.close()
            flash("Job title is required.")
            return redirect(url_for("jobs.jobs"))

        conflict = check_schedule_conflict(
            conn,
            cid,
            scheduled_date,
            scheduled_start_time,
            scheduled_end_time,
            assigned_to,
        )

        if conflict:
            conn.close()
            flash(
                f"Schedule conflict: '{conflict['title']}' is already scheduled for {assigned_to} "
                f"from {conflict['start']} to {conflict['end']}."
            )
            return redirect(url_for("jobs.jobs"))

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO jobs (
                company_id,
                customer_id,
                title,
                scheduled_date,
                scheduled_start_time,
                scheduled_end_time,
                assigned_to,
                status,
                address,
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                cid,
                customer_id,
                title,
                scheduled_date or None,
                scheduled_start_time or None,
                scheduled_end_time or None,
                assigned_to or None,
                status,
                address,
                notes,
            ),
        )
        row = cur.fetchone()
        job_id = row["id"] if row and "id" in row else None

        conn.commit()
        conn.close()

        if not job_id:
            flash("Could not create job.")
            return redirect(url_for("jobs.jobs"))

        flash("Job created.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    rows = conn.execute(
        """
        SELECT j.*, c.name AS customer_name
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.company_id = %s
          AND COALESCE(j.status, '') != 'Finished'
        ORDER BY
            j.scheduled_date NULLS LAST,
            j.scheduled_start_time NULLS LAST,
            j.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    job_row_list = []
    for r in rows:
        delete_csrf = generate_csrf()
        job_row_list.append(
            f"""
            <tr>
                <td>#{r['id']}</td>
                <td>{escape(clean_text_display(r['title']))}</td>
                <td>{escape(clean_text_display(r['customer_name']))}</td>
                <td>{escape(clean_text_display(r['scheduled_date']))}</td>
                <td>{escape(clean_text_display(r['scheduled_start_time']))}</td>
                <td>{escape(clean_text_display(r['scheduled_end_time']))}</td>
                <td>{escape(clean_text_display(r['assigned_to']))}</td>
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
                            <input type="hidden" name="csrf_token" value="{delete_csrf}">
                            <button class='btn danger small' type='submit'>Delete Job</button>
                        </form>
                    </div>
                </td>
            </tr>
            """
        )
    job_rows = "".join(job_row_list)

    create_job_csrf = generate_csrf()

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
            <input type="hidden" name="csrf_token" value="{create_job_csrf}">
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
                    <label>Start Time</label>
                    <input type='time' name='scheduled_start_time'>
                </div>

                <div>
                    <label>End Time</label>
                    <input type='time' name='scheduled_end_time'>
                </div>

                <div>
                    <label>Assigned To</label>
                    <input name='assigned_to' placeholder='Crew / Employee'>
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
                    <th>Date</th>
                    <th>Start</th>
                    <th>End</th>
                    <th>Assigned To</th>
                    <th>Status</th>
                    <th>Revenue</th>
                    <th>Costs</th>
                    <th>Profit/Loss</th>
                    <th>Actions</th>
                </tr>
                {job_rows or '<tr><td colspan="12" class="muted">No jobs yet.</td></tr>'}
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
@subscription_required
@require_permission("can_manage_jobs")
def export_jobs():
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT
            j.id,
            j.title,
            j.scheduled_date,
            j.scheduled_start_time,
            j.scheduled_end_time,
            j.assigned_to,
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
        "Start Time",
        "End Time",
        "Assigned To",
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
            clean_text_input(r["scheduled_start_time"]),
            clean_text_input(r["scheduled_end_time"]),
            clean_text_input(r["assigned_to"]),
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
@subscription_required
@require_permission("can_manage_jobs")
def view_job(job_id):
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    job = conn.execute(
        """
        SELECT
            j.*,
            c.name AS customer_name,
            c.email AS customer_email
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
        unit_cost = safe_float(request.form.get("unit_cost"))
        billable = 1 if request.form.get("billable") == "1" else 0

        if not description:
            conn.close()
            flash("Description is required.")
            return redirect(url_for("jobs.view_job", job_id=job_id))

        if item_type == "mulch" and not unit:
            unit = "Yards"
        elif item_type == "stone" and not unit:
            unit = "Tons"
        elif item_type == "soil" and not unit:
            unit = "Yards"
        elif item_type == "hardscape_material" and not unit:
            unit = "Tons"
        elif item_type == "fuel" and not unit:
            unit = "Gallons"
        elif item_type == "delivery" and not unit:
            unit = "Miles"
        elif item_type == "labor" and not unit:
            unit = "Hours"
        elif item_type == "equipment" and not unit:
            unit = "Rentals"
        elif item_type in ["plants", "trees", "misc", "dump_fee"]:
            unit = ""

        if item_type == "labor":
            unit_cost = 0.0

        if item_type == "dump_fee":
            unit = ""
            if qty <= 0:
                qty = 1
            unit_cost = 0.0

        line_total = qty * sale_price
        cost_amount = qty * unit_cost

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO job_items (
                job_id, item_type, description, quantity, unit,
                unit_cost, unit_price, sale_price, cost_amount, line_total, billable
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                job_id,
                item_type,
                description,
                qty,
                unit,
                unit_cost,
                sale_price,
                sale_price,
                cost_amount,
                line_total,
                billable,
            ),
        )
        row = cur.fetchone()
        job_item_id = row["id"] if row and "id" in row else None

        if not job_item_id:
            conn.rollback()
            conn.close()
            flash("Could not add job item.")
            return redirect(url_for("jobs.view_job", job_id=job_id))

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

    item_row_list = []
    for i in items:
        delete_item_csrf = generate_csrf()
        item_row_list.append(
            f"""
            <tr>
                <td>{escape(display_item_type(i['item_type']))}</td>
                <td>{escape(clean_text_display(i['description']))}</td>
                <td>{safe_float(i['quantity']):g}</td>
                <td>{escape(clean_text_display(i['unit']))}</td>
                <td>${safe_float(i['sale_price']):.2f}</td>
                <td>{"-" if clean_text_input(i['item_type']).lower() in ['dump_fee', 'labor'] else f"${((safe_float(i['cost_amount']) / safe_float(i['quantity'])) if safe_float(i['quantity']) else 0):.2f}"}</td>
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
                            <input type="hidden" name="csrf_token" value="{delete_item_csrf}">
                            <button class='btn danger small' type='submit'>Delete</button>
                        </form>
                    </div>
                </td>
            </tr>
            """
        )
    item_rows = "".join(item_row_list)

    schedule_bits = []
    if clean_text_input(job["scheduled_date"]):
        schedule_bits.append(f"<strong>Date:</strong> {escape(clean_text_display(job['scheduled_date']))}")
    if clean_text_input(job["scheduled_start_time"]):
        if clean_text_input(job["scheduled_end_time"]):
            schedule_bits.append(
                f"<strong>Time:</strong> {escape(clean_text_display(job['scheduled_start_time']))} - {escape(clean_text_display(job['scheduled_end_time']))}"
            )
        else:
            schedule_bits.append(f"<strong>Start:</strong> {escape(clean_text_display(job['scheduled_start_time']))}")
    if clean_text_input(job["assigned_to"]):
        schedule_bits.append(f"<strong>Assigned To:</strong> {escape(clean_text_display(job['assigned_to']))}")

    schedule_html = "<br>".join(schedule_bits) if schedule_bits else "<strong>Schedule:</strong> -"

    customer_email = clean_text_input(job["customer_email"])

    if customer_email:
        email_csrf_1 = generate_csrf()
        email_csrf_2 = generate_csrf()
        email_csrf_3 = generate_csrf()
        email_csrf_custom = generate_csrf()

        email_buttons = f"""
        <style>
            .updates-menu-wrap {{
                position: relative;
                display: inline-block;
            }}
            .updates-menu {{
                display: none;
                position: absolute;
                top: calc(100% + 6px);
                left: 0;
                min-width: 240px;
                background: #fff;
                border: 1px solid #d8e2d0;
                border-radius: 10px;
                box-shadow: 0 10px 24px rgba(0,0,0,0.10);
                z-index: 1000;
                overflow: hidden;
            }}
            .updates-menu form {{
                margin: 0;
            }}
            .updates-menu button {{
                width: 100%;
                text-align: left;
                background: #fff;
                border: none;
                padding: 12px 14px;
                cursor: pointer;
                font-weight: 600;
                color: #1f2933;
            }}
            .updates-menu button:hover {{
                background: #f7f7f5;
            }}
            .custom-update-card {{
                display: none;
                margin-top: 14px;
            }}
        </style>

        <div class="updates-menu-wrap">
            <button class="btn secondary" type="button" onclick="toggleUpdatesMenu(event)">Updates ▼</button>

            <div id="updatesMenu" class="updates-menu">
                <form method="post" action="{url_for("jobs.send_update_email", job_id=job_id)}">
                    <input type="hidden" name="csrf_token" value="{email_csrf_1}">
                    <input type="hidden" name="update_type" value="on_the_way">
                    <button type="submit">Send On The Way Email</button>
                </form>

                <form method="post" action="{url_for("jobs.send_update_email", job_id=job_id)}">
                    <input type="hidden" name="csrf_token" value="{email_csrf_2}">
                    <input type="hidden" name="update_type" value="job_started">
                    <button type="submit">Send Job Started Email</button>
                </form>

                <form method="post" action="{url_for("jobs.send_update_email", job_id=job_id)}">
                    <input type="hidden" name="csrf_token" value="{email_csrf_3}">
                    <input type="hidden" name="update_type" value="job_completed">
                    <button type="submit">Send Job Finished Email</button>
                </form>

                <div style="border-top:1px solid #e8ece7;"></div>

                <button type="button" onclick="toggleCustomUpdateCard()">Compose Custom Update</button>
            </div>
        </div>

        <div id="customUpdateCard" class="card custom-update-card">
            <h3>Custom Job Update</h3>

            <form method="post" action="{url_for("jobs.send_custom_email", job_id=job_id)}">
                <input type="hidden" name="csrf_token" value="{email_csrf_custom}">
                <div class="grid">
                    <div>
                        <label>To Email</label>
                        <input
                            type="email"
                            name="to_email"
                            value="{escape(customer_email)}"
                            placeholder="Enter customer email"
                            required
                        >
                    </div>

                    <div>
                        <label>Subject</label>
                        <input
                            type="text"
                            name="subject"
                            value="Job Update - {escape(clean_text_display(job['title']))}"
                            required
                        >
                    </div>
                </div>

                <div style="margin-top:14px;">
                    <label>Message</label>
                    <textarea name="message" required>Hello {escape(clean_text_display(job['customer_name']))},

This is an update regarding your job "{escape(clean_text_display(job['title']))}".

Thank you,
{escape(session.get("company_name") or "Your Company")}</textarea>
                </div>

                <div class="row-actions" style="margin-top:12px;">
                    <button class="btn success" type="submit">Send Email</button>
                    <button class="btn secondary" type="button" onclick="toggleCustomUpdateCard()">Cancel</button>
                </div>
            </form>
        </div>
        """
    else:
        email_csrf_custom_empty = generate_csrf()
        email_buttons = """
        <div class='muted small'>Add a customer email address to send job updates.</div>
        <div id="customUpdateCard" class="card custom-update-card" style="display:block; margin-top:14px;">
            <h3>Custom Job Update</h3>
            <div class="muted small" style="margin-bottom:12px;">No customer email is on file, but you can still enter one manually below.</div>

            <form method="post" action="{send_custom_url}">
                <input type="hidden" name="csrf_token" value="{csrf_token_value}">
                <div class="grid">
                    <div>
                        <label>To Email</label>
                        <input
                            type="email"
                            name="to_email"
                            value=""
                            placeholder="Enter recipient email"
                            required
                        >
                    </div>

                    <div>
                        <label>Subject</label>
                        <input
                            type="text"
                            name="subject"
                            value="Job Update - {job_title}"
                            required
                        >
                    </div>
                </div>

                <div style="margin-top:14px;">
                    <label>Message</label>
                    <textarea name="message" required>Hello {customer_name},

This is an update regarding your job "{job_title}".

Thank you,
{company_name}</textarea>
                </div>

                <div class="row-actions" style="margin-top:12px;">
                    <button class="btn success" type="submit">Send Email</button>
                </div>
            </form>
        </div>
        """.format(
            send_custom_url=url_for("jobs.send_custom_email", job_id=job_id),
            csrf_token_value=email_csrf_custom_empty,
            job_title=escape(clean_text_display(job["title"])),
            customer_name=escape(clean_text_display(job["customer_name"])),
            company_name=escape(session.get("company_name") or "Your Company"),
        )

    add_item_csrf = generate_csrf()

    content = f"""
        <div class='card'>
            <h1>Job #{job['id']} - {escape(clean_text_display(job['title']))}</h1>
            <p>
                <strong>Customer:</strong> {escape(clean_text_display(job['customer_name']))}<br>
                <strong>Email:</strong> {escape(clean_text_display(job['customer_email']))}<br>
                {schedule_html}<br>
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

            <div class="row-actions" style="margin-top:12px;">
                {email_buttons}
            </div>
        </div>

        <div class='card'>
            <h2>Add Job Item</h2>
            <p class='muted'>Any cost you enter here is automatically pushed into bookkeeping as an expense.</p>

            <form method='post'>
                <input type="hidden" name="csrf_token" value="{add_item_csrf}">
                <div class='grid'>

                    <div>
                        <label>Type</label>
                        <select name='item_type' id='item_type' onchange='toggleJobItemMode()'>
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
                        <input type='number' step='0.01' name='quantity' id='quantity' required>
                    </div>

                    <div>
                        <label>Unit</label>
                        <input name='unit' id='unit' placeholder='Unit'>
                    </div>

                    <div id='sale_price_wrap'>
                        <label id='sale_price_label'>Sale Price</label>
                        <input type='number' step='0.01' name='sale_price' id='sale_price' value='0' required>
                    </div>

                    <div id='unit_cost_wrap'>
                        <label id='cost_label'>Unit Cost</label>
                        <input type='number' step='0.01' name='unit_cost' id='unit_cost' value='0'>
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
        function toggleJobItemMode() {{
            const type = document.getElementById('item_type').value;

            const quantityLabel = document.getElementById('quantity_label');
            const costLabel = document.getElementById('cost_label');
            const salePriceLabel = document.getElementById('sale_price_label');
            const salePriceWrap = document.getElementById('sale_price_wrap');
            const unitCostWrap = document.getElementById('unit_cost_wrap');
            const unitInput = document.getElementById('unit');
            const quantityInput = document.getElementById('quantity');
            const unitCostInput = document.getElementById('unit_cost');

            quantityLabel.innerText = 'Quantity';
            salePriceLabel.innerText = 'Sale Price';
            costLabel.innerText = 'Unit Cost';
            if (salePriceWrap) salePriceWrap.style.display = 'block';
            if (unitCostWrap) unitCostWrap.style.display = 'block';

            if (quantityInput) {{
                quantityInput.readOnly = false;
                quantityInput.step = '0.01';
            }}

            if (unitInput) unitInput.value = '';

            if (type === 'mulch') {{
                quantityLabel.innerText = 'Yards';
                unitInput.value = 'Yards';
            }} else if (type === 'stone') {{
                quantityLabel.innerText = 'Tons';
                unitInput.value = 'Tons';
            }} else if (type === 'soil') {{
                quantityLabel.innerText = 'Yards';
                unitInput.value = 'Yards';
            }} else if (type === 'hardscape_material') {{
                quantityLabel.innerText = 'Tons';
                unitInput.value = 'Tons';
            }} else if (type === 'fuel') {{
                quantityLabel.innerText = 'Gallons';
                unitInput.value = 'Gallons';
            }} else if (type === 'delivery') {{
                quantityLabel.innerText = 'Miles';
                unitInput.value = 'Miles';
            }} else if (type === 'labor') {{
                quantityLabel.innerText = 'Billable Hours';
                salePriceLabel.innerText = 'Hourly Rate';
                unitInput.value = 'Hours';
                if (unitCostWrap) unitCostWrap.style.display = 'none';
                if (unitCostInput) unitCostInput.value = '0';
            }} else if (type === 'equipment') {{
                quantityLabel.innerText = 'Rentals';
                unitInput.value = 'Rentals';
            }} else if (type === 'plants' || type === 'trees' || type === 'misc') {{
                quantityLabel.innerText = 'Quantity';
                unitInput.value = '';
            }} else if (type === 'dump_fee') {{
                quantityLabel.innerText = 'Fee';
                salePriceLabel.innerText = 'Fee Amount';
                unitInput.value = '';
                if (unitCostWrap) unitCostWrap.style.display = 'none';
                if (unitCostInput) unitCostInput.value = '0';
                if (quantityInput) {{
                    quantityInput.value = '1';
                    quantityInput.readOnly = true;
                }}
            }} else if (type === 'fertilizer') {{
                quantityLabel.innerText = 'Quantity';
                unitInput.value = '';
            }}
        }}

        function toggleUpdatesMenu(event) {{
            event.stopPropagation();
            const menu = document.getElementById('updatesMenu');
            if (!menu) return;
            menu.style.display = menu.style.display === 'block' ? 'none' : 'block';
        }}

        function toggleCustomUpdateCard() {{
            const card = document.getElementById('customUpdateCard');
            const menu = document.getElementById('updatesMenu');
            if (menu) menu.style.display = 'none';
            if (!card) return;
            card.style.display = card.style.display === 'block' ? 'none' : 'block';
        }}

        document.addEventListener('DOMContentLoaded', function() {{
            toggleJobItemMode();
        }});

        document.addEventListener('click', function() {{
            const menu = document.getElementById('updatesMenu');
            if (menu) {{
                menu.style.display = 'none';
            }}
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


@jobs_bp.route("/jobs/<int:job_id>/send_update_email", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def send_update_email(job_id):
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]
    uid = session.get("user_id")

    job = conn.execute(
        """
        SELECT
            j.*,
            c.name AS customer_name,
            c.email AS customer_email
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.id = %s AND j.company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    conn.close()

    if not job:
        abort(404)

    customer_email = clean_text_input(job["customer_email"])
    if not customer_email:
        flash("This customer does not have an email address.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    update_type = clean_text_input(request.form.get("update_type", ""))
    if update_type not in {"on_the_way", "job_started", "job_completed"}:
        flash("Invalid email update type.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    success, error_message = send_job_update_email(
        company_id=cid,
        customer_email=customer_email,
        job=job,
        update_type=update_type,
        user_id=uid,
    )

    if success:
        if update_type == "on_the_way":
            flash("On the way email sent.")
        elif update_type == "job_started":
            flash("Job started email sent.")
        elif update_type == "job_completed":
            flash("Job completed email sent.")
        else:
            flash("Job update email sent.")
    else:
        flash(f"Could not send email: {error_message}")

    return redirect(url_for("jobs.view_job", job_id=job_id))


@jobs_bp.route("/jobs/<int:job_id>/send_custom_email", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def send_custom_email(job_id):
    conn = get_db_connection()
    cid = session["company_id"]
    uid = session.get("user_id")

    job = conn.execute(
        """
        SELECT j.id
        FROM jobs j
        WHERE j.id = %s AND j.company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    conn.close()

    if not job:
        abort(404)

    to_email = clean_text_input(request.form.get("to_email", ""))
    subject = clean_text_input(request.form.get("subject", ""))
    message = (request.form.get("message", "") or "").strip()

    if not to_email:
        flash("Recipient email is required.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    if not subject:
        flash("Email subject is required.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    if not message:
        flash("Email message is required.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    try:
        send_company_email(
            company_id=cid,
            user_id=uid,
            to_email=to_email,
            subject=subject,
            html=message.replace("\n", "<br>"),
            body=message,
        )
        flash("Custom job update email sent.")
    except Exception as e:
        flash(f"Could not send email: {e}")

    return redirect(url_for("jobs.view_job", job_id=job_id))


@jobs_bp.route("/jobs/<int:job_id>/edit", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def edit_job(job_id):
    ensure_job_schedule_columns()

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
        scheduled_start_time = clean_text_input(request.form.get("scheduled_start_time", ""))
        scheduled_end_time = clean_text_input(request.form.get("scheduled_end_time", ""))
        assigned_to = clean_text_input(request.form.get("assigned_to", ""))
        status = clean_text_input(request.form.get("status", ""))
        address = clean_text_input(request.form.get("address", ""))
        notes = clean_text_input(request.form.get("notes", ""))

        if not customer_id or not title:
            conn.close()
            flash("Customer and title are required.")
            return redirect(url_for("jobs.edit_job", job_id=job_id))

        conflict = check_schedule_conflict(
            conn,
            cid,
            scheduled_date,
            scheduled_start_time,
            scheduled_end_time,
            assigned_to,
            exclude_job_id=job_id,
        )

        if conflict:
            conn.close()
            flash(
                f"Schedule conflict: '{conflict['title']}' already scheduled for {assigned_to} "
                f"from {conflict['start']} to {conflict['end']}."
            )
            return redirect(url_for("jobs.edit_job", job_id=job_id))

        conn.execute(
            """
            UPDATE jobs
            SET customer_id = %s,
                title = %s,
                scheduled_date = %s,
                scheduled_start_time = %s,
                scheduled_end_time = %s,
                assigned_to = %s,
                status = %s,
                address = %s,
                notes = %s
            WHERE id = %s AND company_id = %s
            """,
            (
                customer_id,
                title,
                scheduled_date or None,
                scheduled_start_time or None,
                scheduled_end_time or None,
                assigned_to or None,
                status,
                address,
                notes,
                job_id,
                cid,
            ),
        )
        conn.commit()
        conn.close()

        flash("Job updated.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    customer_opts = "".join(
        f"<option value='{c['id']}' {'selected' if c['id'] == job['customer_id'] else ''}>{escape(clean_text_display(c['name'], 'Customer #' + str(c['id'])))}</option>"
        for c in customers
    )

    edit_job_csrf = generate_csrf()

    content = f"""
    <div class='card'>
        <h1>Edit Job #{job['id']}</h1>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{edit_job_csrf}">
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
                    <label>Start Time</label>
                    <input type='time' name='scheduled_start_time' value="{escape(clean_text_input(job['scheduled_start_time']))}">
                </div>
                <div>
                    <label>End Time</label>
                    <input type='time' name='scheduled_end_time' value="{escape(clean_text_input(job['scheduled_end_time']))}">
                </div>
                <div>
                    <label>Assigned To</label>
                    <input name='assigned_to' value="{escape(clean_text_input(job['assigned_to']))}">
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
@subscription_required
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
        unit_cost = safe_float(request.form.get("unit_cost"))
        billable = 1 if request.form.get("billable") == "1" else 0

        if not description:
            conn.close()
            flash("Description is required.")
            return redirect(url_for("jobs.edit_job_item", job_id=job_id, item_id=item_id))

        if item_type == "mulch" and not unit:
            unit = "Yards"
        elif item_type == "stone" and not unit:
            unit = "Tons"
        elif item_type == "soil" and not unit:
            unit = "Yards"
        elif item_type == "hardscape_material" and not unit:
            unit = "Tons"
        elif item_type == "fuel" and not unit:
            unit = "Gallons"
        elif item_type == "delivery" and not unit:
            unit = "Miles"
        elif item_type == "labor" and not unit:
            unit = "Hours"
        elif item_type == "equipment" and not unit:
            unit = "Rentals"
        elif item_type in ["plants", "trees", "misc", "dump_fee"]:
            unit = ""

        if item_type == "labor":
            unit_cost = 0.0

        if item_type == "dump_fee":
            unit = ""
            if qty <= 0:
                qty = 1
            unit_cost = 0.0

        line_total = qty * sale_price
        cost_amount = qty * unit_cost

        conn.execute(
            """
            UPDATE job_items
            SET item_type = %s,
                description = %s,
                quantity = %s,
                unit = %s,
                unit_cost = %s,
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
                unit_cost,
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
    hide_cost = item_type_val in ["dump_fee", "labor"]
    edit_item_csrf = generate_csrf()

    content = f"""
    <div class='card'>
        <h1>Edit Job Item</h1>
        <p>
            <strong>Job:</strong> #{job['id']} - {escape(clean_text_display(job['title']))}<br>
            <strong>Customer:</strong> {escape(clean_text_display(job['customer_name']))}
        </p>

        <form method='post'>
            <input type="hidden" name="csrf_token" value="{edit_item_csrf}">
            <div class='grid'>
                <div>
                    <label>Type</label>
                    <select name='item_type' id='edit_item_type' onchange='toggleEditJobItemMode()'>
                        <option value='mulch' {'selected' if item_type_val == 'mulch' else ''}>Mulch</option>
                        <option value='stone' {'selected' if item_type_val == 'stone' else ''}>Stone</option>
                        <option value='dump_fee' {'selected' if item_type_val == 'dump_fee' else ''}>Dump Fee</option>
                        <option value='plants' {'selected' if item_type_val == 'plants' else ''}>Plants</option>
                        <option value='trees' {'selected' if item_type_val == 'trees' else ''}>Trees</option>
                        <option value='soil' {'selected' if item_type_val == 'soil' else ''}>Soil</option>
                        <option value='fertilizer' {'selected' if item_type_val == 'fertilizer' else ''}>Fertilizer</option>
                        <option value='hardscape_material' {'selected' if item_type_val == 'hardscape_material' else ''}>Hardscape Material</option>
                        <option value='labor' {'selected' if item_type_val == 'labor' else ''}>Labor</option>
                        <option value='equipment' {'selected' if item_type_val == 'equipment' else ''}>Equipment</option>
                        <option value='delivery' {'selected' if item_type_val == 'delivery' else ''}>Delivery</option>
                        <option value='fuel' {'selected' if item_type_val == 'fuel' else ''}>Fuel</option>
                        <option value='misc' {'selected' if item_type_val == 'misc' else ''}>Misc</option>
                    </select>
                </div>

                <div>
                    <label>Description</label>
                    <input name='description' value="{escape(clean_text_input(item['description']))}" required>
                </div>

                <div>
                    <label id='edit_quantity_label'>Quantity</label>
                    <input type='number' step='0.01' name='quantity' id='edit_quantity' value="{qty_val:.2f}" required>
                </div>

                <div>
                    <label>Unit</label>
                    <input name='unit' id='edit_unit' value="{escape(clean_text_input(item['unit']))}">
                </div>

                <div id='edit_sale_price_wrap'>
                    <label id='edit_sale_price_label'>Sale Price</label>
                    <input type='number' step='0.01' name='sale_price' id='edit_sale_price' value="{sale_price_val:.2f}">
                </div>

                <div id='edit_unit_cost_wrap' style="display:{'none' if hide_cost else 'block'};">
                    <label id='edit_cost_label'>Unit Cost</label>
                    <input type='number' step='0.01' name='unit_cost' id='edit_unit_cost' value="{unit_cost_val:.2f}">
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
    function toggleEditJobItemMode() {{
        const type = document.getElementById('edit_item_type').value;

        const quantityLabel = document.getElementById('edit_quantity_label');
        const costLabel = document.getElementById('edit_cost_label');
        const salePriceLabel = document.getElementById('edit_sale_price_label');
        const salePriceWrap = document.getElementById('edit_sale_price_wrap');
        const unitCostWrap = document.getElementById('edit_unit_cost_wrap');
        const unitInput = document.getElementById('edit_unit');
        const quantityInput = document.getElementById('edit_quantity');
        const unitCostInput = document.getElementById('edit_unit_cost');

        quantityLabel.innerText = 'Quantity';
        salePriceLabel.innerText = 'Sale Price';
        costLabel.innerText = 'Unit Cost';
        if (salePriceWrap) salePriceWrap.style.display = 'block';
        if (unitCostWrap) unitCostWrap.style.display = 'block';

        if (quantityInput) {{
            quantityInput.readOnly = false;
            quantityInput.step = '0.01';
        }}

        if (unitInput) unitInput.value = '';

        if (type === 'mulch') {{
            quantityLabel.innerText = 'Yards';
            unitInput.value = 'Yards';
        }} else if (type === 'stone') {{
            quantityLabel.innerText = 'Tons';
            unitInput.value = 'Tons';
        }} else if (type === 'soil') {{
            quantityLabel.innerText = 'Yards';
            unitInput.value = 'Yards';
        }} else if (type === 'hardscape_material') {{
            quantityLabel.innerText = 'Tons';
            unitInput.value = 'Tons';
        }} else if (type === 'fuel') {{
            quantityLabel.innerText = 'Gallons';
            unitInput.value = 'Gallons';
        }} else if (type === 'delivery') {{
            quantityLabel.innerText = 'Miles';
            unitInput.value = 'Miles';
        }} else if (type === 'labor') {{
            quantityLabel.innerText = 'Billable Hours';
            salePriceLabel.innerText = 'Hourly Rate';
            unitInput.value = 'Hours';
            if (unitCostWrap) unitCostWrap.style.display = 'none';
            if (unitCostInput) unitCostInput.value = '0';
        }} else if (type === 'equipment') {{
            quantityLabel.innerText = 'Rentals';
            unitInput.value = 'Rentals';
        }} else if (type === 'plants' || type === 'trees' || type === 'misc') {{
            quantityLabel.innerText = 'Quantity';
            unitInput.value = '';
        }} else if (type === 'dump_fee') {{
            quantityLabel.innerText = 'Fee';
            salePriceLabel.innerText = 'Fee Amount';
            unitInput.value = '';
            if (unitCostWrap) unitCostWrap.style.display = 'none';
            if (unitCostInput) unitCostInput.value = '0';
            if (quantityInput) {{
                quantityInput.value = '1';
                quantityInput.readOnly = true;
            }}
        }} else if (type === 'fertilizer') {{
            quantityLabel.innerText = 'Quantity';
            unitInput.value = '';
        }}
    }}

    document.addEventListener('DOMContentLoaded', function() {{
        toggleEditJobItemMode();
    }});
    </script>
    """

    conn.close()
    return render_page(content, f"Edit Job Item #{item_id}")


@jobs_bp.route("/jobs/<int:job_id>/items/<int:item_id>/delete", methods=["POST"])
@login_required
@subscription_required
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

    if "ledger_entry_id" in item.keys() and item["ledger_entry_id"]:
        conn.execute("DELETE FROM ledger_entries WHERE id = %s", (item["ledger_entry_id"],))

    conn.execute("DELETE FROM job_items WHERE id = %s", (item_id,))
    recalc_job(conn, job_id)
    conn.commit()
    conn.close()
    flash("Job item deleted.")
    return redirect(url_for("jobs.view_job", job_id=job_id))


@jobs_bp.route("/jobs/<int:job_id>/convert_to_invoice")
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def convert_job_to_invoice(job_id):
    conn = get_db_connection()
    cid = session["company_id"]

    try:
        job = conn.execute(
            """
            SELECT *
            FROM jobs
            WHERE id = %s AND company_id = %s
            """,
            (job_id, cid),
        ).fetchone()

        if not job:
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
            flash("This job has already been converted to an invoice.")
            return redirect(url_for("invoices.view_invoice", invoice_id=existing_invoice["id"]))

        items = conn.execute(
            """
            SELECT *
            FROM job_items
            WHERE job_id = %s AND COALESCE(billable, 1) = 1
            ORDER BY id
            """,
            (job_id,),
        ).fetchall()

        if not items:
            flash("This job has no billable items to invoice.")
            return redirect(url_for("jobs.view_job", job_id=job_id))

        invoice_date = date.today().isoformat()
        due_date = invoice_date
        notes = clean_text_input(job["notes"]) if "notes" in job.keys() and job["notes"] else ""
        invoice_number = f"INV-{int(datetime.now().timestamp())}"

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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                job["company_id"],
                job["customer_id"],
                job_id,
                job["quote_id"] if "quote_id" in job.keys() else None,
                invoice_number,
                invoice_date,
                due_date,
                "Unpaid",
                notes,
                0,
                0,
            ),
        )

        row = cur.fetchone()
        if not row or "id" not in row:
            raise Exception("Failed to create invoice record.")

        invoice_id = row["id"]

        for i in items:
            description = clean_text_input(i["description"]) if i["description"] else ""
            quantity = safe_float(i["quantity"])
            unit = clean_text_input(i["unit"]) if i["unit"] else ""
            unit_price = safe_float(i["sale_price"] if i["sale_price"] is not None else i["unit_price"])
            line_total = safe_float(i["line_total"])

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
                    description,
                    quantity,
                    unit,
                    unit_price,
                    line_total,
                ),
            )

        recalc_invoice(conn, invoice_id)

        cur.execute(
            """
            UPDATE jobs
            SET status = %s
            WHERE id = %s AND company_id = %s
            """,
            ("Invoiced", job_id, cid),
        )

        conn.commit()
        flash("Job converted to invoice.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    except Exception as e:
        conn.rollback()
        flash(f"Could not convert job to invoice: {e}")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    finally:
        conn.close()


@jobs_bp.route("/jobs/<int:job_id>/delete", methods=["POST"])
@login_required
@subscription_required
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
@subscription_required
@require_permission("can_manage_jobs")
def finished_jobs():
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT j.*, c.name AS customer_name
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.company_id = %s
          AND j.status = 'Finished'
        ORDER BY
            j.scheduled_date NULLS LAST,
            j.scheduled_start_time NULLS LAST,
            j.id DESC
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
            <td>{escape(clean_text_display(r['scheduled_date']))}</td>
            <td>{escape(clean_text_display(r['scheduled_start_time']))}</td>
            <td>{escape(clean_text_display(r['scheduled_end_time']))}</td>
            <td>{escape(clean_text_display(r['assigned_to']))}</td>
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
                    <th>Date</th>
                    <th>Start</th>
                    <th>End</th>
                    <th>Assigned To</th>
                    <th>Status</th>
                    <th>Revenue</th>
                    <th>Costs</th>
                    <th>Profit/Loss</th>
                    <th>Actions</th>
                </tr>
                {job_rows or '<tr><td colspan="12" class="muted">No finished jobs yet.</td></tr>'}
            </table>
        </div>
    </div>
    """
    return render_page(content, "Finished Jobs")


@jobs_bp.route("/jobs/<int:job_id>/reopen")
@login_required
@subscription_required
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