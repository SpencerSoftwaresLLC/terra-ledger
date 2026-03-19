from flask import Blueprint, request, redirect, url_for, session, flash, make_response
from datetime import date, datetime
from html import escape
import csv
import io

from ..db import ensure_job_cost_ledger, repair_all_job_item_ledgers, get_db_connection
from ..decorators import login_required, require_permission, subscription_required
from ..page_helpers import *
from ..helpers import *
from ..calculations import *

ledger_bp = Blueprint("ledger", __name__)


def table_exists(conn, table_name):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def table_columns(conn, table_name):
    if not table_exists(conn, table_name):
        return []
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    cols = []
    for row in rows:
        try:
            cols.append(row["name"])
        except Exception:
            cols.append(row[1])
    return cols


def has_col(conn, table_name, col_name):
    return col_name in table_columns(conn, table_name)


def safe_get(row, key, default=None):
    try:
        if key in row.keys():
            value = row[key]
            return default if value is None else value
    except Exception:
        pass
    return default


def safe_float(value, default=0.0):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return default


def build_job_item_query(conn, start_date, end_date):
    """
    Best-effort query builder for job_items across the different versions
    of your schema. This version matches TerraLedger's actual job_items structure.
    """
    if not table_exists(conn, "job_items") or not table_exists(conn, "jobs"):
        return None, None

    cols = table_columns(conn, "job_items")

    date_col = None
    for candidate in ("entry_date", "item_date", "created_date", "date_created", "created_at", "date"):
        if candidate in cols:
            date_col = candidate
            break

    job_id_col = "job_id" if "job_id" in cols else "id"

    category_expr = "'Material'"
    if "item_type" in cols:
        category_expr = """
            CASE LOWER(COALESCE(NULLIF(TRIM(ji.item_type), ''), 'material'))
                WHEN 'labor' THEN 'Labor'
                WHEN 'fuel' THEN 'Fuel'
                WHEN 'equipment' THEN 'Equipment'
                WHEN 'delivery' THEN 'Delivery'
                WHEN 'misc' THEN 'Misc'
                ELSE 'Material'
            END
        """
    else:
        for candidate in ("category", "cost_category", "item_category", "type"):
            if candidate in cols:
                category_expr = f"COALESCE(NULLIF(TRIM(ji.{candidate}), ''), 'Material')"
                break

    description_expr = "'Job item'"
    for candidate in ("description", "name", "item_name", "title"):
        if candidate in cols:
            description_expr = f"COALESCE(NULLIF(TRIM(ji.{candidate}), ''), 'Job item')"
            break

    amount_expr = None
    for candidate in (
        "cost_amount",
        "line_total_cost",
        "total_cost",
        "cost_total",
        "extended_cost",
        "amount",
        "cost",
        "unit_cost",
        "price",
        "line_total",
        "total",
    ):
        if candidate in cols:
            amount_expr = f"ABS(COALESCE(ji.{candidate}, 0))"
            break

    if amount_expr is None:
        qty_col = None
        unit_cost_col = None

        for candidate in ("quantity", "qty", "units"):
            if candidate in cols:
                qty_col = candidate
                break

        for candidate in ("unit_cost", "cost", "price"):
            if candidate in cols:
                unit_cost_col = candidate
                break

        if qty_col and unit_cost_col:
            amount_expr = f"ABS(COALESCE(ji.{qty_col}, 0) * COALESCE(ji.{unit_cost_col}, 0))"
        else:
            amount_expr = "0"

    customer_expr = "j.customer_id"

    ledger_entry_expr = "NULL"
    if "ledger_entry_id" in cols:
        ledger_entry_expr = "ji.ledger_entry_id"

    where_parts = ["j.company_id = ?"]
    params = []

    if date_col:
        where_parts.append(f"ji.{date_col} BETWEEN ? AND ?")
        params.extend([start_date, end_date])

    where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT
            ji.id,
            ji.{job_id_col} AS job_id,
            {customer_expr} AS customer_id,
            {ledger_entry_expr} AS ledger_entry_id,
            {category_expr} AS category,
            {description_expr} AS description,
            {amount_expr} AS amount,
            {f"ji.{date_col}" if date_col else "NULL"} AS entry_date
        FROM job_items ji
        JOIN jobs j ON ji.job_id = j.id
        WHERE {where_sql}
        ORDER BY {f"ji.{date_col}" if date_col else "ji.id"} DESC, ji.id DESC
    """

    return sql, params


def normalize_entry_type(raw_type, source_type, amount):
    raw = (raw_type or "").strip().lower()
    source = (source_type or "").strip().lower()
    amt = safe_float(amount, 0)

    if raw in ("income", "payment"):
        return "Income"
    if raw in ("expense", "cost"):
        return "Expense"

    if source in (
        "job_item",
        "job_line",
        "job_material",
        "job_labor",
        "job_cost",
        "payroll",
    ):
        return "Expense"

    if source in (
        "invoice_payment",
        "invoice_paid",
        "invoice_mark_paid",
        "payment",
    ):
        return "Income"

    return "Expense" if amt < 0 else "Income"


def normalize_ledger_rows(ledger_rows):
    normalized = []

    for r in ledger_rows:
        source_type = (safe_get(r, "source_type", "manual") or "manual").strip()
        raw_amount = safe_float(safe_get(r, "amount", 0), 0)

        if source_type in ("invoice_payment", "invoice_paid", "invoice_mark_paid"):
            continue

        entry_type = normalize_entry_type(
            safe_get(r, "entry_type", ""),
            source_type,
            raw_amount,
        )

        normalized.append({
            "id": safe_get(r, "id"),
            "entry_date": safe_get(r, "entry_date", ""),
            "entry_type": entry_type,
            "category": (safe_get(r, "category", "Uncategorized") or "Uncategorized").strip() or "Uncategorized",
            "description": safe_get(r, "description", "") or "",
            "amount": abs(raw_amount),
            "source_type": source_type,
            "source_id": safe_get(r, "source_id"),
            "customer_id": safe_get(r, "customer_id"),
            "invoice_id": safe_get(r, "invoice_id"),
            "job_id": safe_get(r, "job_id"),
            "can_delete": True,
        })

    return normalized


def normalize_payroll_rows(payroll_rows):
    normalized = []

    for r in payroll_rows:
        employee_name = f"{safe_get(r, 'first_name', '') or ''} {safe_get(r, 'last_name', '') or ''}".strip()
        normalized.append({
            "id": f"payroll-{safe_get(r, 'id')}",
            "entry_date": safe_get(r, "pay_date", ""),
            "entry_type": "Expense",
            "category": "Payroll",
            "description": f"Payroll - {employee_name}" + (
                f" ({safe_get(r, 'notes', '')})" if safe_get(r, "notes") else ""
            ),
            "amount": abs(safe_float(safe_get(r, "gross_pay", 0), 0)),
            "source_type": "payroll",
            "source_id": safe_get(r, "id"),
            "customer_id": None,
            "invoice_id": None,
            "job_id": None,
            "can_delete": False,
            "employee_id": safe_get(r, "employee_id"),
            "employee_name": employee_name,
        })

    return normalized


def normalize_invoice_payment_rows(payment_rows):
    normalized = []
    grouped_paid = {}

    for r in payment_rows:
        invoice_status = (safe_get(r, "invoice_status", "") or "").strip()

        if invoice_status == "Paid":
            key = safe_get(r, "invoice_id")

            if key not in grouped_paid:
                grouped_paid[key] = {
                    "id": f"payment-group-{safe_get(r, 'invoice_id')}",
                    "entry_date": safe_get(r, "payment_date", ""),
                    "entry_type": "Income",
                    "category": "Invoice Payment",
                    "description": f"Invoice #{safe_get(r, 'invoice_number') or safe_get(r, 'invoice_id')} paid in full" + (
                        f" ({safe_get(r, 'customer_name')})" if safe_get(r, "customer_name") else ""
                    ),
                    "amount": abs(safe_float(safe_get(r, "invoice_total", 0), 0)),
                    "source_type": "invoice_payment",
                    "source_id": safe_get(r, "invoice_id"),
                    "customer_id": safe_get(r, "customer_id"),
                    "invoice_id": safe_get(r, "invoice_id"),
                    "job_id": None,
                    "can_delete": False,
                }

            if (safe_get(r, "payment_date", "") or "") > (grouped_paid[key]["entry_date"] or ""):
                grouped_paid[key]["entry_date"] = safe_get(r, "payment_date", "")

        else:
            normalized.append({
                "id": f"payment-{safe_get(r, 'id')}",
                "entry_date": safe_get(r, "payment_date", ""),
                "entry_type": "Income",
                "category": "Invoice Payment",
                "description": f"Partial payment for Invoice #{safe_get(r, 'invoice_number') or safe_get(r, 'invoice_id')}" + (
                    f" ({safe_get(r, 'customer_name')})" if safe_get(r, "customer_name") else ""
                ),
                "amount": abs(safe_float(safe_get(r, "amount", 0), 0)),
                "source_type": "invoice_payment",
                "source_id": safe_get(r, "id"),
                "customer_id": safe_get(r, "customer_id"),
                "invoice_id": safe_get(r, "invoice_id"),
                "job_id": None,
                "can_delete": False,
            })

    normalized.extend(grouped_paid.values())
    return normalized


def normalize_job_item_rows(job_item_rows, existing_ledger_rows):
    """
    Pull job_items directly into P&L only when they are not already represented
    in ledger_entries, so you do not double-count.
    """
    normalized = []

    existing_pairs = set()
    existing_ledger_ids = set()

    for r in existing_ledger_rows:
        source_type = (r.get("source_type") or "").strip().lower()
        source_id = r.get("source_id")
        row_id = r.get("id")

        if source_type and source_id not in (None, ""):
            existing_pairs.add((source_type, str(source_id)))

        if row_id not in (None, ""):
            existing_ledger_ids.add(str(row_id))

    for r in job_item_rows:
        item_id = safe_get(r, "id")
        ledger_entry_id = safe_get(r, "ledger_entry_id")
        source_pair = ("job_item", str(item_id)) if item_id not in (None, "") else None

        if source_pair and source_pair in existing_pairs:
            continue

        if ledger_entry_id not in (None, "") and str(ledger_entry_id) in existing_ledger_ids:
            continue

        entry_date = safe_get(r, "entry_date", "") or date.today().isoformat()
        amount = abs(safe_float(safe_get(r, "amount", 0), 0))

        if amount == 0:
            continue

        normalized.append({
            "id": f"job-item-{item_id}",
            "entry_date": entry_date,
            "entry_type": "Expense",
            "category": (safe_get(r, "category", "Material") or "Material").strip() or "Material",
            "description": safe_get(r, "description", "Job item") or "Job item",
            "amount": amount,
            "source_type": "job_item",
            "source_id": item_id,
            "customer_id": safe_get(r, "customer_id"),
            "invoice_id": None,
            "job_id": safe_get(r, "job_id"),
            "can_delete": False,
        })

    return normalized


def fetch_ledger_rows(conn, cid, start_date, end_date):
    return conn.execute(
        """
        SELECT *
        FROM ledger_entries
        WHERE company_id = ?
          AND entry_date BETWEEN ? AND ?
        ORDER BY entry_date DESC, id DESC
        """,
        (cid, start_date, end_date),
    ).fetchall()


def fetch_payroll_rows(conn, cid, start_date, end_date):
    if not table_exists(conn, "payroll_entries") or not table_exists(conn, "employees"):
        return []

    return conn.execute(
        """
        SELECT p.*, e.first_name, e.last_name
        FROM payroll_entries p
        JOIN employees e ON p.employee_id = e.id
        WHERE p.company_id = ?
          AND p.pay_date BETWEEN ? AND ?
        ORDER BY p.pay_date DESC, p.id DESC
        """,
        (cid, start_date, end_date),
    ).fetchall()


def fetch_invoice_payment_rows(conn, cid, start_date, end_date):
    if not table_exists(conn, "invoice_payments") or not table_exists(conn, "invoices"):
        return []

    customer_join = ""
    customer_name_select = "NULL AS customer_name"
    if table_exists(conn, "customers"):
        customer_join = "LEFT JOIN customers c ON i.customer_id = c.id"
        customer_name_select = "c.name AS customer_name"

    return conn.execute(
        f"""
        SELECT ip.*, i.customer_id, i.invoice_number, i.total AS invoice_total,
               i.status AS invoice_status, {customer_name_select}
        FROM invoice_payments ip
        JOIN invoices i ON ip.invoice_id = i.id
        {customer_join}
        WHERE ip.company_id = ?
          AND ip.payment_date BETWEEN ? AND ?
        ORDER BY ip.payment_date DESC, ip.id DESC
        """,
        (cid, start_date, end_date),
    ).fetchall()


def fetch_job_item_rows(conn, cid, start_date, end_date):
    sql, extra_params = build_job_item_query(conn, start_date, end_date)
    if not sql:
        return []

    params = [cid] + list(extra_params)
    return conn.execute(sql, params).fetchall()


def build_combined_rows(conn, cid, start_date, end_date):
    ledger_rows_db = fetch_ledger_rows(conn, cid, start_date, end_date)
    payroll_rows_db = fetch_payroll_rows(conn, cid, start_date, end_date)
    payment_rows_db = fetch_invoice_payment_rows(conn, cid, start_date, end_date)
    job_item_rows_db = fetch_job_item_rows(conn, cid, start_date, end_date)

    normalized_ledger = normalize_ledger_rows(ledger_rows_db)
    normalized_payroll = normalize_payroll_rows(payroll_rows_db)
    normalized_payments = normalize_invoice_payment_rows(payment_rows_db)
    normalized_job_items = normalize_job_item_rows(job_item_rows_db, normalized_ledger)

    rows = normalized_ledger + normalized_payroll + normalized_payments + normalized_job_items
    rows.sort(key=lambda x: (x.get("entry_date") or "", str(x.get("id") or "")), reverse=True)

    return rows


@ledger_bp.route("/ledger", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_bookkeeping")
def ledger():
    conn = get_db_connection()
    cid = session["company_id"]

    if request.method == "POST":
        entry_date = request.form.get("entry_date") or date.today().isoformat()
        entry_type = (request.form.get("entry_type") or "Expense").strip().title()
        category = request.form.get("category") or "Misc"

        if category == "Custom":
            category = (request.form.get("custom_category") or "").strip() or "Misc"

        description = (request.form.get("description") or "").strip()

        amount = safe_float(request.form.get("amount"), 0)

        if amount < 0:
            amount = abs(amount)

        conn.execute(
            """
            INSERT INTO ledger_entries (
                company_id, entry_date, entry_type, category, description, amount,
                source_type, source_id, customer_id, invoice_id, job_id
            )
            VALUES (?, ?, ?, ?, ?, ?, 'manual', NULL, NULL, NULL, NULL)
            """,
            (
                cid,
                entry_date,
                entry_type,
                category,
                description or "Manual entry",
                amount,
            ),
        )
        conn.commit()
        conn.close()

        flash("Bookkeeping entry added.")
        return redirect(url_for("ledger.ledger"))

    view_type = request.args.get("view", "monthly")
    valid_views = ["daily", "weekly", "monthly", "quarterly", "yearly", "yoy"]
    if view_type not in valid_views:
        view_type = "monthly"

    anchor_date = request.args.get("anchor_date", date.today().isoformat())

    yoy_html = ""
    category_html = ""

    if view_type == "yoy":
        anchor_year = datetime.strptime(anchor_date, "%Y-%m-%d").date().year
        current_year = anchor_year
        prior_year = current_year - 1

        current_rows = build_combined_rows(
            conn,
            cid,
            f"{current_year}-01-01",
            f"{current_year}-12-31",
        )
        prior_rows = build_combined_rows(
            conn,
            cid,
            f"{prior_year}-01-01",
            f"{prior_year}-12-31",
        )

        rows = current_rows

        current_income = sum(r["amount"] for r in current_rows if r["entry_type"] == "Income")
        current_expense = sum(r["amount"] for r in current_rows if r["entry_type"] == "Expense")
        current_net = current_income - current_expense

        prior_income = sum(r["amount"] for r in prior_rows if r["entry_type"] == "Income")
        prior_expense = sum(r["amount"] for r in prior_rows if r["entry_type"] == "Expense")
        prior_net = prior_income - prior_expense

        income = current_income
        expense = current_expense
        net = current_net
        period_label = f"{current_year} vs {prior_year}"

        yoy_html = f"""
        <div class='card'>
            <h2>Year over Year Comparison</h2>
            <table>
                <tr>
                    <th>Year</th>
                    <th>Income</th>
                    <th>Expenses</th>
                    <th>Net</th>
                </tr>
                <tr>
                    <td>{prior_year}</td>
                    <td style="color:green; font-weight:600;">+${prior_income:.2f}</td>
                    <td style="color:red; font-weight:600;">-${prior_expense:.2f}</td>
                    <td style="color:{'green' if prior_net >= 0 else 'red'}; font-weight:700;">
                        {'+' if prior_net >= 0 else '-'}${abs(prior_net):.2f}
                    </td>
                </tr>
                <tr>
                    <td>{current_year}</td>
                    <td style="color:green; font-weight:600;">+${current_income:.2f}</td>
                    <td style="color:red; font-weight:600;">-${current_expense:.2f}</td>
                    <td style="color:{'green' if current_net >= 0 else 'red'}; font-weight:700;">
                        {'+' if current_net >= 0 else '-'}${abs(current_net):.2f}
                    </td>
                </tr>
            </table>
        </div>
        """
    else:
        start_date, end_date = get_period_range(view_type, anchor_date)
        rows = build_combined_rows(conn, cid, start_date, end_date)

        income = sum(r["amount"] for r in rows if r["entry_type"] == "Income")
        expense = sum(r["amount"] for r in rows if r["entry_type"] == "Expense")
        net = income - expense
        period_label = f"{start_date} to {end_date}"

    conn.close()

    category_totals = {}
    for r in rows:
        if r["entry_type"] not in ("Income", "Expense"):
            continue

        cat = (r["category"] or "Uncategorized").strip() or "Uncategorized"
        if cat not in category_totals:
            category_totals[cat] = {"Income": 0.0, "Expense": 0.0}

        category_totals[cat][r["entry_type"]] += abs(safe_float(r["amount"], 0))

    category_rows = "".join(
        f"""
        <tr>
            <td>{escape(cat)}</td>
            <td style="color:green; font-weight:600;">+${vals['Income']:.2f}</td>
            <td style="color:red; font-weight:600;">-${vals['Expense']:.2f}</td>
            <td style="color:{'green' if (vals['Income'] - vals['Expense']) >= 0 else 'red'}; font-weight:700;">
                {'+' if (vals['Income'] - vals['Expense']) >= 0 else '-'}${abs(vals['Income'] - vals['Expense']):.2f}
            </td>
        </tr>
        """
        for cat, vals in sorted(category_totals.items())
    )

    category_html = f"""
    <div class='card'>
        <h2>P&amp;L by Category</h2>
        <table>
            <tr>
                <th>Category</th>
                <th>Income</th>
                <th>Expenses</th>
                <th>Net</th>
            </tr>
            {category_rows or '<tr><td colspan="4" class="muted">No category data for this period.</td></tr>'}
        </table>
    </div>
    """

    ledger_rows = "".join(
        f"""
        <tr>
            <td>{escape(str(r.get('entry_date') or '-'))}</td>
            <td>{escape(str(r.get('entry_type') or '-'))}</td>
            <td>{escape(str(r.get('category') or '-'))}</td>
            <td>{escape(str(r.get('description') or '-'))}</td>
            <td style="color:{'green' if r.get('entry_type') == 'Income' else 'red'}; font-weight:600;">
                {'+' if r.get('entry_type') == 'Income' else '-'}${abs(safe_float(r.get('amount'), 0)):.2f}
            </td>
            <td>
                {
                    f"<a class='btn secondary small' href='{url_for('invoices.view_invoice', invoice_id=r.get('invoice_id'))}'>Open Invoice</a>"
                    if r.get('invoice_id')
                    else (
                        f"<a class='btn secondary small' href='{url_for('jobs.view_job', job_id=r.get('job_id'))}'>Open Job</a>"
                        if r.get('job_id')
                        else (
                            f"<a class='btn secondary small' href='{url_for('employees.view_employee', employee_id=r.get('employee_id'))}'>Open Employee</a>"
                            if r.get('source_type') == 'payroll' and r.get('employee_id')
                            else escape(str(r.get('source_type') or '-'))
                        )
                    )
                }
            </td>
            <td>
                {
                    f'''
                    <form method='post'
                          action='{url_for("ledger.delete_ledger_entry", entry_id=r.get("id"))}'
                          onsubmit="return confirm('Delete this bookkeeping entry?');"
                          style='display:inline;'>
                        <button class='btn danger small' type='submit'>Delete</button>
                    </form>
                    '''
                    if r.get("can_delete") and isinstance(r.get("id"), int)
                    else "<span class='muted small'>Auto</span>"
                }
            </td>
        </tr>
        """
        for r in rows
    )

    filter_bar = f"""
    <div class='card'>
        <h1>Bookkeeping / P&amp;L</h1>
        <form method='get'>
            <div class='grid'>
                <div>
                    <label>View</label>
                    <select name='view'>
                        <option value='daily' {'selected' if view_type == 'daily' else ''}>Daily</option>
                        <option value='weekly' {'selected' if view_type == 'weekly' else ''}>Weekly</option>
                        <option value='monthly' {'selected' if view_type == 'monthly' else ''}>Monthly</option>
                        <option value='quarterly' {'selected' if view_type == 'quarterly' else ''}>Quarterly</option>
                        <option value='yearly' {'selected' if view_type == 'yearly' else ''}>Yearly</option>
                        <option value='yoy' {'selected' if view_type == 'yoy' else ''}>YoY</option>
                    </select>
                </div>
                <div>
                    <label>Anchor Date</label>
                    <input type='date' name='anchor_date' value='{anchor_date}'>
                </div>
            </div>
            <br>
            <button class='btn' type='submit'>Apply</button>
            <a class='btn secondary' href='{url_for("ledger.export_ledger_csv", view=view_type, anchor_date=anchor_date)}'>Export CSV</a>
        </form>
        <br>
        <div class='muted'><strong>Viewing:</strong> {period_label}</div>
    </div>
    """

    manual_entry_form = f"""
    <div class='card'>
        <h2>Add Manual Bookkeeping Entry</h2>
        <form method='post'>
            <div class='grid'>
                <div>
                    <label>Date</label>
                    <input type='date' name='entry_date' value='{date.today().isoformat()}' required>
                </div>
                <div>
                    <label>Type</label>
                    <select name='entry_type' required>
                        <option value='Expense'>Expense</option>
                        <option value='Income'>Income</option>
                    </select>
                </div>
                <div>
                    <label>Category</label>
                    <select name='category'>
                        <option value='Material'>Material</option>
                        <option value='Labor'>Labor</option>
                        <option value='Fuel'>Fuel</option>
                        <option value='Equipment'>Equipment</option>
                        <option value='Payroll'>Payroll</option>
                        <option value='Rent'>Rent</option>
                        <option value='Insurance'>Insurance</option>
                        <option value='Utilities'>Utilities</option>
                        <option value='Office Supplies'>Office Supplies</option>
                        <option value='Repairs'>Repairs</option>
                        <option value='Bank Fees'>Bank Fees</option>
                        <option value='Loan Payment'>Loan Payment</option>
                        <option value='Misc'>Misc</option>
                        <option value='Custom'>Custom</option>
                    </select>
                </div>
                <div>
                    <label>Custom Category</label>
                    <input type='text' name='custom_category' placeholder='Only used if Custom is selected'>
                </div>
                <div>
                    <label>Amount</label>
                    <input type='number' step='0.01' name='amount' required>
                </div>
            </div>
            <br>
            <label>Description</label>
            <textarea name='description'></textarea>
            <br>
            <button class='btn success' type='submit'>Add Entry</button>
        </form>
    </div>
    """

    content = f"""
    {filter_bar}

    <div style="display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:16px; margin-bottom:20px;">
        <div class='card'>
            <h3>Total Income</h3>
            <div style="color:green; font-weight:700; font-size:1.4rem;">+${income:.2f}</div>
        </div>
        <div class='card'>
            <h3>Total Expenses</h3>
            <div style="color:red; font-weight:700; font-size:1.4rem;">-${expense:.2f}</div>
        </div>
        <div class='card'>
            <h3>Net</h3>
            <div style="color:{'green' if net >= 0 else 'red'}; font-weight:700; font-size:1.4rem;">
                {'+' if net >= 0 else '-'}${abs(net):.2f}
            </div>
        </div>
    </div>

    {yoy_html}
    {category_html}
    {manual_entry_form}

    <div class='card'>
        <h2>Ledger Entries</h2>

        <div style="max-height:500px; overflow-y:auto; border:1px solid #ddd; border-radius:10px;">
            <table style="width:100%; border-collapse:collapse;">
                <thead>
                    <tr>
                        <th style="position:sticky; top:0; background:#fff; z-index:2;">Date</th>
                        <th style="position:sticky; top:0; background:#fff; z-index:2;">Type</th>
                        <th style="position:sticky; top:0; background:#fff; z-index:2;">Category</th>
                        <th style="position:sticky; top:0; background:#fff; z-index:2;">Description</th>
                        <th style="position:sticky; top:0; background:#fff; z-index:2;">Amount</th>
                        <th style="position:sticky; top:0; background:#fff; z-index:2;">Source</th>
                        <th style="position:sticky; top:0; background:#fff; z-index:2;">Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {ledger_rows or '<tr><td colspan="7" class="muted">No bookkeeping entries for this period.</td></tr>'}
                </tbody>
            </table>
        </div>
    </div>
    """

    return render_page(content, "Bookkeeping / P&L")


@ledger_bp.route("/ledger/export")
@login_required
@require_permission("can_manage_bookkeeping")
def export_ledger_csv():
    conn = get_db_connection()
    cid = session["company_id"]

    view_type = request.args.get("view", "monthly")
    valid_views = ["daily", "weekly", "monthly", "quarterly", "yearly", "yoy"]
    if view_type not in valid_views:
        view_type = "monthly"

    anchor_date = request.args.get("anchor_date", date.today().isoformat())

    if view_type == "yoy":
        anchor_year = datetime.strptime(anchor_date, "%Y-%m-%d").date().year
        start_date = f"{anchor_year}-01-01"
        end_date = f"{anchor_year}-12-31"
    else:
        start_date, end_date = get_period_range(view_type, anchor_date)

    rows = build_combined_rows(conn, cid, start_date, end_date)
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Date",
        "Type",
        "Category",
        "Description",
        "Amount",
        "Source Type",
        "Invoice ID",
        "Job ID",
    ])

    for r in rows:
        signed_amount = abs(safe_float(r.get("amount"), 0))
        if r.get("entry_type") == "Expense":
            signed_amount = -signed_amount

        writer.writerow([
            r.get("entry_date") or "",
            r.get("entry_type") or "",
            r.get("category") or "",
            r.get("description") or "",
            f"{signed_amount:.2f}",
            r.get("source_type") or "",
            r.get("invoice_id") or "",
            r.get("job_id") or "",
        ])

    filename = f"ledger_{view_type}_{anchor_date}.csv"

    response = make_response(output.getvalue())
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers["Content-type"] = "text/csv"
    return response


@ledger_bp.route("/ledger/<int:entry_id>/delete", methods=["POST"])
@login_required
@require_permission("can_manage_bookkeeping")
def delete_ledger_entry(entry_id):
    conn = get_db_connection()
    cid = session["company_id"]

    entry = conn.execute(
        """
        SELECT *
        FROM ledger_entries
        WHERE id = ? AND company_id = ?
        """,
        (entry_id, cid),
    ).fetchone()

    if not entry:
        conn.close()
        flash("Bookkeeping entry not found.")
        return redirect(url_for("ledger.ledger"))

    conn.execute(
        "DELETE FROM ledger_entries WHERE id = ? AND company_id = ?",
        (entry_id, cid),
    )

    source_type = safe_get(entry, "source_type", "")

    if source_type == "job_item" and safe_get(entry, "source_id"):
        if table_exists(conn, "job_items") and has_col(conn, "job_items", "ledger_entry_id"):
            conn.execute(
                "UPDATE job_items SET ledger_entry_id = NULL WHERE id = ?",
                (safe_get(entry, "source_id"),),
            )

    if source_type == "payroll" and safe_get(entry, "source_id"):
        if table_exists(conn, "payroll_entries") and has_col(conn, "payroll_entries", "ledger_entry_id"):
            conn.execute(
                "UPDATE payroll_entries SET ledger_entry_id = NULL WHERE id = ?",
                (safe_get(entry, "source_id"),),
            )

    conn.commit()
    conn.close()

    flash("Bookkeeping entry deleted.")
    return redirect(url_for("ledger.ledger"))
