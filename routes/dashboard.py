from flask import Blueprint, session, url_for
from db import get_db_connection
from decorators import login_required, subscription_required
from page_helpers import render_page

dashboard_bp = Blueprint("dashboard", __name__)


EXPENSE_TYPES = {
    "expense",
    "cost",
    "job cost",
    "material",
    "materials",
    "labor",
    "labour",
    "fuel",
    "equipment",
    "delivery",
    "misc",
    "payroll",
}


def _safe_float(value):
    try:
        return float(value or 0)
    except:
        return 0.0


def dashboard():
    conn = get_db_connection()
    cid = session["company_id"]

    # COUNTS
    customers_count = conn.execute(
        "SELECT COUNT(*) AS count FROM customers WHERE company_id = %s",
        (cid,)
    ).fetchone()["count"]

    quotes_count = conn.execute(
        "SELECT COUNT(*) AS count FROM quotes WHERE company_id = %s",
        (cid,)
    ).fetchone()["count"]

    jobs_count = conn.execute(
        "SELECT COUNT(*) AS count FROM jobs WHERE company_id = %s",
        (cid,)
    ).fetchone()["count"]

    invoices_count = conn.execute(
        "SELECT COUNT(*) AS count FROM invoices WHERE company_id = %s",
        (cid,)
    ).fetchone()["count"]

    # 🔥 FIXED INCOME / EXPENSE LOGIC
    ledger_rows = conn.execute(
        """
        SELECT entry_type, amount
        FROM ledger_entries
        WHERE company_id = %s
        """,
        (cid,)
    ).fetchall()

    total_income = 0.0
    total_expenses = 0.0

    for r in ledger_rows:
        amount = abs(_safe_float(r["amount"]))
        entry_type = str(r["entry_type"] or "").lower()

        if any(x in entry_type for x in EXPENSE_TYPES):
            total_expenses += amount
        else:
            total_income += amount

    invoice_payments = conn.execute(
        "SELECT COALESCE(SUM(amount),0) AS total FROM invoice_payments WHERE company_id = %s",
        (cid,)
    ).fetchone()["total"]

    total_income += _safe_float(invoice_payments)

    profit_total = total_income - total_expenses

    # JOBS / INVOICES
    upcoming_jobs = conn.execute(
        """
        SELECT j.id, j.title, j.status, c.name AS customer_name
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.company_id = %s
        ORDER BY j.id DESC
        LIMIT 8
        """,
        (cid,)
    ).fetchall()

    unpaid_invoices = conn.execute(
        """
        SELECT i.id, i.status, i.balance_due, c.name AS customer_name
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.company_id = %s
          AND COALESCE(i.status, '') != 'Paid'
        ORDER BY i.id DESC
        LIMIT 8
        """,
        (cid,)
    ).fetchall()

    conn.close()

    upcoming_rows = "".join(
        f"""
        <tr>
            <td>#{r['id']}</td>
            <td>{r['title'] or '-'}</td>
            <td>{r['customer_name'] or '-'}</td>
            <td>{r['status'] or '-'}</td>
            <td><a class='btn secondary small' href='{url_for("jobs.view_job", job_id=r["id"])}'>View</a></td>
        </tr>
        """
        for r in upcoming_jobs
    )

    unpaid_rows = "".join(
        f"""
        <tr>
            <td>#{r['id']}</td>
            <td>{r['customer_name'] or '-'}</td>
            <td>{r['status'] or '-'}</td>
            <td>${_safe_float(r['balance_due']):,.2f}</td>
            <td><a class='btn secondary small' href='{url_for("invoices.view_invoice", invoice_id=r["id"])}'>View</a></td>
        </tr>
        """
        for r in unpaid_invoices
    )

    content = f"""

    <style>
    .dashboard-grid {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 18px;
        margin-top: 18px;
    }}

    @media (max-width: 900px) {{
        .dashboard-grid {{
            grid-template-columns: 1fr;
        }}
    }}
    </style>

    <h1>Dashboard</h1>

    <div class='stats-grid'>
        <div class='card stat-card'><div class='stat-label'>Customers</div><div class='stat-value'>{customers_count}</div></div>
        <div class='card stat-card'><div class='stat-label'>Quotes</div><div class='stat-value'>{quotes_count}</div></div>
        <div class='card stat-card'><div class='stat-label'>Jobs</div><div class='stat-value'>{jobs_count}</div></div>
        <div class='card stat-card'><div class='stat-label'>Invoices</div><div class='stat-value'>{invoices_count}</div></div>
    </div>

    <div style="display:flex; gap:16px; justify-content:center; flex-wrap:wrap; margin-top:18px;">

        <div class='card stat-card' style="width:260px;">
            <div class='stat-label'>Income</div>
            <div class='stat-value' style="color:#16a34a;">+${total_income:,.2f}</div>
        </div>

        <div class='card stat-card' style="width:260px;">
            <div class='stat-label'>Expenses</div>
            <div class='stat-value' style="color:#dc2626;">-${total_expenses:,.2f}</div>
        </div>

        <div class='card stat-card' style="width:260px;">
            <div class='stat-label'>Profit</div>
            <div class='stat-value' style="color:{'#16a34a' if profit_total >= 0 else '#dc2626'};">
                {'+' if profit_total >= 0 else '-'}${abs(profit_total):,.2f}
            </div>
        </div>

    </div>

    <div class='dashboard-grid'>

        <div class='card'>
            <h2>Upcoming Jobs</h2>
            <table>
                <tr><th>ID</th><th>Title</th><th>Customer</th><th>Status</th><th></th></tr>
                {upcoming_rows or "<tr><td colspan='5'>No jobs.</td></tr>"}
            </table>
        </div>

        <div class='card'>
            <h2>Unpaid Invoices</h2>
            <table>
                <tr><th>ID</th><th>Customer</th><th>Status</th><th>Balance</th><th></th></tr>
                {unpaid_rows or "<tr><td colspan='5'>No unpaid invoices.</td></tr>"}
            </table>
        </div>

    </div>
    """

    return render_page(content, "Dashboard")