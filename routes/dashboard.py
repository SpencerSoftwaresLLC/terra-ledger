from flask import Blueprint, session, url_for
from db import get_db_connection
from decorators import login_required, subscription_required
from page_helpers import render_page

dashboard_bp = Blueprint("dashboard", __name__)


EXPENSE_TYPES = {
    "expense",
    "expenses",
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
    except Exception:
        return 0.0


def _is_expense_type(entry_type):
    text = str(entry_type or "").strip().lower()
    if not text:
        return False
    if text in EXPENSE_TYPES:
        return True
    return any(term in text for term in EXPENSE_TYPES)


@dashboard_bp.route("/dashboard")
@login_required
@subscription_required
def dashboard():
    conn = get_db_connection()
    cid = session["company_id"]

    customers_count_row = conn.execute(
        "SELECT COUNT(*) AS count FROM customers WHERE company_id = %s",
        (cid,),
    ).fetchone()
    customers_count = int(customers_count_row["count"] or 0) if customers_count_row else 0

    quotes_count_row = conn.execute(
        "SELECT COUNT(*) AS count FROM quotes WHERE company_id = %s",
        (cid,),
    ).fetchone()
    quotes_count = int(quotes_count_row["count"] or 0) if quotes_count_row else 0

    jobs_count_row = conn.execute(
        "SELECT COUNT(*) AS count FROM jobs WHERE company_id = %s",
        (cid,),
    ).fetchone()
    jobs_count = int(jobs_count_row["count"] or 0) if jobs_count_row else 0

    invoices_count_row = conn.execute(
        "SELECT COUNT(*) AS count FROM invoices WHERE company_id = %s",
        (cid,),
    ).fetchone()
    invoices_count = int(invoices_count_row["count"] or 0) if invoices_count_row else 0

    ledger_rows = conn.execute(
        """
        SELECT entry_type, amount, description, source_type
        FROM ledger_entries
        WHERE company_id = %s
        """,
        (cid,),
    ).fetchall()

    ledger_income_total = 0.0
    expense_total = 0.0

    for row in ledger_rows:
        amount = abs(_safe_float(row["amount"]))
        entry_type = row["entry_type"] or ""
        source_type = str(row["source_type"] or "").strip().lower()

        if source_type in {"invoice_payment", "invoice_paid", "invoice_mark_paid"}:
            continue

        if _is_expense_type(entry_type):
            expense_total += amount
        else:
            ledger_income_total += amount

    invoice_payment_total_row = conn.execute(
        """
        SELECT COALESCE(SUM(amount), 0) AS total
        FROM invoice_payments
        WHERE company_id = %s
        """,
        (cid,),
    ).fetchone()
    invoice_payment_total = _safe_float(invoice_payment_total_row["total"]) if invoice_payment_total_row else 0.0

    income_total = ledger_income_total + invoice_payment_total
    profit_total = income_total - expense_total

    upcoming_jobs = conn.execute(
        """
        SELECT j.id, j.title, j.status, c.name AS customer_name
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.company_id = %s
        ORDER BY j.id DESC
        LIMIT 8
        """,
        (cid,),
    ).fetchall()

    unpaid_invoices = conn.execute(
        """
        SELECT i.id, i.status, i.total, i.balance_due, c.name AS customer_name
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.company_id = %s
          AND COALESCE(i.status, '') != 'Paid'
        ORDER BY i.id DESC
        LIMIT 8
        """,
        (cid,),
    ).fetchall()

    aging_rows = conn.execute(
        """
        SELECT
            i.id,
            i.invoice_number,
            i.invoice_date,
            i.due_date,
            i.status,
            i.balance_due,
            c.name AS customer_name,
            CASE
                WHEN (
                    CURRENT_DATE - COALESCE(i.due_date, i.invoice_date, CURRENT_DATE)
                ) <= 0 THEN 'Current'
                WHEN (
                    CURRENT_DATE - COALESCE(i.due_date, i.invoice_date, CURRENT_DATE)
                ) BETWEEN 1 AND 30 THEN '1-30'
                WHEN (
                    CURRENT_DATE - COALESCE(i.due_date, i.invoice_date, CURRENT_DATE)
                ) BETWEEN 31 AND 60 THEN '31-60'
                WHEN (
                    CURRENT_DATE - COALESCE(i.due_date, i.invoice_date, CURRENT_DATE)
                ) BETWEEN 61 AND 90 THEN '61-90'
                ELSE '90+'
            END AS aging_bucket
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.company_id = %s
          AND COALESCE(i.balance_due, 0) > 0
        ORDER BY COALESCE(i.due_date, i.invoice_date, CURRENT_DATE) ASC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    aging_totals = {
        "Current": 0.0,
        "1-30": 0.0,
        "31-60": 0.0,
        "61-90": 0.0,
        "90+": 0.0,
    }

    for row in aging_rows:
        bucket = row["aging_bucket"] or "Current"
        aging_totals[bucket] = aging_totals.get(bucket, 0.0) + _safe_float(row["balance_due"])

    total_outstanding = sum(aging_totals.values())

    upcoming_rows = "".join(
        f"""
        <tr>
            <td>#{r['id']}</td>
            <td>{r['title'] or '-'}</td>
            <td>{r['customer_name'] or '-'}</td>
            <td>{r['status'] or '-'}</td>
            <td>
                <a class='btn secondary small' href='{url_for("jobs.view_job", job_id=r["id"])}'>View</a>
            </td>
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
            <td>
                <a class='btn secondary small' href='{url_for("invoices.view_invoice", invoice_id=r["id"])}'>View</a>
            </td>
        </tr>
        """
        for r in unpaid_invoices
    )

    aging_table_rows = "".join(
        f"""
        <tr>
            <td>{r['invoice_number'] or f"#{r['id']}"}</td>
            <td>{r['customer_name'] or '-'}</td>
            <td>{r['invoice_date'] or '-'}</td>
            <td>{r['due_date'] or '-'}</td>
            <td>{r['aging_bucket']}</td>
            <td>${_safe_float(r['balance_due']):,.2f}</td>
            <td>
                <a class='btn secondary small' href='{url_for("invoices.view_invoice", invoice_id=r["id"])}'>View</a>
            </td>
        </tr>
        """
        for r in aging_rows
    )

    content = f"""
    <style>
    .dashboard-grid {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 18px;
        margin-top: 18px;
        align-items: start;
    }}

    .dashboard-grid .card {{
        min-width: 0;
        width: 100%;
        overflow: hidden;
    }}

    .dashboard-grid .table-wrap {{
        width: 100%;
        overflow-x: auto;
    }}

    @media (max-width: 900px) {{
        .dashboard-grid {{
            grid-template-columns: 1fr;
        }}
    }}
    </style>

    <h1>Dashboard</h1>

    <div class='stats-grid'>
        <div class='card stat-card'>
            <div class='stat-label'>Customers</div>
            <div class='stat-value'>{customers_count}</div>
        </div>

        <div class='card stat-card'>
            <div class='stat-label'>Quotes</div>
            <div class='stat-value'>{quotes_count}</div>
        </div>

        <div class='card stat-card'>
            <div class='stat-label'>Jobs</div>
            <div class='stat-value'>{jobs_count}</div>
        </div>

        <div class='card stat-card'>
            <div class='stat-label'>Invoices</div>
            <div class='stat-value'>{invoices_count}</div>
        </div>
    </div>

    <div style="display:flex; gap:16px; justify-content:center; flex-wrap:wrap; margin-top:18px;">

        <div class='card stat-card' style="width:260px;">
            <div class='stat-label'>Income</div>
            <div class='stat-value' style="color:#16a34a;">
                +${income_total:,.2f}
            </div>
        </div>

        <div class='card stat-card' style="width:260px;">
            <div class='stat-label'>Expenses</div>
            <div class='stat-value' style="color:#dc2626;">
                -${expense_total:,.2f}
            </div>
        </div>

        <div class='card stat-card' style="width:260px;">
            <div class='stat-label'>Profit</div>
            <div class='stat-value' style="color:{'#16a34a' if profit_total >= 0 else '#dc2626'};">
                {'+' if profit_total >= 0 else '-'}${abs(profit_total):,.2f}
            </div>
        </div>

    </div>

    <div class='card' style='padding:20px;'>
        <div style='display:flex; justify-content:space-between; align-items:flex-start; gap:16px; flex-wrap:wrap; margin-bottom:18px;'>
            <div>
                <h2 style='margin:0 0 6px 0;'>Outstanding Invoice Aging</h2>
            </div>
            <div style='text-align:right;'>
                <div class='muted' style='font-size:.9rem;'>Total Outstanding</div>
                <div style='font-size:1.6rem; font-weight:800; color:#0f172a;'>${total_outstanding:,.2f}</div>
            </div>
        </div>

        <div style='overflow-x:auto;'>
            <table style='width:100%; table-layout:fixed; border-collapse:collapse; margin-bottom:18px;'>
                <tr>
                    <th style='text-align:center; background:#f8fafc; width:20%; padding:12px;'>Current</th>
                    <th style='text-align:center; background:#f8fafc; width:20%; padding:12px;'>1-30</th>
                    <th style='text-align:center; background:#f8fafc; width:20%; padding:12px;'>31-60</th>
                    <th style='text-align:center; background:#f8fafc; width:20%; padding:12px;'>61-90</th>
                    <th style='text-align:center; background:#f8fafc; width:20%; padding:12px;'>90+</th>
                </tr>
                <tr>
                    <td style='text-align:center; font-size:1.2rem; font-weight:700; padding:14px;'>${aging_totals["Current"]:,.2f}</td>
                    <td style='text-align:center; font-size:1.2rem; font-weight:700; padding:14px;'>${aging_totals["1-30"]:,.2f}</td>
                    <td style='text-align:center; font-size:1.2rem; font-weight:700; padding:14px;'>${aging_totals["31-60"]:,.2f}</td>
                    <td style='text-align:center; font-size:1.2rem; font-weight:700; padding:14px;'>${aging_totals["61-90"]:,.2f}</td>
                    <td style='text-align:center; font-size:1.2rem; font-weight:700; padding:14px;'>${aging_totals["90+"]:,.2f}</td>
                </tr>
            </table>
        </div>

        <div class='table-wrap'>
            <table>
                <tr>
                    <th>Invoice</th>
                    <th>Customer</th>
                    <th>Invoice Date</th>
                    <th>Due Date</th>
                    <th>Bucket</th>
                    <th>Balance Due</th>
                    <th></th>
                </tr>
                {aging_table_rows or "<tr><td colspan='7'>No outstanding invoices.</td></tr>"}
            </table>
        </div>
    </div>

    <div class='dashboard-grid'>

        <div class='card'>
            <div class='section-head'>
                <h2>Upcoming Jobs</h2>
                <a class='btn small' href='{url_for("jobs.jobs")}'>View All</a>
            </div>

            <div class='table-wrap'>
                <table>
                    <tr>
                        <th>ID</th>
                        <th>Title</th>
                        <th>Customer</th>
                        <th>Status</th>
                        <th></th>
                    </tr>
                    {upcoming_rows or "<tr><td colspan='5'>No jobs.</td></tr>"}
                </table>
            </div>
        </div>

        <div class='card'>
            <div class='section-head'>
                <h2>Unpaid Invoices</h2>
                <a class='btn small' href='{url_for("invoices.invoices")}'>View All</a>
            </div>

            <div class='table-wrap'>
                <table>
                    <tr>
                        <th>ID</th>
                        <th>Customer</th>
                        <th>Status</th>
                        <th>Balance</th>
                        <th></th>
                    </tr>
                    {unpaid_rows or "<tr><td colspan='5'>No unpaid invoices.</td></tr>"}
                </table>
            </div>
        </div>

    </div>
    """

    return render_page(content, "Dashboard")