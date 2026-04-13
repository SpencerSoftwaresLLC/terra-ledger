from flask import Blueprint, session, url_for, redirect, flash
from datetime import date, datetime

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


def _get_lang():
    # Prefer the newer session key first, then fall back to the older one.
    lang = str(session.get("language") or session.get("language_preference") or "en").strip().lower()
    return "es" if lang == "es" else "en"


def _t(lang, en, es):
    return es if lang == "es" else en


def _safe_float(value):
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _safe_text(value, fallback="-"):
    text = str(value or "").strip()
    return text if text else fallback


def _is_expense_type(entry_type):
    text = str(entry_type or "").strip().lower()
    if not text:
        return False
    if text in EXPENSE_TYPES:
        return True
    return any(term in text for term in EXPENSE_TYPES)


def _parse_possible_date(value):
    if value is None:
        return None

    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        try:
            if isinstance(value, datetime):
                return value.date()
            return value
        except Exception:
            pass

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt).date()
        except Exception:
            continue

    return None


def _get_table_columns(conn, table_name):
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (table_name,),
    ).fetchall()
    return {str(r["column_name"]).strip().lower() for r in rows}


def _table_exists(conn, table_name):
    row = conn.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
        ) AS exists_flag
        """,
        (table_name,),
    ).fetchone()
    return bool(row["exists_flag"]) if row else False


def _get_payroll_expense_total(conn, company_id):
    if not _table_exists(conn, "payroll_entries"):
        return 0.0

    payroll_columns = _get_table_columns(conn, "payroll_entries")
    if not payroll_columns or "company_id" not in payroll_columns:
        return 0.0

    gross_candidates = [
        "gross_pay",
        "gross_wages",
        "gross_income",
        "gross_total",
        "total_gross",
        "total_gross_pay",
        "pay_amount",
        "wages",
        "amount",
        "net_pay",
    ]

    employer_tax_candidates = [
        "employer_taxes",
        "employer_tax",
        "employer_tax_total",
        "employer_payroll_taxes",
        "employer_fica",
        "employer_social_security",
        "employer_medicare",
        "futa",
        "suta",
        "state_unemployment",
        "federal_unemployment",
    ]

    selected_gross_col = None
    for col in gross_candidates:
        if col in payroll_columns:
            selected_gross_col = col
            break

    if not selected_gross_col:
        return 0.0

    select_parts = [f"COALESCE(SUM({selected_gross_col}), 0) AS gross_total"]

    for col in employer_tax_candidates:
        if col in payroll_columns:
            select_parts.append(f"COALESCE(SUM({col}), 0) AS {col}")

    sql = f"""
        SELECT {", ".join(select_parts)}
        FROM payroll_entries
        WHERE company_id = %s
    """

    row = conn.execute(sql, (company_id,)).fetchone()
    if not row:
        return 0.0

    total = _safe_float(row["gross_total"])

    for col in employer_tax_candidates:
        if col in payroll_columns:
            total += _safe_float(row[col])

    return total


def _ledger_has_payroll_source_entries(conn, company_id):
    if not _table_exists(conn, "ledger_entries"):
        return False

    ledger_columns = _get_table_columns(conn, "ledger_entries")
    if "company_id" not in ledger_columns or "source_type" not in ledger_columns:
        return False

    row = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM ledger_entries
        WHERE company_id = %s
          AND LOWER(COALESCE(source_type, '')) IN (
              'payroll',
              'payroll_entry',
              'payroll_entries',
              'payroll_run',
              'paycheck'
          )
        """,
        (company_id,),
    ).fetchone()

    return bool(row and int(row["count"] or 0) > 0)


@dashboard_bp.route("/dashboard")
@login_required
@subscription_required
def dashboard():
    lang = _get_lang()
    cid = session.get("company_id")

    if not cid:
        flash(_t(lang, "Company session not found.", "No se encontró la sesión de la empresa."))
        return redirect(url_for("auth.login"))

    conn = get_db_connection()

    try:
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

        payroll_expense_total = 0.0
        if not _ledger_has_payroll_source_entries(conn, cid):
            payroll_expense_total = _get_payroll_expense_total(conn, cid)

        expense_total += payroll_expense_total

        income_total = ledger_income_total + invoice_payment_total
        profit_total = income_total - expense_total

        upcoming_jobs = conn.execute(
            """
            SELECT
                j.id,
                j.title,
                j.status,
                j.scheduled_date,
                j.scheduled_start_time,
                c.name AS customer_name
            FROM jobs j
            JOIN customers c ON j.customer_id = c.id
            WHERE j.company_id = %s
            ORDER BY j.scheduled_date ASC NULLS LAST, j.id ASC
            LIMIT 8
            """,
            (cid,),
        ).fetchall()

        unpaid_invoices = conn.execute(
            """
            SELECT i.id, i.invoice_number, i.status, i.total, i.balance_due,
                   i.invoice_date, i.due_date, c.name AS customer_name
            FROM invoices i
            JOIN customers c ON i.customer_id = c.id
            WHERE i.company_id = %s
              AND LOWER(COALESCE(i.status, '')) NOT IN ('paid', 'pagada')
            ORDER BY i.id DESC
            LIMIT 8
            """,
            (cid,),
        ).fetchall()

        aging_source_rows = conn.execute(
            """
            SELECT
                i.id,
                i.invoice_number,
                i.invoice_date,
                i.due_date,
                i.status,
                i.balance_due,
                c.name AS customer_name
            FROM invoices i
            JOIN customers c ON i.customer_id = c.id
            WHERE i.company_id = %s
              AND COALESCE(i.balance_due, 0) > 0
            ORDER BY i.id DESC
            """,
            (cid,),
        ).fetchall()

    finally:
        conn.close()

    today = date.today()
    aging_rows = []
    aging_totals = {
        "Current": 0.0,
        "1-30": 0.0,
        "31-60": 0.0,
        "61-90": 0.0,
        "90+": 0.0,
    }

    for row in aging_source_rows:
        due_dt = _parse_possible_date(row["due_date"])
        invoice_dt = _parse_possible_date(row["invoice_date"])
        base_date = due_dt or invoice_dt or today

        days_past_due = (today - base_date).days

        if days_past_due <= 0:
            bucket = "Current"
        elif days_past_due <= 30:
            bucket = "1-30"
        elif days_past_due <= 60:
            bucket = "31-60"
        elif days_past_due <= 90:
            bucket = "61-90"
        else:
            bucket = "90+"

        balance_due = _safe_float(row["balance_due"])
        aging_totals[bucket] += balance_due

        aging_rows.append(
            {
                "id": row["id"],
                "invoice_number": row["invoice_number"],
                "invoice_date": row["invoice_date"],
                "due_date": row["due_date"],
                "status": row["status"],
                "balance_due": balance_due,
                "customer_name": row["customer_name"],
                "aging_bucket": bucket,
            }
        )

    total_outstanding = sum(aging_totals.values())

    upcoming_rows = "".join(
        f"""
        <tr>
            <td>#{r['id']}</td>
            <td>{_safe_text(r['title'])}</td>
            <td>{_safe_text(r['customer_name'])}</td>
            <td>{_safe_text(r['scheduled_date'])}</td>
            <td>{_safe_text(r['status'])}</td>
            <td>
                <a class='btn secondary small dashboard-btn' href='{url_for("jobs.view_job", job_id=r["id"])}'>{_t(lang, "View", "Ver")}</a>
            </td>
        </tr>
        """
        for r in upcoming_jobs
    )

    unpaid_rows = "".join(
        f"""
        <tr>
            <td>#{r['id']}</td>
            <td>{_safe_text(r['customer_name'])}</td>
            <td>{_safe_text(r['status'])}</td>
            <td>${_safe_float(r['balance_due']):,.2f}</td>
            <td>
                <a class='btn secondary small dashboard-btn' href='{url_for("invoices.view_invoice", invoice_id=r["id"])}'>{_t(lang, "View", "Ver")}</a>
            </td>
        </tr>
        """
        for r in unpaid_invoices
    )

    aging_table_rows = "".join(
        f"""
        <tr>
            <td>{_safe_text(r['invoice_number'], f"#{r['id']}")}</td>
            <td>{_safe_text(r['customer_name'])}</td>
            <td>{_safe_text(r['invoice_date'])}</td>
            <td>{_safe_text(r['due_date'])}</td>
            <td>{r['aging_bucket']}</td>
            <td>${_safe_float(r['balance_due']):,.2f}</td>
            <td>
                <a class='btn secondary small dashboard-btn' href='{url_for("invoices.view_invoice", invoice_id=r["id"])}'>{_t(lang, "View Invoice", "Ver Factura")}</a>
            </td>
        </tr>
        """
        for r in aging_rows
    )

    aging_mobile_cards = "".join(
        f"""
        <div class='mobile-list-card'>
            <div class='mobile-list-top'>
                <div class='mobile-list-title'>{_safe_text(r['invoice_number'], f"#{r['id']}")}</div>
                <div class='mobile-list-amount'>${_safe_float(r['balance_due']):,.2f}</div>
            </div>
            <div class='mobile-list-grid'>
                <div><span>{_t(lang, "Customer", "Cliente")}</span><strong>{_safe_text(r['customer_name'])}</strong></div>
                <div><span>{_t(lang, "Bucket", "Grupo")}</span><strong>{_safe_text(r['aging_bucket'])}</strong></div>
                <div><span>{_t(lang, "Invoice Date", "Fecha de Factura")}</span><strong>{_safe_text(r['invoice_date'])}</strong></div>
                <div><span>{_t(lang, "Due Date", "Fecha de Vencimiento")}</span><strong>{_safe_text(r['due_date'])}</strong></div>
            </div>
            <div class='mobile-list-actions'>
                <a class='btn secondary small dashboard-btn' href='{url_for("invoices.view_invoice", invoice_id=r["id"])}'>{_t(lang, "View Invoice", "Ver Factura")}</a>
            </div>
        </div>
        """
        for r in aging_rows
    )

    upcoming_mobile_cards = "".join(
        f"""
        <div class='mobile-list-card'>
            <div class='mobile-list-top'>
                <div class='mobile-list-title'>#{r['id']} - {_safe_text(r['title'])}</div>
                <div class='mobile-badge'>{_safe_text(r['status'])}</div>
            </div>
            <div class='mobile-list-grid'>
                <div><span>{_t(lang, "Customer", "Cliente")}</span><strong>{_safe_text(r['customer_name'])}</strong></div>
                <div><span>{_t(lang, "Date", "Fecha")}</span><strong>{_safe_text(r['scheduled_date'])}</strong></div>
                <div><span>{_t(lang, "Start", "Inicio")}</span><strong>{_safe_text(r['scheduled_start_time'])}</strong></div>
                <div><span>{_t(lang, "Status", "Estado")}</span><strong>{_safe_text(r['status'])}</strong></div>
            </div>
            <div class='mobile-list-actions'>
                <a class='btn secondary small dashboard-btn' href='{url_for("jobs.view_job", job_id=r["id"])}'>{_t(lang, "View Job", "Ver Trabajo")}</a>
            </div>
        </div>
        """
        for r in upcoming_jobs
    )

    unpaid_mobile_cards = "".join(
        f"""
        <div class='mobile-list-card'>
            <div class='mobile-list-top'>
                <div class='mobile-list-title'>#{r['id']} - {_safe_text(r['customer_name'])}</div>
                <div class='mobile-list-amount'>${_safe_float(r['balance_due']):,.2f}</div>
            </div>
            <div class='mobile-list-grid'>
                <div><span>{_t(lang, "Status", "Estado")}</span><strong>{_safe_text(r['status'])}</strong></div>
                <div><span>{_t(lang, "Balance", "Saldo")}</span><strong>${_safe_float(r['balance_due']):,.2f}</strong></div>
            </div>
            <div class='mobile-list-actions'>
                <a class='btn secondary small dashboard-btn' href='{url_for("invoices.view_invoice", invoice_id=r["id"])}'>{_t(lang, "View Invoice", "Ver Factura")}</a>
            </div>
        </div>
        """
        for r in unpaid_invoices
    )

    content = f"""
    <style>
    .dashboard-page {{
        display: grid;
        gap: 18px;
    }}

    .dashboard-grid {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 18px;
        align-items: start;
    }}

    .dashboard-grid .card {{
        min-width: 0;
        width: 100%;
        overflow: hidden;
    }}

    .dashboard-grid .table-wrap,
    .dashboard-aging-table-wrap {{
        width: 100%;
        overflow-x: auto;
    }}

    .dashboard-stats-grid {{
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 16px;
    }}

    .dashboard-financials {{
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 16px;
    }}

    .dashboard-financial-card,
    .dashboard-stat-card {{
        min-width: 0;
    }}

    .dashboard-btn {{
        white-space: nowrap;
    }}

    .dashboard-section-head {{
        display:flex;
        justify-content:space-between;
        align-items:center;
        gap:12px;
        flex-wrap:wrap;
        margin-bottom:14px;
    }}

    .dashboard-total-outstanding {{
        text-align:right;
    }}

    .dashboard-aging-summary {{
        width:100%;
        table-layout:fixed;
        border-collapse:collapse;
        margin-bottom:18px;
    }}

    .dashboard-aging-summary th {{
        text-align:center;
        background:#f8fafc;
        padding:12px;
    }}

    .dashboard-aging-summary td {{
        text-align:center;
        font-size:1.2rem;
        font-weight:700;
        padding:14px;
    }}

    .mobile-only {{
        display: none;
    }}

    .desktop-only {{
        display: block;
    }}

    .mobile-list {{
        display:grid;
        gap:12px;
    }}

    .mobile-list-card {{
        border:1px solid rgba(15, 23, 42, 0.08);
        border-radius:14px;
        padding:14px;
        background:#fff;
        box-shadow:0 1px 2px rgba(15, 23, 42, 0.04);
    }}

    .mobile-list-top {{
        display:flex;
        justify-content:space-between;
        align-items:flex-start;
        gap:10px;
        margin-bottom:10px;
    }}

    .mobile-list-title {{
        font-weight:700;
        color:#0f172a;
        line-height:1.25;
        word-break:break-word;
    }}

    .mobile-list-amount {{
        font-weight:800;
        color:#0f172a;
        white-space:nowrap;
    }}

    .mobile-badge {{
        font-size:.85rem;
        font-weight:700;
        color:#334155;
        background:#f1f5f9;
        padding:6px 10px;
        border-radius:999px;
        white-space:nowrap;
    }}

    .mobile-list-grid {{
        display:grid;
        grid-template-columns:1fr 1fr;
        gap:10px 12px;
        margin-bottom:12px;
    }}

    .mobile-list-grid span {{
        display:block;
        font-size:.78rem;
        color:#64748b;
        margin-bottom:3px;
    }}

    .mobile-list-grid strong {{
        display:block;
        color:#0f172a;
        font-size:.95rem;
        line-height:1.25;
        word-break:break-word;
    }}

    .mobile-list-actions {{
        display:flex;
        gap:8px;
        flex-wrap:wrap;
    }}

    @media (max-width: 1000px) {{
        .dashboard-stats-grid {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
        }}

        .dashboard-financials {{
            grid-template-columns: 1fr;
        }}

        .dashboard-grid {{
            grid-template-columns: 1fr;
        }}
    }}

    @media (max-width: 640px) {{
        .dashboard-page {{
            gap: 14px;
        }}

        .dashboard-stats-grid {{
            grid-template-columns: 1fr 1fr;
            gap: 10px;
        }}

        .dashboard-financials {{
            grid-template-columns: 1fr;
            gap: 10px;
        }}

        .dashboard-section-head {{
            align-items:flex-start;
        }}

        .dashboard-total-outstanding {{
            text-align:left;
            width:100%;
        }}

        .dashboard-btn,
        .btn.small {{
            padding: 8px 10px !important;
            font-size: 0.84rem !important;
            line-height: 1.2 !important;
            min-height: auto !important;
        }}

        .stat-card {{
            padding: 12px !important;
        }}

        .stat-value {{
            font-size: 1.15rem !important;
        }}

        table th,
        table td {{
            font-size: 0.85rem;
        }}

        .desktop-only {{
            display: none !important;
        }}

        .mobile-only {{
            display: block !important;
        }}

        .mobile-list-grid {{
            grid-template-columns:1fr;
        }}
    }}
    </style>

    <div class='dashboard-page'>
        <h1>{_t(lang, "Dashboard", "Tablero")}</h1>

        <div class='dashboard-stats-grid'>
            <div class='card stat-card dashboard-stat-card'>
                <div class='stat-label'>{_t(lang, "Customers", "Clientes")}</div>
                <div class='stat-value'>{customers_count}</div>
            </div>

            <div class='card stat-card dashboard-stat-card'>
                <div class='stat-label'>{_t(lang, "Quotes", "Cotizaciones")}</div>
                <div class='stat-value'>{quotes_count}</div>
            </div>

            <div class='card stat-card dashboard-stat-card'>
                <div class='stat-label'>{_t(lang, "Jobs", "Trabajos")}</div>
                <div class='stat-value'>{jobs_count}</div>
            </div>

            <div class='card stat-card dashboard-stat-card'>
                <div class='stat-label'>{_t(lang, "Invoices", "Facturas")}</div>
                <div class='stat-value'>{invoices_count}</div>
            </div>
        </div>

        <div class='dashboard-financials'>
            <div class='card stat-card dashboard-financial-card'>
                <div class='stat-label'>{_t(lang, "Income", "Ingresos")}</div>
                <div class='stat-value' style="color:#16a34a;">+${income_total:,.2f}</div>
            </div>

            <div class='card stat-card dashboard-financial-card'>
                <div class='stat-label'>{_t(lang, "Expenses", "Gastos")}</div>
                <div class='stat-value' style="color:#dc2626;">-${expense_total:,.2f}</div>
            </div>

            <div class='card stat-card dashboard-financial-card'>
                <div class='stat-label'>{_t(lang, "Profit", "Ganancia")}</div>
                <div class='stat-value' style="color:{'#16a34a' if profit_total >= 0 else '#dc2626'};">
                    {'+' if profit_total >= 0 else '-'}${abs(profit_total):,.2f}
                </div>
            </div>
        </div>

        <div class='card' style='padding:20px;'>
            <div class='dashboard-section-head'>
                <div>
                    <h2 style='margin:0 0 6px 0;'>{_t(lang, "Outstanding Invoice Aging", "Antigüedad de facturas pendientes")}</h2>
                </div>
                <div class='dashboard-total-outstanding'>
                    <div class='muted' style='font-size:.9rem;'>{_t(lang, "Total Outstanding", "Total pendiente")}</div>
                    <div style='font-size:1.6rem; font-weight:800; color:#0f172a;'>${total_outstanding:,.2f}</div>
                </div>
            </div>

            <div class='dashboard-aging-table-wrap desktop-only'>
                <table class='dashboard-aging-summary'>
                    <tr>
                        <th style='width:20%;'>{_t(lang, "Current", "Actual")}</th>
                        <th style='width:20%;'>1-30</th>
                        <th style='width:20%;'>31-60</th>
                        <th style='width:20%;'>61-90</th>
                        <th style='width:20%;'>90+</th>
                    </tr>
                    <tr>
                        <td>${aging_totals["Current"]:,.2f}</td>
                        <td>${aging_totals["1-30"]:,.2f}</td>
                        <td>${aging_totals["31-60"]:,.2f}</td>
                        <td>${aging_totals["61-90"]:,.2f}</td>
                        <td>${aging_totals["90+"]:,.2f}</td>
                    </tr>
                </table>
            </div>

            <div class='mobile-only' style='margin-bottom:14px;'>
                <div class='mobile-list'>
                    <div class='mobile-list-card'>
                        <div class='mobile-list-grid'>
                            <div><span>{_t(lang, "Current", "Actual")}</span><strong>${aging_totals["Current"]:,.2f}</strong></div>
                            <div><span>1-30</span><strong>${aging_totals["1-30"]:,.2f}</strong></div>
                            <div><span>31-60</span><strong>${aging_totals["31-60"]:,.2f}</strong></div>
                            <div><span>61-90</span><strong>${aging_totals["61-90"]:,.2f}</strong></div>
                            <div><span>90+</span><strong>${aging_totals["90+"]:,.2f}</strong></div>
                        </div>
                    </div>
                </div>
            </div>

            <div class='table-wrap desktop-only'>
                <table>
                    <tr>
                        <th>{_t(lang, "Invoice", "Factura")}</th>
                        <th>{_t(lang, "Customer", "Cliente")}</th>
                        <th>{_t(lang, "Invoice Date", "Fecha de factura")}</th>
                        <th>{_t(lang, "Due Date", "Fecha de vencimiento")}</th>
                        <th>{_t(lang, "Bucket", "Grupo")}</th>
                        <th>{_t(lang, "Balance Due", "Saldo pendiente")}</th>
                        <th></th>
                    </tr>
                    {aging_table_rows or f"<tr><td colspan='7'>{_t(lang, 'No outstanding invoices.', 'No hay facturas pendientes.')}</td></tr>"}
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {aging_mobile_cards or f"<div class='mobile-list-card'>{_t(lang, 'No outstanding invoices.', 'No hay facturas pendientes.')}</div>"}
                </div>
            </div>
        </div>

        <div class='dashboard-grid'>
            <div class='card'>
                <div class='dashboard-section-head'>
                    <h2 style='margin:0;'>{_t(lang, "Upcoming Jobs", "Próximos trabajos")}</h2>
                    <a class='btn small dashboard-btn' href='{url_for("jobs.jobs")}'>{_t(lang, "View All", "Ver todo")}</a>
                </div>

                <div class='table-wrap desktop-only'>
                    <table>
                        <tr>
                            <th>ID</th>
                            <th>{_t(lang, "Title", "Título")}</th>
                            <th>{_t(lang, "Customer", "Cliente")}</th>
                            <th>{_t(lang, "Date", "Fecha")}</th>
                            <th>{_t(lang, "Status", "Estado")}</th>
                            <th></th>
                        </tr>
                        {upcoming_rows or f"<tr><td colspan='6'>{_t(lang, 'No jobs.', 'No hay trabajos.')}</td></tr>"}
                    </table>
                </div>

                <div class='mobile-only'>
                    <div class='mobile-list'>
                        {upcoming_mobile_cards or f"<div class='mobile-list-card'>{_t(lang, 'No jobs.', 'No hay trabajos.')}</div>"}
                    </div>
                </div>
            </div>

            <div class='card'>
                <div class='dashboard-section-head'>
                    <h2 style='margin:0;'>{_t(lang, "Unpaid Invoices", "Facturas no pagadas")}</h2>
                    <a class='btn small dashboard-btn' href='{url_for("invoices.invoices")}'>{_t(lang, "View All", "Ver todo")}</a>
                </div>

                <div class='table-wrap desktop-only'>
                    <table>
                        <tr>
                            <th>ID</th>
                            <th>{_t(lang, "Customer", "Cliente")}</th>
                            <th>{_t(lang, "Status", "Estado")}</th>
                            <th>{_t(lang, "Balance", "Saldo")}</th>
                            <th></th>
                        </tr>
                        {unpaid_rows or f"<tr><td colspan='5'>{_t(lang, 'No unpaid invoices.', 'No hay facturas sin pagar.')}</td></tr>"}
                    </table>
                </div>

                <div class='mobile-only'>
                    <div class='mobile-list'>
                        {unpaid_mobile_cards or f"<div class='mobile-list-card'>{_t(lang, 'No unpaid invoices.', 'No hay facturas sin pagar.')}</div>"}
                    </div>
                </div>
            </div>
        </div>
    </div>
    """

    return render_page(content, _t(lang, "Dashboard", "Tablero"))