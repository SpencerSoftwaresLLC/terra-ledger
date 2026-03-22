from flask import Blueprint, session, url_for
from decorators import login_required, subscription_required
from page_helpers import render_page

mobile_bp = Blueprint("mobile", __name__)


@mobile_bp.route("/mobile")
@login_required
@subscription_required
def mobile_home():
    company_name = session.get("company_name") or "TerraLedger"

    content = f"""
    <style>
        .mobile-view-wrap {{
            max-width: 700px;
            margin: 0 auto;
        }}

        .mobile-hero {{
            padding: 18px;
            border-radius: 18px;
            background: linear-gradient(135deg, #1e293b, #334155);
            color: #fff;
            margin-bottom: 18px;
        }}

        .mobile-hero h1 {{
            margin: 0 0 6px 0;
            font-size: 1.7rem;
        }}

        .mobile-hero p {{
            margin: 0;
            color: rgba(255,255,255,.85);
        }}

        .mobile-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 14px;
        }}

        .mobile-card-btn {{
            display: block;
            text-decoration: none;
            background: #fff;
            border: 1px solid #dbe2ea;
            border-radius: 18px;
            padding: 18px 16px;
            min-height: 110px;
            box-shadow: 0 8px 20px rgba(0,0,0,.05);
            transition: transform .15s ease, box-shadow .15s ease;
        }}

        .mobile-card-btn:hover {{
            transform: translateY(-2px);
            box-shadow: 0 10px 22px rgba(0,0,0,.08);
        }}

        .mobile-card-title {{
            font-size: 1.05rem;
            font-weight: 700;
            color: #0f172a;
            margin-bottom: 6px;
        }}

        .mobile-card-text {{
            font-size: .92rem;
            color: #64748b;
            line-height: 1.45;
        }}

        .mobile-actions {{
            display: grid;
            grid-template-columns: 1fr;
            gap: 12px;
            margin-top: 18px;
        }}

        .mobile-wide-btn {{
            display: block;
            text-align: center;
            text-decoration: none;
            padding: 14px 16px;
            border-radius: 14px;
            font-weight: 700;
            border: 1px solid #dbe2ea;
            background: #fff;
            color: #0f172a;
        }}

        .mobile-wide-btn.primary {{
            background: #2563eb;
            color: #fff;
            border-color: #2563eb;
        }}

        @media (max-width: 640px) {{
            .mobile-grid {{
                grid-template-columns: 1fr;
            }}

            .mobile-hero h1 {{
                font-size: 1.45rem;
            }}
        }}
    </style>

    <div class="mobile-view-wrap">
        <div class="mobile-hero">
            <h1>{company_name} Mobile Viewer</h1>
            <p>Quick phone-friendly access to your most important TerraLedger pages.</p>
        </div>

        <div class="mobile-grid">
            <a class="mobile-card-btn" href="{url_for('dashboard.dashboard')}">
                <div class="mobile-card-title">Dashboard</div>
                <div class="mobile-card-text">See income, expenses, profit, jobs, and invoices.</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('customers.customers')}">
                <div class="mobile-card-title">Customers</div>
                <div class="mobile-card-text">View and manage customer records quickly.</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('quotes.quotes')}">
                <div class="mobile-card-title">Quotes</div>
                <div class="mobile-card-text">Open quotes, add new ones, and email them out.</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('jobs.jobs')}">
                <div class="mobile-card-title">Jobs</div>
                <div class="mobile-card-text">Track active jobs and convert work to invoices.</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('invoices.invoices')}">
                <div class="mobile-card-title">Invoices</div>
                <div class="mobile-card-text">Manage open invoices, balances, and payments.</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('bookkeeping.bookkeeping')}">
                <div class="mobile-card-title">Bookkeeping</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('employees.employees')}">
                <div class="mobile-card-title">Employees</div>
                <div class="mobile-card-text">View employee records and payroll details.</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('settings.settings')}">
                <div class="mobile-card-title">Settings</div>
                <div class="mobile-card-text">Open company info, billing, email, and tax settings.</div>
            </a>
        </div>

        <div class="mobile-actions">
            <a class="mobile-wide-btn primary" href="{url_for('quotes.quotes')}">Create / View Quotes</a>
            <a class="mobile-wide-btn primary" href="{url_for('invoices.new_invoice')}">Create Invoice</a>
            <a class="mobile-wide-btn" href="{url_for('payroll.employee_payroll')}">Open Payroll</a>
        </div>
    </div>
    """
    return render_page(content, "Mobile Viewer")