from flask import Blueprint, session, url_for
from decorators import login_required, subscription_required
from page_helpers import render_page

mobile_bp = Blueprint("mobile", __name__)


def _lang():
    value = str(session.get("language") or "en").strip().lower()
    return "es" if value == "es" else "en"


def _is_es():
    return _lang() == "es"


def _t(en, es):
    return es if _is_es() else en


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
            padding: 10px;
        }}

        .mobile-hero {{
            padding: 14px;
            border-radius: 14px;
            background: linear-gradient(135deg, #1e293b, #334155);
            color: #fff;
            margin-bottom: 14px;
        }}

        .mobile-hero h1 {{
            margin: 0 0 4px 0;
            font-size: 1.4rem;
        }}

        .mobile-hero p {{
            margin: 0;
            font-size: .9rem;
            color: rgba(255,255,255,.85);
        }}

        .mobile-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
        }}

        .mobile-card-btn {{
            display: block;
            text-decoration: none;
            background: #fff;
            border: 1px solid #dbe2ea;
            border-radius: 14px;
            padding: 12px;
            min-height: auto;
            box-shadow: 0 4px 12px rgba(0,0,0,.05);
        }}

        .mobile-card-title {{
            font-size: .95rem;
            font-weight: 700;
            color: #0f172a;
            margin-bottom: 4px;
        }}

        .mobile-card-text {{
            font-size: .82rem;
            color: #64748b;
            line-height: 1.3;
        }}

        .mobile-actions {{
            display: grid;
            grid-template-columns: 1fr;
            gap: 10px;
            margin-top: 14px;
        }}

        .mobile-wide-btn {{
            display: block;
            text-align: center;
            text-decoration: none;
            padding: 10px;
            border-radius: 10px;
            font-weight: 600;
            font-size: .9rem;
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

            .mobile-card-btn {{
                padding: 10px;
                border-radius: 12px;
            }}

            .mobile-card-title {{
                font-size: .9rem;
            }}

            .mobile-card-text {{
                font-size: .8rem;
            }}

            .mobile-wide-btn {{
                padding: 9px;
                font-size: .85rem;
            }}

            .mobile-hero h1 {{
                font-size: 1.2rem;
            }}

            .mobile-hero p {{
                font-size: .85rem;
            }}
        }}
    </style>

    <div class="mobile-view-wrap">
        <div class="mobile-hero">
            <h1>{company_name} {_t("Mobile Viewer", "Visor móvil")}</h1>
            <p>{_t("Quick access to your TerraLedger tools.", "Acceso rápido a tus herramientas de TerraLedger.")}</p>
        </div>

        <div class="mobile-grid">
            <a class="mobile-card-btn" href="{url_for('dashboard.dashboard')}">
                <div class="mobile-card-title">{_t("Dashboard", "Panel")}</div>
                <div class="mobile-card-text">{_t("Income, jobs, invoices", "Ingresos, trabajos, facturas")}</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('customers.customers')}">
                <div class="mobile-card-title">{_t("Customers", "Clientes")}</div>
                <div class="mobile-card-text">{_t("Manage contacts", "Administrar contactos")}</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('quotes.quotes')}">
                <div class="mobile-card-title">{_t("Quotes", "Cotizaciones")}</div>
                <div class="mobile-card-text">{_t("Create & send", "Crear y enviar")}</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('jobs.jobs')}">
                <div class="mobile-card-title">{_t("Jobs", "Trabajos")}</div>
                <div class="mobile-card-text">{_t("Track work", "Seguimiento del trabajo")}</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('invoices.invoices')}">
                <div class="mobile-card-title">{_t("Invoices", "Facturas")}</div>
                <div class="mobile-card-text">{_t("Balances & payments", "Saldos y pagos")}</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('bookkeeping.bookkeeping')}">
                <div class="mobile-card-title">{_t("Bookkeeping", "Contabilidad")}</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('employees.employees')}">
                <div class="mobile-card-title">{_t("Employees", "Empleados")}</div>
            </a>

            <a class="mobile-card-btn" href="{url_for('settings.settings')}">
                <div class="mobile-card-title">{_t("Settings", "Configuración")}</div>
            </a>
        </div>

        <div class="mobile-actions">
            <a class="mobile-wide-btn primary" href="{url_for('quotes.quotes')}">{_t("Quotes", "Cotizaciones")}</a>
            <a class="mobile-wide-btn primary" href="{url_for('invoices.new_invoice')}">{_t("New Invoice", "Nueva factura")}</a>
            <a class="mobile-wide-btn" href="{url_for('payroll.employee_payroll')}">{_t("Payroll", "Nómina")}</a>
        </div>
    </div>
    """
    return render_page(content, _t("Mobile Viewer", "Visor móvil"))