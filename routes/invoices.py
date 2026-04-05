from flask import Blueprint, request, redirect, url_for, session, flash, abort, make_response, current_app
from flask_wtf.csrf import generate_csrf
from datetime import date, datetime
from html import escape
import json
import re
import os
import tempfile
import io
import stripe

from urllib.parse import urlparse
from urllib.request import urlopen

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader

from db import get_db_connection
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page
from utils.emailing import send_company_email


invoices_bp = Blueprint("invoices", __name__)


# =========================================================
# Helpers
# =========================================================

def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _clean_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"none", "null", "n/a"}:
        return ""
    return text


def _clean_display(value):
    text = _clean_text(value)
    return text if text else "-"


def _table_columns(conn, table_name):
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """,
        (table_name,),
    ).fetchall()

    cols = set()
    for row in rows:
        if hasattr(row, "keys"):
            cols.add(row["column_name"])
        else:
            cols.add(row[0])
    return cols


def _get_public_base_url():
    """
    Returns a production-safe absolute base URL for Stripe redirects.

    Examples:
    - https://terraledger.net
    - https://www.terraledger.net

    If APP_BASE_URL is missing a scheme, https:// will be added automatically.
    """
    base_url = (os.environ.get("APP_BASE_URL") or "").strip()

    if not base_url:
        raise ValueError("APP_BASE_URL is not set")

    if not base_url.startswith(("http://", "https://")):
        base_url = f"https://{base_url}"

    return base_url.rstrip("/")


def ensure_document_number_columns():
    conn = get_db_connection()
    try:
        company_cols = _table_columns(conn, "companies")

        if "next_invoice_number" not in company_cols:
            conn.execute(
                """
                ALTER TABLE companies
                ADD COLUMN next_invoice_number BIGINT NOT NULL DEFAULT 1001
                """
            )

        invoice_cols = _table_columns(conn, "invoices")
        if "invoice_number" not in invoice_cols:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN invoice_number TEXT
                """
            )

        if "amount_paid" not in invoice_cols:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN amount_paid NUMERIC(12,2) NOT NULL DEFAULT 0
                """
            )

        if "balance_due" not in invoice_cols:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN balance_due NUMERIC(12,2) NOT NULL DEFAULT 0
                """
            )

        if "notes" not in invoice_cols:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN notes TEXT
                """
            )

        if "quote_id" not in invoice_cols:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN quote_id BIGINT
                """
            )

        if "job_id" not in invoice_cols:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN job_id BIGINT
                """
            )

        conn.commit()
    finally:
        conn.close()


def ensure_invoice_payment_table():
    conn = get_db_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS invoice_payments (
                id BIGSERIAL PRIMARY KEY,
                company_id BIGINT NOT NULL,
                invoice_id BIGINT NOT NULL,
                payment_date DATE,
                amount NUMERIC(12,2) NOT NULL DEFAULT 0,
                payment_method TEXT,
                reference TEXT,
                notes TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _extract_numeric_invoice_number(invoice_number):
    if not invoice_number:
        return None
    match = re.search(r"(\d+)$", str(invoice_number).strip())
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def get_next_invoice_number(company_id):
    conn = get_db_connection()
    try:
        company = conn.execute(
            """
            SELECT next_invoice_number
            FROM companies
            WHERE id = %s
            """,
            (company_id,),
        ).fetchone()

        if company and company["next_invoice_number"] is not None:
            next_num = int(company["next_invoice_number"])
            conn.execute(
                """
                UPDATE companies
                SET next_invoice_number = %s
                WHERE id = %s
                """,
                (next_num + 1, company_id),
            )
            conn.commit()
            return str(next_num)

        rows = conn.execute(
            """
            SELECT invoice_number
            FROM invoices
            WHERE company_id = %s
            ORDER BY id DESC
            LIMIT 100
            """,
            (company_id,),
        ).fetchall()

        max_num = 1000
        for row in rows:
            parsed = _extract_numeric_invoice_number(row["invoice_number"])
            if parsed and parsed > max_num:
                max_num = parsed

        next_num = max_num + 1
        conn.execute(
            """
            UPDATE companies
            SET next_invoice_number = %s
            WHERE id = %s
            """,
            (next_num + 1, company_id),
        )
        conn.commit()
        return str(next_num)

    finally:
        conn.close()


def recalc_invoice(conn, invoice_id):
    invoice_row = conn.execute(
        """
        SELECT
            id,
            subtotal,
            total,
            amount_paid,
            balance_due
        FROM invoices
        WHERE id = %s
        """,
        (invoice_id,),
    ).fetchone()

    if not invoice_row:
        return

    item_row = conn.execute(
        """
        SELECT
            COALESCE(SUM(line_total), 0) AS items_total,
            COUNT(*) AS item_count
        FROM invoice_items
        WHERE invoice_id = %s
        """,
        (invoice_id,),
    ).fetchone()

    payment_row = conn.execute(
        """
        SELECT COALESCE(SUM(amount), 0) AS paid_total
        FROM invoice_payments
        WHERE invoice_id = %s
        """,
        (invoice_id,),
    ).fetchone()

    item_count = int(item_row["item_count"] or 0) if item_row else 0
    items_total = _safe_float(item_row["items_total"] if item_row else 0)
    paid_total = _safe_float(payment_row["paid_total"] if payment_row else 0)

    if item_count > 0:
        subtotal = items_total
        total = items_total
    else:
        subtotal = _safe_float(invoice_row["subtotal"])
        total = _safe_float(invoice_row["total"])

    balance_due = max(0.0, total - paid_total)

    if total <= 0:
        status = "Draft"
    elif paid_total <= 0:
        status = "Unpaid"
    elif balance_due > 0:
        status = "Partial"
    else:
        status = "Paid"

    conn.execute(
        """
        UPDATE invoices
        SET subtotal = %s,
            total = %s,
            amount_paid = %s,
            balance_due = %s,
            status = %s
        WHERE id = %s
        """,
        (subtotal, total, paid_total, balance_due, status, invoice_id),
    )


def _sync_invoice_status_and_bookkeeping(invoice_id):
    conn = get_db_connection()
    try:
        recalc_invoice(conn, invoice_id)

        invoice = conn.execute(
            """
            SELECT i.*, c.name AS customer_name
            FROM invoices i
            LEFT JOIN customers c ON i.customer_id = c.id
            WHERE i.id = %s
            """,
            (invoice_id,),
        ).fetchone()

        if not invoice:
            conn.commit()
            return

        paid_row = conn.execute(
            """
            SELECT
                COALESCE(SUM(amount), 0) AS paid_total,
                MAX(payment_date) AS latest_payment_date
            FROM invoice_payments
            WHERE invoice_id = %s
            """,
            (invoice_id,),
        ).fetchone()

        paid_total = _safe_float(paid_row["paid_total"] if paid_row else 0)
        latest_payment_date = (
            paid_row["latest_payment_date"]
            if paid_row and paid_row["latest_payment_date"]
            else (invoice["invoice_date"] or date.today())
        )

        total = _safe_float(invoice["total"])
        balance_due = _safe_float(invoice["balance_due"])

        if total > 0 and balance_due <= 0 and paid_total > 0:
            new_status = "Paid"
        elif paid_total > 0:
            new_status = "Partial"
        elif total > 0:
            new_status = "Unpaid"
        else:
            new_status = "Draft"

        conn.execute(
            """
            UPDATE invoices
            SET status = %s
            WHERE id = %s
            """,
            (new_status, invoice_id),
        )

        if "job_id" in invoice.keys() and invoice["job_id"]:
            conn.execute(
                """
                UPDATE jobs
                SET status = %s
                WHERE id = %s AND company_id = %s
                """,
                (
                    "Finished" if new_status == "Paid" else "Invoiced",
                    invoice["job_id"],
                    invoice["company_id"],
                ),
            )

        if "quote_id" in invoice.keys() and invoice["quote_id"]:
            conn.execute(
                """
                UPDATE quotes
                SET status = %s
                WHERE id = %s AND company_id = %s
                """,
                (
                    "Finished" if new_status == "Paid" else "Converted",
                    invoice["quote_id"],
                    invoice["company_id"],
                ),
            )
        conn.commit()
    finally:
        conn.close()


def build_invoice_pdf(invoice, items, company, profile):
    pdf_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf_temp.close()

    try:
        invoice_number = invoice["invoice_number"] or invoice["id"]

        company_name = (
            profile["invoice_header_name"]
            if profile and "invoice_header_name" in profile.keys() and profile["invoice_header_name"]
            else (
                profile["display_name"]
                if profile and "display_name" in profile.keys() and profile["display_name"]
                else (company["name"] if company else "Your Company")
            )
        )

        footer_note = (
            profile["invoice_footer_note"]
            if profile and "invoice_footer_note" in profile.keys() and profile["invoice_footer_note"]
            else ""
        )
        logo_url = profile["logo_url"] if profile and "logo_url" in profile.keys() and profile["logo_url"] else ""

        address_parts = []
        if company:
            if "address_line_1" in company.keys() and company["address_line_1"] and str(company["address_line_1"]).strip().lower() != "none":
                address_parts.append(company["address_line_1"])
            if "address_line_2" in company.keys() and company["address_line_2"] and str(company["address_line_2"]).strip().lower() != "none":
                address_parts.append(company["address_line_2"])

            city_state_zip = " ".join(
                part for part in [
                    f"{company['city']}," if "city" in company.keys() and company["city"] and str(company["city"]).strip().lower() != "none" else "",
                    company["state"] if "state" in company.keys() and company["state"] and str(company["state"]).strip().lower() != "none" else "",
                    company["zip_code"] if "zip_code" in company.keys() and company["zip_code"] and str(company["zip_code"]).strip().lower() != "none" else "",
                ] if part
            ).strip()

            if city_state_zip:
                address_parts.append(city_state_zip)

        company_contact_lines = []
        if address_parts:
            company_contact_lines.extend(address_parts)
        if company and "phone" in company.keys() and company["phone"] and str(company["phone"]).strip().lower() != "none":
            company_contact_lines.append(company["phone"])
        if company and "email" in company.keys() and company["email"] and str(company["email"]).strip().lower() != "none":
            company_contact_lines.append(company["email"])
        if company and "website" in company.keys() and company["website"] and str(company["website"]).strip().lower() != "none":
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

        ensure_space(110)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(50, y, f"Invoice #: {invoice_number}")
        y -= 16
        c.drawString(50, y, f"Customer: {invoice['customer_name'] or ''}")
        y -= 16
        c.drawString(50, y, f"Status: {invoice['status'] or ''}")
        y -= 16
        c.drawString(50, y, f"Invoice Date: {invoice['invoice_date'] or date.today().isoformat()}")
        y -= 16
        c.drawString(50, y, f"Due Date: {invoice['due_date'] or '-'}")
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

        ensure_space(80)
        y -= 8
        c.line(380, y, 560, y)
        y -= 18
        c.setFont("Helvetica-Bold", 12)
        c.drawRightString(560, y, f"Total: ${float(invoice['total'] or 0):.2f}")
        y -= 16
        c.drawRightString(560, y, f"Amount Paid: ${float(invoice['amount_paid'] or 0):.2f}")
        y -= 16
        c.drawRightString(560, y, f"Balance Due: ${float(invoice['balance_due'] or 0):.2f}")
        y -= 24

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

def get_invoice_email_context(invoice_id, company_id):
    conn = get_db_connection()
    try:
        invoice = conn.execute(
            """
            SELECT
                i.*,
                c.name AS customer_name,
                c.email AS customer_email
            FROM invoices i
            LEFT JOIN customers c
              ON i.customer_id = c.id
            WHERE i.id = %s
              AND i.company_id = %s
            """,
            (invoice_id, company_id),
        ).fetchone()

        if not invoice:
            return None, None, None, None

        items = conn.execute(
            """
            SELECT *
            FROM invoice_items
            WHERE invoice_id = %s
            ORDER BY id ASC
            """,
            (invoice_id,),
        ).fetchall()

        company = conn.execute(
            """
            SELECT *
            FROM companies
            WHERE id = %s
            """,
            (company_id,),
        ).fetchone()

        profile = conn.execute(
            """
            SELECT *
            FROM company_profile
            WHERE company_id = %s
            """,
            (company_id,),
        ).fetchone()

        return invoice, items, company, profile
    finally:
        conn.close()


def get_stripe_settings_for_company(company_id):
    conn = get_db_connection()
    row = conn.execute(
        """
        SELECT *
        FROM company_payment_settings
        WHERE company_id = %s
        """,
        (company_id,),
    ).fetchone()
    conn.close()
    return row


def create_invoice_checkout_session(invoice, company_id):
    stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")

    if not stripe.api_key:
        raise ValueError("STRIPE_SECRET_KEY is not set")

    settings = get_stripe_settings_for_company(company_id)

    if not settings or not settings["stripe_account_id"]:
        return None

    if not settings["payments_enabled"]:
        return None

    amount = int(round(_safe_float(invoice["balance_due"]) * 100))

    if amount <= 0:
        return None

    base_url = _get_public_base_url()

    session_kwargs = {
        "payment_method_types": ["card"],
        "mode": "payment",
        "line_items": [
            {
                "price_data": {
                    "currency": "usd",
                    "product_data": {
                        "name": f"Invoice #{invoice['invoice_number'] or invoice['id']}",
                    },
                    "unit_amount": amount,
                },
                "quantity": 1,
            }
        ],
        "metadata": {
            "invoice_id": str(invoice["id"]),
            "company_id": str(company_id),
        },
        "success_url": f"{base_url}/payment-success",
        "cancel_url": f"{base_url}/payment-cancel",
    }

    stripe_account_id = (settings["stripe_account_id"] or "").strip()
    if stripe_account_id:
        checkout_session = stripe.checkout.Session.create(
            **session_kwargs,
            stripe_account=stripe_account_id,
        )
    else:
        checkout_session = stripe.checkout.Session.create(**session_kwargs)

    return checkout_session.url


# =========================================================
# Routes
# =========================================================

@invoices_bp.route("/invoices")
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def invoices():
    ensure_document_number_columns()
    ensure_invoice_payment_table()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT i.*, c.name AS customer_name
        FROM invoices i
        LEFT JOIN customers c ON i.customer_id = c.id
        WHERE i.company_id = %s
          AND COALESCE(i.status, '') != 'Paid'
        ORDER BY i.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    invoice_rows_html = ""
    invoice_mobile_cards = ""

    for inv in rows:
        status = _clean_display(inv["status"])
        invoice_number = _clean_display(inv["invoice_number"] or inv["id"])

        invoice_rows_html += f"""
        <tr>
            <td>#{escape(str(invoice_number))}</td>
            <td>{escape(_clean_display(inv["customer_name"]))}</td>
            <td>{escape(str(inv["invoice_date"] or "-"))}</td>
            <td>${_safe_float(inv["total"]):,.2f}</td>
            <td>${_safe_float(inv["amount_paid"]):,.2f}</td>
            <td>${_safe_float(inv["balance_due"]):,.2f}</td>
            <td>{escape(status)}</td>
            <td>
                <a class='btn small' href='{url_for("invoices.view_invoice", invoice_id=inv["id"])}'>Open</a>
            </td>
        </tr>
        """

        invoice_mobile_cards += f"""
        <div class='mobile-list-card'>
            <div class='mobile-list-top'>
                <div class='mobile-list-title'>Invoice #{escape(str(invoice_number))}</div>
                <div class='mobile-badge'>{escape(status)}</div>
            </div>
            <div class='mobile-list-grid'>
                <div><span>Customer</span><strong>{escape(_clean_display(inv["customer_name"]))}</strong></div>
                <div><span>Date</span><strong>{escape(str(inv["invoice_date"] or "-"))}</strong></div>
                <div><span>Total</span><strong>${_safe_float(inv["total"]):,.2f}</strong></div>
                <div><span>Paid</span><strong>${_safe_float(inv["amount_paid"]):,.2f}</strong></div>
                <div><span>Balance</span><strong>${_safe_float(inv["balance_due"]):,.2f}</strong></div>
            </div>
            <div class='mobile-list-actions'>
                <a class='btn small' href='{url_for("invoices.view_invoice", invoice_id=inv["id"])}'>Open</a>
            </div>
        </div>
        """

    if not invoice_rows_html:
        invoice_rows_html = """
        <tr>
            <td colspan="8" class="muted">No active invoices found.</td>
        </tr>
        """

    if not invoice_mobile_cards:
        invoice_mobile_cards = "<div class='mobile-list-card muted'>No active invoices found.</div>"

    content = f"""
    <style>
        .invoice-page {{
            display:grid;
            gap:18px;
        }}

        .invoice-page-head {{
            display:flex;
            justify-content:space-between;
            gap:12px;
            align-items:center;
            flex-wrap:wrap;
        }}

        .invoice-table-wrap {{
            width:100%;
            overflow-x:auto;
        }}

        .mobile-only {{
            display:none;
        }}

        .desktop-only {{
            display:block;
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

        @media (max-width: 640px) {{
            .desktop-only {{
                display:none !important;
            }}

            .mobile-only {{
                display:block !important;
            }}

            .mobile-list-grid {{
                grid-template-columns:1fr;
            }}
        }}
    </style>

    <div class='invoice-page'>
        <div class='card'>
            <div class='invoice-page-head'>
                <div>
                    <h1>Invoices</h1>
                    <p class='muted'>Track active invoice totals, payments, balances, and status.</p>
                </div>
                <div class='row-actions'>
                    <a class='btn secondary' href='{url_for("invoices.paid_invoices")}'>Paid Invoices</a>
                    <a class='btn success' href='{url_for("invoices.new_invoice")}'>New Invoice</a>
                </div>
            </div>
        </div>

        <div class='card'>
            <div class='invoice-table-wrap desktop-only'>
                <table class='table'>
                    <thead>
                        <tr>
                            <th>Invoice</th>
                            <th>Customer</th>
                            <th>Date</th>
                            <th>Total</th>
                            <th>Paid</th>
                            <th>Balance</th>
                            <th>Status</th>
                            <th></th>
                        </tr>
                    </thead>
                    <tbody>
                        {invoice_rows_html}
                    </tbody>
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {invoice_mobile_cards}
                </div>
            </div>
        </div>
    </div>
    """
    return render_page(content, "Invoices")


@invoices_bp.route("/invoices/paid")
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def paid_invoices():
    ensure_document_number_columns()
    ensure_invoice_payment_table()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT i.*, c.name AS customer_name
        FROM invoices i
        LEFT JOIN customers c ON i.customer_id = c.id
        WHERE i.company_id = %s
          AND i.status = 'Paid'
        ORDER BY i.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    invoice_rows_html = ""
    invoice_mobile_cards = ""

    for inv in rows:
        status = _clean_display(inv["status"])
        invoice_number = _clean_display(inv["invoice_number"] or inv["id"])

        invoice_rows_html += f"""
        <tr>
            <td>#{escape(str(invoice_number))}</td>
            <td>{escape(_clean_display(inv["customer_name"]))}</td>
            <td>{escape(str(inv["invoice_date"] or "-"))}</td>
            <td>${_safe_float(inv["total"]):,.2f}</td>
            <td>${_safe_float(inv["amount_paid"]):,.2f}</td>
            <td>${_safe_float(inv["balance_due"]):,.2f}</td>
            <td>{escape(status)}</td>
            <td>
                <a class='btn small' href='{url_for("invoices.view_invoice", invoice_id=inv["id"])}'>Open</a>
            </td>
        </tr>
        """

        invoice_mobile_cards += f"""
        <div class='mobile-list-card'>
            <div class='mobile-list-top'>
                <div class='mobile-list-title'>Invoice #{escape(str(invoice_number))}</div>
                <div class='mobile-badge'>{escape(status)}</div>
            </div>
            <div class='mobile-list-grid'>
                <div><span>Customer</span><strong>{escape(_clean_display(inv["customer_name"]))}</strong></div>
                <div><span>Date</span><strong>{escape(str(inv["invoice_date"] or "-"))}</strong></div>
                <div><span>Total</span><strong>${_safe_float(inv["total"]):,.2f}</strong></div>
                <div><span>Paid</span><strong>${_safe_float(inv["amount_paid"]):,.2f}</strong></div>
                <div><span>Balance</span><strong>${_safe_float(inv["balance_due"]):,.2f}</strong></div>
            </div>
            <div class='mobile-list-actions'>
                <a class='btn small' href='{url_for("invoices.view_invoice", invoice_id=inv["id"])}'>Open</a>
            </div>
        </div>
        """

    if not invoice_rows_html:
        invoice_rows_html = """
        <tr>
            <td colspan="8" class="muted">No paid invoices found.</td>
        </tr>
        """

    if not invoice_mobile_cards:
        invoice_mobile_cards = "<div class='mobile-list-card muted'>No paid invoices found.</div>"

    content = f"""
    <style>
        .invoice-page {{
            display:grid;
            gap:18px;
        }}

        .invoice-page-head {{
            display:flex;
            justify-content:space-between;
            gap:12px;
            align-items:center;
            flex-wrap:wrap;
        }}

        .invoice-table-wrap {{
            width:100%;
            overflow-x:auto;
        }}

        .mobile-only {{
            display:none;
        }}

        .desktop-only {{
            display:block;
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

        @media (max-width: 640px) {{
            .desktop-only {{
                display:none !important;
            }}

            .mobile-only {{
                display:block !important;
            }}

            .mobile-list-grid {{
                grid-template-columns:1fr;
            }}
        }}
    </style>

    <div class='invoice-page'>
        <div class='card'>
            <div class='invoice-page-head'>
                <div>
                    <h1>Paid Invoices</h1>
                    <p class='muted'>Invoices that have been paid in full.</p>
                </div>
                <div class='row-actions'>
                    <a class='btn secondary' href='{url_for("invoices.invoices")}'>Back to Invoices</a>
                </div>
            </div>
        </div>

        <div class='card'>
            <div class='invoice-table-wrap desktop-only'>
                <table class='table'>
                    <thead>
                        <tr>
                            <th>Invoice</th>
                            <th>Customer</th>
                            <th>Date</th>
                            <th>Total</th>
                            <th>Paid</th>
                            <th>Balance</th>
                            <th>Status</th>
                            <th></th>
                        </tr>
                    </thead>
                    <tbody>
                        {invoice_rows_html}
                    </tbody>
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {invoice_mobile_cards}
                </div>
            </div>
        </div>
    </div>
    """
    return render_page(content, "Paid Invoices")


@invoices_bp.route("/invoices/new", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def new_invoice():
    ensure_document_number_columns()
    ensure_invoice_payment_table()

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
        SELECT next_invoice_number
        FROM companies
        WHERE id = %s
        """,
        (cid,),
    ).fetchone()

    next_invoice_number_preview = "1001"
    if company_row and company_row["next_invoice_number"] is not None:
        next_invoice_number_preview = str(company_row["next_invoice_number"])

    customer_list = [
        {
            "id": c["id"],
            "name": _clean_text(c["name"]),
            "company": _clean_text(c["company"]),
            "email": _clean_text(c["email"]),
        }
        for c in customers
    ]

    if request.method == "POST":
        customer_id = request.form.get("customer_id", type=int)
        invoice_number = _clean_text(request.form.get("invoice_number", ""))
        invoice_date = _clean_text(request.form.get("invoice_date", ""))
        due_date = _clean_text(request.form.get("due_date", ""))
        description = _clean_text(request.form.get("description", ""))
        status = _clean_text(request.form.get("status", "Unpaid")) or "Unpaid"

        total = _safe_float(request.form.get("total"))

        if not customer_id:
            conn.close()
            flash("Please select a customer.")
            return redirect(url_for("invoices.new_invoice"))

        if not invoice_number:
            conn.close()
            invoice_number = get_next_invoice_number(cid)
            conn = get_db_connection()

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO invoices (
                company_id,
                customer_id,
                invoice_number,
                invoice_date,
                due_date,
                notes,
                subtotal,
                total,
                amount_paid,
                balance_due,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                cid,
                customer_id,
                invoice_number,
                invoice_date or None,
                due_date or None,
                description,
                total,
                total,
                total if status == "Paid" else 0,
                0 if status == "Paid" else total,
                status,
            ),
        )

        row = cur.fetchone()
        if not row or "id" not in row:
            conn.rollback()
            conn.close()
            flash("Could not create invoice.")
            return redirect(url_for("invoices.new_invoice"))

        invoice_id = row["id"]

        if total > 0:
            conn.execute(
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
                    description or "Manual Invoice",
                    1,
                    "ea",
                    total,
                    total,
                ),
            )

        if status == "Paid" and total > 0:
            conn.execute(
                """
                INSERT INTO invoice_payments
                (company_id, invoice_id, payment_date, amount, payment_method, reference, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    cid,
                    invoice_id,
                    invoice_date or date.today().isoformat(),
                    total,
                    "Manual Entry",
                    "",
                    "Invoice created as paid",
                ),
            )

        conn.commit()
        conn.close()

        _sync_invoice_status_and_bookkeeping(invoice_id)

        flash(f"Invoice #{invoice_number} created successfully.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    conn.close()

    csrf_token_value = generate_csrf()

    content = f"""
    <style>
        .customer-search-wrap {{
            position: relative;
            width: 100%;
        }}

        .customer-search-wrap label {{
            display: block;
            margin-bottom: 6px;
        }}

        .customer-search-input-wrap {{
            position: relative;
            width: 100%;
        }}

        .customer-search-input-wrap input {{
            width: 100%;
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
            box-shadow: 0 8px 20px rgba(0,0,0,.08);
            z-index: 1000;
            max-height: 260px;
            overflow-y: auto;
            margin-top: 0;
            padding: 0;
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

        .invoice-form-helper {{
            margin-top: 6px;
            font-size: .8rem;
            color: #64748b;
            line-height: 1.35;
        }}
    </style>

    <div class='card'>
        <h1>Create Invoice</h1>
        <p class='muted'>Create an invoice manually without converting a job.</p>
    </div>

    <div class='card'>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{csrf_token_value}">
            <div class='grid'>
                <div class='customer-search-wrap'>
                    <label>Customer</label>
                    <div class='customer-search-input-wrap'>
                        <input type='text'
                               id='customer_search'
                               placeholder='Search customer name, company, or email...'
                               autocomplete='off'
                               required>
                        <input type='hidden' name='customer_id' id='customer_id' required>
                        <div id='customer_results' class='customer-results'></div>
                    </div>
                </div>

                <div>
                    <label>Invoice Number</label>
                    <input type='text' name='invoice_number' placeholder='Auto-assigned if left blank'>
                    <div class='invoice-form-helper'>Next invoice number on file: {escape(next_invoice_number_preview)}</div>
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
                    <textarea name='description' placeholder='Mowing service, mulch, labor, fuel, delivery, or other invoice details.'></textarea>
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
                        : (customer.name || "Unnamed Customer");

                    closeResults();
                }});
            }});
        }}

        function getMatches(query) {{
            const q = String(query || "").trim().toLowerCase();
            if (!q) return [];

            return customers.filter(c =>
                (c.name && c.name.toLowerCase().includes(q)) ||
                (c.company && c.company.toLowerCase().includes(q)) ||
                (c.email && c.email.toLowerCase().includes(q))
            ).slice(0, 8);
        }}

        searchInput.addEventListener("input", function () {{
            customerIdInput.value = "";
            const matches = getMatches(this.value);

            if (!this.value.trim()) {{
                closeResults();
                return;
            }}

            renderCustomerResults(matches);
        }});

        searchInput.addEventListener("focus", function () {{
            const matches = getMatches(this.value);
            if (matches.length) {{
                renderCustomerResults(matches);
            }}
        }});

        document.addEventListener("click", function (e) {{
            if (!e.target.closest(".customer-search-wrap")) {{
                closeResults();
            }}
        }});
    </script>
    """
    return render_page(content, "Create Invoice")


@invoices_bp.route("/invoices/<int:invoice_id>")
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def view_invoice(invoice_id):
    ensure_document_number_columns()
    ensure_invoice_payment_table()

    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT i.*, c.name AS customer_name, c.company AS customer_company, c.email AS customer_email
        FROM invoices i
        LEFT JOIN customers c ON i.customer_id = c.id
        WHERE i.id = %s AND i.company_id = %s
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        abort(404)

    items = conn.execute(
        "SELECT * FROM invoice_items WHERE invoice_id = %s ORDER BY id",
        (invoice_id,),
    ).fetchall()

    payments = conn.execute(
        """
        SELECT *
        FROM invoice_payments
        WHERE invoice_id = %s AND company_id = %s
        ORDER BY payment_date DESC, id DESC
        """,
        (invoice_id, cid),
    ).fetchall()

    conn.close()

    # ---------- ITEMS ----------
    item_rows = ""
    item_mobile_cards = ""

    for item in items:
        item_rows += f"""
        <tr>
            <td>{escape(_clean_display(item["description"]))}</td>
            <td>{_safe_float(item["quantity"]):.2f}</td>
            <td>{escape(_clean_display(item["unit"]))}</td>
            <td>${_safe_float(item["unit_price"]):.2f}</td>
            <td>${_safe_float(item["line_total"]):.2f}</td>
        </tr>
        """

        item_mobile_cards += f"""
        <div class='mobile-list-card'>
            <div class='mobile-list-title'>{escape(_clean_display(item["description"]))}</div>
            <div class='mobile-list-grid'>
                <div><span>Qty</span><strong>{_safe_float(item["quantity"]):.2f}</strong></div>
                <div><span>Unit</span><strong>{escape(_clean_display(item["unit"]))}</strong></div>
                <div><span>Unit Price</span><strong>${_safe_float(item["unit_price"]):.2f}</strong></div>
                <div><span>Total</span><strong>${_safe_float(item["line_total"]):.2f}</strong></div>
            </div>
        </div>
        """

    if not item_rows:
        item_rows = "<tr><td colspan='5' class='muted'>No invoice items found.</td></tr>"

    if not item_mobile_cards:
        item_mobile_cards = "<div class='mobile-list-card muted'>No invoice items found.</div>"

    # ---------- PAYMENTS ----------
    payment_rows = ""
    payment_mobile_cards = ""

    for p in payments:
        csrf = generate_csrf()

        payment_rows += f"""
        <tr>
            <td>{escape(str(p["payment_date"] or "-"))}</td>
            <td>${_safe_float(p["amount"]):.2f}</td>
            <td>{escape(_clean_display(p["payment_method"]))}</td>
            <td>{escape(_clean_display(p["reference"]))}</td>
            <td>
                <a class='btn small' href='{url_for("invoices.edit_invoice_payment", invoice_id=invoice_id, payment_id=p["id"])}'>Edit</a>
                <form method='post' action='{url_for("invoices.delete_invoice_payment", invoice_id=invoice_id, payment_id=p["id"])}' style='display:inline;'>
                    <input type="hidden" name="csrf_token" value="{csrf}">
                    <button class='btn secondary small'>Delete</button>
                </form>
            </td>
        </tr>
        """

        payment_mobile_cards += f"""
        <div class='mobile-list-card'>
            <div class='mobile-list-title'>${_safe_float(p["amount"]):.2f}</div>
            <div class='mobile-list-grid'>
                <div><span>Date</span><strong>{escape(str(p["payment_date"] or "-"))}</strong></div>
                <div><span>Method</span><strong>{escape(_clean_display(p["payment_method"]))}</strong></div>
                <div><span>Ref</span><strong>{escape(_clean_display(p["reference"]))}</strong></div>
            </div>
        </div>
        """

    if not payment_rows:
        payment_rows = "<tr><td colspan='5' class='muted'>No payments recorded.</td></tr>"

    if not payment_mobile_cards:
        payment_mobile_cards = "<div class='mobile-list-card muted'>No payments recorded.</div>"

    # ---------- META ----------
    invoice_number = _clean_display(invoice["invoice_number"] or invoice["id"])
    is_paid = str(invoice["status"]).lower() == "paid"

    toggle_btn = f"""
    <form method='post' action='{url_for("invoices.toggle_invoice_paid", invoice_id=invoice_id)}'>
        <input type="hidden" name="csrf_token" value="{generate_csrf()}">
        <button class='btn {"secondary" if is_paid else "success"}'>
            {"Mark Unpaid" if is_paid else "Mark Paid"}
        </button>
    </form>
    """

    email_invoice_btn = f"""
        <form method='post' action='{url_for("invoices.send_invoice_email", invoice_id=invoice_id)}'>
            <input type="hidden" name="csrf_token" value="{generate_csrf()}">
            <button class='btn success'>Email Invoice</button>
        </form>
        """

    preview_invoice_btn = f"""
        <a class='btn secondary' href='{url_for("invoices.email_invoice_preview", invoice_id=invoice_id)}'>Email Preview</a>
        """

    add_payment_btn = f"""
        <a class='btn warning' href='{url_for("invoices.add_invoice_payment", invoice_id=invoice_id)}'>Add Payment</a>
        """

    delete_invoice_btn = f"""
        <form method='post' action='{url_for("invoices.delete_invoice", invoice_id=invoice_id)}' onsubmit="return confirm('Delete this invoice, its items, and its payments?');">
            <input type="hidden" name="csrf_token" value="{generate_csrf()}">
            <button class='btn danger'>Delete Invoice</button>
        </form>
        """

    # ---------- PAGE ----------
    content = f"""
    <style>
        .invoice-page {{ display:grid; gap:16px; }}

        .invoice-header {{
            display:flex;
            justify-content:space-between;
            flex-wrap:wrap;
            gap:12px;
        }}

        .invoice-grid {{
            display:grid;
            grid-template-columns:repeat(3,1fr);
            gap:12px;
        }}

        .invoice-card {{
            border:1px solid #e5e7eb;
            border-radius:12px;
            padding:12px;
        }}

        .invoice-card span {{
            display:block;
            font-size:.8rem;
            color:#64748b;
            margin-bottom:4px;
        }}

        .invoice-card strong {{
            display:block;
            color:#0f172a;
            line-height:1.3;
            word-break:break-word;
        }}

        .invoice-summary-grid {{
            display:grid;
            grid-template-columns:repeat(3,1fr);
            gap:12px;
        }}

        .mobile-only {{ display:none; }}

        @media (max-width:700px){{
            .invoice-grid {{ grid-template-columns:1fr; }}
            .invoice-summary-grid {{ grid-template-columns:1fr; }}
            .mobile-only {{ display:block; }}
            .desktop-only {{ display:none; }}
        }}
    </style>

    <div class='invoice-page'>

        <div class='card invoice-header'>
            <h1>Invoice #{invoice_number}</h1>
            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("invoices.invoices")}'>Back</a>
                {email_invoice_btn}
                {preview_invoice_btn}
                {add_payment_btn}
                {toggle_btn}
                {delete_invoice_btn}
            </div>
        </div>

        <div class='invoice-grid'>
            <div class='invoice-card'><span>Customer</span><strong>{escape(_clean_display(invoice["customer_name"]))}</strong></div>
            <div class='invoice-card'><span>Date</span><strong>{escape(str(invoice["invoice_date"] or "-"))}</strong></div>
            <div class='invoice-card'><span>Status</span><strong>{escape(_clean_display(invoice["status"]))}</strong></div>
        </div>

        <div class='invoice-summary-grid'>
            <div class='invoice-card'><span>Total</span><strong>${_safe_float(invoice["total"]):.2f}</strong></div>
            <div class='invoice-card'><span>Amount Paid</span><strong>${_safe_float(invoice["amount_paid"]):.2f}</strong></div>
            <div class='invoice-card'><span>Balance Due</span><strong>${_safe_float(invoice["balance_due"]):.2f}</strong></div>
        </div>

        <div class='card'>
            <h2>Items</h2>

            <div class='desktop-only'>
                <table class='table'>
                    <thead>
                        <tr>
                            <th>Description</th>
                            <th>Qty</th>
                            <th>Unit</th>
                            <th>Price</th>
                            <th>Total</th>
                        </tr>
                    </thead>
                    <tbody>{item_rows}</tbody>
                </table>
            </div>

            <div class='mobile-only'>
                {item_mobile_cards}
            </div>
        </div>

        <div class='card'>
            <h2>Payments</h2>

            <div class='desktop-only'>
                <table class='table'>
                    <thead>
                        <tr>
                            <th>Date</th>
                            <th>Amount</th>
                            <th>Method</th>
                            <th>Reference</th>
                            <th></th>
                        </tr>
                    </thead>
                    <tbody>{payment_rows}</tbody>
                </table>
            </div>

            <div class='mobile-only'>
                {payment_mobile_cards}
            </div>
        </div>

    </div>
    """

    return render_page(content, f"Invoice #{invoice_number}")


@invoices_bp.route("/invoices/<int:invoice_id>/toggle_paid", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def toggle_invoice_paid(invoice_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT *
        FROM invoices
        WHERE id = %s AND company_id = %s
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        flash("Invoice not found.")
        return redirect(url_for("invoices.invoices"))

    current_status = str(invoice["status"] or "").strip().lower()

    if current_status == "paid":
        conn.execute(
            """
            DELETE FROM invoice_payments
            WHERE invoice_id = %s AND company_id = %s
            """,
            (invoice_id, cid),
        )

        conn.commit()
        conn.close()

        _sync_invoice_status_and_bookkeeping(invoice_id)

        flash("Invoice marked unpaid and payment history cleared.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    remaining_balance = _safe_float(invoice["balance_due"])

    if remaining_balance <= 0:
        conn.close()
        flash("Invoice is already fully paid.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    conn.execute(
        """
        INSERT INTO invoice_payments
        (company_id, invoice_id, payment_date, amount, payment_method, reference, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
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
    conn.close()

    _sync_invoice_status_and_bookkeeping(invoice_id)

    flash("Invoice marked paid.")
    return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))


@invoices_bp.route("/invoices/<int:invoice_id>/email_preview")
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def email_invoice_preview(invoice_id):
    ensure_document_number_columns()
    ensure_invoice_payment_table()

    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT i.*, c.name AS customer_name, c.email AS customer_email
        FROM invoices i
        LEFT JOIN customers c ON i.customer_id = c.id
        WHERE i.id = %s AND i.company_id = %s
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        abort(404)

    company = conn.execute(
        """
        SELECT name, email
        FROM companies
        WHERE id = %s
        """,
        (cid,),
    ).fetchone()

    profile = conn.execute(
        """
        SELECT display_name, invoice_header_name
        FROM company_profile
        WHERE company_id = %s
        """,
        (cid,),
    ).fetchone()

    conn.close()

    invoice_number = invoice["invoice_number"] or invoice["id"]
    customer_name = _clean_text(invoice["customer_name"]) or "Customer"

    company_name = "Your Company"
    if profile and "invoice_header_name" in profile.keys() and profile["invoice_header_name"]:
        company_name = profile["invoice_header_name"]
    elif profile and "display_name" in profile.keys() and profile["display_name"]:
        company_name = profile["display_name"]
    elif company and "name" in company.keys() and company["name"]:
        company_name = company["name"]

    payment_url = None
    try:
        payment_url = create_invoice_checkout_session(invoice, cid)
    except Exception:
        payment_url = None

    payment_html = ""
    if payment_url:
        payment_html = f"""
        <p style="margin:16px 0;">
            Pay your invoice securely online:<br>
            <a href="{payment_url}">{payment_url}</a>
        </p>
        """

    preview_csrf = generate_csrf()

    content = f"""
    <div class='card'>
        <h1>Email Preview - Invoice #{escape(str(invoice_number))}</h1>

        <div class='card' style='margin-top:14px; background:#fafafa;'>
            <div style="font-family: Arial, sans-serif; font-size: 15px; line-height: 1.6; color: #222;">
                <p>Hello {escape(customer_name)},</p>

                <p>Please find attached Invoice #{escape(str(invoice_number))} from {escape(company_name)}.</p>

                <p>
                    <strong>Total:</strong> ${_safe_float(invoice['total']):.2f}<br>
                    <strong>Amount Paid:</strong> ${_safe_float(invoice['amount_paid']):.2f}<br>
                    <strong>Balance Due:</strong> ${_safe_float(invoice['balance_due']):.2f}
                </p>

                {payment_html}

                <p>Thank you.</p>
            </div>
        </div>

        <div class='row-actions' style='margin-top:16px;'>
            <a class='btn secondary' href='{url_for("invoices.view_invoice", invoice_id=invoice_id)}'>Back to Invoice</a>

            <form method='post' action='{url_for("invoices.send_invoice_email", invoice_id=invoice_id)}' style='margin:0;'>
                <input type="hidden" name="csrf_token" value="{preview_csrf}">
                <button class='btn success' type='submit'>Send Invoice Email</button>
            </form>
        </div>
    </div>
    """
    return render_page(content, f"Email Preview - Invoice #{invoice_number}")

@invoices_bp.route("/invoices/<int:invoice_id>/send_email", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def send_invoice_email(invoice_id):
    ensure_document_number_columns()
    ensure_invoice_payment_table()

    conn = get_db_connection()
    cid = session["company_id"]
    uid = session.get("user_id")

    invoice = conn.execute(
        """
        SELECT
            i.*,
            c.name AS customer_name,
            c.email AS customer_email
        FROM invoices i
        LEFT JOIN customers c
          ON i.customer_id = c.id
        WHERE i.id = %s
          AND i.company_id = %s
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        abort(404)

    items = conn.execute(
        """
        SELECT *
        FROM invoice_items
        WHERE invoice_id = %s
        ORDER BY id
        """,
        (invoice_id,),
    ).fetchall()

    company = conn.execute(
        """
        SELECT
            name,
            email,
            phone,
            website,
            address_line_1,
            address_line_2,
            city,
            state,
            zip_code
        FROM companies
        WHERE id = %s
        """,
        (cid,),
    ).fetchone()

    profile = conn.execute(
        """
        SELECT
            display_name,
            legal_name,
            logo_url,
            invoice_header_name,
            invoice_footer_note,
            email
        FROM company_profile
        WHERE company_id = %s
        """,
        (cid,),
    ).fetchone()

    conn.close()

    recipient = (invoice["customer_email"] or "").strip()
    if not recipient:
        flash("This customer does not have an email address.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    invoice_number = invoice["invoice_number"] or invoice["id"]
    customer_name = (invoice["customer_name"] or "Customer").strip()

    company_name = "Your Company"
    if profile and "invoice_header_name" in profile.keys() and profile["invoice_header_name"]:
        company_name = profile["invoice_header_name"]
    elif profile and "display_name" in profile.keys() and profile["display_name"]:
        company_name = profile["display_name"]
    elif company and "name" in company.keys() and company["name"]:
        company_name = company["name"]

    try:
        pdf_data = build_invoice_pdf(invoice, items, company, profile)

        payment_url = None
        try:
            payment_url = create_invoice_checkout_session(invoice, cid)
        except Exception:
            payment_url = None

        payment_text_section = ""
        payment_html_section = ""
        if payment_url:
            payment_text_section = f"\n\nPay your invoice securely online:\n{payment_url}\n"
            payment_html_section = f"""
                <p style="margin: 16px 0;">
                    Pay your invoice securely online:<br>
                    <a href="{payment_url}">{payment_url}</a>
                </p>
            """

        text_body = (
            f"Hello {customer_name},\n\n"
            f"Please find attached Invoice #{invoice_number} from {company_name}.\n\n"
            f"Total: ${_safe_float(invoice['total']):.2f}\n"
            f"Amount Paid: ${_safe_float(invoice['amount_paid']):.2f}\n"
            f"Balance Due: ${_safe_float(invoice['balance_due']):.2f}"
            f"{payment_text_section}\n"
            f"Thank you."
        )

        html_body = f"""
            <div style="font-family: Arial, sans-serif; font-size: 15px; line-height: 1.6; color: #222;">
                <p>Hello {escape(customer_name)},</p>

                <p>Please find attached Invoice #{escape(str(invoice_number))} from {escape(company_name)}.</p>

                <p>
                    <strong>Total:</strong> ${_safe_float(invoice['total']):.2f}<br>
                    <strong>Amount Paid:</strong> ${_safe_float(invoice['amount_paid']):.2f}<br>
                    <strong>Balance Due:</strong> ${_safe_float(invoice['balance_due']):.2f}
                </p>

                {payment_html_section}

                <p>Thank you.</p>
            </div>
        """

        send_company_email(
            company_id=cid,
            user_id=uid,
            to_email=recipient,
            subject=f"Invoice #{invoice_number}",
            html=html_body,
            body=text_body,
            attachment_bytes=pdf_data,
            attachment_filename=f"Invoice_{invoice_number}.pdf",
        )

        flash("Invoice emailed successfully as PDF.")

    except Exception as e:
        flash(f"Could not email invoice: {e}")

    return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))


@invoices_bp.route("/invoices/<int:invoice_id>/add_payment", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def add_invoice_payment(invoice_id):
    ensure_invoice_payment_table()

    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT i.*, c.name AS customer_name
        FROM invoices i
        LEFT JOIN customers c ON i.customer_id = c.id
        WHERE i.id = %s AND i.company_id = %s
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        abort(404)

    if request.method == "POST":
        payment_date = request.form.get("payment_date") or date.today().isoformat()
        amount = _safe_float(request.form.get("amount"))
        payment_method = _clean_text(request.form.get("payment_method"))
        reference = _clean_text(request.form.get("reference"))
        notes = _clean_text(request.form.get("notes"))

        if amount <= 0:
            conn.close()
            flash("Payment amount must be greater than 0.")
            return redirect(url_for("invoices.add_invoice_payment", invoice_id=invoice_id))
        
        current_paid = _safe_float(invoice["amount_paid"])
        invoice_total = _safe_float(invoice["total"])

        if current_paid + amount > invoice_total:
            conn.close()
            flash("Payment total cannot exceed the invoice total.")
            return redirect(url_for("invoices.add_invoice_payment", invoice_id=invoice_id))

        conn.execute(
            """
            INSERT INTO invoice_payments (
                company_id,
                invoice_id,
                payment_date,
                amount,
                payment_method,
                reference,
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                cid,
                invoice_id,
                payment_date,
                amount,
                payment_method,
                reference,
                notes,
            ),
        )

        conn.commit()
        conn.close()

        _sync_invoice_status_and_bookkeeping(invoice_id)

        flash("Payment added.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    add_payment_csrf = generate_csrf()

    content = f"""
    <div class='card'>
        <h1>Add Payment - Invoice #{escape(str(invoice["invoice_number"] or invoice["id"]))}</h1>
        <p class='muted'>
            Customer: {escape(_clean_display(invoice["customer_name"]))}<br>
            Total: ${_safe_float(invoice["total"]):.2f}<br>
            Amount Paid: ${_safe_float(invoice["amount_paid"]):.2f}<br>
            Balance Due: ${_safe_float(invoice["balance_due"]):.2f}
        </p>

        <form method='post'>
            <input type="hidden" name="csrf_token" value="{add_payment_csrf}">

            <div class='grid'>
                <div>
                    <label>Payment Date</label>
                    <input type='date' name='payment_date' value='{date.today().isoformat()}' required>
                </div>

                <div>
                    <label>Amount</label>
                    <input type='number' step='0.01' name='amount' min='0.01' required>
                </div>

                <div>
                    <label>Payment Method</label>
                    <input name='payment_method' placeholder='Cash, Card, Check, ACH'>
                </div>

                <div>
                    <label>Reference</label>
                    <input name='reference' placeholder='Check #, transaction ID, etc.'>
                </div>
            </div>

            <br>
            <label>Notes</label>
            <textarea name='notes'></textarea>

            <br>
            <div class='row-actions'>
                <button class='btn success' type='submit'>Save Payment</button>
                <a class='btn secondary' href='{url_for("invoices.view_invoice", invoice_id=invoice_id)}'>Cancel</a>
            </div>
        </form>
    </div>
    """
    conn.close()
    return render_page(content, f"Add Payment - Invoice #{invoice['id']}")

@invoices_bp.route("/invoices/<int:invoice_id>/payments/<int:payment_id>/delete", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def delete_invoice_payment(invoice_id, payment_id):
    conn = get_db_connection()
    cid = session["company_id"]

    payment = conn.execute(
        """
        SELECT *
        FROM invoice_payments
        WHERE id = %s AND invoice_id = %s AND company_id = %s
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
        WHERE id = %s AND invoice_id = %s AND company_id = %s
        """,
        (payment_id, invoice_id, cid),
    )
    conn.commit()
    conn.close()

    _sync_invoice_status_and_bookkeeping(invoice_id)

    flash("Payment deleted.")
    return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))


@invoices_bp.route("/invoices/<int:invoice_id>/payments/<int:payment_id>/edit", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def edit_invoice_payment(invoice_id, payment_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT *
        FROM invoices
        WHERE id = %s AND company_id = %s
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
        WHERE id = %s AND invoice_id = %s AND company_id = %s
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
        payment_method = _clean_text(request.form.get("payment_method"))
        reference = _clean_text(request.form.get("reference"))
        notes = _clean_text(request.form.get("notes"))

        if amount <= 0:
            conn.close()
            flash("Payment amount must be greater than 0.")
            return redirect(url_for("invoices.edit_invoice_payment", invoice_id=invoice_id, payment_id=payment_id))

        other_paid_row = conn.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS other_paid_total
            FROM invoice_payments
            WHERE invoice_id = %s AND id != %s
            """,
            (invoice_id, payment_id),
        ).fetchone()

        other_paid_total = _safe_float(other_paid_row["other_paid_total"] if other_paid_row else 0)
        invoice_total = _safe_float(invoice["total"])

        if other_paid_total + amount > invoice_total:
            conn.close()
            flash("Payment total cannot exceed the invoice total.")
            return redirect(url_for("invoices.edit_invoice_payment", invoice_id=invoice_id, payment_id=payment_id))

        conn.execute(
            """
            UPDATE invoice_payments
            SET payment_date = %s, amount = %s, payment_method = %s, reference = %s, notes = %s
            WHERE id = %s AND invoice_id = %s AND company_id = %s
            """,
            (payment_date, amount, payment_method, reference, notes, payment_id, invoice_id, cid),
        )
        conn.commit()
        conn.close()

        _sync_invoice_status_and_bookkeeping(invoice_id)

        flash("Payment updated.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    payment_method_val = escape(payment["payment_method"] or "", quote=True)
    reference_val = escape(payment["reference"] or "", quote=True)
    notes_val = escape(payment["notes"] or "")
    payment_date_val = payment["payment_date"] or date.today().isoformat()
    amount_val = payment["amount"] or 0

    conn.close()

    edit_payment_csrf = generate_csrf()

    content = f"""
    <div class='card'>
        <h1>Edit Payment</h1>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{edit_payment_csrf}">
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
                <a class='btn secondary' href='{url_for("invoices.view_invoice", invoice_id=invoice_id)}'>Cancel</a>
                <button class='btn success' type='submit'>Save Changes</button>
            </div>
        </form>
    </div>
    """
    return render_page(content, "Edit Payment")


@invoices_bp.route("/invoices/<int:invoice_id>/delete", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def delete_invoice(invoice_id):
    ensure_invoice_payment_table()

    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT *
        FROM invoices
        WHERE id = %s AND company_id = %s
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        abort(404)

    notes_text = (invoice["notes"] or "").strip() if "notes" in invoice.keys() and invoice["notes"] else ""
    recurring_schedule_id = None

    # Handle standard job invoice
    if "job_id" in invoice.keys() and invoice["job_id"]:
        conn.execute(
            """
            UPDATE jobs
            SET status = 'Scheduled'
            WHERE id = %s AND company_id = %s
            """,
            (invoice["job_id"], cid),
        )

    # Handle standard quote invoice
    if "quote_id" in invoice.keys() and invoice["quote_id"]:
        conn.execute(
            """
            UPDATE quotes
            SET status = 'Approved'
            WHERE id = %s AND company_id = %s
            """,
            (invoice["quote_id"], cid),
        )

    # Handle recurring schedule aggregate invoice
    # Notes format created earlier:
    # "Recurring schedule invoice for Schedule #<id> - <title>"
    if notes_text.lower().startswith("recurring schedule invoice for schedule #"):
        try:
            prefix = "Recurring schedule invoice for Schedule #"
            remainder = notes_text[len(prefix):]
            recurring_schedule_id = int(remainder.split(" ", 1)[0].split("-", 1)[0].strip())
        except Exception:
            recurring_schedule_id = None

    if recurring_schedule_id:
        conn.execute(
            """
            UPDATE jobs
            SET status = 'Completed'
            WHERE company_id = %s
              AND recurring_schedule_id = %s
              AND COALESCE(generated_from_schedule, FALSE) = TRUE
              AND COALESCE(status, '') = 'Invoiced'
            """,
            (cid, recurring_schedule_id),
        )

    conn.execute(
        """
        DELETE FROM invoice_payments
        WHERE invoice_id = %s AND company_id = %s
        """,
        (invoice_id, cid),
    )

    conn.execute(
        """
        DELETE FROM invoice_items
        WHERE invoice_id = %s
        """,
        (invoice_id,),
    )

    conn.execute(
        """
        DELETE FROM invoices
        WHERE id = %s AND company_id = %s
        """,
        (invoice_id, cid),
    )

    conn.commit()
    conn.close()

    flash("Invoice deleted.")
    return redirect(url_for("invoices.invoices"))


@invoices_bp.route("/stripe/webhook", methods=["POST"])
def stripe_webhook():
    payload = request.data
    sig_header = request.headers.get("Stripe-Signature")
    endpoint_secret = os.environ.get("STRIPE_WEBHOOK_SECRET")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, endpoint_secret
        )
    except Exception:
        return "Invalid", 400

    if event["type"] == "checkout.session.completed":
        session_data = event["data"]["object"]

        invoice_id = int(session_data["metadata"]["invoice_id"])
        company_id = int(session_data["metadata"]["company_id"])

        amount_paid = session_data["amount_total"] / 100

        conn = get_db_connection()

        conn.execute(
            """
            INSERT INTO invoice_payments
            (company_id, invoice_id, payment_date, amount, payment_method, reference, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                company_id,
                invoice_id,
                date.today().isoformat(),
                amount_paid,
                "Stripe",
                session_data.get("payment_intent"),
                "Online payment",
            ),
        )

        conn.commit()
        conn.close()

        _sync_invoice_status_and_bookkeeping(invoice_id)

    return "OK", 200