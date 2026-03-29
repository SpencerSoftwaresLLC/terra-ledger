import os
import uuid
import json
import io

from flask import Blueprint, request, redirect, url_for, session, flash, render_template_string, make_response
from flask_wtf.csrf import generate_csrf
from markupsafe import escape
from html import escape
from werkzeug.utils import secure_filename
from datetime import datetime

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from db import (
    get_db_connection,
    ensure_company_profile_table,
    ensure_company_profile_columns,
    ensure_company_tax_settings_table,
    ensure_company_profile_location_columns,
)
from utils.w2_service import (
    get_company_w2_readiness,
    get_employee_w2_readiness,
    build_w2_summary_data,
    get_company_w2_year_summary,
    list_employee_w2_summaries,
)
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page
from utils.emailing import send_company_email
from utils.backups import create_company_backup, export_company_backup_data, load_backup_file, restore_company_backup

settings_bp = Blueprint("settings", __name__)

ALLOWED_LOGO_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp", "svg"}


def allowed_logo_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_LOGO_EXTENSIONS


def ensure_logo_upload_folder():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    upload_folder = os.path.join(base_dir, "static", "uploads", "company_logos")
    os.makedirs(upload_folder, exist_ok=True)
    return upload_folder


def ensure_w2_company_profile_columns():
    ensure_company_profile_table()
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS ein TEXT")
        cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS state_employer_id TEXT")
        cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS w2_contact_name TEXT")
        cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS w2_contact_phone TEXT")
        cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS w2_contact_email TEXT")
        conn.commit()
    finally:
        conn.close()


def get_company_profile(cid):
    ensure_company_profile_table()
    ensure_company_profile_columns()
    ensure_company_profile_location_columns()
    ensure_w2_company_profile_columns()

    conn = get_db_connection()
    profile = conn.execute(
        "SELECT * FROM company_profile WHERE company_id = %s",
        (cid,),
    ).fetchone()
    conn.close()
    return profile


def get_company_profile_values(profile):
    display_name = profile["display_name"] if profile and profile["display_name"] else session.get("company_name", "")
    legal_name = profile["legal_name"] if profile and profile["legal_name"] else ""
    logo_url = profile["logo_url"] if profile and profile["logo_url"] else ""
    phone = profile["phone"] if profile and profile["phone"] else ""
    email = profile["email"] if profile and profile["email"] else ""
    website = profile["website"] if profile and profile["website"] else ""
    address_line_1 = profile["address_line_1"] if profile and profile["address_line_1"] else ""
    address_line_2 = profile["address_line_2"] if profile and profile["address_line_2"] else ""
    city = profile["city"] if profile and profile["city"] else ""
    state = profile["state"] if profile and profile["state"] else ""
    county = profile["county"] if profile and profile["county"] else ""
    zip_code = profile["zip_code"] if profile and profile["zip_code"] else ""
    invoice_header_name = profile["invoice_header_name"] if profile and profile["invoice_header_name"] else (display_name or "")
    quote_header_name = profile["quote_header_name"] if profile and profile["quote_header_name"] else (display_name or "")
    invoice_footer_note = profile["invoice_footer_note"] if profile and profile["invoice_footer_note"] else ""
    quote_footer_note = profile["quote_footer_note"] if profile and profile["quote_footer_note"] else ""
    email_from_name = profile["email_from_name"] if profile and profile["email_from_name"] else (display_name or "")
    reply_to_email = profile["reply_to_email"] if profile and profile["reply_to_email"] else (email or "")
    platform_sender_enabled = int(profile["platform_sender_enabled"] or 1) if profile and "platform_sender_enabled" in profile.keys() else 1
    reply_to_mode = profile["reply_to_mode"] if profile and "reply_to_mode" in profile.keys() and profile["reply_to_mode"] else "company"

    ein = profile["ein"] if profile and "ein" in profile.keys() and profile["ein"] else ""
    state_employer_id = profile["state_employer_id"] if profile and "state_employer_id" in profile.keys() and profile["state_employer_id"] else ""
    w2_contact_name = profile["w2_contact_name"] if profile and "w2_contact_name" in profile.keys() and profile["w2_contact_name"] else ""
    w2_contact_phone = profile["w2_contact_phone"] if profile and "w2_contact_phone" in profile.keys() and profile["w2_contact_phone"] else ""
    w2_contact_email = profile["w2_contact_email"] if profile and "w2_contact_email" in profile.keys() and profile["w2_contact_email"] else ""

    return {
        "display_name": display_name,
        "legal_name": legal_name,
        "logo_url": logo_url,
        "phone": phone,
        "email": email,
        "website": website,
        "address_line_1": address_line_1,
        "address_line_2": address_line_2,
        "city": city,
        "state": state,
        "county": county,
        "zip_code": zip_code,
        "invoice_header_name": invoice_header_name,
        "quote_header_name": quote_header_name,
        "invoice_footer_note": invoice_footer_note,
        "quote_footer_note": quote_footer_note,
        "email_from_name": email_from_name,
        "reply_to_email": reply_to_email,
        "platform_sender_enabled": platform_sender_enabled,
        "reply_to_mode": reply_to_mode,
        "ein": ein,
        "state_employer_id": state_employer_id,
        "w2_contact_name": w2_contact_name,
        "w2_contact_phone": w2_contact_phone,
        "w2_contact_email": w2_contact_email,
    }


def _money(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _company_display_name_for_reports(cid):
    profile = get_company_profile(cid)
    values = get_company_profile_values(profile)
    return values.get("legal_name") or values.get("display_name") or session.get("company_name", "TerraLedger")


def _build_w2_summary_pdf(company_name, tax_year, employee_name, summary):
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    def money(value):
        return float(_money(value or 0))

    def text(value):
        return str(value or "").strip()

    def draw_box(x, y_top, w, h, label, value="", label_size=6.5, value_size=9, bold=False, align="left"):
        y_bottom = y_top - h
        pdf.rect(x, y_bottom, w, h)

        pdf.setFont("Helvetica", label_size)
        pdf.drawString(x + 3, y_top - 9, label)

        pdf.setFont("Helvetica-Bold" if bold else "Helvetica", value_size)

        value = text(value)
        if not value:
            return

        if align == "right":
            pdf.drawRightString(x + w - 4, y_bottom + 7, value)
        elif align == "center":
            pdf.drawCentredString(x + (w / 2), y_bottom + 7, value)
        else:
            pdf.drawString(x + 4, y_bottom + 7, value)

    def draw_money_box(x, y_top, w, h, label, value):
        draw_box(
            x=x,
            y_top=y_top,
            w=w,
            h=h,
            label=label,
            value=f"{money(value):,.2f}" if money(value) else "0.00",
            label_size=6.5,
            value_size=9,
            bold=True,
            align="right",
        )

    # Data
    wages = money(summary.get("wages"))
    federal = money(summary.get("federal_withholding"))
    social_security = money(summary.get("social_security"))
    medicare = money(summary.get("medicare"))
    state = money(summary.get("state_withholding"))
    local = money(summary.get("local_tax"))

    # Page title
    pdf.setTitle(f"W2_{text(employee_name).replace(' ', '_')}_{text(tax_year)}")

    pdf.setFont("Helvetica-Bold", 15)
    pdf.drawString(36, height - 32, "Wage and Tax Statement")

    pdf.setFont("Helvetica-Bold", 28)
    pdf.drawRightString(width - 36, height - 30, "W-2")

    pdf.setFont("Helvetica", 8)
    pdf.drawString(36, height - 44, f"Tax Year {text(tax_year)}")
    pdf.drawRightString(width - 36, height - 44, "TerraLedger Employee Copy")

    # Top form area
    left = 36
    right = width - 36
    top = height - 60
    form_width = right - left

    row1_h = 52
    row2_h = 52
    row3_h = 52
    row4_h = 52
    row5_h = 52

    # Column layout
    c1 = 64
    c2 = 185
    c3 = 74
    c4 = 74
    c5 = 74
    c6 = form_width - (c1 + c2 + c3 + c4 + c5)

    # Row 1
    y = top
    draw_box(left, y, c1, row1_h, "a  Employee's social security number", "")
    draw_box(left + c1, y, c2, row1_h, "b  Employer identification number (EIN)", "")
    draw_box(left + c1 + c2, y, c3, row1_h, "1  Wages, tips, other compensation", f"{wages:,.2f}", value_size=10, bold=True, align="right")
    draw_box(left + c1 + c2 + c3, y, c4, row1_h, "2  Federal income tax withheld", f"{federal:,.2f}", value_size=10, bold=True, align="right")
    draw_box(left + c1 + c2 + c3 + c4, y, c5, row1_h, "3  Social security wages", f"{wages:,.2f}", value_size=10, bold=True, align="right")
    draw_box(left + c1 + c2 + c3 + c4 + c5, y, c6, row1_h, "4  Social security tax withheld", f"{social_security:,.2f}", value_size=10, bold=True, align="right")

    # Row 2
    y -= row1_h
    c7 = 249
    c8 = 74
    c9 = 74
    c10 = 74
    c11 = form_width - (c7 + c8 + c9 + c10)

    draw_box(left, y, c7, row2_h, "c  Employer's name, address, and ZIP code", text(company_name), value_size=10, bold=True)
    draw_box(left + c7, y, c8, row2_h, "5  Medicare wages and tips", f"{wages:,.2f}", value_size=10, bold=True, align="right")
    draw_box(left + c7 + c8, y, c9, row2_h, "6  Medicare tax withheld", f"{medicare:,.2f}", value_size=10, bold=True, align="right")
    draw_box(left + c7 + c8 + c9, y, c10, row2_h, "7  Social security tips", "0.00", value_size=10, bold=True, align="right")
    draw_box(left + c7 + c8 + c9 + c10, y, c11, row2_h, "8  Allocated tips", "0.00", value_size=10, bold=True, align="right")

    # Row 3
    y -= row2_h
    c12 = 249
    c13 = 74
    c14 = 74
    c15 = 74
    c16 = form_width - (c12 + c13 + c14 + c15)

    draw_box(left, y, c12, row3_h, "d  Control number", "")
    draw_box(left + c12, y, c13, row3_h, "9", "")
    draw_box(left + c12 + c13, y, c14, row3_h, "10  Dependent care benefits", "0.00", value_size=10, bold=True, align="right")
    draw_box(left + c12 + c13 + c14, y, c15, row3_h, "11  Nonqualified plans", "0.00", value_size=10, bold=True, align="right")
    draw_box(left + c12 + c13 + c14 + c15, y, c16, row3_h, "12a  See instructions for box 12", "")

    # Row 4
    y -= row3_h
    c17 = 249
    c18 = 74
    c19 = 74
    c20 = 74
    c21 = form_width - (c17 + c18 + c19 + c20)

    draw_box(left, y, c17, row4_h, "e  Employee's first name and initial / last name", text(employee_name), value_size=10, bold=True)
    draw_box(left + c17, y, c18, row4_h, "12b", "")
    draw_box(left + c17 + c18, y, c19, row4_h, "12c", "")
    draw_box(left + c17 + c18 + c19, y, c20, row4_h, "12d", "")
    draw_box(left + c17 + c18 + c19 + c20, y, c21, row4_h, "13", "Statutory employee / Retirement plan / Third-party sick pay", value_size=7)

    # Row 5
    y -= row4_h
    c22 = 249
    c23 = 149
    c24 = 74
    c25 = form_width - (c22 + c23 + c24)

    draw_box(left, y, c22, row5_h, "f  Employee's address and ZIP code", "")
    draw_box(left + c22, y, c23, row5_h, "14  Other", "")
    draw_box(left + c22 + c23, y, c24, row5_h, "15  State / Employer's state ID no.", "")
    draw_box(left + c22 + c23 + c24, y, c25, row5_h, "16  State wages, tips, etc.", f"{wages:,.2f}", value_size=10, bold=True, align="right")

    # Row 6 - state/local continuation
    y -= row5_h
    c26 = 74
    c27 = 74
    c28 = 110
    c29 = 74
    c30 = 74
    c31 = form_width - (c26 + c27 + c28 + c29 + c30)

    draw_box(left, y, c26, row1_h, "17  State income tax", f"{state:,.2f}", value_size=10, bold=True, align="right")
    draw_box(left + c26, y, c27, row1_h, "18  Local wages, tips, etc.", f"{wages:,.2f}", value_size=10, bold=True, align="right")
    draw_box(left + c26 + c27, y, c28, row1_h, "19  Local income tax", f"{local:,.2f}", value_size=10, bold=True, align="right")
    draw_box(left + c26 + c27 + c28, y, c29, row1_h, "20  Locality name", "")
    draw_box(left + c26 + c27 + c28 + c29, y, c30 + c31, row1_h, "Employer/employee records note", "Generated from TerraLedger payroll data", value_size=8)

    # Footer / disclaimer
    footer_y = y - 22
    pdf.setFont("Helvetica", 7.5)
    pdf.drawString(36, footer_y, "This layout is designed to resemble an employee W-2 copy using the payroll data currently stored in TerraLedger.")
    pdf.drawString(36, footer_y - 10, "Boxes that require data TerraLedger does not yet store are intentionally left blank.")
    pdf.drawString(36, footer_y - 20, "Review all year-end values before distributing to employees or using for filing.")

    pdf.showPage()
    pdf.save()

    pdf_data = buffer.getvalue()
    buffer.close()
    return pdf_data


def _build_w2_all_summary_pdf(company_name, tax_year, employee_rows):
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    def money(value):
        return float(_money(value or 0))

    def text(value):
        return str(value or "").strip()

    def start_page(page_no):
        pdf.setFont("Helvetica-Bold", 16)
        pdf.drawString(36, height - 36, "W-2 Employee Totals Report")

        pdf.setFont("Helvetica", 9)
        pdf.drawString(36, height - 50, f"Company: {text(company_name)}")
        pdf.drawString(36, height - 62, f"Tax Year: {text(tax_year)}")
        pdf.drawRightString(width - 36, height - 50, f"Page {page_no}")

        table_top = height - 84
        pdf.setLineWidth(1)
        pdf.line(36, table_top, width - 36, table_top)

        header_y = table_top - 14
        pdf.setFont("Helvetica-Bold", 8.5)
        pdf.drawString(40, header_y, "Employee")
        pdf.drawRightString(300, header_y, "Box 1 Wages")
        pdf.drawRightString(370, header_y, "Box 2 Federal")
        pdf.drawRightString(435, header_y, "Box 4 SS Tax")
        pdf.drawRightString(500, header_y, "Box 6 Medicare")
        pdf.drawRightString(560, header_y, "Box 17 State")
        pdf.drawRightString(612, header_y, "Box 19 Local")

        pdf.line(36, header_y - 6, width - 36, header_y - 6)
        return header_y - 18

    page_no = 1
    y = start_page(page_no)

    total_wages = 0.0
    total_federal = 0.0
    total_ss = 0.0
    total_medicare = 0.0
    total_state = 0.0
    total_local = 0.0

    pdf.setFont("Helvetica", 8)

    for row in employee_rows:
        if y < 66:
            pdf.showPage()
            page_no += 1
            y = start_page(page_no)
            pdf.setFont("Helvetica", 8)

        employee_name = text(row.get("employee_name"))[:42]
        wages = money(row.get("wages"))
        federal = money(row.get("federal_withholding"))
        social_security = money(row.get("social_security"))
        medicare = money(row.get("medicare"))
        state = money(row.get("state_withholding"))
        local = money(row.get("local_tax"))

        total_wages += wages
        total_federal += federal
        total_ss += social_security
        total_medicare += medicare
        total_state += state
        total_local += local

        pdf.drawString(40, y, employee_name)
        pdf.drawRightString(300, y, f"{wages:,.2f}")
        pdf.drawRightString(370, y, f"{federal:,.2f}")
        pdf.drawRightString(435, y, f"{social_security:,.2f}")
        pdf.drawRightString(500, y, f"{medicare:,.2f}")
        pdf.drawRightString(560, y, f"{state:,.2f}")
        pdf.drawRightString(612, y, f"{local:,.2f}")

        y -= 15

    if y < 96:
        pdf.showPage()
        page_no += 1
        y = start_page(page_no)
        pdf.setFont("Helvetica", 8)

    y -= 6
    pdf.line(36, y, width - 36, y)
    y -= 16

    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(40, y, "Totals")
    pdf.drawRightString(300, y, f"{total_wages:,.2f}")
    pdf.drawRightString(370, y, f"{total_federal:,.2f}")
    pdf.drawRightString(435, y, f"{total_ss:,.2f}")
    pdf.drawRightString(500, y, f"{total_medicare:,.2f}")
    pdf.drawRightString(560, y, f"{total_state:,.2f}")
    pdf.drawRightString(612, y, f"{total_local:,.2f}")

    y -= 22
    pdf.setFont("Helvetica", 8)
    pdf.drawString(40, y, "This report summarizes employee W-2 payroll totals from TerraLedger for review and year-end preparation.")

    pdf.showPage()
    pdf.save()

    pdf_data = buffer.getvalue()
    buffer.close()
    return pdf_data


def _w2_company_readiness(values):
    checks = [
        ("Legal Business Name", values.get("legal_name")),
        ("EIN", values.get("ein")),
        ("Address Line 1", values.get("address_line_1")),
        ("City", values.get("city")),
        ("State", values.get("state")),
        ("ZIP Code", values.get("zip_code")),
        ("W-2 Contact Name", values.get("w2_contact_name")),
        ("W-2 Contact Phone", values.get("w2_contact_phone")),
        ("W-2 Contact Email", values.get("w2_contact_email")),
    ]

    missing = [label for label, val in checks if not (str(val or "").strip())]
    return {"missing": missing, "ready": len(missing) == 0}


@settings_bp.route("/settings")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def settings():
    settings_html = f"""
    <style>
        .settings-card {{
            display: flex;
            flex-direction: column;
            min-height: 230px;
        }}

        .settings-card-head {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 10px;
            flex-wrap: wrap;
            margin-bottom: 10px;
        }}

        .settings-card p {{
            flex-grow: 1;
        }}

        .settings-actions {{
            margin-top: auto;
            display: flex;
        }}

        .settings-actions .btn {{
            width: 100%;
            min-width: 0;
        }}
    </style>

    <div class="settings-page">
        <div class="settings-header card">
            <div>
                <h1 style="margin-bottom:6px;">Settings</h1>
                <div class="muted">Manage company information, branding, email delivery, taxes, billing, and users.</div>
            </div>
        </div>

        <div class="settings-grid">

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>Company Info</h3>
                    <span class="settings-badge">General</span>
                </div>
                <p class="muted">Update your company name, contact info, address, and tax ID.</p>
                <div class="settings-actions">
                    <a class="btn" href="{url_for('settings.settings_company')}">Open Company Info</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>Branding</h3>
                    <span class="settings-badge">Appearance</span>
                </div>
                <p class="muted">Manage your logo, invoice names, quote names, and document footer notes.</p>
                <div class="settings-actions">
                    <a class="btn" href="{url_for('settings.settings_branding')}">Open Branding</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>Email Settings</h3>
                    <span class="settings-badge">Delivery</span>
                </div>
                <p class="muted">Set sender identity, reply-to behavior, and send a test email.</p>
                <div class="settings-actions">
                    <a class="btn" href="{url_for('settings.settings_email')}">Open Email Settings</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>Tax Defaults</h3>
                    <span class="settings-badge">Financial</span>
                </div>
                <p class="muted">Set default payroll tax rates for federal, state, local, and company-side taxes.</p>
                <div class="settings-actions">
                    <a class="btn" href="{url_for('settings.settings_taxes')}">Configure Taxes</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>Users & Permissions</h3>
                    <span class="settings-badge">Access</span>
                </div>
                <p class="muted">Manage employees, logins, roles, and access levels for your company.</p>
                <div class="settings-actions">
                    <a class="btn" href="{url_for('users.users')}">Open Users</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>Billing</h3>
                    <span class="settings-badge">Subscription</span>
                </div>
                <p class="muted">Review your subscription, payment methods, and billing details.</p>
                <div class="settings-actions">
                    <a class="btn" href="{url_for('billing.billing_page')}">View Billing</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>Year-End / W-2</h3>
                    <span class="settings-badge">Payroll</span>
                </div>
                <p class="muted">Review yearly payroll totals, manage W-2 company filing settings, and generate printable year-end summaries.</p>
                <div class="settings-actions">
                    <a class="btn success" href="{url_for('settings.settings_w2')}">Open W-2 Center</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>Backups</h3>
                    <span class="settings-badge">Safety</span>
                </div>
                <p class="muted">Download a full backup of your company data anytime.</p>
                <div class="settings-actions">
                    <a class="btn warning" href="/settings/backup/download">Download Backup</a>
                </div>
            </div>
            
            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>Restore Backup</h3>
                    <span class="settings-badge">Recovery</span>
                </div>
                <p class="muted">Upload a backup file and restore your company data.</p>
                <div class="settings-actions">
                    <a class="btn warning" href="{url_for('settings.restore_backup')}">Open Restore</a>
                </div>
            </div>
        </div>
    </div>
    """
    return render_page(settings_html, "Settings")


@settings_bp.route("/settings/company", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def settings_company():
    ensure_company_profile_columns()
    ensure_company_profile_location_columns()
    ensure_company_profile_table()
    ensure_w2_company_profile_columns()

    def clean_text_input(value):
        if value is None:
            return ""
        text = str(value).strip()
        if not text:
            return ""
        if text.lower() in {"none", "null", "n/a", "0", "0.0", "0.00"}:
            return ""
        return text

    company_id = session.get("company_id")
    if not company_id:
        flash("No company is associated with this account.")
        return redirect(url_for("dashboard.dashboard"))

    conn = get_db_connection()

    if request.method == "POST":
        name = clean_text_input(request.form.get("name", ""))
        phone = clean_text_input(request.form.get("phone", ""))
        email = clean_text_input(request.form.get("email", ""))
        website = clean_text_input(request.form.get("website", ""))
        tax_id = clean_text_input(request.form.get("tax_id", ""))
        address_line_1 = clean_text_input(request.form.get("address_line_1", ""))
        address_line_2 = clean_text_input(request.form.get("address_line_2", ""))
        city = clean_text_input(request.form.get("city", ""))
        state = clean_text_input(request.form.get("state", "")).upper()
        county = clean_text_input(request.form.get("county", ""))
        zip_code = clean_text_input(request.form.get("zip_code", ""))

        conn.execute(
            """
            UPDATE companies
            SET name = %s,
                phone = %s,
                email = %s,
                website = %s,
                tax_id = %s,
                address_line_1 = %s,
                address_line_2 = %s,
                city = %s,
                state = %s,
                zip_code = %s
            WHERE id = %s
            """,
            (
                name,
                phone,
                email,
                website,
                tax_id,
                address_line_1,
                address_line_2,
                city,
                state,
                zip_code,
                company_id,
            ),
        )

        existing_profile = conn.execute(
            "SELECT id FROM company_profile WHERE company_id = %s",
            (company_id,),
        ).fetchone()

        if existing_profile:
            conn.execute(
                """
                UPDATE company_profile
                SET display_name = %s,
                    phone = %s,
                    email = %s,
                    website = %s,
                    address_line_1 = %s,
                    address_line_2 = %s,
                    city = %s,
                    state = %s,
                    county = %s,
                    zip_code = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE company_id = %s
                """,
                (
                    name,
                    phone,
                    email,
                    website,
                    address_line_1,
                    address_line_2,
                    city,
                    state,
                    county,
                    zip_code,
                    company_id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO company_profile (
                    company_id,
                    display_name,
                    phone,
                    email,
                    website,
                    address_line_1,
                    address_line_2,
                    city,
                    state,
                    county,
                    zip_code
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    company_id,
                    name,
                    phone,
                    email,
                    website,
                    address_line_1,
                    address_line_2,
                    city,
                    state,
                    county,
                    zip_code,
                ),
            )

        conn.commit()
        conn.close()

        session["company_name"] = name or "TerraLedger"
        flash("Company profile updated successfully.")
        return redirect(url_for("settings.settings_company"))

    company = conn.execute(
        """
        SELECT id, name, phone, email, website, tax_id,
               address_line_1, address_line_2, city, state, zip_code
        FROM companies
        WHERE id = %s
        """,
        (company_id,),
    ).fetchone()

    profile = conn.execute(
        """
        SELECT county
        FROM company_profile
        WHERE company_id = %s
        """,
        (company_id,),
    ).fetchone()

    conn.close()

    company_county = clean_text_input(profile["county"]) if profile and "county" in profile.keys() else ""

    company_profile_html = """
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Company Info</h1>
                <p class='muted' style='margin:0;'>Manage your main business information used across the system.</p>
            </div>
            <div class='row-actions'>
                <a class='btn secondary' href='{{ url_for("settings.settings") }}'>Back to Settings</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <div class='grid'>
                <div>
                    <label>Company Name</label>
                    <input name='name' value='{{ clean_text_input(company["name"]) if company else "" }}'>
                </div>
                <div>
                    <label>Phone</label>
                    <input name='phone' value='{{ clean_text_input(company["phone"]) if company else "" }}'>
                </div>
                <div>
                    <label>Email</label>
                    <input name='email' value='{{ clean_text_input(company["email"]) if company else "" }}'>
                </div>
                <div>
                    <label>Website</label>
                    <input name='website' value='{{ clean_text_input(company["website"]) if company else "" }}'>
                </div>
                <div>
                    <label>Tax ID</label>
                    <input name='tax_id' value='{{ clean_text_input(company["tax_id"]) if company else "" }}'>
                </div>
                <div>
                    <label>Address Line 1</label>
                    <input name='address_line_1' value='{{ clean_text_input(company["address_line_1"]) if company else "" }}'>
                </div>
                <div>
                    <label>Address Line 2</label>
                    <input name='address_line_2' value='{{ clean_text_input(company["address_line_2"]) if company else "" }}'>
                </div>
                <div>
                    <label>City</label>
                    <input name='city' value='{{ clean_text_input(company["city"]) if company else "" }}'>
                </div>
                <div>
                    <label>State</label>
                    <input name='state' value='{{ clean_text_input(company["state"]) if company else "" }}' maxlength='2'>
                </div>
                <div>
                    <label>County</label>
                    <input name='county' value='{{ clean_text_input(company_county) }}' placeholder='Tippecanoe'>
                </div>
                <div>
                    <label>Zip Code</label>
                    <input name='zip_code' value='{{ clean_text_input(company["zip_code"]) if company else "" }}'>
                </div>
            </div>
            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>Save Company Info</button>
            </div>
        </form>
    </div>
    """

    return render_page(
        render_template_string(
            company_profile_html,
            company=company,
            company_county=company_county,
            clean_text_input=clean_text_input,
        ),
        "Company Info",
    )


@settings_bp.route("/settings/taxes", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def settings_taxes():
    ensure_company_tax_settings_table()

    conn = get_db_connection()
    cid = session["company_id"]

    if request.method == "POST":
        federal_withholding_rate = float(request.form.get("federal_withholding_rate") or 0)
        state_withholding_rate = float(request.form.get("state_withholding_rate") or 0)
        social_security_rate = float(request.form.get("social_security_rate") or 0)
        medicare_rate = float(request.form.get("medicare_rate") or 0)
        local_tax_rate = float(request.form.get("local_tax_rate") or 0)
        unemployment_rate = float(request.form.get("unemployment_rate") or 0)
        workers_comp_rate = float(request.form.get("workers_comp_rate") or 0)

        existing = conn.execute(
            "SELECT id FROM company_tax_settings WHERE company_id = %s",
            (cid,),
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE company_tax_settings
                SET federal_withholding_rate = %s,
                    state_withholding_rate = %s,
                    social_security_rate = %s,
                    medicare_rate = %s,
                    local_tax_rate = %s,
                    unemployment_rate = %s,
                    workers_comp_rate = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE company_id = %s
                """,
                (
                    federal_withholding_rate,
                    state_withholding_rate,
                    social_security_rate,
                    medicare_rate,
                    local_tax_rate,
                    unemployment_rate,
                    workers_comp_rate,
                    cid,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO company_tax_settings (
                    company_id,
                    federal_withholding_rate,
                    state_withholding_rate,
                    social_security_rate,
                    medicare_rate,
                    local_tax_rate,
                    unemployment_rate,
                    workers_comp_rate
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    cid,
                    federal_withholding_rate,
                    state_withholding_rate,
                    social_security_rate,
                    medicare_rate,
                    local_tax_rate,
                    unemployment_rate,
                    workers_comp_rate,
                ),
            )

        conn.commit()
        flash("Tax settings saved successfully.")
        conn.close()
        return redirect(url_for("settings.settings_taxes"))

    settings = conn.execute(
        "SELECT * FROM company_tax_settings WHERE company_id = %s",
        (cid,),
    ).fetchone()

    conn.close()

    tax_default_html = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Tax Defaults</h1>
                <p class='muted' style='margin:0;'>Set default payroll tax rates for your company.</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("settings.settings")}' class='btn secondary'>Back to Settings</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Current Tax Defaults</h2>
        <div style='display:grid; grid-template-columns:repeat(auto-fit, minmax(180px, 1fr)); gap:14px; align-items:stretch; margin-top:12px;'>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>Federal</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['federal_withholding_rate']) if settings and settings['federal_withholding_rate'] is not None else 0:.2f}%</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>State</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['state_withholding_rate']) if settings and settings['state_withholding_rate'] is not None else 0:.2f}%</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>Social Security</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['social_security_rate']) if settings and float(settings['social_security_rate'] or 0) > 0 else 6.20:.2f}%</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>Medicare</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['medicare_rate']) if settings and float(settings['medicare_rate'] or 0) > 0 else 1.45:.2f}%</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>Local Tax</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['local_tax_rate']) if settings and settings['local_tax_rate'] is not None else 0:.2f}%</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>Unemployment</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['unemployment_rate']) if settings and settings['unemployment_rate'] is not None else 0:.2f}%</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>Workers Comp</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['workers_comp_rate']) if settings and settings['workers_comp_rate'] is not None else 0:.2f}%</div>
            </div>

        </div>
    </div>

    <div class='card'>
        <h2>Edit Tax Defaults</h2>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <div style='display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr)); gap:16px; align-items:end;'>

                <div>
                    <label>Federal Withholding %</label>
                    <input type="number" step="0.01" min="0"
                        name="federal_withholding_rate"
                        value="{{ settings['federal_withholding_rate'] if settings and settings['federal_withholding_rate'] is not none else '' }}"
                        placeholder="Auto">
                </div>

                <div>
                    <label>State Withholding %</label>
                    <input type="number" step="0.01" min="0"
                        name="state_withholding_rate"
                        value="{{ settings['state_withholding_rate'] if settings and settings['state_withholding_rate'] is not none else '' }}"
                        placeholder="Auto">
                </div>

                <div>
                    <label>Social Security %</label>
                    <input type='number' step='0.01' min='0' name='social_security_rate'
                           value='{float(settings["social_security_rate"]) if settings and settings["social_security_rate"] is not None else 6.20:.2f}'>
                </div>

                <div>
                    <label>Medicare %</label>
                    <input type='number' step='0.01' min='0' name='medicare_rate'
                           value='{float(settings["medicare_rate"]) if settings and settings["medicare_rate"] is not None else 1.45:.2f}'>
                </div>

                <div>
                    <label>Local Tax %</label>
                    <input type='number' step='0.01' min='0' name='local_tax_rate'
                           value='{float(settings["local_tax_rate"]) if settings and settings["local_tax_rate"] is not None else 0:.2f}'>
                </div>

                <div>
                    <label>Unemployment %</label>
                    <input type='number' step='0.01' min='0' name='unemployment_rate'
                           value='{float(settings["unemployment_rate"]) if settings and settings["unemployment_rate"] is not None else 0:.2f}'>
                </div>

                <div>
                    <label>Workers Comp %</label>
                    <input type='number' step='0.01' min='0' name='workers_comp_rate'
                           value='{float(settings["workers_comp_rate"]) if settings and settings["workers_comp_rate"] is not None else 0:.2f}'>
                </div>

            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>Save Tax Settings</button>
            </div>
        </form>
    </div>
    """

    return render_page(
        render_template_string(tax_default_html, settings=settings),
        "Tax Defaults",
    )


@settings_bp.route("/settings/w2")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def settings_w2():
    ensure_w2_company_profile_columns()

    cid = session["company_id"]

    year = (request.args.get("year") or str(datetime.utcnow().year)).strip()
    if not year.isdigit():
        year = str(datetime.utcnow().year)

    profile = get_company_profile(cid)
    values = get_company_profile_values(profile)
    company_readiness = get_company_w2_readiness(values)

    conn = get_db_connection()
    year_summary = get_company_w2_year_summary(conn, cid, year)
    employee_summaries = list_employee_w2_summaries(conn, cid, year)
    conn.close()

    total_wages = float(year_summary.get("total_wages", 0) or 0)
    total_federal = float(year_summary.get("total_federal_withholding", 0) or 0)
    total_ss = float(year_summary.get("total_social_security_tax", 0) or 0)
    total_medicare = float(year_summary.get("total_medicare_tax", 0) or 0)
    total_state = float(year_summary.get("total_state_withholding", 0) or 0)
    total_local = float(year_summary.get("total_local_tax", 0) or 0)

    rows = ""
    for row in employee_summaries:
        employee_id = row.get("employee_id")
        employee_name = escape(str(row.get("employee_name") or "Unnamed Employee"))
        gross_pay = float(row.get("gross_pay", 0) or 0)
        federal_withholding = float(row.get("federal_withholding", 0) or 0)
        social_security_tax = float(row.get("social_security_tax", 0) or 0)
        medicare_tax = float(row.get("medicare_tax", 0) or 0)
        state_withholding = float(row.get("state_withholding", 0) or 0)
        local_tax = float(row.get("local_tax", 0) or 0)
        has_payroll_data = bool(row.get("has_payroll_data"))

        print_button = (
            f"<a class='btn secondary small' target='_blank' href='{url_for('settings.print_w2_summary', employee_id=employee_id, year=year)}'>Print</a>"
            if has_payroll_data and employee_id
            else "<span class='muted'>No data</span>"
        )

        rows += f"""
        <tr>
            <td>{employee_name}</td>
            <td>${gross_pay:,.2f}</td>
            <td>${federal_withholding:,.2f}</td>
            <td>${social_security_tax:,.2f}</td>
            <td>${medicare_tax:,.2f}</td>
            <td>${state_withholding:,.2f}</td>
            <td>${local_tax:,.2f}</td>
            <td>{print_button}</td>
        </tr>
        """

    if not rows:
        rows = """
        <tr>
            <td colspan="8" class="muted" style="text-align:center; padding:18px;">
                No employee W-2 data found for this year.
            </td>
        </tr>
        """

    if company_readiness.get("missing"):
        missing_html = "".join(
            f"<li>{escape(str(item))}</li>" for item in company_readiness["missing"]
        )
        readiness_card = f"""
        <div class='card' style='border:1px solid #f59e0b; background:#fffaf0;'>
            <div style='display:flex; justify-content:space-between; align-items:flex-start; gap:16px; flex-wrap:wrap;'>
                <div>
                    <h2 style='margin-bottom:8px;'>W-2 Filing Readiness</h2>
                    <ul style='margin:0;'>{missing_html}</ul>
                </div>
                <div>
                    <a class='btn warning' href='{url_for("settings.settings_w2_company")}'>Complete Profile</a>
                </div>
            </div>
        </div>
        """
    else:
        readiness_card = """
        <div class='card' style='border:1px solid #16a34a; background:#f0fdf4;'>
            <h2 style='margin-bottom:8px;'>W-2 Filing Readiness</h2>
            <p style='margin:0; color:#166534; font-weight:700;'>Ready</p>
        </div>
        """

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>W-2 Center</h1>
                <p class='muted' style='margin:0;'>Review yearly payroll totals and print employee W-2 summaries.</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("settings.settings")}' class='btn secondary'>Back</a>
            </div>
        </div>
    </div>

    {readiness_card}

    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:end; gap:12px; flex-wrap:wrap;'>
            <div>
                <h2 style='margin-bottom:6px;'>Year Summary</h2>
                <p class='muted' style='margin:0;'>Viewing tax year {escape(year)}</p>
            </div>

            <form method='get' style='display:flex; gap:10px; align-items:end; flex-wrap:wrap; margin:0;'>
                <div>
                    <label for='year' style='display:block; margin-bottom:6px;'>Year</label>
                    <input
                        id='year'
                        name='year'
                        value='{escape(year)}'
                        inputmode='numeric'
                        pattern='[0-9]*'
                        maxlength='4'
                        placeholder='2026'
                        style='min-width:110px;'
                    >
                </div>
                <div>
                    <button type='submit' class='btn secondary'>Load Year</button>
                </div>
            </form>
        </div>

        <div class='stats-grid' style='margin-top:18px; display:grid; grid-template-columns:repeat(auto-fit, minmax(180px, 1fr)); gap:12px;'>
            <div class='card' style='margin:0;'>
                <div class='muted'>Total Wages</div>
                <div style='font-size:1.2rem; font-weight:700;'>${total_wages:,.2f}</div>
            </div>
            <div class='card' style='margin:0;'>
                <div class='muted'>Federal</div>
                <div style='font-size:1.2rem; font-weight:700;'>${total_federal:,.2f}</div>
            </div>
            <div class='card' style='margin:0;'>
                <div class='muted'>Social Security</div>
                <div style='font-size:1.2rem; font-weight:700;'>${total_ss:,.2f}</div>
            </div>
            <div class='card' style='margin:0;'>
                <div class='muted'>Medicare</div>
                <div style='font-size:1.2rem; font-weight:700;'>${total_medicare:,.2f}</div>
            </div>
            <div class='card' style='margin:0;'>
                <div class='muted'>State</div>
                <div style='font-size:1.2rem; font-weight:700;'>${total_state:,.2f}</div>
            </div>
            <div class='card' style='margin:0;'>
                <div class='muted'>Local</div>
                <div style='font-size:1.2rem; font-weight:700;'>${total_local:,.2f}</div>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Employee W-2 Summary</h2>
        <div style='overflow-x:auto; width:100%;'>
            <table style='width:100%; min-width:900px;'>
                <thead>
                    <tr>
                        <th>Employee</th>
                        <th>Wages</th>
                        <th>Federal</th>
                        <th>SS</th>
                        <th>Medicare</th>
                        <th>State</th>
                        <th>Local</th>
                        <th>Print</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        </div>
    </div>
    """

    return render_page(content, "W-2 Center")

@settings_bp.route("/settings/w2/company", methods=["GET", "POST"])
@login_required
@settings_bp.route("/settings/w2")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def settings_w2():
    ensure_w2_company_profile_columns()

    cid = session["company_id"]

    year = (request.args.get("year") or str(datetime.utcnow().year)).strip()
    if not year.isdigit():
        year = str(datetime.utcnow().year)

    profile = get_company_profile(cid)
    values = get_company_profile_values(profile)
    company_readiness = get_company_w2_readiness(values)

    conn = get_db_connection()
    year_summary = get_company_w2_year_summary(conn, cid, year)
    employee_summaries = list_employee_w2_summaries(conn, cid, year)
    conn.close()

    total_wages = float(year_summary.get("total_wages", 0) or 0)
    total_federal = float(year_summary.get("total_federal_withholding", 0) or 0)
    total_ss = float(year_summary.get("total_social_security_tax", 0) or 0)
    total_medicare = float(year_summary.get("total_medicare_tax", 0) or 0)
    total_state = float(year_summary.get("total_state_withholding", 0) or 0)
    total_local = float(year_summary.get("total_local_tax", 0) or 0)

    rows = ""
    for row in employee_summaries:
        employee_id = row.get("employee_id")
        employee_name = escape(str(row.get("employee_name") or "Unnamed Employee"))
        gross_pay = float(row.get("gross_pay", 0) or 0)
        federal_withholding = float(row.get("federal_withholding", 0) or 0)
        social_security_tax = float(row.get("social_security_tax", 0) or 0)
        medicare_tax = float(row.get("medicare_tax", 0) or 0)
        state_withholding = float(row.get("state_withholding", 0) or 0)
        local_tax = float(row.get("local_tax", 0) or 0)
        has_payroll_data = bool(row.get("has_payroll_data"))

        print_button = (
            f"<a class='btn secondary small' target='_blank' href='{url_for('settings.print_w2_summary', employee_id=employee_id, year=year)}'>Print</a>"
            if has_payroll_data and employee_id
            else "<span class='muted'>No data</span>"
        )

        rows += f"""
        <tr>
            <td>{employee_name}</td>
            <td>${gross_pay:,.2f}</td>
            <td>${federal_withholding:,.2f}</td>
            <td>${social_security_tax:,.2f}</td>
            <td>${medicare_tax:,.2f}</td>
            <td>${state_withholding:,.2f}</td>
            <td>${local_tax:,.2f}</td>
            <td>{print_button}</td>
        </tr>
        """

    if not rows:
        rows = """
        <tr>
            <td colspan="8" class="muted" style="text-align:center; padding:18px;">
                No employee W-2 data found for this year.
            </td>
        </tr>
        """

    if company_readiness.get("missing"):
        missing_html = "".join(
            f"<li>{escape(str(item))}</li>" for item in company_readiness["missing"]
        )
        readiness_card = f"""
        <div class='card' style='border:1px solid #f59e0b; background:#fffaf0;'>
            <div style='display:flex; justify-content:space-between; align-items:flex-start; gap:16px; flex-wrap:wrap;'>
                <div>
                    <h2 style='margin-bottom:8px;'>W-2 Filing Readiness</h2>
                    <ul style='margin:0;'>{missing_html}</ul>
                </div>
                <div>
                    <a class='btn warning' href='{url_for("settings.settings_w2_company")}'>Complete Profile</a>
                </div>
            </div>
        </div>
        """
    else:
        readiness_card = """
        <div class='card' style='border:1px solid #16a34a; background:#f0fdf4;'>
            <h2 style='margin-bottom:8px;'>W-2 Filing Readiness</h2>
            <p style='margin:0; color:#166534; font-weight:700;'>Ready</p>
        </div>
        """

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>W-2 Center</h1>
                <p class='muted' style='margin:0;'>Review yearly payroll totals and print employee W-2 summaries.</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("settings.settings")}' class='btn secondary'>Back</a>
            </div>
        </div>
    </div>

    {readiness_card}

    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:end; gap:12px; flex-wrap:wrap;'>
            <div>
                <h2 style='margin-bottom:6px;'>Year Summary</h2>
                <p class='muted' style='margin:0;'>Viewing tax year {escape(year)}</p>
            </div>

            <form method='get' style='display:flex; gap:10px; align-items:end; flex-wrap:wrap; margin:0;'>
                <div>
                    <label for='year' style='display:block; margin-bottom:6px;'>Year</label>
                    <input
                        id='year'
                        name='year'
                        value='{escape(year)}'
                        inputmode='numeric'
                        pattern='[0-9]*'
                        maxlength='4'
                        placeholder='2026'
                        style='min-width:110px;'
                    >
                </div>
                <div class='row-actions'>
                    <button type='submit' class='btn secondary'>Load Year</button>
                    <a target='_blank' href='{url_for("settings.print_all_w2_summaries", year=year)}' class='btn success'>Print All</a>
                    <a href='{url_for("settings.settings_w2_company")}' class='btn secondary'>Company W-2 Profile</a>
                </div>
            </form>
        </div>

        <div class='stats-grid' style='margin-top:18px; display:grid; grid-template-columns:repeat(auto-fit, minmax(180px, 1fr)); gap:12px;'>
            <div class='card' style='margin:0;'>
                <div class='muted'>Total Wages</div>
                <div style='font-size:1.2rem; font-weight:700;'>${total_wages:,.2f}</div>
            </div>
            <div class='card' style='margin:0;'>
                <div class='muted'>Federal</div>
                <div style='font-size:1.2rem; font-weight:700;'>${total_federal:,.2f}</div>
            </div>
            <div class='card' style='margin:0;'>
                <div class='muted'>Social Security</div>
                <div style='font-size:1.2rem; font-weight:700;'>${total_ss:,.2f}</div>
            </div>
            <div class='card' style='margin:0;'>
                <div class='muted'>Medicare</div>
                <div style='font-size:1.2rem; font-weight:700;'>${total_medicare:,.2f}</div>
            </div>
            <div class='card' style='margin:0;'>
                <div class='muted'>State</div>
                <div style='font-size:1.2rem; font-weight:700;'>${total_state:,.2f}</div>
            </div>
            <div class='card' style='margin:0;'>
                <div class='muted'>Local</div>
                <div style='font-size:1.2rem; font-weight:700;'>${total_local:,.2f}</div>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Employee W-2 Summary</h2>
        <div style='overflow-x:auto; width:100%;'>
            <table style='width:100%; min-width:900px;'>
                <thead>
                    <tr>
                        <th>Employee</th>
                        <th>Wages</th>
                        <th>Federal</th>
                        <th>SS</th>
                        <th>Medicare</th>
                        <th>State</th>
                        <th>Local</th>
                        <th>Print</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        </div>
    </div>
    """

    return render_page(content, "W-2 Center")


@settings_bp.route("/settings/w2/company", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def settings_w2_company():
    ensure_company_profile_table()
    ensure_company_profile_columns()
    ensure_company_profile_location_columns()
    ensure_w2_company_profile_columns()

    cid = session["company_id"]
    conn = get_db_connection()

    existing = conn.execute(
        "SELECT * FROM company_profile WHERE company_id = %s",
        (cid,),
    ).fetchone()

    if request.method == "POST":
        values = get_company_profile_values(existing)

        legal_name = (request.form.get("legal_name") or "").strip()
        ein = (request.form.get("ein") or "").strip()
        state_employer_id = (request.form.get("state_employer_id") or "").strip()
        address_line_1 = (request.form.get("address_line_1") or "").strip()
        address_line_2 = (request.form.get("address_line_2") or "").strip()
        city = (request.form.get("city") or "").strip()
        state = (request.form.get("state") or "").strip().upper()
        zip_code = (request.form.get("zip_code") or "").strip()
        w2_contact_name = (request.form.get("w2_contact_name") or "").strip()
        w2_contact_phone = (request.form.get("w2_contact_phone") or "").strip()
        w2_contact_email = (request.form.get("w2_contact_email") or "").strip()

        if existing:
            conn.execute(
                """
                UPDATE company_profile
                SET display_name = %s,
                    legal_name = %s,
                    logo_url = %s,
                    phone = %s,
                    email = %s,
                    website = %s,
                    address_line_1 = %s,
                    address_line_2 = %s,
                    city = %s,
                    state = %s,
                    county = %s,
                    zip_code = %s,
                    invoice_header_name = %s,
                    quote_header_name = %s,
                    invoice_footer_note = %s,
                    quote_footer_note = %s,
                    email_from_name = %s,
                    reply_to_email = %s,
                    platform_sender_enabled = %s,
                    reply_to_mode = %s,
                    ein = %s,
                    state_employer_id = %s,
                    w2_contact_name = %s,
                    w2_contact_phone = %s,
                    w2_contact_email = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE company_id = %s
                """,
                (
                    values["display_name"],
                    legal_name,
                    values["logo_url"],
                    values["phone"],
                    values["email"],
                    values["website"],
                    address_line_1,
                    address_line_2,
                    city,
                    state,
                    values["county"],
                    zip_code,
                    values["invoice_header_name"],
                    values["quote_header_name"],
                    values["invoice_footer_note"],
                    values["quote_footer_note"],
                    values["email_from_name"],
                    values["reply_to_email"],
                    values["platform_sender_enabled"],
                    values["reply_to_mode"],
                    ein,
                    state_employer_id,
                    w2_contact_name,
                    w2_contact_phone,
                    w2_contact_email,
                    cid,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO company_profile (
                    company_id,
                    display_name,
                    legal_name,
                    logo_url,
                    phone,
                    email,
                    website,
                    address_line_1,
                    address_line_2,
                    city,
                    state,
                    county,
                    zip_code,
                    invoice_header_name,
                    quote_header_name,
                    invoice_footer_note,
                    quote_footer_note,
                    email_from_name,
                    reply_to_email,
                    platform_sender_enabled,
                    reply_to_mode,
                    ein,
                    state_employer_id,
                    w2_contact_name,
                    w2_contact_phone,
                    w2_contact_email
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    cid,
                    values["display_name"],
                    legal_name,
                    values["logo_url"],
                    values["phone"],
                    values["email"],
                    values["website"],
                    address_line_1,
                    address_line_2,
                    city,
                    state,
                    values["county"],
                    zip_code,
                    values["invoice_header_name"],
                    values["quote_header_name"],
                    values["invoice_footer_note"],
                    values["quote_footer_note"],
                    values["email_from_name"],
                    values["reply_to_email"],
                    values["platform_sender_enabled"],
                    values["reply_to_mode"],
                    ein,
                    state_employer_id,
                    w2_contact_name,
                    w2_contact_phone,
                    w2_contact_email,
                ),
            )

        conn.commit()
        conn.close()
        flash("Company W-2 profile saved.")
        return redirect(url_for("settings.settings_w2_company"))

    conn.close()

    values = get_company_profile_values(existing)
    readiness = _w2_company_readiness(values)
    csrf_token = generate_csrf()

    missing_html = ""
    if readiness["missing"]:
        missing_html = "".join(f"<li>{escape(item)}</li>" for item in readiness["missing"])
        readiness_block = f"""
        <div class='card' style='border:1px solid #f59e0b; background:#fffaf0;'>
            <h2>Missing Items</h2>
            <ul>{missing_html}</ul>
        </div>
        """
    else:
        readiness_block = """
        <div class='card' style='border:1px solid #16a34a; background:#f0fdf4;'>
            <h2>Ready</h2>
            <p style='margin:0; color:#166534; font-weight:700;'>Company W-2 profile looks complete.</p>
        </div>
        """

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Company W-2 Profile</h1>
                <p class='muted' style='margin:0;'>Store the company filing details needed for year-end W-2 preparation.</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("settings.settings_w2")}' class='btn secondary'>Back to W-2 Center</a>
            </div>
        </div>
    </div>

    {readiness_block}

    <div class='card'>
        <h2>W-2 Filing Details</h2>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{csrf_token}">
            <div class='grid'>
                <div>
                    <label>Legal Business Name</label>
                    <input name='legal_name' value='{escape(values["legal_name"])}' placeholder='Your legal business name'>
                </div>

                <div>
                    <label>EIN</label>
                    <input name='ein' value='{escape(values["ein"])}' placeholder='12-3456789'>
                </div>

                <div>
                    <label>State Employer ID</label>
                    <input name='state_employer_id' value='{escape(values["state_employer_id"])}' placeholder='State employer account ID'>
                </div>

                <div>
                    <label>Address Line 1</label>
                    <input name='address_line_1' value='{escape(values["address_line_1"])}' placeholder='Street address'>
                </div>

                <div>
                    <label>Address Line 2</label>
                    <input name='address_line_2' value='{escape(values["address_line_2"])}' placeholder='Suite / unit / additional details'>
                </div>

                <div>
                    <label>City</label>
                    <input name='city' value='{escape(values["city"])}' placeholder='City'>
                </div>

                <div>
                    <label>State</label>
                    <input name='state' value='{escape(values["state"])}' placeholder='IN' maxlength='2'>
                </div>

                <div>
                    <label>ZIP Code</label>
                    <input name='zip_code' value='{escape(values["zip_code"])}' placeholder='47905'>
                </div>

                <div>
                    <label>W-2 Contact Name</label>
                    <input name='w2_contact_name' value='{escape(values["w2_contact_name"])}' placeholder='Contact person for W-2 filing'>
                </div>

                <div>
                    <label>W-2 Contact Phone</label>
                    <input name='w2_contact_phone' value='{escape(values["w2_contact_phone"])}' placeholder='(765) 555-1234'>
                </div>

                <div>
                    <label>W-2 Contact Email</label>
                    <input name='w2_contact_email' value='{escape(values["w2_contact_email"])}' placeholder='payroll@yourcompany.com'>
                </div>
            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>Save Company W-2 Profile</button>
            </div>
        </form>
    </div>
    """

    return render_page(content, "Company W-2 Profile")


@settings_bp.route("/settings/w2/<int:employee_id>/print")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def print_w2_summary(employee_id):
    cid = session["company_id"]

    year = (request.args.get("year") or str(datetime.utcnow().year)).strip()
    if not year.isdigit():
        year = str(datetime.utcnow().year)

    conn = get_db_connection()

    employee = conn.execute(
        """
        SELECT id, first_name, last_name, full_name
        FROM employees
        WHERE id = %s AND company_id = %s
        """,
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        flash("Employee not found.")
        return redirect(url_for("settings.settings_w2", year=year))

    summary = conn.execute(
        """
        SELECT
            COALESCE(SUM(gross_pay), 0) AS wages,
            COALESCE(SUM(federal_withholding), 0) AS federal_withholding,
            COALESCE(SUM(social_security), 0) AS social_security,
            COALESCE(SUM(medicare), 0) AS medicare,
            COALESCE(SUM(state_withholding), 0) AS state_withholding,
            COALESCE(SUM(local_tax), 0) AS local_tax
        FROM payroll_entries
        WHERE company_id = %s
          AND employee_id = %s
          AND pay_date IS NOT NULL
          AND NULLIF(pay_date::text, '') IS NOT NULL
          AND EXTRACT(YEAR FROM NULLIF(pay_date::text, '')::date) = %s
        """,
        (cid, employee_id, int(year)),
    ).fetchone()

    conn.close()

    employee_name = (
        (employee["full_name"] or "").strip()
        or f"{(employee['first_name'] or '').strip()} {(employee['last_name'] or '').strip()}".strip()
        or f"Employee #{employee_id}"
    )

    summary_data = {
        "wages": float((summary or {}).get("wages", 0) or 0),
        "federal_withholding": float((summary or {}).get("federal_withholding", 0) or 0),
        "social_security": float((summary or {}).get("social_security", 0) or 0),
        "medicare": float((summary or {}).get("medicare", 0) or 0),
        "state_withholding": float((summary or {}).get("state_withholding", 0) or 0),
        "local_tax": float((summary or {}).get("local_tax", 0) or 0),
    }

    company_name = _company_display_name_for_reports(cid)
    pdf_data = _build_w2_summary_pdf(
        company_name=company_name,
        tax_year=year,
        employee_name=employee_name,
        summary=summary_data,
    )

    safe_employee_name = "".join(
        ch if ch.isalnum() or ch in ("-", "_") else "_"
        for ch in employee_name.replace(" ", "_")
    ).strip("_") or f"employee_{employee_id}"

    response = make_response(pdf_data)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = (
        f"inline; filename=w2_{safe_employee_name}_{year}.pdf"
    )
    return response


@settings_bp.route("/settings/w2/print-all")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def print_all_w2_summaries():
    cid = session["company_id"]

    year = (request.args.get("year") or str(datetime.utcnow().year)).strip()
    if not year.isdigit():
        year = str(datetime.utcnow().year)

    conn = get_db_connection()

    employees = conn.execute(
        """
        SELECT id, first_name, last_name, full_name
        FROM employees
        WHERE company_id = %s
        ORDER BY first_name, last_name, id
        """,
        (cid,),
    ).fetchall()

    payroll = conn.execute(
        """
        SELECT
            employee_id,
            COALESCE(SUM(gross_pay), 0) AS wages,
            COALESCE(SUM(federal_withholding), 0) AS federal_withholding,
            COALESCE(SUM(social_security), 0) AS social_security,
            COALESCE(SUM(medicare), 0) AS medicare,
            COALESCE(SUM(state_withholding), 0) AS state_withholding,
            COALESCE(SUM(local_tax), 0) AS local_tax
        FROM payroll_entries
        WHERE company_id = %s
          AND pay_date IS NOT NULL
          AND NULLIF(pay_date::text, '') IS NOT NULL
          AND EXTRACT(YEAR FROM NULLIF(pay_date::text, '')::date) = %s
        GROUP BY employee_id
        """,
        (cid, int(year)),
    ).fetchall()

    conn.close()

    payroll_map = {row["employee_id"]: row for row in payroll}
    employee_rows = []

    for e in employees:
        summary = payroll_map.get(e["id"])
        if not summary:
            continue

        employee_name = (
            (e["full_name"] or "").strip()
            or f"{(e['first_name'] or '').strip()} {(e['last_name'] or '').strip()}".strip()
            or f"Employee #{e['id']}"
        )

        employee_rows.append({
            "employee_name": employee_name,
            "wages": float(summary["wages"] or 0),
            "federal_withholding": float(summary["federal_withholding"] or 0),
            "social_security": float(summary["social_security"] or 0),
            "medicare": float(summary["medicare"] or 0),
            "state_withholding": float(summary["state_withholding"] or 0),
            "local_tax": float(summary["local_tax"] or 0),
        })

    company_name = _company_display_name_for_reports(cid)
    pdf_data = _build_w2_all_summary_pdf(company_name, year, employee_rows)

    response = make_response(pdf_data)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f"inline; filename=w2_summary_all_{year}.pdf"
    return response


@settings_bp.route("/settings/logo")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def settings_logo():
    return redirect(url_for("settings.settings_branding"))


@settings_bp.route("/settings/company-profile")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def settings_company_profile():
    return redirect(url_for("settings.settings_branding"))


@settings_bp.route("/settings/branding", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def settings_branding():
    ensure_company_profile_table()
    ensure_company_profile_columns()
    ensure_company_profile_location_columns()
    ensure_w2_company_profile_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    existing = conn.execute(
        "SELECT * FROM company_profile WHERE company_id = %s",
        (cid,),
    ).fetchone()

    if request.method == "POST":
        display_name = (request.form.get("display_name") or "").strip()
        legal_name = (request.form.get("legal_name") or "").strip()
        logo_url = (request.form.get("logo_url") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        email = (request.form.get("email") or "").strip()
        website = (request.form.get("website") or "").strip()
        address_line_1 = (request.form.get("address_line_1") or "").strip()
        address_line_2 = (request.form.get("address_line_2") or "").strip()
        city = (request.form.get("city") or "").strip()
        state = (request.form.get("state") or "").strip().upper()
        county = (request.form.get("county") or "").strip()
        zip_code = (request.form.get("zip_code") or "").strip()
        invoice_header_name = (request.form.get("invoice_header_name") or "").strip()
        quote_header_name = (request.form.get("quote_header_name") or "").strip()
        invoice_footer_note = (request.form.get("invoice_footer_note") or "").strip()
        quote_footer_note = (request.form.get("quote_footer_note") or "").strip()
        remove_logo = (request.form.get("remove_logo") or "").strip() == "1"

        current_logo = existing["logo_url"] if existing and existing["logo_url"] else ""
        email_from_name = existing["email_from_name"] if existing and existing["email_from_name"] else ""
        reply_to_email = existing["reply_to_email"] if existing and existing["reply_to_email"] else ""
        platform_sender_enabled = int(existing["platform_sender_enabled"] or 1) if existing else 1
        reply_to_mode = existing["reply_to_mode"] if existing and existing["reply_to_mode"] else "company"
        ein = existing["ein"] if existing and "ein" in existing.keys() else ""
        state_employer_id = existing["state_employer_id"] if existing and "state_employer_id" in existing.keys() else ""
        w2_contact_name = existing["w2_contact_name"] if existing and "w2_contact_name" in existing.keys() else ""
        w2_contact_phone = existing["w2_contact_phone"] if existing and "w2_contact_phone" in existing.keys() else ""
        w2_contact_email = existing["w2_contact_email"] if existing and "w2_contact_email" in existing.keys() else ""

        uploaded_file = request.files.get("logo_file")

        if remove_logo:
            logo_url = ""
        elif uploaded_file and uploaded_file.filename:
            if not allowed_logo_file(uploaded_file.filename):
                conn.close()
                flash("Invalid logo file type. Please upload PNG, JPG, JPEG, GIF, WEBP, or SVG.")
                return redirect(url_for("settings.settings_branding"))

            upload_folder = ensure_logo_upload_folder()
            original_name = secure_filename(uploaded_file.filename)
            ext = original_name.rsplit(".", 1)[1].lower()
            new_filename = f"company_{cid}_{uuid.uuid4().hex}.{ext}"
            absolute_path = os.path.join(upload_folder, new_filename)

            uploaded_file.save(absolute_path)
            logo_url = f"/static/uploads/company_logos/{new_filename}"
        elif not logo_url:
            logo_url = current_logo

        if existing:
            conn.execute(
                """
                UPDATE company_profile
                SET display_name = %s,
                    legal_name = %s,
                    logo_url = %s,
                    phone = %s,
                    email = %s,
                    website = %s,
                    address_line_1 = %s,
                    address_line_2 = %s,
                    city = %s,
                    state = %s,
                    county = %s,
                    zip_code = %s,
                    invoice_header_name = %s,
                    quote_header_name = %s,
                    invoice_footer_note = %s,
                    quote_footer_note = %s,
                    email_from_name = %s,
                    reply_to_email = %s,
                    platform_sender_enabled = %s,
                    reply_to_mode = %s,
                    ein = %s,
                    state_employer_id = %s,
                    w2_contact_name = %s,
                    w2_contact_phone = %s,
                    w2_contact_email = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE company_id = %s
                """,
                (
                    display_name,
                    legal_name,
                    logo_url,
                    phone,
                    email,
                    website,
                    address_line_1,
                    address_line_2,
                    city,
                    state,
                    county,
                    zip_code,
                    invoice_header_name,
                    quote_header_name,
                    invoice_footer_note,
                    quote_footer_note,
                    email_from_name,
                    reply_to_email,
                    platform_sender_enabled,
                    reply_to_mode,
                    ein,
                    state_employer_id,
                    w2_contact_name,
                    w2_contact_phone,
                    w2_contact_email,
                    cid,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO company_profile (
                    company_id,
                    display_name,
                    legal_name,
                    logo_url,
                    phone,
                    email,
                    website,
                    address_line_1,
                    address_line_2,
                    city,
                    state,
                    county,
                    zip_code,
                    invoice_header_name,
                    quote_header_name,
                    invoice_footer_note,
                    quote_footer_note,
                    email_from_name,
                    reply_to_email,
                    platform_sender_enabled,
                    reply_to_mode,
                    ein,
                    state_employer_id,
                    w2_contact_name,
                    w2_contact_phone,
                    w2_contact_email
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    cid,
                    display_name,
                    legal_name,
                    logo_url,
                    phone,
                    email,
                    website,
                    address_line_1,
                    address_line_2,
                    city,
                    state,
                    county,
                    zip_code,
                    invoice_header_name,
                    quote_header_name,
                    invoice_footer_note,
                    quote_footer_note,
                    email_from_name,
                    reply_to_email,
                    platform_sender_enabled,
                    reply_to_mode,
                    ein,
                    state_employer_id,
                    w2_contact_name,
                    w2_contact_phone,
                    w2_contact_email,
                ),
            )

        conn.commit()
        conn.close()
        flash("Branding saved.")
        return redirect(url_for("settings.settings_branding"))

    conn.close()

    values = get_company_profile_values(existing)

    company_logo_preview = (
        f"<img src='{escape(values['logo_url'])}' alt='Company Logo Preview' style='max-height:84px; max-width:240px; object-fit:contain; border-radius:10px;'>"
        if values["logo_url"]
        else f"<div style='width:84px; height:84px; border-radius:18px; background:#334155; color:#fff; display:flex; align-items:center; justify-content:center; font-size:1.25rem; font-weight:800;'>{escape((values['display_name'][:2] or 'CP').upper())}</div>"
    )

    address_preview_parts = [
        values["address_line_1"],
        values["address_line_2"],
        f"{values['city']}, {values['state']} {values['zip_code']}".strip(" ,"),
        values["county"],
    ]
    address_preview = "<br>".join(escape(part) for part in address_preview_parts if part)

    contact_lines = "<br>".join(
        filter(
            None,
            [
                escape(values["phone"]) if values["phone"] else "",
                escape(values["email"]) if values["email"] else "",
                escape(values["website"]) if values["website"] else "",
            ],
        )
    )

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Branding</h1>
                <p class='muted' style='margin:0;'>Manage your logo, company branding, and document branding.</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("settings.settings")}' class='btn secondary'>Back to Settings</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Company Document Branding Preview</h2>
        <div style='display:grid; grid-template-columns:110px 1fr; gap:20px; align-items:center;'>
            <div>{company_logo_preview}</div>
            <div>
                <div style='font-size:1.35rem; font-weight:800; color:#334155;'>{escape(values["display_name"] or "Your Company Name")}</div>
                <div style='margin-top:8px; color:#555;'>{escape(values["legal_name"]) if values["legal_name"] else ''}</div>
                <div style='margin-top:10px; color:#666; line-height:1.55;'>
                    {address_preview if address_preview else "<span class='muted'>No business address set yet.</span>"}
                </div>
                <div style='margin-top:10px; color:#666; line-height:1.55;'>
                    {contact_lines if contact_lines else "<span class='muted'>No phone, email, or website set yet.</span>"}
                </div>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Branding Details</h2>
        <form method='post' enctype='multipart/form-data'>
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <div style='display:grid; grid-template-columns:repeat(auto-fit, minmax(240px, 1fr)); gap:16px;'>

                <div>
                    <label>Display Name</label>
                    <input name='display_name' value='{escape(values["display_name"])}' placeholder='Wrede & Sons Lafayette'>
                </div>

                <div>
                    <label>Legal Business Name</label>
                    <input name='legal_name' value='{escape(values["legal_name"])}' placeholder='Wrede & Sons of Lafayette, Inc.'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>Upload Company Logo</label>
                    <input type='file' name='logo_file' accept='.png,.jpg,.jpeg,.gif,.webp,.svg'>
                    <div class='muted' style='margin-top:6px;'>Upload a company logo from your computer.</div>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>Or Use Company Logo URL</label>
                    <input name='logo_url' value='{escape(values["logo_url"])}' placeholder='https://yourdomain.com/logo.png'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label style='display:flex; align-items:center; gap:8px;'>
                        <input type='checkbox' name='remove_logo' value='1'>
                        Remove current company logo
                    </label>
                </div>

                <div>
                    <label>Phone</label>
                    <input name='phone' value='{escape(values["phone"])}' placeholder='(765) 555-1234'>
                </div>

                <div>
                    <label>Email</label>
                    <input name='email' value='{escape(values["email"])}' placeholder='office@yourcompany.com'>
                </div>

                <div>
                    <label>Website</label>
                    <input name='website' value='{escape(values["website"])}' placeholder='https://yourcompany.com'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>Address Line 1</label>
                    <input name='address_line_1' value='{escape(values["address_line_1"])}' placeholder='123 Main Street'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>Address Line 2</label>
                    <input name='address_line_2' value='{escape(values["address_line_2"])}' placeholder='Suite, building, or additional details'>
                </div>

                <div>
                    <label>City</label>
                    <input name='city' value='{escape(values["city"])}' placeholder='Lafayette'>
                </div>

                <div>
                    <label>State</label>
                    <input name='state' value='{escape(values["state"])}' placeholder='IN' maxlength='2'>
                </div>

                <div>
                    <label>County</label>
                    <input name='county' value='{escape(values["county"])}' placeholder='Tippecanoe'>
                </div>

                <div>
                    <label>ZIP Code</label>
                    <input name='zip_code' value='{escape(values["zip_code"])}' placeholder='47905'>
                </div>

                <div>
                    <label>Invoice Header Name</label>
                    <input name='invoice_header_name' value='{escape(values["invoice_header_name"])}' placeholder='Name shown at top of invoices'>
                </div>

                <div>
                    <label>Quote Header Name</label>
                    <input name='quote_header_name' value='{escape(values["quote_header_name"])}' placeholder='Name shown at top of quotes'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>Invoice Footer Note</label>
                    <textarea name='invoice_footer_note' placeholder='Thank you for your business.'>{escape(values["invoice_footer_note"])}</textarea>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>Quote Footer Note</label>
                    <textarea name='quote_footer_note' placeholder='Pricing valid for 30 days unless otherwise stated.'>{escape(values["quote_footer_note"])}</textarea>
                </div>

            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>Save Branding</button>
            </div>
        </form>
    </div>
    """

    return render_page(content, "Branding")


@settings_bp.route("/settings/email", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def settings_email():
    ensure_company_profile_table()
    ensure_company_profile_columns()
    ensure_company_profile_location_columns()
    ensure_w2_company_profile_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    existing = conn.execute(
        "SELECT * FROM company_profile WHERE company_id = %s",
        (cid,),
    ).fetchone()

    if request.method == "POST":
        values = get_company_profile_values(existing)

        email_from_name = (request.form.get("email_from_name") or "").strip()
        reply_to_email = (request.form.get("reply_to_email") or "").strip()
        platform_sender_enabled = 1 if (request.form.get("platform_sender_enabled") or "1") == "1" else 0
        reply_to_mode = (request.form.get("reply_to_mode") or "company").strip()

        if existing:
            conn.execute(
                """
                UPDATE company_profile
                SET display_name = %s,
                    legal_name = %s,
                    logo_url = %s,
                    phone = %s,
                    email = %s,
                    website = %s,
                    address_line_1 = %s,
                    address_line_2 = %s,
                    city = %s,
                    state = %s,
                    county = %s,
                    zip_code = %s,
                    invoice_header_name = %s,
                    quote_header_name = %s,
                    invoice_footer_note = %s,
                    quote_footer_note = %s,
                    email_from_name = %s,
                    reply_to_email = %s,
                    platform_sender_enabled = %s,
                    reply_to_mode = %s,
                    ein = %s,
                    state_employer_id = %s,
                    w2_contact_name = %s,
                    w2_contact_phone = %s,
                    w2_contact_email = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE company_id = %s
                """,
                (
                    values["display_name"],
                    values["legal_name"],
                    values["logo_url"],
                    values["phone"],
                    values["email"],
                    values["website"],
                    values["address_line_1"],
                    values["address_line_2"],
                    values["city"],
                    values["state"],
                    values["county"],
                    values["zip_code"],
                    values["invoice_header_name"],
                    values["quote_header_name"],
                    values["invoice_footer_note"],
                    values["quote_footer_note"],
                    email_from_name,
                    reply_to_email,
                    platform_sender_enabled,
                    reply_to_mode,
                    values["ein"],
                    values["state_employer_id"],
                    values["w2_contact_name"],
                    values["w2_contact_phone"],
                    values["w2_contact_email"],
                    cid,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO company_profile (
                    company_id,
                    display_name,
                    legal_name,
                    logo_url,
                    phone,
                    email,
                    website,
                    address_line_1,
                    address_line_2,
                    city,
                    state,
                    county,
                    zip_code,
                    invoice_header_name,
                    quote_header_name,
                    invoice_footer_note,
                    quote_footer_note,
                    email_from_name,
                    reply_to_email,
                    platform_sender_enabled,
                    reply_to_mode,
                    ein,
                    state_employer_id,
                    w2_contact_name,
                    w2_contact_phone,
                    w2_contact_email
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    cid,
                    values["display_name"],
                    values["legal_name"],
                    values["logo_url"],
                    values["phone"],
                    values["email"],
                    values["website"],
                    values["address_line_1"],
                    values["address_line_2"],
                    values["city"],
                    values["state"],
                    values["county"],
                    values["zip_code"],
                    values["invoice_header_name"],
                    values["quote_header_name"],
                    values["invoice_footer_note"],
                    values["quote_footer_note"],
                    email_from_name,
                    reply_to_email,
                    platform_sender_enabled,
                    reply_to_mode,
                    values["ein"],
                    values["state_employer_id"],
                    values["w2_contact_name"],
                    values["w2_contact_phone"],
                    values["w2_contact_email"],
                ),
            )

        conn.commit()
        conn.close()
        flash("Email settings saved.")
        return redirect(url_for("settings.settings_email"))

    conn.close()

    values = get_company_profile_values(existing)

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Email Settings</h1>
                <p class='muted' style='margin:0;'>Manage how quote and invoice emails appear to your customers.</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("settings.settings")}' class='btn secondary'>Back to Settings</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Email Delivery Identity</h2>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <div style='display:grid; grid-template-columns:repeat(auto-fit, minmax(240px, 1fr)); gap:16px;'>

                <div>
                    <label>Email From Name</label>
                    <input name='email_from_name' value='{escape(values["email_from_name"])}' placeholder='Wrede & Sons Lafayette'>
                    <div class='muted' style='margin-top:6px;'>This is the company name customers will recognize when emails are sent.</div>
                </div>

                <div>
                    <label>Reply-To Email</label>
                    <input name='reply_to_email' value='{escape(values["reply_to_email"])}' placeholder='sales@yourcompany.com'>
                    <div class='muted' style='margin-top:6px;'>When customers reply to emailed quotes or invoices, replies go here.</div>
                </div>

                <div>
                    <label>Platform Email Sending</label>
                    <select name='platform_sender_enabled'>
                        <option value='1' {"selected" if values["platform_sender_enabled"] == 1 else ""}>Enabled</option>
                        <option value='0' {"selected" if values["platform_sender_enabled"] == 0 else ""}>Disabled</option>
                    </select>
                    <div class='muted' style='margin-top:6px;'>Uses TerraLedger's sending mailbox while keeping your company reply-to address.</div>
                </div>

                <div>
                    <label>Reply-To Behavior</label>
                    <select name='reply_to_mode'>
                        <option value='company' {"selected" if values["reply_to_mode"] == "company" else ""}>Company Email</option>
                        <option value='logged_in_user' {"selected" if values["reply_to_mode"] == "logged_in_user" else ""}>Logged-In User</option>
                    </select>
                    <div class='muted' style='margin-top:6px;'>Choose whether replies go to the company email or the user who sent the email.</div>
                </div>

            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>Save Email Settings</button>
            </div>
        </form>
    </div>

    <div class='card'>
        <h2>Send Test Email</h2>
        <p class='muted'>Use this to confirm your platform email sending is working before testing quote or invoice emails.</p>

        <form method='post' action='{url_for("settings.test_email")}'>
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <div class='grid'>
                <div>
                    <label>Send Test To</label>
                    <input type='email' name='test_email' placeholder='you@example.com' required>
                </div>
            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn' type='submit'>Send Test Email</button>
            </div>
        </form>
    </div>
    """

    return render_page(content, "Email Settings")


@settings_bp.route("/settings/test_email", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def test_email():
    ensure_company_profile_table()
    ensure_w2_company_profile_columns()

    cid = session.get("company_id")
    if not cid:
        flash("No company is associated with this account.")
        return redirect(url_for("settings.settings"))

    test_email_address = (request.form.get("test_email") or "").strip()

    if not test_email_address:
        flash("Please enter a test email address.")
        return redirect(url_for("settings.settings_email"))

    try:
        send_company_email(
            company_id=cid,
            to_email=test_email_address,
            subject="TerraLedger Test Email",
            body=(
                "This is a TerraLedger test email.\n\n"
                "If you received this message, your company email settings and platform sender are working."
            ),
            user_id=session.get("user_id"),
        )
        flash("Test email sent successfully.")
    except Exception as e:
        flash(f"Test email failed: {e}")

    return redirect(url_for("settings.settings_email"))


@settings_bp.route("/settings/backup/download")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def download_backup():
    cid = session["company_id"]

    data = export_company_backup_data(cid)

    filename = f"terraledger_backup_{cid}_{datetime.utcnow().strftime('%Y_%m_%d')}.json"

    response = make_response(json.dumps(data, indent=2, default=str))
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers["Content-Type"] = "application/json"

    return response


@settings_bp.route("/settings/backup/restore", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def restore_backup():
    if request.method == "POST":
        cid = session["company_id"]
        uploaded_file = request.files.get("backup_file")

        if not uploaded_file or not uploaded_file.filename:
            flash("Please choose a backup file.")
            return redirect(url_for("settings.restore_backup"))

        if not uploaded_file.filename.lower().endswith(".json"):
            flash("Please upload a valid JSON backup file.")
            return redirect(url_for("settings.restore_backup"))

        try:
            backup_data = load_backup_file(uploaded_file)
            result = restore_company_backup(cid, backup_data)

            flash(
                "Backup restored successfully. "
                f"A pre-restore backup was also saved locally at: {result['pre_restore_backup_path']}"
            )
            return redirect(url_for("settings.settings"))

        except Exception as e:
            flash(f"Restore failed: {e}")
            return redirect(url_for("settings.restore_backup"))

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Restore Backup</h1>
                <p class='muted' style='margin:0;'>
                    Upload a TerraLedger backup file to replace your current company data.
                </p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("settings.settings")}' class='btn secondary'>Back to Settings</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Important</h2>
        <p class='muted'>
            Restoring a backup will replace your current company data. TerraLedger will create a local
            pre-restore backup automatically before the restore begins.
        </p>

        <form method='post' enctype='multipart/form-data'>
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <div class='grid'>
                <div>
                    <label>Backup File (.json)</label>
                    <input type='file' name='backup_file' accept='.json' required>
                </div>
            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn warning' type='submit'>Restore Backup</button>
            </div>
        </form>
    </div>
    """

    return render_page(content, "Restore Backup")