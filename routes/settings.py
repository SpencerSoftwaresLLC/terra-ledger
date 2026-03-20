import os
import uuid

from flask import Blueprint, request, redirect, url_for, session, flash, render_template_string
from markupsafe import escape
from werkzeug.utils import secure_filename

from db import (
    get_db_connection,
    ensure_company_profile_columns,
    ensure_company_tax_settings_table,
    ensure_company_profile_location_columns,
)
from decorators import login_required, require_permission
from page_helpers import render_page
from utils.emailing import send_company_email

settings_bp = Blueprint("settings", __name__)

ALLOWED_LOGO_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp", "svg"}

def allowed_logo_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_LOGO_EXTENSIONS


def ensure_logo_upload_folder():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    upload_folder = os.path.join(base_dir, "static", "uploads", "company_logos")
    os.makedirs(upload_folder, exist_ok=True)
    return upload_folder


def ensure_company_profile_table():
    conn = get_db_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_profile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL UNIQUE,
            display_name TEXT,
            legal_name TEXT,
            logo_url TEXT,
            phone TEXT,
            email TEXT,
            website TEXT,
            address_line_1 TEXT,
            address_line_2 TEXT,
            city TEXT,
            state TEXT,
            county TEXT,
            zip_code TEXT,
            invoice_header_name TEXT,
            quote_header_name TEXT,
            invoice_footer_note TEXT,
            quote_footer_note TEXT,
            email_from_name TEXT,
            reply_to_email TEXT,
            platform_sender_enabled INTEGER NOT NULL DEFAULT 1,
            reply_to_mode TEXT DEFAULT 'company',
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(company_profile)")
    cols = [row[1] for row in cur.fetchall()]

    needed_columns = {
        "phone": "TEXT",
        "email": "TEXT",
        "website": "TEXT",
        "address_line_1": "TEXT",
        "address_line_2": "TEXT",
        "city": "TEXT",
        "state": "TEXT",
        "county": "TEXT",
        "zip_code": "TEXT",
        "invoice_header_name": "TEXT",
        "quote_header_name": "TEXT",
        "invoice_footer_note": "TEXT",
        "quote_footer_note": "TEXT",
        "email_from_name": "TEXT",
        "reply_to_email": "TEXT",
        "platform_sender_enabled": "INTEGER NOT NULL DEFAULT 1",
        "reply_to_mode": "TEXT DEFAULT 'company'",
        "updated_at": "TEXT DEFAULT CURRENT_TIMESTAMP",
    }

    for col, col_type in needed_columns.items():
        if col not in cols:
            cur.execute(f"ALTER TABLE company_profile ADD COLUMN {col} {col_type}")

    conn.commit()
    conn.close()


def get_company_profile(cid):
    ensure_company_profile_table()
    ensure_company_profile_columns()
    ensure_company_profile_location_columns()

    conn = get_db_connection()
    profile = conn.execute(
        "SELECT * FROM company_profile WHERE company_id = ?",
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
    }


@settings_bp.route("/settings")
@login_required
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

        </div>
    </div>
    """
    return render_page(settings_html, "Settings")


@settings_bp.route("/settings/company", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_settings")
def settings_company():
    ensure_company_profile_columns()
    ensure_company_profile_location_columns()
    ensure_company_profile_table()

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
            SET name = ?,
                phone = ?,
                email = ?,
                website = ?,
                tax_id = ?,
                address_line_1 = ?,
                address_line_2 = ?,
                city = ?,
                state = ?,
                zip_code = ?
            WHERE id = ?
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
            "SELECT id FROM company_profile WHERE company_id = ?",
            (company_id,),
        ).fetchone()

        if existing_profile:
            conn.execute(
                """
                UPDATE company_profile
                SET display_name = ?,
                    phone = ?,
                    email = ?,
                    website = ?,
                    address_line_1 = ?,
                    address_line_2 = ?,
                    city = ?,
                    state = ?,
                    county = ?,
                    zip_code = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE company_id = ?
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        WHERE id = ?
        """,
        (company_id,),
    ).fetchone()

    profile = conn.execute(
        """
        SELECT county
        FROM company_profile
        WHERE company_id = ?
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
            "SELECT id FROM company_tax_settings WHERE company_id = ?",
            (cid,),
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE company_tax_settings
                SET federal_withholding_rate = ?,
                    state_withholding_rate = ?,
                    social_security_rate = ?,
                    medicare_rate = ?,
                    local_tax_rate = ?,
                    unemployment_rate = ?,
                    workers_comp_rate = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE company_id = ?
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
        "SELECT * FROM company_tax_settings WHERE company_id = ?",
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
            <div style='display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr)); gap:16px; align-items:end;'>

                <div>
                    <label>Federal Withholding %</label>
                    <input type='number' step='0.01' min='0' name='federal_withholding_rate'
                           value='{float(settings["federal_withholding_rate"]) if settings and settings["federal_withholding_rate"] is not None else 0:.2f}'>
                </div>

                <div>
                    <label>State Withholding %</label>
                    <input type='number' step='0.01' min='0' name='state_withholding_rate'
                           value='{float(settings["state_withholding_rate"]) if settings and settings["state_withholding_rate"] is not None else 0:.2f}'>
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


@settings_bp.route("/settings/logo")
@login_required
@require_permission("can_manage_settings")
def settings_logo():
    return redirect(url_for("settings.settings_branding"))


@settings_bp.route("/settings/company-profile")
@login_required
@require_permission("can_manage_settings")
def settings_company_profile():
    return redirect(url_for("settings.settings_branding"))


@settings_bp.route("/settings/branding", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_settings")
def settings_branding():
    ensure_company_profile_table()
    ensure_company_profile_columns()
    ensure_company_profile_location_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    existing = conn.execute(
        "SELECT * FROM company_profile WHERE company_id = ?",
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
                SET display_name = ?,
                    legal_name = ?,
                    logo_url = ?,
                    phone = ?,
                    email = ?,
                    website = ?,
                    address_line_1 = ?,
                    address_line_2 = ?,
                    city = ?,
                    state = ?,
                    county = ?,
                    zip_code = ?,
                    invoice_header_name = ?,
                    quote_header_name = ?,
                    invoice_footer_note = ?,
                    quote_footer_note = ?,
                    email_from_name = ?,
                    reply_to_email = ?,
                    platform_sender_enabled = ?,
                    reply_to_mode = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE company_id = ?
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
                    reply_to_mode
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
@require_permission("can_manage_settings")
def settings_email():
    ensure_company_profile_table()
    ensure_company_profile_columns()
    ensure_company_profile_location_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    existing = conn.execute(
        "SELECT * FROM company_profile WHERE company_id = ?",
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
                SET display_name = ?,
                    legal_name = ?,
                    logo_url = ?,
                    phone = ?,
                    email = ?,
                    website = ?,
                    address_line_1 = ?,
                    address_line_2 = ?,
                    city = ?,
                    state = ?,
                    county = ?,
                    zip_code = ?,
                    invoice_header_name = ?,
                    quote_header_name = ?,
                    invoice_footer_note = ?,
                    quote_footer_note = ?,
                    email_from_name = ?,
                    reply_to_email = ?,
                    platform_sender_enabled = ?,
                    reply_to_mode = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE company_id = ?
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
                    reply_to_mode
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
@require_permission("can_manage_settings")
def test_email():
    ensure_company_profile_table()

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