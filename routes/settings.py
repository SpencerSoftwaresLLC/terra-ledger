import os
import uuid
import json
import io

from flask import Blueprint, request, redirect, url_for, session, flash, render_template_string, make_response
from flask_wtf.csrf import generate_csrf
from markupsafe import escape
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


def _record_get(record, key, default=""):
    try:
        if record and hasattr(record, "keys") and key in record.keys():
            value = record[key]
            return value if value is not None else default
    except Exception:
        pass
    return default


def _first_nonempty(*values):
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _digits_only(value):
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _format_ein(value):
    digits = _digits_only(value)
    if len(digits) == 9:
        return f"{digits[:2]}-{digits[2:]}"
    return str(value or "").strip()


def _format_ssn_employee_copy(value):
    digits = _digits_only(value)
    if len(digits) == 9:
        return f"***-**-{digits[-4:]}"
    return ""


def _safe_line(text_value, limit=None):
    text_value = " ".join(str(text_value or "").split())
    if limit and len(text_value) > limit:
        return text_value[:limit].rstrip()
    return text_value


def _split_name_for_w2(employee_record):
    full_name = _first_nonempty(_record_get(employee_record, "full_name"))
    first_name = _first_nonempty(_record_get(employee_record, "first_name"))
    middle_name = _first_nonempty(
        _record_get(employee_record, "middle_name"),
        _record_get(employee_record, "middle_initial"),
    )
    last_name = _first_nonempty(_record_get(employee_record, "last_name"))

    if full_name and not (first_name or last_name):
        parts = full_name.split()
        if len(parts) == 1:
            first_name = parts[0]
            last_name = ""
        elif len(parts) >= 2:
            first_name = parts[0]
            last_name = parts[-1]
            if len(parts) > 2 and not middle_name:
                middle_name = " ".join(parts[1:-1])

    return (
        _safe_line(first_name, 20),
        _safe_line(middle_name[:1] if middle_name else "", 1),
        _safe_line(last_name, 26),
    )


def _company_display_name_for_reports(cid):
    profile = get_company_profile(cid)
    values = get_company_profile_values(profile)
    return values.get("legal_name") or values.get("display_name") or session.get("company_name", "TerraLedger")

def _build_ssa_export(company_id, year, employees):
    export = {
        "company_id": company_id,
        "year": year,
        "employees": []
    }

    for e in employees:
        export["employees"].append({
            "name": e.get("employee_name"),
            "wages": float(e.get("wages", 0)),
            "federal": float(e.get("federal_withholding", 0)),
            "ss": float(e.get("social_security", 0)),
            "medicare": float(e.get("medicare", 0)),
        })

    return json.dumps(export, indent=2)

def _build_w3_pdf(company_profile, tax_year, totals):
    import io
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import landscape, letter
    from reportlab.lib.colors import HexColor, white

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=landscape(letter))
    width, height = landscape(letter)

    scale = 0.90
    pdf.translate((width * (1 - scale)) / 2, (height * (1 - scale)) / 2)
    pdf.scale(scale, scale)

    company_values = get_company_profile_values(company_profile or {})

    red = HexColor("#d62828")
    light_red = HexColor("#f7d6dc")
    medium_red = HexColor("#efb7c0")
    dark_text = HexColor("#202020")

    def clean(v):
        return str(v or "").strip()

    def money(v):
        try:
            return f"{float(v or 0):,.2f}"
        except Exception:
            return "0.00"

    def rect(x, y, w, h, stroke=1, fill=0, fill_color=None, line_width=1):
        old_line = pdf._lineWidth
        old_fill = pdf._fillColorObj
        pdf.setLineWidth(line_width)
        if fill_color is not None:
            pdf.setFillColor(fill_color)
            pdf.rect(x, y, w, h, stroke=stroke, fill=1)
            pdf.setFillColor(red)
        else:
            pdf.rect(x, y, w, h, stroke=stroke, fill=fill)
        pdf.setLineWidth(old_line)
        pdf.setFillColor(old_fill)

    def line(x1, y1, x2, y2, lw=1):
        old_line = pdf._lineWidth
        pdf.setLineWidth(lw)
        pdf.line(x1, y1, x2, y2)
        pdf.setLineWidth(old_line)

    def text(x, y, value, size=8, bold=False, color=red, align="left", max_width=None):
        value = clean(value)
        if not value:
            return
        font_name = "Helvetica-Bold" if bold else "Helvetica"
        pdf.setFont(font_name, size)
        pdf.setFillColor(color)

        if max_width:
            while value and pdf.stringWidth(value, font_name, size) > max_width:
                value = value[:-1].rstrip()

        if align == "center":
            pdf.drawCentredString(x, y, value)
        elif align == "right":
            pdf.drawRightString(x, y, value)
        else:
            pdf.drawString(x, y, value)

        pdf.setFillColor(red)

    def wrap_lines(value, font_name, size, max_width, max_lines=2):
        value = clean(value)
        if not value:
            return []
        words = value.split()
        lines = []
        current = ""
        for word in words:
            test = f"{current} {word}".strip()
            if pdf.stringWidth(test, font_name, size) <= max_width:
                current = test
            else:
                if current:
                    lines.append(current)
                current = word
        if current:
            lines.append(current)
        return lines[:max_lines]

    def multiline(x, y_top, value, size=8, bold=False, color=dark_text, max_width=120, line_gap=10, max_lines=2):
        font_name = "Helvetica-Bold" if bold else "Helvetica"
        lines = wrap_lines(value, font_name, size, max_width, max_lines=max_lines)
        pdf.setFont(font_name, size)
        pdf.setFillColor(color)
        for i, ln in enumerate(lines):
            pdf.drawString(x, y_top - (i * line_gap), ln)
        pdf.setFillColor(red)

    def box_label_value(x, y_top, w, h, label, value="", label_size=7, value_size=11, fill_color=None, value_align="left"):
        y = y_top - h
        rect(x, y, w, h, stroke=1, fill_color=fill_color)
        if label:
            text(x + 4, y_top - 11, label, size=label_size, color=red, max_width=w - 8)
        if clean(value):
            if value_align == "center":
                text(x + w / 2, y + 6, value, size=value_size, bold=False, color=dark_text, align="center", max_width=w - 8)
            elif value_align == "right":
                text(x + w - 4, y + 6, value, size=value_size, bold=False, color=dark_text, align="right", max_width=w - 8)
            else:
                text(x + 4, y + 6, value, size=value_size, bold=False, color=dark_text, max_width=w - 8)

    def _build_w3_pdf(company_profile, tax_year, totals):
        import io
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import landscape, letter
        from reportlab.lib.colors import HexColor, white
        buffer = io.BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=landscape(letter))
        width, height = landscape(letter)

        # Slight scale-down so the form fits better in browser PDF viewers
        scale = 0.88
        pdf.translate((width * (1 - scale)) / 2, (height * (1 - scale)) / 2)
        pdf.scale(scale, scale)

        company_values = get_company_profile_values(company_profile or {})

        red = HexColor("#d62828")
        light_red = HexColor("#f7d6dc")
        medium_red = HexColor("#efb7c0")
        dark_text = HexColor("#202020")

    def clean(v):
        return str(v or "").strip()

    def money(v):
        try:
            return f"{float(v or 0):,.2f}"
        except Exception:
            return "0.00"

    def rect(x, y, w, h, stroke=1, fill=0, fill_color=None, line_width=1):
        old_line = pdf._lineWidth
        old_fill = pdf._fillColorObj
        pdf.setLineWidth(line_width)
        if fill_color is not None:
            pdf.setFillColor(fill_color)
            pdf.rect(x, y, w, h, stroke=stroke, fill=1)
            pdf.setFillColor(red)
        else:
            pdf.rect(x, y, w, h, stroke=stroke, fill=fill)
        pdf.setLineWidth(old_line)
        pdf.setFillColor(old_fill)

    def line(x1, y1, x2, y2, lw=1):
        old_line = pdf._lineWidth
        pdf.setLineWidth(lw)
        pdf.line(x1, y1, x2, y2)
        pdf.setLineWidth(old_line)

    def text(x, y, value, size=8, bold=False, color=red, align="left", max_width=None):
        value = clean(value)
        if not value:
            return
        font_name = "Helvetica-Bold" if bold else "Helvetica"
        pdf.setFont(font_name, size)
        pdf.setFillColor(color)

        if max_width:
            while value and pdf.stringWidth(value, font_name, size) > max_width:
                value = value[:-1].rstrip()

        if align == "center":
            pdf.drawCentredString(x, y, value)
        elif align == "right":
            pdf.drawRightString(x, y, value)
        else:
            pdf.drawString(x, y, value)

        pdf.setFillColor(red)

    def wrap_lines(value, font_name, size, max_width, max_lines=2):
        value = clean(value)
        if not value:
            return []
        words = value.split()
        lines = []
        current = ""
        for word in words:
            test = f"{current} {word}".strip()
            if pdf.stringWidth(test, font_name, size) <= max_width:
                current = test
            else:
                if current:
                    lines.append(current)
                current = word
        if current:
            lines.append(current)
        return lines[:max_lines]

    def multiline(x, y_top, value, size=8, bold=False, color=dark_text, max_width=120, line_gap=10, max_lines=2):
        font_name = "Helvetica-Bold" if bold else "Helvetica"
        lines = wrap_lines(value, font_name, size, max_width, max_lines=max_lines)
        pdf.setFont(font_name, size)
        pdf.setFillColor(color)
        for i, ln in enumerate(lines):
            pdf.drawString(x, y_top - (i * line_gap), ln)
        pdf.setFillColor(red)

    def box_label_value(
        x,
        y_top,
        w,
        h,
        label,
        value="",
        label_size=7,
        value_size=11,
        fill_color=None,
        value_align="left",
    ):
        y = y_top - h
        rect(x, y, w, h, stroke=1, fill_color=fill_color)

        if label:
            text(x + 4, y_top - 11, label, size=label_size, color=red, max_width=w - 8)

        if clean(value):
            if value_align == "center":
                text(
                    x + w / 2,
                    y + 6,
                    value,
                    size=value_size,
                    color=dark_text,
                    align="center",
                    max_width=w - 8,
                )
            elif value_align == "right":
                text(
                    x + w - 4,
                    y + 6,
                    value,
                    size=value_size,
                    color=dark_text,
                    align="right",
                    max_width=w - 8,
                )
            else:
                text(
                    x + 4,
                    y + 6,
                    value,
                    size=value_size,
                    color=dark_text,
                    max_width=w - 8,
                )

    def checkbox_cell(x, y_top, w, h, title, options, checked_index=None):
        y = y_top - h
        rect(x, y, w, h, stroke=1)

        title_lines = str(title or "").split("\n")
        for i, line_text in enumerate(title_lines[:3]):
            text(x + 4, y_top - 10 - (i * 8), line_text, size=6.8, bold=True)

        text(x + 4, y_top - 32, "(Check one)", size=5.5)

        if not options:
            return

        seg_w = w / len(options)

        for idx, (code, label) in enumerate(options):
            ox = x + (idx * seg_w)

            if idx > 0:
                line(ox, y, ox, y_top, lw=1)

            cb_size = 11
            cb_x = ox + 10
            cb_y = y + h - 44
            rect(cb_x, cb_y, cb_size, cb_size, stroke=1)

            if checked_index == idx:
                text(
                    cb_x + cb_size / 2,
                    cb_y + 2,
                    "X",
                    size=9,
                    bold=True,
                    color=dark_text,
                    align="center",
                )

            if code:
                text(ox + (seg_w / 2), y + h - 12, code, size=5.6, align="center")

            label_lines = str(label or "").split("\n")
            for i, line_txt in enumerate(label_lines[:4]):
                text(
                    cb_x + cb_size + 5,
                    y + h - 18 - (i * 7),
                    line_txt,
                    size=5.8,
                    max_width=seg_w - (cb_x + cb_size + 10 - ox),
                )

    employer_name = (
        company_values.get("legal_name")
        or company_values.get("display_name")
        or ""
    )
    employer_ein = _format_ein(company_values.get("ein"))
    employer_address = " ".join(
        part for part in [
            company_values.get("address") or company_values.get("address_line_1") or "",
            company_values.get("address_line_2") or "",
        ] if clean(part)
    ).strip()
    employer_city_state_zip = " ".join(
        part for part in [
            f"{company_values.get('city')}," if clean(company_values.get("city")) else "",
            company_values.get("state") or "",
            company_values.get("zip_code") or "",
        ] if clean(part)
    ).strip()

    contact_name = company_values.get("w2_contact_name") or ""
    contact_phone = company_values.get("w2_contact_phone") or company_values.get("phone") or ""
    contact_email = company_values.get("w2_contact_email") or company_values.get("email") or ""

    total_forms = int(float(totals.get("total_forms", 0) or 0))
    wages = money(totals.get("wages", 0))
    federal = money(totals.get("federal_withholding", 0))
    ss_wages = money(totals.get("social_security_wages", totals.get("wages", 0)))
    ss_tax = money(totals.get("social_security", 0))
    medicare_wages = money(totals.get("medicare_wages", totals.get("wages", 0)))
    medicare_tax = money(totals.get("medicare", 0))
    ss_tips = money(totals.get("social_security_tips", 0))
    allocated_tips = money(totals.get("allocated_tips", 0))
    dependent_care = money(totals.get("dependent_care_benefits", 0))
    nonqualified = money(totals.get("nonqualified_plans", 0))
    deferred_comp = money(totals.get("deferred_compensation", 0))
    state = clean(company_values.get("state")).upper()
    state_id = clean(company_values.get("state_employer_id"))
    state_wages = money(totals.get("state_wages", totals.get("wages", 0)))
    state_tax = money(totals.get("state_withholding", 0))
    local_wages = money(totals.get("local_wages", 0))
    local_tax = money(totals.get("local_tax", 0))

    control_number = clean(totals.get("control_number", "33333"))
    kind_of_payer_index = int(totals.get("kind_of_payer_index", 0) or 0)
    kind_of_employer_index = int(totals.get("kind_of_employer_index", 2) or 2)
    establishment_number = clean(totals.get("establishment_number", ""))
    other_ein_used = clean(totals.get("other_ein_used_this_year", ""))
    fax_number = clean(totals.get("fax_number", ""))

    left = 14
    right = width - 14
    top = height - 16
    bottom = 18

    form_w = right - left
    form_h = top - bottom

    pdf.setStrokeColor(red)
    pdf.setFillColor(red)
    pdf.setLineWidth(1)

    rect(left, bottom, form_w, form_h, stroke=1, line_width=1.5)
    text(width / 2, top + 2, "DO NOT STAPLE", size=14, bold=True, align="center")

    header_h = 38
    y_top = top
    header_bottom = y_top - header_h

    rect(left, header_bottom, 88, header_h)
    text(left + 6, y_top - 20, control_number, size=15, color=dark_text)

    rect(left + 88, header_bottom, 176, header_h)
    text(left + 94, y_top - 12, "a   Control number", size=8, bold=True)

    rect(left + 264, header_bottom, 476, header_h)
    text(left + 270, y_top - 12, "For Official Use Only", size=8, bold=True)
    text(left + 270, y_top - 28, "OMB No. 1545-0008", size=8, bold=True)

    current_y = header_bottom

    left_col_w = 312
    mid_col_w = 248
    right_col_w = form_w - left_col_w - mid_col_w

    x1 = left
    x2 = left + left_col_w
    x3 = x2 + mid_col_w

    row_b_h = 88
    checkbox_cell(
        x1,
        current_y,
        left_col_w,
        row_b_h,
        "b   Kind\nof\nPayer",
        [
            ("941", "CT-1"),
            ("Military", "Hshld.\nemp."),
            ("943", "Medicare\ngovt. emp."),
            ("944", ""),
        ],
        checked_index=kind_of_payer_index,
    )

    checkbox_cell(
        x2,
        current_y,
        mid_col_w + right_col_w,
        row_b_h,
        "Kind\nof\nEmployer",
        [
            ("None appl.", "State/local\nnon-501c"),
            ("501c non-govt.", "State/local 501c"),
            ("", "Federal govt."),
            ("", "Third-party\nsick pay\n(Check if\napplicable)"),
        ],
        checked_index=kind_of_employer_index,
    )

    current_y -= row_b_h

    row_cd_h = 40
    box_label_value(x1, current_y, left_col_w / 2, row_cd_h, "c Total number of Forms W-2", str(total_forms or 0), value_size=11)
    box_label_value(x1 + left_col_w / 2, current_y, left_col_w / 2, row_cd_h, "d Establishment number", establishment_number, value_size=11)
    box_label_value(x2, current_y, mid_col_w, row_cd_h, "1 Wages, tips, other compensation", wages, value_align="right", value_size=10.5)
    box_label_value(x3, current_y, right_col_w, row_cd_h, "2 Federal income tax withheld", federal, value_align="right", value_size=10.5)
    current_y -= row_cd_h

    row_e_h = 40
    box_label_value(x1, current_y, left_col_w, row_e_h, "e Employer identification number (EIN)", employer_ein, value_size=11)
    box_label_value(x2, current_y, mid_col_w, row_e_h, "3 Social security wages", ss_wages, value_align="right", value_size=10.5)
    box_label_value(x3, current_y, right_col_w, row_e_h, "4 Social security tax withheld", ss_tax, value_align="right", value_size=10.5)
    current_y -= row_e_h

    row_f_h = 40
    box_label_value(x1, current_y, left_col_w, row_f_h, "f Employer's name", employer_name, value_size=10)
    box_label_value(x2, current_y, mid_col_w, row_f_h, "5 Medicare wages and tips", medicare_wages, value_align="right", value_size=10.5)
    box_label_value(x3, current_y, right_col_w, row_f_h, "6 Medicare tax withheld", medicare_tax, value_align="right", value_size=10.5)
    current_y -= row_f_h

    row_g1_h = 58
    box_label_value(x1, current_y, left_col_w, row_g1_h, "", "", value_size=10)
    multiline(x1 + 4, current_y - 16, employer_address, size=9.5, color=dark_text, max_width=left_col_w - 8, max_lines=2)
    try:
        ss_tips_zero = float(str(ss_tips).replace(",", "") or 0) == 0
    except Exception:
        ss_tips_zero = True
    box_label_value(
        x2,
        current_y,
        mid_col_w,
        row_g1_h,
        "7 Social security tips",
        ss_tips,
        fill_color=white if ss_tips_zero else light_red,
        value_align="right",
        value_size=10.5,
    )
    box_label_value(x3, current_y, right_col_w, row_g1_h, "8 Allocated tips", allocated_tips, value_align="right", value_size=10.5)
    current_y -= row_g1_h

    row_g2_h = 46
    box_label_value(x1, current_y, left_col_w, row_g2_h, "g Employer's address and ZIP code", employer_city_state_zip, value_size=9)
    box_label_value(x2, current_y, mid_col_w, row_g2_h, "9", "", fill_color=medium_red)
    box_label_value(x3, current_y, right_col_w, row_g2_h, "10 Dependent care benefits", dependent_care, value_align="right", value_size=10.5)
    current_y -= row_g2_h

    row_h_h = 40
    box_label_value(x1, current_y, left_col_w, row_h_h, "h Other EIN used this year", other_ein_used, value_size=10)
    box_label_value(x2, current_y, mid_col_w, row_h_h, "11 Nonqualified plans", nonqualified, value_align="right", value_size=10.5)
    box_label_value(x3, current_y, right_col_w, row_h_h, "12a Deferred compensation", deferred_comp, value_align="right", value_size=10.5)
    current_y -= row_h_h

    row_i_h = 40
    box_label_value(x1, current_y, left_col_w, row_i_h, "", "")
    box_label_value(x2, current_y, mid_col_w, row_i_h, "13 For third-party sick pay use only", "", fill_color=white)
    box_label_value(x3, current_y, right_col_w, row_i_h, "12b", "", fill_color=medium_red)
    current_y -= row_i_h

    row_state_h = 42
    state_left_w = 62
    state_id_w = 106
    state_wages_w = 166
    state_tax_w = left_col_w - state_left_w - state_id_w - state_wages_w

    box_label_value(x1, current_y, state_left_w, row_state_h, "15 State", state, value_size=10)
    box_label_value(x1 + state_left_w, current_y, state_id_w, row_state_h, "Employer's state ID number", state_id, value_size=9)
    box_label_value(x1 + state_left_w + state_id_w, current_y, state_wages_w, row_state_h, "16 State wages, tips, etc.", state_wages, value_align="right", value_size=10.5)
    box_label_value(x1 + state_left_w + state_id_w + state_wages_w, current_y, state_tax_w, row_state_h, "17 State income tax", state_tax, value_align="right", value_size=10.5)
    box_label_value(x2, current_y, mid_col_w, row_state_h, "18 Local wages, tips, etc.", local_wages, value_align="right", value_size=10.5)
    box_label_value(x3, current_y, right_col_w, row_state_h, "19 Local income tax", local_tax, value_align="right", value_size=10.5)
    current_y -= row_state_h

    row_contact1_h = 38
    row_contact2_h = 38
    bottom_block_h = row_contact1_h + row_contact2_h

    rect(x1, current_y - bottom_block_h, form_w, bottom_block_h, stroke=1, line_width=1.2)
    box_label_value(x1, current_y, left_col_w, row_contact1_h, "Employer's contact person", contact_name, value_size=9)
    box_label_value(x2, current_y, mid_col_w, row_contact1_h, "Employer's telephone number", contact_phone, value_size=9)
    box_label_value(x3, current_y, right_col_w, row_contact1_h, "For Official Use Only", "", value_size=9)
    current_y -= row_contact1_h

    box_label_value(x1, current_y, left_col_w, row_contact2_h, "Employer's fax number", fax_number, value_size=9)
    box_label_value(x2, current_y, mid_col_w, row_contact2_h, "Employer's email address", contact_email, value_size=9)
    box_label_value(x3, current_y, right_col_w, row_contact2_h, "", "", value_size=9)
    current_y -= row_contact2_h

    text(
        left + 2,
        current_y - 16,
        "Under penalties of perjury, I declare that I have examined this return and accompanying documents, and, to the best of my knowledge and belief, they are true, correct, and",
        size=7,
    )
    text(left + 2, current_y - 28, "complete.", size=7)

    sig_y = current_y - 54
    text(left + 2, sig_y, "Signature", size=7)
    line(left + 56, sig_y + 2, left + 250, sig_y + 2)
    text(left + 260, sig_y, "Title", size=7)
    line(left + 286, sig_y + 2, left + 450, sig_y + 2)
    text(left + 460, sig_y, "Date", size=7)
    line(left + 486, sig_y + 2, left + 610, sig_y + 2)

    footer_y = bottom + 8
    text(left + 2, footer_y, "Form", size=8)
    text(left + 34, footer_y - 2, "W-3", size=19, bold=True, color=dark_text)
    text(left + 92, footer_y - 1, "Transmittal of Wage and Tax Statements", size=16, bold=True)
    text(width / 2 + 40, footer_y - 4, str(tax_year), size=26, color=dark_text, align="center")
    text(right - 2, footer_y + 5, "Department of the Treasury", size=7, align="right")
    text(right - 2, footer_y - 7, "Internal Revenue Service", size=7, align="right")

    pdf.showPage()
    pdf.save()

    data = buffer.getvalue()
    buffer.close()
    return data


def _build_w2_summary_pdf(company_profile, tax_year, employee_record, summary):
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    page_w, page_h = letter

    def money(v):
        return float(_money(v or 0))

    def clean(v):
        return str(v or "").strip()

    def draw_text(x, y, value, size=6.2, bold=False, align="left", max_width=None):
        value = clean(value)
        if not value:
            return

        font_name = "Helvetica-Bold" if bold else "Helvetica"
        pdf.setFont(font_name, size)

        if max_width:
            while value and pdf.stringWidth(value, font_name, size) > max_width:
                value = value[:-1].rstrip()

        if align == "right":
            pdf.drawRightString(x, y, value)
        elif align == "center":
            pdf.drawCentredString(x, y, value)
        else:
            pdf.drawString(x, y, value)

    def box(
        x,
        y_top,
        w,
        h,
        label="",
        value="",
        value_size=6.2,
        bold=False,
        align="left",
        label_size=4.3,
    ):
        y_bottom = y_top - h
        pdf.rect(x, y_bottom, w, h)

        if label:
            draw_text(
                x + 2,
                y_top - 5,
                label,
                size=label_size,
                max_width=w - 4,
            )

        if value:
            value_y = y_bottom + (h * 0.40)
            if align == "right":
                draw_text(
                    x + w - 2,
                    value_y,
                    value,
                    size=value_size,
                    bold=bold,
                    align="right",
                    max_width=w - 4,
                )
            elif align == "center":
                draw_text(
                    x + (w / 2),
                    value_y,
                    value,
                    size=value_size,
                    bold=bold,
                    align="center",
                    max_width=w - 4,
                )
            else:
                draw_text(
                    x + 2,
                    value_y,
                    value,
                    size=value_size,
                    bold=bold,
                    align="left",
                    max_width=w - 4,
                )

    def checkbox_line(x, y_top, w, h, label, checked=False):
        y_bottom = y_top - h
        pdf.rect(x, y_bottom, w, h)

        cb_size = 7
        cb_x = x + 3
        cb_y = y_bottom + (h - cb_size) / 2
        pdf.rect(cb_x, cb_y, cb_size, cb_size)

        if checked:
            draw_text(cb_x + cb_size / 2, cb_y + 1.0, "X", size=5.8, bold=True, align="center")

        draw_text(
            x + 14,
            y_bottom + (h * 0.38),
            label,
            size=4.7,
            max_width=w - 16,
        )

    company_values = get_company_profile_values(company_profile)

    wages = money(summary.get("wages"))
    federal = money(summary.get("federal_withholding"))
    social_security_tax = money(summary.get("social_security"))
    medicare_tax = money(summary.get("medicare"))
    state_tax = money(summary.get("state_withholding"))
    local_tax = money(summary.get("local_tax"))

    social_security_wages = money(summary.get("social_security_wages", wages))
    medicare_wages = money(summary.get("medicare_wages", wages))
    state_wages = money(summary.get("state_wages", wages))
    local_wages = money(summary.get("local_wages", wages))

    employer_name = _first_nonempty(
        company_values.get("legal_name"),
        company_values.get("display_name"),
    )
    employer_ein = _format_ein(company_values.get("ein"))
    employer_state = clean(company_values.get("state")).upper()
    employer_state_id = clean(company_values.get("state_employer_id"))

    employer_address_1 = clean(company_values.get("address_line_1"))
    employer_address_2 = clean(company_values.get("address_line_2"))
    employer_city = clean(company_values.get("city"))
    employer_zip = clean(company_values.get("zip_code"))

    employer_addr_line = " ".join(
        part for part in [employer_address_1, employer_address_2] if part
    ).strip()
    employer_city_state_zip = " ".join(
        part
        for part in [
            f"{employer_city}," if employer_city else "",
            employer_state,
            employer_zip,
        ]
        if part
    ).strip()

    first_name, middle_initial, last_name = _split_name_for_w2(employee_record)
    employee_name_line = " ".join(
        part for part in [first_name, middle_initial, last_name] if part
    ).strip()

    employee_ssn = _format_ssn_employee_copy(
        _first_nonempty(
            _record_get(employee_record, "ssn"),
            _record_get(employee_record, "social_security_number"),
        )
    )

    employee_addr_1 = _first_nonempty(
        _record_get(employee_record, "address_line_1"),
        _record_get(employee_record, "street"),
        _record_get(employee_record, "address"),
    )
    employee_addr_2 = clean(_record_get(employee_record, "address_line_2"))
    employee_city = clean(_record_get(employee_record, "city"))
    employee_state = clean(_record_get(employee_record, "state")).upper()
    employee_zip = _first_nonempty(
        _record_get(employee_record, "zip_code"),
        _record_get(employee_record, "zipcode"),
    )
    employee_locality = _first_nonempty(
        _record_get(employee_record, "locality"),
        _record_get(employee_record, "county"),
        employer_state,
    )

    employee_addr_line = " ".join(
        part for part in [employee_addr_1, employee_addr_2] if part
    ).strip()
    employee_city_state_zip = " ".join(
        part
        for part in [
            f"{employee_city}," if employee_city else "",
            employee_state,
            employee_zip,
        ]
        if part
    ).strip()

    retirement_401k = money(summary.get("retirement_401k"))
    health_insurance = money(summary.get("health_insurance"))
    union_dues = money(summary.get("union_dues"))
    other_deductions = money(summary.get("other_deductions"))

    box12_items = []
    if retirement_401k > 0:
        box12_items.append(f"D {retirement_401k:,.2f}")
    if health_insurance > 0:
        box12_items.append(f"DD {health_insurance:,.2f}")

    for key in ("box12a", "box12b", "box12c", "box12d"):
        val = clean(summary.get(key))
        if val:
            box12_items.append(val)

    box14_value = ""
    if union_dues > 0:
        box14_value = f"Union {union_dues:,.2f}"
    elif other_deductions > 0:
        box14_value = f"Other {other_deductions:,.2f}"
    elif clean(summary.get("box14")):
        box14_value = clean(summary.get("box14"))

    statutory_employee = bool(summary.get("statutory_employee"))
    retirement_plan = bool(summary.get("retirement_plan")) or retirement_401k > 0
    third_party_sick_pay = bool(summary.get("third_party_sick_pay"))

    pdf.setLineWidth(0.35)
    pdf.line(page_w / 2, 14, page_w / 2, page_h - 14)
    pdf.line(14, page_h / 2, page_w - 14, page_h / 2)

    def draw_form(origin_x, origin_y, copy_title):
        left = origin_x
        top = origin_y

        form_w = 272

        # Header only, no outer frame
        draw_text(left + 2, top - 8, copy_title, size=5.0, bold=True, max_width=190)
        draw_text(left + form_w - 36, top - 8, str(tax_year), size=7.8, bold=True, align="right")
        draw_text(left + form_w - 2, top - 8, "OMB No. 1545-0008", size=5.0, align="right")
        pdf.line(left, top - 12, left + form_w, top - 12)

        row_h = 20
        small_h = 18

        # Top row
        box(left,       top - 12, 82, row_h, "a Employee's social sec. no.", employee_ssn, 6.2, False, "center")
        box(left + 82,  top - 12, 64, row_h, "1 Wages, tips, other compensation", f"{wages:,.2f}", 6.2, False, "right")
        box(left + 146, top - 12, 64, row_h, "2 Federal income tax withheld", f"{federal:,.2f}", 6.2, False, "right")
        box(left + 210, top - 12, 62, row_h, "", "", 6.2)

        # Second row
        box(left,       top - 32, 82, row_h, "b Employer's ID number (EIN)", employer_ein, 6.2)
        box(left + 82,  top - 32, 64, row_h, "3 Social Security Wages", f"{social_security_wages:,.2f}", 6.2, False, "right")
        box(left + 146, top - 32, 64, row_h, "4 Social security tax withheld", f"{social_security_tax:,.2f}", 6.2, False, "right")
        box(left + 210, top - 32, 62, row_h, "", "", 6.2)

        # Third row
        box(left,       top - 52, 82, row_h, "(EIN)", "", 6.2)
        box(left + 82,  top - 52, 64, row_h, "5 Medicare wages and tips", f"{medicare_wages:,.2f}", 6.2, False, "right")
        box(left + 146, top - 52, 64, row_h, "6 Medicare tax withheld", f"{medicare_tax:,.2f}", 6.2, False, "right")
        box(left + 210, top - 52, 62, row_h, "", "", 6.2)

        # Employer block
        box(left, top - 72, 272, 42, "c Employer's name, address, and ZIP code", "", 6.2)
        draw_text(left + 5, top - 86, employer_name, size=6.1, max_width=262)
        draw_text(left + 5, top - 98, employer_addr_line, size=6.1, max_width=262)
        draw_text(left + 5, top - 110, employer_city_state_zip, size=6.1, max_width=262)

        # Control
        box(left, top - 114, 272, 17, "d Control number", clean(summary.get("control_number")), 6.0)

        # Employee block
        box(left, top - 131, 272, 42, "e Employee's name, address, and ZIP code", "", 6.2)
        draw_text(left + 5, top - 145, employee_name_line, size=6.1, max_width=262)
        draw_text(left + 5, top - 157, employee_addr_line, size=6.1, max_width=262)
        draw_text(left + 5, top - 169, employee_city_state_zip, size=6.1, max_width=262)

        # Middle rows
        box(left,       top - 173, 91, small_h, "7 Social security tips", "", 5.9)
        box(left + 91,  top - 173, 91, small_h, "8 Allocated tips", "", 5.9)
        box(left + 182, top - 173, 90, small_h, "9", "", 5.9)

        box(left,       top - 191, 91, small_h, "10 Dependent care benefits", "", 5.9)
        box(left + 91,  top - 191, 91, small_h, "11 Nonqualified plans", "", 5.9)
        box(left + 182, top - 191, 90, small_h, "12a Code", box12_items[0] if len(box12_items) > 0 else "", 5.7)

        checkbox_line(left, top - 209, 91, small_h, "Statutory employee", statutory_employee)
        box(left + 91,  top - 209, 91, small_h, "14 Other", box14_value, 5.7)
        box(left + 182, top - 209, 90, small_h, "12b Code", box12_items[1] if len(box12_items) > 1 else "", 5.7)

        checkbox_line(left, top - 227, 91, small_h, "Retirement plan", retirement_plan)
        box(left + 91,  top - 227, 91, small_h, "", "", 5.9)
        box(left + 182, top - 227, 90, small_h, "12c Code", box12_items[2] if len(box12_items) > 2 else "", 5.7)

        checkbox_line(left, top - 245, 91, small_h, "Third-party sick pay", third_party_sick_pay)
        box(left + 91,  top - 245, 91, small_h, "", "", 5.9)
        box(left + 182, top - 245, 90, small_h, "12d Code", box12_items[3] if len(box12_items) > 3 else "", 5.7)

        # Footer row
        footer_top = top - 263
        box(left,       footer_top, 26, 16, "15 State", employer_state, 5.7, False, "center", 4.0)
        box(left + 26,  footer_top, 70, 16, "Employer's state ID number", employer_state_id, 5.3, False, "left", 4.0)
        box(left + 96,  footer_top, 60, 16, "16 State wages, tips, etc.", f"{state_wages:,.2f}", 5.7, False, "right", 4.0)
        box(left + 156, footer_top, 42, 16, "17 State income tax", f"{state_tax:,.2f}" if state_tax else "", 5.7, False, "right", 4.0)
        box(left + 198, footer_top, 38, 16, "18 Local wages, tips, etc.", f"{local_wages:,.2f}" if local_wages else "", 5.2, False, "right", 4.0)
        box(left + 236, footer_top, 36, 16, "19 Local income tax", f"{local_tax:,.2f}" if local_tax else "", 5.2, False, "right", 4.0)

        # Bottom line
        box(left, top - 279, 272, 13, "20 Locality name", employee_locality, 5.9, False, "left", 4.0)

        draw_text(left + 2, top - 289, "Form W-2 Wage and Tax Statement", size=4.7)
        draw_text(left + form_w - 2, top - 289, "Department of the Treasury - Internal Revenue Service", size=4.7, align="right")

    margin_x = 18
    margin_y = 18

    draw_form(
        margin_x,
        page_h - margin_y,
        "Copy B—To be filed with employee's FEDERAL tax return.",
    )
    draw_form(
        page_w / 2 + margin_x - 8,
        page_h - margin_y,
        "Copy 2—To be filed with employee's state, city, or local income tax return.",
    )
    draw_form(
        margin_x,
        page_h / 2 - margin_y + 4,
        "Copy B—To be filed with employee's FEDERAL tax return.",
    )
    draw_form(
        page_w / 2 + margin_x - 8,
        page_h / 2 - margin_y + 4,
        "Copy 2—To be filed with employee's state, city, or local income tax return.",
    )

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
        pdf.setFont("Helvetica-Bold", 15)
        pdf.drawString(36, height - 36, "W-2 Employee Totals Report")

        pdf.setFont("Helvetica", 9)
        pdf.drawString(36, height - 52, f"Company: {text(company_name)}")
        pdf.drawString(36, height - 64, f"Tax Year: {text(tax_year)}")
        pdf.drawRightString(width - 36, height - 52, f"Page {page_no}")

        table_top = height - 84
        left = 36
        right = width - 36

        pdf.setLineWidth(1)
        pdf.line(left, table_top, right, table_top)

        header_y = table_top - 14
        pdf.setFont("Helvetica-Bold", 8.2)
        pdf.drawString(40, header_y, "Employee")
        pdf.drawRightString(315, header_y, "Box 1 Wages")
        pdf.drawRightString(385, header_y, "Box 2 Federal")
        pdf.drawRightString(450, header_y, "Box 4 SS Tax")
        pdf.drawRightString(515, header_y, "Box 6 Medicare")
        pdf.drawRightString(575, header_y, "Box 17 State")
        pdf.drawRightString(620, header_y, "Box 19 Local")

        pdf.line(left, header_y - 6, right, header_y - 6)
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
        if y < 72:
            pdf.showPage()
            page_no += 1
            y = start_page(page_no)
            pdf.setFont("Helvetica", 8)

        employee_name = text(row.get("employee_name"))[:46]
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
        pdf.drawRightString(315, y, f"{wages:,.2f}")
        pdf.drawRightString(385, y, f"{federal:,.2f}")
        pdf.drawRightString(450, y, f"{social_security:,.2f}")
        pdf.drawRightString(515, y, f"{medicare:,.2f}")
        pdf.drawRightString(575, y, f"{state:,.2f}")
        pdf.drawRightString(620, y, f"{local:,.2f}")

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
    pdf.drawRightString(315, y, f"{total_wages:,.2f}")
    pdf.drawRightString(385, y, f"{total_federal:,.2f}")
    pdf.drawRightString(450, y, f"{total_ss:,.2f}")
    pdf.drawRightString(515, y, f"{total_medicare:,.2f}")
    pdf.drawRightString(575, y, f"{total_state:,.2f}")
    pdf.drawRightString(620, y, f"{total_local:,.2f}")

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


# ===============================
# 🔥 1099 SYSTEM (FULL ADD)
# ===============================

def _build_1099_pdf(company_profile, tax_year, contractor, summary):
    import io
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import landscape, letter
    from reportlab.lib.colors import black, HexColor

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=landscape(letter))
    width, height = landscape(letter)

    # Scale so the whole form fits well in browser viewers
    scale = 0.86
    pdf.translate((width * (1 - scale)) / 2, (height * (1 - scale)) / 2)
    pdf.scale(scale, scale)

    company_values = get_company_profile_values(company_profile or {})

    def clean(v):
        return str(v or "").strip()

    def money(v):
        try:
            return f"{float(v or 0):,.2f}"
        except Exception:
            return "0.00"

    def rect(x, y, w, h, fill_color=None, stroke=1, lw=1):
        old_lw = pdf._lineWidth
        pdf.setLineWidth(lw)
        if fill_color is not None:
            pdf.setFillColor(fill_color)
            pdf.rect(x, y, w, h, stroke=stroke, fill=1)
            pdf.setFillColor(black)
        else:
            pdf.rect(x, y, w, h, stroke=stroke, fill=0)
        pdf.setLineWidth(old_lw)

    def hline(x1, y, x2, lw=1):
        old_lw = pdf._lineWidth
        pdf.setLineWidth(lw)
        pdf.line(x1, y, x2, y)
        pdf.setLineWidth(old_lw)

    def vline(x, y1, y2, lw=1):
        old_lw = pdf._lineWidth
        pdf.setLineWidth(lw)
        pdf.line(x, y1, x, y2)
        pdf.setLineWidth(old_lw)

    def text(x, y, value, size=9, bold=False, align="left", max_width=None):
        value = clean(value)
        if not value:
            return
        font_name = "Helvetica-Bold" if bold else "Helvetica"
        pdf.setFont(font_name, size)
        if max_width:
            while value and pdf.stringWidth(value, font_name, size) > max_width:
                value = value[:-1].rstrip()
        if align == "center":
            pdf.drawCentredString(x, y, value)
        elif align == "right":
            pdf.drawRightString(x, y, value)
        else:
            pdf.drawString(x, y, value)

    def wrap_lines(value, font_name, size, max_width, max_lines=3):
        value = clean(value)
        if not value:
            return []
        words = value.split()
        lines = []
        current = ""
        for word in words:
            test = f"{current} {word}".strip()
            if pdf.stringWidth(test, font_name, size) <= max_width:
                current = test
            else:
                if current:
                    lines.append(current)
                current = word
        if current:
            lines.append(current)
        return lines[:max_lines]

    def multiline(x, y_top, value, size=9, bold=False, max_width=120, line_gap=11, max_lines=3):
        font_name = "Helvetica-Bold" if bold else "Helvetica"
        lines = wrap_lines(value, font_name, size, max_width, max_lines=max_lines)
        pdf.setFont(font_name, size)
        for i, line_text in enumerate(lines):
            pdf.drawString(x, y_top - (i * line_gap), line_text)

    fill_gray = HexColor("#ececec")
    fill_blue = HexColor("#eef0fb")
    fill_dark = HexColor("#b8b8b8")

    payer_name = company_values.get("legal_name") or company_values.get("display_name") or ""
    payer_ein = _format_ein(company_values.get("ein"))
    payer_addr_1 = company_values.get("address") or company_values.get("address_line_1") or ""
    payer_addr_2 = company_values.get("address_line_2") or ""
    payer_city = company_values.get("city") or ""
    payer_state = company_values.get("state") or ""
    payer_zip = company_values.get("zip_code") or ""
    payer_phone = company_values.get("phone") or ""

    recipient_name = (
        contractor.get("name")
        or contractor.get("business_name")
        or contractor.get("legal_name")
        or "Contractor"
    )
    recipient_tin = clean(contractor.get("tin") or contractor.get("ssn") or contractor.get("ein") or "")
    recipient_addr_1 = contractor.get("address") or contractor.get("address_line_1") or ""
    recipient_addr_2 = contractor.get("address_line_2") or ""
    recipient_city = contractor.get("city") or ""
    recipient_state = contractor.get("state") or ""
    recipient_zip = contractor.get("zip_code") or ""

    nonemployee_comp = float(summary.get("nonemployee_comp", 0) or 0)
    federal_withholding = float(summary.get("federal_withholding", 0) or 0)
    state_tax_withheld = float(summary.get("state_tax_withheld", 0) or 0)
    state_income = float(summary.get("state_income", nonemployee_comp) or 0)
    payer_state_no = clean(summary.get("payer_state_no") or company_values.get("state_tax_id") or "")
    state_code = clean(contractor.get("state") or company_values.get("state") or "")
    state2_tax = float(summary.get("state2_tax_withheld", 0) or 0)
    state2_income = float(summary.get("state2_income", 0) or 0)
    state2_code = clean(summary.get("state2_code") or "")
    state2_payer_no = clean(summary.get("state2_payer_state_no") or "")

    account_number = contractor.get("account_number") or contractor.get("vendor_number") or ""
    direct_sales_checked = bool(contractor.get("direct_sales"))

    pdf.setTitle(f"1099-NEC_{tax_year}_{recipient_name}".replace("/", "-"))
    pdf.setStrokeColor(black)
    pdf.setFillColor(black)
    pdf.setLineWidth(1)

    # Outer form box
    outer_x = 58
    outer_y = 78
    outer_w = width - 116
    outer_h = height - 146
    outer_top = outer_y + outer_h

    rect(outer_x, outer_y, outer_w, outer_h, fill_color=None, stroke=1, lw=1.2)

    # Corrected checkbox row
    corrected_y = outer_top + 10
    text(outer_x + outer_w / 2 + 18, corrected_y, "CORRECTED (if checked)", size=15, align="center")
    rect(outer_x + outer_w / 2 - 122, corrected_y - 6, 18, 18, fill_color=fill_blue, stroke=1)

    # Main grid widths
    left_info_w = 450
    center_tax_w = 228
    right_copy_w = outer_w - left_info_w - center_tax_w

    x_info = outer_x
    x_tax = x_info + left_info_w
    x_copy = x_tax + center_tax_w
    x_right = outer_x + outer_w

    # Top header heights
    top_h = 150
    y_top_bottom = outer_top - top_h

    # Top row widths
    top_left_w = left_info_w
    top_mid_blank_w = 170
    top_year_w = 120
    top_title_w = outer_w - top_left_w - top_mid_blank_w - top_year_w

    x_top_left = outer_x
    x_top_mid = x_top_left + top_left_w
    x_top_year = x_top_mid + top_mid_blank_w
    x_top_title = x_top_year + top_year_w

    # Top left payer block
    rect(x_top_left, y_top_bottom, top_left_w, top_h, fill_color=fill_gray)
    rect(x_top_left + 1, y_top_bottom + 1, top_left_w - 2, top_h - 48, fill_color=fill_blue, stroke=0)
    text(x_top_left + 10, outer_top - 18, "PAYER'S name, street address, city or town, state or province, country, ZIP", size=10)
    text(x_top_left + 10, outer_top - 38, "or foreign postal code, and telephone no.", size=10)

    payer_lines = []
    for v in [
        payer_name,
        " ".join([p for p in [payer_addr_1, payer_addr_2] if clean(p)]).strip(),
        " ".join(
            p for p in [
                f"{payer_city}," if clean(payer_city) else "",
                payer_state,
                payer_zip,
            ] if clean(p)
        ).strip(),
        payer_phone,
    ]:
        if clean(v):
            payer_lines.append(v)

    payer_start_y = y_top_bottom + 30
    for i, line_text in enumerate(payer_lines[:4]):
        text(x_top_left + 10, payer_start_y + (i * 14), line_text, size=10, bold=(i == 0), max_width=top_left_w - 20)

    # Top middle gray block
    rect(x_top_mid, y_top_bottom, top_mid_blank_w, top_h, fill_color=fill_dark)

    # Top form/year block
    rect(x_top_year, y_top_bottom, top_year_w, top_h, fill_color=fill_gray)
    hline(x_top_year, y_top_bottom + 56, x_top_year + top_year_w)
    hline(x_top_year, y_top_bottom + 101, x_top_year + top_year_w)

    text(x_top_year + 8, outer_top - 18, "OMB No. 1545-0116", size=10)
    text(x_top_year + 8, outer_top - 55, "Form", size=11)
    text(x_top_year + 46, outer_top - 55, "1099-NEC", size=15, bold=True)
    text(x_top_year + 8, outer_top - 93, "(Rev. January 2022)", size=8.5)
    text(x_top_year + 12, y_top_bottom + 37, "For calendar year", size=10)
    text(x_top_year + top_year_w / 2, y_top_bottom + 10, str(tax_year), size=15, bold=False, align="center")

    # Top far-right title block
    rect(x_top_title, y_top_bottom, top_title_w, top_h, fill_color=None, stroke=0)
    text(x_top_title + top_title_w / 2, outer_top - 72, "Nonemployee", size=18, bold=True, align="center")
    text(x_top_title + top_title_w / 2, outer_top - 104, "Compensation", size=18, bold=True, align="center")

    # Lower section sizing
    tin_h = 42
    name_h = 63
    street_h = 57
    city_h = 57
    box2_h = 57
    box3_h = 47
    box4_h = 56
    account_h = 38
    state_h = 76
    footer_h = 28

    y_tin_top = y_top_bottom
    y_tin_bottom = y_tin_top - tin_h

    # Right copy column starts at TIN row
    rect(x_copy, outer_y + footer_h, right_copy_w, y_tin_top - (outer_y + footer_h), fill_color=None, stroke=1)
    text(x_copy + right_copy_w - 10, y_tin_top - 18, "Copy B", size=16, bold=True, align="right")
    text(x_copy + right_copy_w / 2, y_tin_top - 50, "For Recipient", size=16, bold=True, align="center")
    text(x_copy + right_copy_w / 2, y_tin_top - 78, "This is important tax", size=10, align="center")
    text(x_copy + right_copy_w / 2, y_tin_top - 95, "information and is being", size=10, align="center")
    text(x_copy + right_copy_w / 2, y_tin_top - 112, "furnished to the IRS. If you are", size=10, align="center")
    text(x_copy + right_copy_w / 2, y_tin_top - 129, "required to file a return,", size=10, align="center")
    text(x_copy + right_copy_w / 2, y_tin_top - 146, "a negligence penalty or other", size=10, align="center")
    text(x_copy + right_copy_w / 2, y_tin_top - 163, "sanction may be imposed on", size=10, align="center")
    text(x_copy + right_copy_w / 2, y_tin_top - 180, "you if this income is taxable", size=10, align="center")
    text(x_copy + right_copy_w / 2, y_tin_top - 197, "and the IRS determines that it", size=10, align="center")
    text(x_copy + right_copy_w / 2, y_tin_top - 214, "has not been reported.", size=10, align="center")

    # TIN row left
    payer_tin_w = left_info_w * 0.5
    recipient_tin_w = left_info_w - payer_tin_w

    rect(x_info, y_tin_bottom, payer_tin_w, tin_h, fill_color=fill_gray)
    rect(x_info + 1, y_tin_bottom + 1, payer_tin_w - 2, 20, fill_color=fill_blue, stroke=0)
    text(x_info + 8, y_tin_bottom + 27, "PAYER'S TIN", size=10)
    text(x_info + 8, y_tin_bottom + 8, payer_ein, size=14, bold=True)

    rect(x_info + payer_tin_w, y_tin_bottom, recipient_tin_w, tin_h, fill_color=fill_gray)
    rect(x_info + payer_tin_w + 1, y_tin_bottom + 1, recipient_tin_w - 2, 20, fill_color=fill_blue, stroke=0)
    text(x_info + payer_tin_w + 8, y_tin_bottom + 27, "RECIPIENT'S TIN", size=10)
    text(x_info + payer_tin_w + 8, y_tin_bottom + 8, recipient_tin, size=14, bold=True)

    # Box 1
    rect(x_tax, y_tin_bottom, center_tax_w, tin_h, fill_color=fill_gray)
    rect(x_tax + 1, y_tin_bottom + 1, center_tax_w - 2, 20, fill_color=fill_blue, stroke=0)
    text(x_tax + 10, y_tin_bottom + 28, "1 Nonemployee compensation", size=10)
    text(x_tax + 7, y_tin_bottom + 5, "$", size=16, bold=True)
    text(x_tax + center_tax_w - 10, y_tin_bottom + 7, money(nonemployee_comp), size=16, bold=True, align="right")

    # Recipient name
    y_name_bottom = y_tin_bottom - name_h
    rect(x_info, y_name_bottom, left_info_w, name_h, fill_color=fill_gray)
    rect(x_info + 1, y_name_bottom + 1, left_info_w - 2, name_h - 23, fill_color=fill_blue, stroke=0)
    text(x_info + 8, y_tin_bottom - 18, "RECIPIENT'S name", size=10)
    multiline(x_info + 10, y_name_bottom + 36, recipient_name, size=11, bold=True, max_width=left_info_w - 20, max_lines=2)

    # Box 2
    rect(x_tax, y_name_bottom, center_tax_w, box2_h, fill_color=fill_gray)
    text(x_tax + 10, y_name_bottom + 37, "2 Payer made direct sales totaling $5,000 or more of", size=9.2)
    text(x_tax + 10, y_name_bottom + 18, "consumer products to recipient for resale", size=9.2)
    cb_size = 18
    cb_x = x_tax + center_tax_w - 30
    cb_y = y_name_bottom + 14
    rect(cb_x, cb_y, cb_size, cb_size, fill_color=fill_blue, stroke=1)
    if direct_sales_checked:
        text(cb_x + cb_size / 2, cb_y + 4, "X", size=13, bold=True, align="center")

    # Street row
    y_street_bottom = y_name_bottom - street_h
    rect(x_info, y_street_bottom, left_info_w, street_h, fill_color=fill_gray)
    rect(x_info + 1, y_street_bottom + 1, left_info_w - 2, street_h - 23, fill_color=fill_blue, stroke=0)
    text(x_info + 8, y_name_bottom - 18, "Street address (including apt. no.)", size=10)
    multiline(
        x_info + 10,
        y_street_bottom + 28,
        " ".join([p for p in [recipient_addr_1, recipient_addr_2] if clean(p)]).strip(),
        size=10.5,
        max_width=left_info_w - 20,
        max_lines=2,
    )

    # Box 3
    y_box3_bottom = y_street_bottom - box3_h
    rect(x_tax, y_box3_bottom, center_tax_w, box3_h, fill_color=fill_dark)
    text(x_tax + 12, y_box3_bottom + 30, "3", size=13, bold=True)

    # City/state row
    y_city_bottom = y_box3_bottom - city_h
    rect(x_info, y_city_bottom, left_info_w, city_h, fill_color=fill_gray)
    rect(x_info + 1, y_city_bottom + 1, left_info_w - 2, city_h - 23, fill_color=fill_blue, stroke=0)
    text(x_info + 8, y_box3_bottom - 18, "City or town, state or province, country, and ZIP or foreign postal code", size=9)
    multiline(
        x_info + 10,
        y_city_bottom + 28,
        " ".join(
            p for p in [
                f"{recipient_city}," if clean(recipient_city) else "",
                recipient_state,
                recipient_zip,
            ] if clean(p)
        ).strip(),
        size=10.5,
        max_width=left_info_w - 20,
        max_lines=2,
    )

    # Box 4
    y_box4_bottom = y_city_bottom - box4_h
    rect(x_tax, y_box4_bottom, center_tax_w, box4_h, fill_color=fill_gray, lw=2.2)
    rect(x_tax + 1, y_box4_bottom + 1, center_tax_w - 2, 24, fill_color=fill_blue, stroke=0)
    text(x_tax + 12, y_box4_bottom + 34, "4 Federal income tax withheld", size=11, bold=True)
    text(x_tax + 7, y_box4_bottom + 5, "$", size=16, bold=True)
    text(x_tax + center_tax_w - 10, y_box4_bottom + 7, money(federal_withholding), size=16, bold=True, align="right")

    # Account row
    y_account_bottom = y_box4_bottom - account_h
    rect(x_info, y_account_bottom, left_info_w, account_h, fill_color=fill_gray)
    rect(x_info + 1, y_account_bottom + 1, left_info_w - 2, account_h - 18, fill_color=fill_blue, stroke=0)
    text(x_info + 8, y_box4_bottom - 18, "Account number (see instructions)", size=10)
    text(x_info + 10, y_account_bottom + 9, account_number, size=10.5)

    # State rows
    state_row_h = state_h / 2.0
    col5_w = 84
    col6_w = 122
    col7_w = center_tax_w - col5_w - col6_w

    def draw_state_row(y_bottom, tax_val, code_val, income_val, dotted=False):
        rect(x_tax, y_bottom, col5_w, state_row_h, fill_color=fill_gray)
        rect(x_tax + col5_w, y_bottom, col6_w, state_row_h, fill_color=fill_gray)
        rect(x_tax + col5_w + col6_w, y_bottom, col7_w, state_row_h, fill_color=fill_gray)

        if not dotted:
            text(x_tax + 6, y_bottom + state_row_h - 15, "5 State tax withheld", size=8)
            text(x_tax + col5_w + 6, y_bottom + state_row_h - 15, "6 State/Payer's state no.", size=8)
            text(x_tax + col5_w + col6_w + 6, y_bottom + state_row_h - 15, "7 State income", size=8)

        if dotted:
            hline(x_tax + 14, y_bottom + 14, x_tax + col5_w - 8)
            hline(x_tax + col5_w + 14, y_bottom + 14, x_tax + col5_w + col6_w - 8)
            hline(x_tax + col5_w + col6_w + 14, y_bottom + 14, x_tax + center_tax_w - 8)

        text(x_tax + 4, y_bottom + 2, "$", size=15)
        if tax_val:
            text(x_tax + col5_w - 6, y_bottom + 5, money(tax_val), size=10.5, align="right")

        state_code_line = " ".join([p for p in [code_val[0], code_val[1]] if clean(p)]).strip()
        if state_code_line:
            text(x_tax + col5_w + 6, y_bottom + 5, state_code_line, size=9.2, max_width=col6_w - 10)

        text(x_tax + col5_w + col6_w + 4, y_bottom + 2, "$", size=15)
        if income_val:
            text(x_tax + center_tax_w - 6, y_bottom + 5, money(income_val), size=10.5, align="right")

    draw_state_row(y_account_bottom, state_tax_withheld, (state_code, payer_state_no), state_income, dotted=False)
    draw_state_row(y_account_bottom - state_row_h, state2_tax, (state2_code, state2_payer_no), state2_income, dotted=True)

    # Footer
    footer_top = outer_y + footer_h
    hline(outer_x, footer_top, x_right)
    text(outer_x, outer_y + 7, "Form", size=11)
    text(outer_x + 34, outer_y + 6, "1099-NEC", size=17, bold=True)
    text(outer_x + 152, outer_y + 7, "(Rev. 1-2022)", size=11)
    text(outer_x + 280, outer_y + 7, "(keep for your records)", size=11)
    text(outer_x + 520, outer_y + 7, "www.irs.gov/Form1099NEC", size=11)
    text(x_right - 2, outer_y + 7, "Department of the Treasury - Internal Revenue Service", size=11, align="right")

    pdf.showPage()
    pdf.save()

    data = buffer.getvalue()
    buffer.close()
    return data

# ===============================
# 🔥 1099 CENTER PAGE
# ===============================
@settings_bp.route("/settings/1099")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def settings_1099():

    cid = session["company_id"]
    year = (request.args.get("year") or str(datetime.utcnow().year)).strip()
    if not year.isdigit():
        year = str(datetime.utcnow().year)

    conn = get_db_connection()

    contractors = conn.execute("""
        SELECT id, name, email
        FROM customers
        WHERE company_id = %s
        ORDER BY name ASC
    """, (cid,)).fetchall()

    rows_html = ""
    mobile_cards = ""

    for c in contractors:
        total = conn.execute("""
            SELECT COALESCE(SUM(ip.amount), 0) AS total
            FROM invoice_payments ip
            JOIN invoices i ON ip.invoice_id = i.id
            WHERE ip.company_id = %s
              AND i.company_id = %s
              AND i.customer_id = %s
        """, (cid, cid, c["id"])).fetchone()

        total_amt = float(total["total"] or 0)
        contractor_name = escape(c["name"] or "Unnamed Contractor")
        contractor_email = escape(c["email"] or "-")

        print_btn = f"""
        <a class='btn small' target='_blank'
           href='{url_for("settings.print_1099", contractor_id=c["id"], year=year)}'>
           Print 1099
        </a>
        """

        rows_html += f"""
        <tr>
            <td>{contractor_name}</td>
            <td>{contractor_email}</td>
            <td>${total_amt:,.2f}</td>
            <td>{print_btn}</td>
        </tr>
        """

        mobile_cards += f"""
        <div class='mobile-list-card'>
            <div class='mobile-list-top'>
                <div>
                    <div class='mobile-list-title'>{contractor_name}</div>
                    <div class='mobile-list-subtitle'>{contractor_email}</div>
                </div>
            </div>

            <div class='mobile-list-grid'>
                <div>
                    <span>Total Paid</span>
                    <strong>${total_amt:,.2f}</strong>
                </div>
            </div>

            <div class='mobile-list-actions'>
                {print_btn}
            </div>
        </div>
        """

    conn.close()

    content = f"""
    <style>
        .desktop-only {{
            display: block;
        }}

        .mobile-only {{
            display: none;
        }}

        .table-wrap {{
            width: 100%;
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
        }}

        .mobile-list {{
            display: grid;
            gap: 12px;
        }}

        .mobile-list-card {{
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 14px;
            padding: 14px;
            background: #fff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }}

        .mobile-list-top {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 10px;
            margin-bottom: 10px;
        }}

        .mobile-list-title {{
            font-weight: 700;
            color: #0f172a;
            line-height: 1.25;
            word-break: break-word;
            font-size: 1rem;
        }}

        .mobile-list-subtitle {{
            margin-top: 4px;
            font-size: .9rem;
            color: #64748b;
            line-height: 1.25;
            word-break: break-word;
        }}

        .mobile-list-grid {{
            display: grid;
            grid-template-columns: 1fr;
            gap: 10px 12px;
            margin-bottom: 12px;
        }}

        .mobile-list-grid span {{
            display: block;
            font-size: .78rem;
            color: #64748b;
            margin-bottom: 3px;
        }}

        .mobile-list-grid strong {{
            display: block;
            color: #0f172a;
            font-size: .95rem;
            line-height: 1.25;
            word-break: break-word;
        }}

        .mobile-list-actions {{
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            align-items: center;
        }}

        @media (max-width: 640px) {{
            .desktop-only {{
                display: none !important;
            }}

            .mobile-only {{
                display: block !important;
            }}

            .mobile-list-actions .btn,
            .mobile-list-actions a,
            .mobile-list-actions button {{
                width: 100%;
                text-align: center;
            }}
        }}
    </style>

    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1>1099 Center</h1>
                <p class='muted' style='margin:0;'>Generate 1099 forms for contractors.</p>
            </div>
            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("settings.settings")}'>Back to Settings</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <div class='table-wrap desktop-only'>
            <table style='width:100%'>
                <thead>
                    <tr>
                        <th>Contractor</th>
                        <th>Email</th>
                        <th>Total Paid</th>
                        <th>Form</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html or "<tr><td colspan='4' class='muted'>No contractors found</td></tr>"}
                </tbody>
            </table>
        </div>

        <div class='mobile-only'>
            <div class='mobile-list'>
                {mobile_cards or "<div class='mobile-list-card muted'>No contractors found</div>"}
            </div>
        </div>
    </div>
    """

    return render_page(content, "1099 Center")


# ===============================
# 🔥 PRINT 1099
# ===============================
@settings_bp.route("/settings/1099/<int:contractor_id>/print")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def print_1099(contractor_id):

    cid = session["company_id"]
    year = request.args.get("year") or str(datetime.utcnow().year)

    conn = get_db_connection()

    contractor = conn.execute("""
        SELECT *
        FROM customers
        WHERE id = %s AND company_id = %s
    """, (contractor_id, cid)).fetchone()

    if not contractor:
        conn.close()
        flash("Contractor not found.")
        return redirect(url_for("settings.settings_1099"))

    total = conn.execute("""
        SELECT COALESCE(SUM(ip.amount), 0) AS total
        FROM invoice_payments ip
        JOIN invoices i ON ip.invoice_id = i.id
        WHERE ip.company_id = %s
          AND i.company_id = %s
          AND i.customer_id = %s
    """, (cid, cid, contractor_id)).fetchone()

    conn.close()

    summary = {
        "nonemployee_comp": float(total["total"] or 0)
    }

    company_profile = get_company_profile(cid)

    pdf_data = _build_1099_pdf(
        company_profile,
        year,
        contractor,
        summary
    )

    response = make_response(pdf_data)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f"inline; filename=1099_{contractor_id}_{year}.pdf"

    return response


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
                    <h3>Year-End / W-2 / 1099</h3>
                    <span class="settings-badge">Payroll</span>
                </div>
                <p class="muted">Review yearly payroll totals, manage W-2 company filing settings, generate W-3s, and access 1099 tools.</p>
                <div class="settings-actions">
                    <a class="btn success" href="{url_for('settings.settings_w2')}">Open Year-End Center</a>
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
    mobile_cards = ""

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
            f"<a class='btn secondary small' target='_blank' href='{url_for('settings.print_w2_summary', employee_id=employee_id, year=year)}'>Employee Copies</a>"
            if has_payroll_data and employee_id
            else "<span class='muted'>No data</span>"
        )

        mobile_button = (
            f"<a class='btn secondary small' target='_blank' href='{url_for('settings.print_w2_summary', employee_id=employee_id, year=year)}'>Employee Copies</a>"
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

        mobile_cards += f"""
        <div class='mobile-list-card'>
            <div class='mobile-list-top'>
                <div class='mobile-list-title'>{employee_name}</div>
            </div>

            <div class='mobile-list-grid'>
                <div><span>Wages</span><strong>${gross_pay:,.2f}</strong></div>
                <div><span>Federal</span><strong>${federal_withholding:,.2f}</strong></div>
                <div><span>SS</span><strong>${social_security_tax:,.2f}</strong></div>
                <div><span>Medicare</span><strong>${medicare_tax:,.2f}</strong></div>
                <div><span>State</span><strong>${state_withholding:,.2f}</strong></div>
                <div><span>Local</span><strong>${local_tax:,.2f}</strong></div>
            </div>

            <div class='mobile-list-actions'>
                {mobile_button}
            </div>
        </div>
        """

    if not rows:
        rows = """
        <tr>
            <td colspan="8" class="muted" style="text-align:center; padding:18px;">
                No employee W-2 data found for this year.
            </td>
        </tr>
        """

    if not mobile_cards:
        mobile_cards = "<div class='mobile-list-card muted'>No employee W-2 data found for this year.</div>"

    if company_readiness.get("missing"):
        missing_html = "".join(
            f"<li>{escape(str(item))}</li>" for item in company_readiness["missing"]
        )
        readiness_card = f"""
        <div class='card' style='border:1px solid #f59e0b; background:#fffaf0;'>
            <h2>W-2 Filing Readiness</h2>
            <ul>{missing_html}</ul>
            <a class='btn warning' href='{url_for("settings.settings_w2_company")}'>Fix Issues</a>
        </div>
        """
    else:
        readiness_card = """
        <div class='card' style='border:1px solid #16a34a; background:#f0fdf4;'>
            <h2>W-2 Filing Readiness</h2>
            <p style='color:#166534; font-weight:700;'>Ready to file</p>
        </div>
        """

    content = f"""
    <style>
        .desktop-only {{
            display: block;
        }}

        .mobile-only {{
            display: none;
        }}

        .table-wrap {{
            width: 100%;
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
        }}

        .w2-summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 10px;
            margin-top: 20px;
        }}

        .w2-summary-card {{
            border: 1px solid #e5e7eb;
            border-radius: 12px;
            padding: 14px;
            background: #f8fafc;
        }}

        .w2-summary-card .label {{
            font-size: .9rem;
            color: #666;
            margin-bottom: 6px;
        }}

        .w2-summary-card .value {{
            font-size: 1.15rem;
            font-weight: 700;
            color: #0f172a;
            word-break: break-word;
        }}

        .mobile-list {{
            display: grid;
            gap: 12px;
        }}

        .mobile-list-card {{
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 14px;
            padding: 14px;
            background: #fff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }}

        .mobile-list-top {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 10px;
            margin-bottom: 10px;
        }}

        .mobile-list-title {{
            font-weight: 700;
            color: #0f172a;
            line-height: 1.25;
            word-break: break-word;
            font-size: 1rem;
        }}

        .mobile-list-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px 12px;
            margin-bottom: 12px;
        }}

        .mobile-list-grid span {{
            display: block;
            font-size: .78rem;
            color: #64748b;
            margin-bottom: 3px;
        }}

        .mobile-list-grid strong {{
            display: block;
            color: #0f172a;
            font-size: .95rem;
            line-height: 1.25;
            word-break: break-word;
        }}

        .mobile-list-actions {{
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            align-items: center;
        }}

        .w2-tools-row {{
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            align-items: end;
        }}

        @media (max-width: 640px) {{
            .desktop-only {{
                display: none !important;
            }}

            .mobile-only {{
                display: block !important;
            }}

            .mobile-list-grid {{
                grid-template-columns: 1fr;
            }}

            .mobile-list-actions .btn,
            .mobile-list-actions a,
            .mobile-list-actions button {{
                width: 100%;
                text-align: center;
            }}

            .w2-tools-row {{
                width: 100%;
            }}

            .w2-tools-row .btn,
            .w2-tools-row a,
            .w2-tools-row button {{
                width: 100%;
                text-align: center;
            }}
        }}
    </style>

    <div class='card'>
        <h1>Year-End Center</h1>
        <p class='muted'>Manage W-2s, W-3s, 1099s, filings, and year-end payroll reports.</p>
    </div>

    {readiness_card}

    <div class='card'>
        <form method='get' style='display:flex; gap:10px; flex-wrap:wrap; align-items:end;'>

            <div>
                <label>Year</label>
                <input name='year' value='{year}' style='width:100px;'>
            </div>

            <div class='w2-tools-row' style='position:relative;'>

                <button type='submit' class='btn secondary'>Load Year</button>

                <a target='_blank'
                   href='{url_for("settings.print_all_w2_summaries", year=year)}'
                   class='btn success'>
                   Print W-2 Totals
                </a>

                <div style='position:relative;'>

                    <button type='button'
                            class='btn warning'
                            onclick="toggleW2Dropdown()">
                        Filing Tools ▾
                    </button>

                    <div id='w2Dropdown'
                         style='display:none; position:absolute; right:0; top:110%; background:white; border:1px solid #e5e7eb; border-radius:10px; box-shadow:0 10px 25px rgba(0,0,0,.08); min-width:200px; z-index:1000;'>

                        <a target='_blank'
                           href='{url_for("settings.print_w3", year=year)}'
                           style='display:block; padding:10px;'>Print W-3</a>

                        <a target='_blank'
                           href='{url_for("settings.export_ssa", year=year)}'
                           style='display:block; padding:10px;'>Export SSA File</a>

                        <a href='{url_for("settings.settings_w2_company")}'
                           style='display:block; padding:10px;'>Company Profile</a>

                        <a href='{url_for("settings.settings_1099", year=year)}'
                           style='display:block; padding:10px;'>Open 1099 Center</a>

                    </div>
                </div>

            </div>
        </form>

        <div class='w2-summary-grid'>
            <div class='w2-summary-card'><div class='label'>Wages</div><div class='value'>${total_wages:,.2f}</div></div>
            <div class='w2-summary-card'><div class='label'>Federal</div><div class='value'>${total_federal:,.2f}</div></div>
            <div class='w2-summary-card'><div class='label'>SS</div><div class='value'>${total_ss:,.2f}</div></div>
            <div class='w2-summary-card'><div class='label'>Medicare</div><div class='value'>${total_medicare:,.2f}</div></div>
            <div class='w2-summary-card'><div class='label'>State</div><div class='value'>${total_state:,.2f}</div></div>
            <div class='w2-summary-card'><div class='label'>Local</div><div class='value'>${total_local:,.2f}</div></div>
        </div>
    </div>

    <div class='card'>
        <h2>Employees</h2>

        <div class='table-wrap desktop-only'>
            <table>
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Wages</th>
                        <th>Federal</th>
                        <th>SS</th>
                        <th>Medicare</th>
                        <th>State</th>
                        <th>Local</th>
                        <th></th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        </div>

        <div class='mobile-only'>
            <div class='mobile-list'>
                {mobile_cards}
            </div>
        </div>
    </div>

    <script>
    function toggleW2Dropdown() {{
        const el = document.getElementById('w2Dropdown');
        el.style.display = (el.style.display === 'block') ? 'none' : 'block';
    }}

    document.addEventListener('click', function(e) {{
        if (!e.target.closest('.w2-tools-row')) {{
            const el = document.getElementById('w2Dropdown');
            if (el) el.style.display = 'none';
        }}
    }});
    </script>
    """

    return render_page(content, "Year-End Center")


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
        SELECT *
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

    company_profile = get_company_profile(cid)

    summary_data = {
        "wages": float((summary or {}).get("wages", 0) or 0),
        "federal_withholding": float((summary or {}).get("federal_withholding", 0) or 0),
        "social_security": float((summary or {}).get("social_security", 0) or 0),
        "medicare": float((summary or {}).get("medicare", 0) or 0),
        "state_withholding": float((summary or {}).get("state_withholding", 0) or 0),
        "local_tax": float((summary or {}).get("local_tax", 0) or 0),
    }

    pdf_data = _build_w2_summary_pdf(
        company_profile=company_profile,
        tax_year=year,
        employee_record=employee,
        summary=summary_data,
    )

    employee_name = _first_nonempty(
        _record_get(employee, "full_name"),
        f"{_record_get(employee, 'first_name')} {_record_get(employee, 'last_name')}".strip(),
        f"Employee #{employee_id}",
    )

    safe_employee_name = "".join(
        ch if ch.isalnum() or ch in ("-", "_") else "_"
        for ch in employee_name.replace(" ", "_")
    ).strip("_") or f"employee_{employee_id}"

    response = make_response(pdf_data)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = (
        f"inline; filename=w2_employee_copies_{safe_employee_name}_{year}.pdf"
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
    response.headers["Content-Disposition"] = f"inline; filename=w2_totals_report_{year}.pdf"
    return response

@settings_bp.route("/settings/w2/print-w3")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def print_w3():
    cid = session["company_id"]
    year = (request.args.get("year") or str(datetime.utcnow().year)).strip()
    if not year.isdigit():
        year = str(datetime.utcnow().year)

    conn = get_db_connection()
    try:
        year_summary = get_company_w2_year_summary(conn, cid, year) or {}
        employee_summaries = list_employee_w2_summaries(conn, cid, year) or []
    finally:
        conn.close()

    company_profile = get_company_profile(cid)

    totals = {
        "total_forms": len([row for row in employee_summaries if row.get("has_payroll_data")]),
        "wages": float(year_summary.get("total_wages", 0) or 0),
        "federal_withholding": float(year_summary.get("total_federal_withholding", 0) or 0),
        "social_security_wages": float(year_summary.get("total_social_security_wages", year_summary.get("total_wages", 0)) or 0),
        "social_security": float(year_summary.get("total_social_security_tax", 0) or 0),
        "medicare_wages": float(year_summary.get("total_medicare_wages", year_summary.get("total_wages", 0)) or 0),
        "medicare": float(year_summary.get("total_medicare_tax", 0) or 0),
        "social_security_tips": float(year_summary.get("total_social_security_tips", 0) or 0),
        "allocated_tips": float(year_summary.get("total_allocated_tips", 0) or 0),
        "dependent_care_benefits": float(year_summary.get("total_dependent_care_benefits", 0) or 0),
        "nonqualified_plans": float(year_summary.get("total_nonqualified_plans", 0) or 0),
        "deferred_compensation": float(year_summary.get("total_deferred_compensation", 0) or 0),
        "state_wages": float(year_summary.get("total_state_wages", year_summary.get("total_wages", 0)) or 0),
        "state_withholding": float(year_summary.get("total_state_withholding", 0) or 0),
        "local_wages": float(year_summary.get("total_local_wages", 0) or 0),
        "local_tax": float(year_summary.get("total_local_tax", 0) or 0),
    }

    pdf_data = _build_w3_pdf(company_profile, year, totals)

    response = make_response(pdf_data)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f"inline; filename=w3_summary_{year}.pdf"
    return response

@settings_bp.route("/settings/w2/export-ssa")
@login_required
@subscription_required
@require_permission("can_manage_settings")
def export_ssa():
    cid = session["company_id"]
    year = request.args.get("year") or str(datetime.utcnow().year)

    conn = get_db_connection()

    rows = conn.execute("""
        SELECT
            e.id as employee_id,
            CONCAT(e.first_name, ' ', e.last_name) as employee_name,
            COALESCE(SUM(p.gross_pay), 0) as wages,
            COALESCE(SUM(p.federal_withholding), 0) as federal_withholding,
            COALESCE(SUM(p.social_security), 0) as social_security,
            COALESCE(SUM(p.medicare), 0) as medicare
        FROM employees e
        LEFT JOIN payroll_entries p
            ON p.employee_id = e.id
            AND p.company_id = %s
        WHERE e.company_id = %s
        GROUP BY e.id
    """, (cid, cid)).fetchall()

    conn.close()

    export_data = _build_ssa_export(cid, year, rows)

    response = make_response(export_data)
    response.headers["Content-Type"] = "application/json"
    response.headers["Content-Disposition"] = f"attachment; filename=ssa_export_{year}.json"
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
    csrf_token = generate_csrf()

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
            <input type="hidden" name="csrf_token" value="{csrf_token}">
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
    csrf_token = generate_csrf()

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
            <input type="hidden" name="csrf_token" value="{csrf_token}">
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
            <input type="hidden" name="csrf_token" value="{csrf_token}">
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

    csrf_token = generate_csrf()

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
            <input type="hidden" name="csrf_token" value="{csrf_token}">
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