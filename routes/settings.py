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
from page_helpers import render_page, csrf_input
from utils.emailing import send_company_email
from utils.backups import create_company_backup, export_company_backup_data, load_backup_file, restore_company_backup

settings_bp = Blueprint("settings", __name__)

ALLOWED_LOGO_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp", "svg"}


def _lang():
    return str(session.get("language") or "en").strip().lower()


def _is_es():
    return _lang() == "es"


def _t(en, es):
    return es if _is_es() else en


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

        draw_text(left + 2, top - 8, copy_title, size=5.0, bold=True, max_width=190)
        draw_text(left + form_w - 36, top - 8, str(tax_year), size=7.8, bold=True, align="right")
        draw_text(left + form_w - 2, top - 8, "OMB No. 1545-0008", size=5.0, align="right")
        pdf.line(left, top - 12, left + form_w, top - 12)

        row_h = 20
        small_h = 18

        box(left,       top - 12, 82, row_h, "a Employee's social sec. no.", employee_ssn, 6.2, False, "center")
        box(left + 82,  top - 12, 64, row_h, "1 Wages, tips, other compensation", f"{wages:,.2f}", 6.2, False, "right")
        box(left + 146, top - 12, 64, row_h, "2 Federal income tax withheld", f"{federal:,.2f}", 6.2, False, "right")
        box(left + 210, top - 12, 62, row_h, "", "", 6.2)

        box(left,       top - 32, 82, row_h, "b Employer's ID number (EIN)", employer_ein, 6.2)
        box(left + 82,  top - 32, 64, row_h, "3 Social Security Wages", f"{social_security_wages:,.2f}", 6.2, False, "right")
        box(left + 146, top - 32, 64, row_h, "4 Social security tax withheld", f"{social_security_tax:,.2f}", 6.2, False, "right")
        box(left + 210, top - 32, 62, row_h, "", "", 6.2)

        box(left,       top - 52, 82, row_h, "(EIN)", "", 6.2)
        box(left + 82,  top - 52, 64, row_h, "5 Medicare wages and tips", f"{medicare_wages:,.2f}", 6.2, False, "right")
        box(left + 146, top - 52, 64, row_h, "6 Medicare tax withheld", f"{medicare_tax:,.2f}", 6.2, False, "right")
        box(left + 210, top - 52, 62, row_h, "", "", 6.2)

        box(left, top - 72, 272, 42, "c Employer's name, address, and ZIP code", "", 6.2)
        draw_text(left + 5, top - 86, employer_name, size=6.1, max_width=262)
        draw_text(left + 5, top - 98, employer_addr_line, size=6.1, max_width=262)
        draw_text(left + 5, top - 110, employer_city_state_zip, size=6.1, max_width=262)

        box(left, top - 114, 272, 17, "d Control number", clean(summary.get("control_number")), 6.0)

        box(left, top - 131, 272, 42, "e Employee's name, address, and ZIP code", "", 6.2)
        draw_text(left + 5, top - 145, employee_name_line, size=6.1, max_width=262)
        draw_text(left + 5, top - 157, employee_addr_line, size=6.1, max_width=262)
        draw_text(left + 5, top - 169, employee_city_state_zip, size=6.1, max_width=262)

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

        footer_top = top - 263
        box(left,       footer_top, 26, 16, "15 State", employer_state, 5.7, False, "center", 4.0)
        box(left + 26,  footer_top, 70, 16, "Employer's state ID number", employer_state_id, 5.3, False, "left", 4.0)
        box(left + 96,  footer_top, 60, 16, "16 State wages, tips, etc.", f"{state_wages:,.2f}", 5.7, False, "right", 4.0)
        box(left + 156, footer_top, 42, 16, "17 State income tax", f"{state_tax:,.2f}" if state_tax else "", 5.7, False, "right", 4.0)
        box(left + 198, footer_top, 38, 16, "18 Local wages, tips, etc.", f"{local_wages:,.2f}" if local_wages else "", 5.2, False, "right", 4.0)
        box(left + 236, footer_top, 36, 16, "19 Local income tax", f"{local_tax:,.2f}" if local_tax else "", 5.2, False, "right", 4.0)

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
        pdf.drawString(36, height - 36, _t("W-2 Employee Totals Report", "Reporte total de empleados W-2"))

        pdf.setFont("Helvetica", 9)
        pdf.drawString(36, height - 52, f"{_t('Company', 'Empresa')}: {text(company_name)}")
        pdf.drawString(36, height - 64, f"{_t('Tax Year', 'Año fiscal')}: {text(tax_year)}")
        pdf.drawRightString(width - 36, height - 52, f"{_t('Page', 'Página')} {page_no}")

        table_top = height - 84
        left = 36
        right = width - 36

        pdf.setLineWidth(1)
        pdf.line(left, table_top, right, table_top)

        header_y = table_top - 14
        pdf.setFont("Helvetica-Bold", 8.2)
        pdf.drawString(40, header_y, _t("Employee", "Empleado"))
        pdf.drawRightString(315, header_y, _t("Box 1 Wages", "Casilla 1 Salarios"))
        pdf.drawRightString(385, header_y, _t("Box 2 Federal", "Casilla 2 Federal"))
        pdf.drawRightString(450, header_y, _t("Box 4 SS Tax", "Casilla 4 Imp. SS"))
        pdf.drawRightString(515, header_y, _t("Box 6 Medicare", "Casilla 6 Medicare"))
        pdf.drawRightString(575, header_y, _t("Box 17 State", "Casilla 17 Estatal"))
        pdf.drawRightString(620, header_y, _t("Box 19 Local", "Casilla 19 Local"))

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
    pdf.drawString(40, y, _t("Totals", "Totales"))
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
        (_t("Legal Business Name", "Nombre legal del negocio"), values.get("legal_name")),
        ("EIN", values.get("ein")),
        (_t("Address Line 1", "Dirección línea 1"), values.get("address_line_1")),
        (_t("City", "Ciudad"), values.get("city")),
        (_t("State", "Estado"), values.get("state")),
        (_t("ZIP Code", "Código postal"), values.get("zip_code")),
        (_t("W-2 Contact Name", "Nombre de contacto W-2"), values.get("w2_contact_name")),
        (_t("W-2 Contact Phone", "Teléfono de contacto W-2"), values.get("w2_contact_phone")),
        (_t("W-2 Contact Email", "Correo de contacto W-2"), values.get("w2_contact_email")),
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
    from reportlab.lib.colors import black, white, HexColor

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=landscape(letter))
    page_w, page_h = landscape(letter)

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

    def fit_text(text_value, font_name, font_size, max_width):
        text_value = clean(text_value)
        if not text_value:
            return ""
        while text_value and pdf.stringWidth(text_value, font_name, font_size) > max_width:
            text_value = text_value[:-1].rstrip()
        return text_value

    def draw_text(x, y, value, size=9, bold=False, align="left", max_width=None):
        value = clean(value)
        if not value:
            return
        font_name = "Helvetica-Bold" if bold else "Helvetica"
        if max_width:
            value = fit_text(value, font_name, size, max_width)
        pdf.setFont(font_name, size)
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
        if not words:
            return []

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

        if len(lines) > max_lines:
            lines = lines[:max_lines]
            last = lines[-1]
            while last and pdf.stringWidth(last + "...", font_name, size) > max_width:
                last = last[:-1].rstrip()
            lines[-1] = (last + "...") if last else "..."
        return lines

    def draw_multiline(x, y_top, value, size=9, bold=False, max_width=120, line_gap=11, max_lines=3):
        font_name = "Helvetica-Bold" if bold else "Helvetica"
        lines = wrap_lines(value, font_name, size, max_width, max_lines=max_lines)
        pdf.setFont(font_name, size)
        for i, line in enumerate(lines):
            pdf.drawString(x, y_top - (i * line_gap), line)

    light_gray = HexColor("#efefef")
    med_gray = HexColor("#d9d9d9")
    dark_gray = HexColor("#c6c6c6")

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

    account_number = contractor.get("account_number") or contractor.get("vendor_number") or ""
    direct_sales_checked = bool(contractor.get("direct_sales"))

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

    pdf.setTitle(f"1099-NEC_{tax_year}_{recipient_name}".replace("/", "-"))
    pdf.setStrokeColor(black)
    pdf.setFillColor(black)
    pdf.setLineWidth(1)

    left_margin = 56
    bottom_margin = 78

    form_x = left_margin
    form_y = bottom_margin
    form_h = 360

    main_w = 700
    copy_w = 130
    gap_between_main_and_copy = 0

    main_x = form_x
    main_y = form_y
    main_top = main_y + form_h
    main_right = main_x + main_w

    copy_x = main_right + gap_between_main_and_copy
    copy_y = main_y
    copy_h = 240
    copy_top = copy_y + copy_h

    draw_text(page_w / 2, main_top + 82, "1099-NEC", size=28, bold=True, align="center")

    cb_size = 14
    cb_x = main_x + 335
    cb_y = main_top + 30
    rect(cb_x, cb_y, cb_size, cb_size, fill_color=white, stroke=1, lw=1)
    draw_text(cb_x + 22, cb_y + 2, "CORRECTED (if checked)", size=16)

    rect(main_x, main_y, main_w, form_h, fill_color=None, stroke=1, lw=1.2)

    top_h = 118
    top_bottom = main_top - top_h

    payer_w = 420
    gray_w = 140
    year_w = main_w - payer_w - gray_w

    payer_x = main_x
    gray_x = payer_x + payer_w
    year_x = gray_x + gray_w

    rect(payer_x, top_bottom, payer_w, top_h, fill_color=light_gray, stroke=1)
    rect(payer_x, top_bottom, payer_w, top_h - 40, fill_color=white, stroke=0)

    draw_text(
        payer_x + 8,
        main_top - 16,
        "PAYER'S name, street address, city or town, state or province, country, ZIP",
        size=8.5
    )
    draw_text(
        payer_x + 8,
        main_top - 30,
        "or foreign postal code, and telephone no.",
        size=8.5
    )

    payer_lines = []
    if payer_name:
        payer_lines.append(payer_name)
    addr_line = " ".join([p for p in [payer_addr_1, payer_addr_2] if clean(p)]).strip()
    if addr_line:
        payer_lines.append(addr_line)
    city_line = " ".join(
        p for p in [f"{payer_city}," if clean(payer_city) else "", payer_state, payer_zip] if clean(p)
    ).strip()
    if city_line:
        payer_lines.append(city_line)
    if payer_phone:
        payer_lines.append(payer_phone)

    payer_text_y = top_bottom + 62
    for i, line in enumerate(payer_lines[:4]):
        draw_text(
            payer_x + 12,
            payer_text_y - (i * 18),
            line,
            size=10,
            bold=(i == 0),
            max_width=payer_w - 24
        )

    rect(gray_x, top_bottom, gray_w, top_h, fill_color=med_gray, stroke=1)

    rect(year_x, top_bottom, year_w, top_h, fill_color=light_gray, stroke=1)
    hline(year_x, top_bottom + 46, year_x + year_w, lw=1)
    hline(year_x, top_bottom + 82, year_x + year_w, lw=1)

    draw_text(year_x + 8, main_top - 16, "OMB No. 1545-0116", size=8.5)
    draw_text(year_x + 8, main_top - 56, "Form", size=9.5)
    draw_text(year_x + 42, main_top - 56, "1099-NEC", size=14, bold=True)
    draw_text(year_x + 8, top_bottom + 14, "For calendar year", size=8.5)
    draw_text(year_x + (year_w / 2), top_bottom + 32, str(tax_year), size=28, bold=True, align="center")

    draw_text(copy_x + copy_w / 2, top_bottom + 56, "Nonemployee", size=17, bold=True, align="center")
    draw_text(copy_x + copy_w / 2, top_bottom + 26, "Compensation", size=17, bold=True, align="center")

    tin_h = 38
    recip_name_h = 62
    street_h = 52
    city_h = 52
    account_h = 34
    lower_h = form_h - top_h - tin_h - recip_name_h - street_h - city_h - account_h

    info_w = 420
    tax_w = main_w - info_w
    info_x = main_x
    tax_x = info_x + info_w

    y = top_bottom

    y -= tin_h
    payer_tin_w = info_w / 2
    recip_tin_w = info_w / 2

    rect(info_x, y, payer_tin_w, tin_h, fill_color=light_gray, stroke=1)
    draw_text(info_x + 8, y + 25, "PAYER'S TIN", size=8.5)
    draw_text(info_x + 20, y + 7, payer_ein, size=16)

    rect(info_x + payer_tin_w, y, recip_tin_w, tin_h, fill_color=light_gray, stroke=1)
    draw_text(info_x + payer_tin_w + 8, y + 25, "RECIPIENT'S TIN", size=8.5)
    draw_text(info_x + payer_tin_w + 20, y + 7, recipient_tin, size=16)

    rect(tax_x, y, tax_w, tin_h, fill_color=light_gray, stroke=1)
    draw_text(tax_x + 8, y + 25, "1 Nonemployee compensation", size=8.5)
    draw_text(tax_x + 8, y + 7, "$", size=16, bold=True)
    draw_text(tax_x + tax_w - 8, y + 8, money(nonemployee_comp), size=16, bold=True, align="right")

    rect(copy_x, y, copy_w, copy_h, fill_color=None, stroke=1, lw=1.2)
    draw_text(copy_x + copy_w - 8, y + copy_h - 18, "Copy B", size=14, bold=True, align="right")
    draw_text(copy_x + copy_w / 2, y + copy_h - 48, "For Recipient", size=14, bold=True, align="center")
    copy_lines = [
        "This is important tax",
        "information and is being",
        "furnished to the IRS. If you are",
        "required to file a return, a",
        "negligence penalty or other",
        "sanction may be imposed on",
        "you if this income is taxable",
        "and the IRS determines that it",
        "has not been reported.",
    ]
    for i, line in enumerate(copy_lines):
        draw_text(copy_x + copy_w / 2, y + copy_h - 72 - (i * 15), line, size=8.5, align="center")

    y -= recip_name_h
    rect(info_x, y, info_w, recip_name_h, fill_color=light_gray, stroke=1)
    draw_text(info_x + 8, y + recip_name_h - 18, "RECIPIENT'S name", size=8.5)
    draw_multiline(info_x + 12, y + 34, recipient_name, size=11, bold=False, max_width=info_w - 24, line_gap=13, max_lines=2)

    box2_h = 42
    rect(tax_x, y + (recip_name_h - box2_h), tax_w, box2_h, fill_color=light_gray, stroke=1)
    draw_text(tax_x + 8, y + (recip_name_h - box2_h) + 25,
              "2 Payer made direct sales totaling $5,000 or more of", size=8.2)
    draw_text(tax_x + 8, y + (recip_name_h - box2_h) + 10,
              "consumer products to recipient for resale", size=8.2)
    small_cb = 14
    rect(tax_x + tax_w - 18, y + (recip_name_h - box2_h) + 8, small_cb, small_cb, fill_color=white, stroke=1)
    if direct_sales_checked:
        draw_text(tax_x + tax_w - 11, y + (recip_name_h - box2_h) + 10, "X", size=10, bold=True, align="center")

    y -= street_h
    rect(info_x, y, info_w, street_h, fill_color=light_gray, stroke=1)
    draw_text(info_x + 8, y + street_h - 18, "Street address (including apt. no.)", size=8.5)
    draw_multiline(
        info_x + 12,
        y + 22,
        " ".join([p for p in [recipient_addr_1, recipient_addr_2] if clean(p)]).strip(),
        size=10,
        max_width=info_w - 24,
        line_gap=12,
        max_lines=2
    )

    box3_h = street_h
    rect(tax_x, y, tax_w, box3_h, fill_color=dark_gray, stroke=1)
    draw_text(tax_x + 8, y + box3_h - 18, "3", size=12, bold=True)

    y -= city_h
    rect(info_x, y, info_w, city_h, fill_color=light_gray, stroke=1)
    draw_text(
        info_x + 8,
        y + city_h - 18,
        "City or town, state or province, country, and ZIP or foreign postal code",
        size=8
    )
    city_state_zip = " ".join(
        p for p in [f"{recipient_city}," if clean(recipient_city) else "", recipient_state, recipient_zip] if clean(p)
    ).strip()
    draw_multiline(
        info_x + 12,
        y + 22,
        city_state_zip,
        size=10,
        max_width=info_w - 24,
        line_gap=12,
        max_lines=2
    )

    box4_h = 44
    rect(tax_x, y, tax_w, box4_h, fill_color=white, stroke=1, lw=2)
    draw_text(tax_x + 8, y + 27, "4 Federal income tax withheld", size=10, bold=True)
    draw_text(tax_x + 8, y + 7, "$", size=16, bold=True)
    draw_text(tax_x + tax_w - 8, y + 8, money(federal_withholding), size=16, align="right")

    y -= account_h
    rect(info_x, y, info_w, account_h, fill_color=light_gray, stroke=1)
    draw_text(info_x + 8, y + 20, "Account number (see instructions)", size=8.5)
    draw_text(info_x + 12, y + 6, account_number, size=10, max_width=info_w - 24)

    row_h = lower_h / 2.0
    col5_w = 95
    col6_w = 120
    col7_w = tax_w - col5_w - col6_w

    def draw_state_row(row_y, tax_val, code_val, payer_no_val, income_val, labels=False):
        rect(tax_x, row_y, col5_w, row_h, fill_color=light_gray, stroke=1)
        rect(tax_x + col5_w, row_y, col6_w, row_h, fill_color=light_gray, stroke=1)
        rect(tax_x + col5_w + col6_w, row_y, col7_w, row_h, fill_color=light_gray, stroke=1)

        if labels:
            draw_text(tax_x + 6, row_y + row_h - 15, "5 State tax withheld", size=8)
            draw_text(tax_x + col5_w + 6, row_y + row_h - 15, "6 State/Payer's state no.", size=8)
            draw_text(tax_x + col5_w + col6_w + 6, row_y + row_h - 15, "7 State income", size=8)

        draw_text(tax_x + 6, row_y + 6, "$", size=14)
        if tax_val:
            draw_text(tax_x + col5_w - 6, row_y + 8, money(tax_val), size=10, align="right")

        code_text = " ".join([p for p in [code_val, payer_no_val] if clean(p)]).strip()
        if code_text:
            draw_text(
                tax_x + col5_w + 6,
                row_y + 8,
                code_text,
                size=9,
                max_width=col6_w - 12
            )

        draw_text(tax_x + col5_w + col6_w + 6, row_y + 6, "$", size=14)
        if income_val:
            draw_text(tax_x + col5_w + col6_w + col7_w - 6, row_y + 8, money(income_val), size=10, align="right")

    draw_state_row(y, state_tax_withheld, state_code, payer_state_no, state_income, labels=True)
    draw_state_row(y - row_h, state2_tax, state2_code, state2_payer_no, state2_income, labels=False)

    draw_text(main_x, main_y - 14, "Form", size=9)
    draw_text(main_x + 28, main_y - 15, "1099-NEC", size=15, bold=True)
    draw_text(main_x + 150, main_y - 14, "(keep for your records)", size=9)
    draw_text(main_x + 370, main_y - 14, "www.irs.gov/Form1099NEC", size=9)
    draw_text(main_x + main_w, main_y - 14, "Department of the Treasury - Internal Revenue Service", size=9, align="right")

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
        contractor_name = escape(c["name"] or _t("Unnamed Contractor", "Contratista sin nombre"))
        contractor_email = escape(c["email"] or "-")

        print_btn = f"""
        <a class='btn small' target='_blank'
           href='{url_for("settings.print_1099", contractor_id=c["id"], year=year)}'>
           {_t("Print 1099", "Imprimir 1099")}
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
                    <span>{_t("Total Paid", "Total pagado")}</span>
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
                <h1>{_t("1099 Center", "Centro 1099")}</h1>
                <p class='muted' style='margin:0;'>{_t("Generate 1099 forms for contractors.", "Genera formularios 1099 para contratistas.")}</p>
            </div>
            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("settings.settings")}'>{_t("Back to Settings", "Volver a Configuración")}</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <div class='table-wrap desktop-only'>
            <table style='width:100%'>
                <thead>
                    <tr>
                        <th>{_t("Contractor", "Contratista")}</th>
                        <th>{_t("Email", "Correo")}</th>
                        <th>{_t("Total Paid", "Total pagado")}</th>
                        <th>{_t("Form", "Formulario")}</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html or f"<tr><td colspan='4' class='muted'>{_t('No contractors found', 'No se encontraron contratistas')}</td></tr>"}
                </tbody>
            </table>
        </div>

        <div class='mobile-only'>
            <div class='mobile-list'>
                {mobile_cards or f"<div class='mobile-list-card muted'>{_t('No contractors found', 'No se encontraron contratistas')}</div>"}
            </div>
        </div>
    </div>
    """

    return render_page(content, _t("1099 Center", "Centro 1099"))


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
        flash(_t("Contractor not found.", "Contratista no encontrado."))
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
                <h1 style="margin-bottom:6px;">{_t("Settings", "Configuración")}</h1>
                <div class="muted">{_t("Manage company information, branding, email delivery, taxes, billing, and users.", "Administra la información de la empresa, marca, envío de correos, impuestos, facturación y usuarios.")}</div>
            </div>
        </div>

        <div class="settings-grid">

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>{_t("Company Info", "Información de la empresa")}</h3>
                    <span class="settings-badge">{_t("General", "General")}</span>
                </div>
                <p class="muted">{_t("Update your company name, contact info, address, and tax ID.", "Actualiza el nombre de tu empresa, información de contacto, dirección e identificación fiscal.")}</p>
                <div class="settings-actions">
                    <a class="btn" href="{url_for('settings.settings_company')}">{_t("Open Company Info", "Abrir información de la empresa")}</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>{_t("Branding", "Marca")}</h3>
                    <span class="settings-badge">{_t("Appearance", "Apariencia")}</span>
                </div>
                <p class="muted">{_t("Manage your logo, invoice names, quote names, and document footer notes.", "Administra tu logotipo, nombres de facturas, nombres de cotizaciones y notas al pie de documentos.")}</p>
                <div class="settings-actions">
                    <a class="btn" href="{url_for('settings.settings_branding')}">{_t("Open Branding", "Abrir marca")}</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>{_t("Email Settings", "Configuración de correo")}</h3>
                    <span class="settings-badge">{_t("Delivery", "Entrega")}</span>
                </div>
                <p class="muted">{_t("Set sender identity, reply-to behavior, and send a test email.", "Configura la identidad del remitente, el comportamiento de respuesta y envía un correo de prueba.")}</p>
                <div class="settings-actions">
                    <a class="btn" href="{url_for('settings.settings_email')}">{_t("Open Email Settings", "Abrir configuración de correo")}</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>{_t("Tax Defaults", "Impuestos predeterminados")}</h3>
                    <span class="settings-badge">{_t("Financial", "Financiero")}</span>
                </div>
                <p class="muted">{_t("Set default payroll tax rates for federal, state, local, and company-side taxes.", "Configura las tasas predeterminadas de impuestos de nómina para impuestos federales, estatales, locales y del lado de la empresa.")}</p>
                <div class="settings-actions">
                    <a class="btn" href="{url_for('settings.settings_taxes')}">{_t("Configure Taxes", "Configurar impuestos")}</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>{_t("Users & Permissions", "Usuarios y permisos")}</h3>
                    <span class="settings-badge">{_t("Access", "Acceso")}</span>
                </div>
                <p class="muted">{_t("Manage employees, logins, roles, and access levels for your company.", "Administra empleados, inicios de sesión, roles y niveles de acceso para tu empresa.")}</p>
                <div class="settings-actions">
                    <a class="btn" href="{url_for('users.users')}">{_t("Open Users", "Abrir usuarios")}</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>{_t("Billing", "Facturación")}</h3>
                    <span class="settings-badge">{_t("Subscription", "Suscripción")}</span>
                </div>
                <p class="muted">{_t("Review your subscription, payment methods, and billing details.", "Revisa tu suscripción, métodos de pago y detalles de facturación.")}</p>
                <div class="settings-actions">
                    <a class="btn" href="{url_for('billing.billing_page')}">{_t("View Billing", "Ver facturación")}</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>{_t("Year-End / W-2 / 1099", "Cierre de año / W-2 / 1099")}</h3>
                    <span class="settings-badge">{_t("Payroll", "Nómina")}</span>
                </div>
                <p class="muted">{_t("Review yearly payroll totals, manage W-2 company filing settings, generate W-3s, and access 1099 tools.", "Revisa los totales anuales de nómina, administra la configuración de presentación W-2 de la empresa, genera formularios W-3 y accede a las herramientas 1099.")}</p>
                <div class="settings-actions">
                    <a class="btn success" href="{url_for('settings.settings_w2')}">{_t("Open Year-End Center", "Abrir centro de cierre de año")}</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>{_t("Backups", "Respaldos")}</h3>
                    <span class="settings-badge">{_t("Safety", "Seguridad")}</span>
                </div>
                <p class="muted">{_t("Download a full backup of your company data anytime.", "Descarga un respaldo completo de los datos de tu empresa en cualquier momento.")}</p>
                <div class="settings-actions">
                    <a class="btn warning" href="/settings/backup/download">{_t("Download Backup", "Descargar respaldo")}</a>
                </div>
            </div>

            <div class="card settings-card">
                <div class="settings-card-head">
                    <h3>{_t("Restore Backup", "Restaurar respaldo")}</h3>
                    <span class="settings-badge">{_t("Recovery", "Recuperación")}</span>
                </div>
                <p class="muted">{_t("Upload a backup file and restore your company data.", "Sube un archivo de respaldo y restaura los datos de tu empresa.")}</p>
                <div class="settings-actions">
                    <a class="btn warning" href="{url_for('settings.restore_backup')}">{_t("Open Restore", "Abrir restauración")}</a>
                </div>
            </div>
        </div>
    </div>
    """
    return render_page(settings_html, _t("Settings", "Configuración"))


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
        flash(_t("No company is associated with this account.", "No hay una empresa asociada con esta cuenta."))
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
        flash(_t("Company profile updated successfully.", "Perfil de la empresa actualizado correctamente."))
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
                <h1 style='margin-bottom:6px;'>{{ _t("Company Info", "Información de la empresa") }}</h1>
                <p class='muted' style='margin:0;'>{{ _t("Manage your main business information used across the system.", "Administra la información principal de tu negocio utilizada en todo el sistema.") }}</p>
            </div>
            <div class='row-actions'>
                <a class='btn secondary' href='{{ url_for("settings.settings") }}'>{{ _t("Back to Settings", "Volver a Configuración") }}</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <div class='grid'>
                <div>
                    <label>{{ _t("Company Name", "Nombre de la empresa") }}</label>
                    <input name='name' value='{{ clean_text_input(company["name"]) if company else "" }}'>
                </div>
                <div>
                    <label>{{ _t("Phone", "Teléfono") }}</label>
                    <input name='phone' value='{{ clean_text_input(company["phone"]) if company else "" }}'>
                </div>
                <div>
                    <label>{{ _t("Email", "Correo") }}</label>
                    <input name='email' value='{{ clean_text_input(company["email"]) if company else "" }}'>
                </div>
                <div>
                    <label>{{ _t("Website", "Sitio web") }}</label>
                    <input name='website' value='{{ clean_text_input(company["website"]) if company else "" }}'>
                </div>
                <div>
                    <label>{{ _t("Tax ID", "ID fiscal") }}</label>
                    <input name='tax_id' value='{{ clean_text_input(company["tax_id"]) if company else "" }}'>
                </div>
                <div>
                    <label>{{ _t("Address Line 1", "Dirección línea 1") }}</label>
                    <input name='address_line_1' value='{{ clean_text_input(company["address_line_1"]) if company else "" }}'>
                </div>
                <div>
                    <label>{{ _t("Address Line 2", "Dirección línea 2") }}</label>
                    <input name='address_line_2' value='{{ clean_text_input(company["address_line_2"]) if company else "" }}'>
                </div>
                <div>
                    <label>{{ _t("City", "Ciudad") }}</label>
                    <input name='city' value='{{ clean_text_input(company["city"]) if company else "" }}'>
                </div>
                <div>
                    <label>{{ _t("State", "Estado") }}</label>
                    <input name='state' value='{{ clean_text_input(company["state"]) if company else "" }}' maxlength='2'>
                </div>
                <div>
                    <label>{{ _t("County", "Condado") }}</label>
                    <input name='county' value='{{ clean_text_input(company_county) }}' placeholder='Tippecanoe'>
                </div>
                <div>
                    <label>{{ _t("Zip Code", "Código postal") }}</label>
                    <input name='zip_code' value='{{ clean_text_input(company["zip_code"]) if company else "" }}'>
                </div>
            </div>
            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>{{ _t("Save Company Info", "Guardar información de la empresa") }}</button>
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
            _t=_t,
        ),
        _t("Company Info", "Información de la empresa"),
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
        flash(_t("Tax settings saved successfully.", "La configuración de impuestos se guardó correctamente."))
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
                <h1 style='margin-bottom:6px;'>{_t("Tax Defaults", "Impuestos predeterminados")}</h1>
                <p class='muted' style='margin:0;'>{_t("Set default payroll tax rates for your company.", "Configura las tasas predeterminadas de impuestos de nómina para tu empresa.")}</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("settings.settings")}' class='btn secondary'>{_t("Back to Settings", "Volver a Configuración")}</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>{_t("Current Tax Defaults", "Impuestos predeterminados actuales")}</h2>
        <div style='display:grid; grid-template-columns:repeat(auto-fit, minmax(180px, 1fr)); gap:14px; align-items:stretch; margin-top:12px;'>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>{_t("Federal", "Federal")}</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['federal_withholding_rate']) if settings and settings['federal_withholding_rate'] is not None else 0:.2f}%</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>{_t("State", "Estatal")}</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['state_withholding_rate']) if settings and settings['state_withholding_rate'] is not None else 0:.2f}%</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>{_t("Social Security", "Seguro Social")}</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['social_security_rate']) if settings and float(settings['social_security_rate'] or 0) > 0 else 6.20:.2f}%</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>{_t("Medicare", "Medicare")}</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['medicare_rate']) if settings and float(settings['medicare_rate'] or 0) > 0 else 1.45:.2f}%</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>{_t("Local Tax", "Impuesto local")}</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['local_tax_rate']) if settings and settings['local_tax_rate'] is not None else 0:.2f}%</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>{_t("Unemployment", "Desempleo")}</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['unemployment_rate']) if settings and settings['unemployment_rate'] is not None else 0:.2f}%</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#f8fafc;'>
                <div style='font-size:.9rem; color:#666; margin-bottom:6px;'>{_t("Workers Comp", "Compensación laboral")}</div>
                <div style='font-size:1.15rem; font-weight:700;'>{float(settings['workers_comp_rate']) if settings and settings['workers_comp_rate'] is not None else 0:.2f}%</div>
            </div>

        </div>
    </div>

    <div class='card'>
        <h2>{_t("Edit Tax Defaults", "Editar impuestos predeterminados")}</h2>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <div style='display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr)); gap:16px; align-items:end;'>

                <div>
                    <label>{_t("Federal Withholding %", "% de retención federal")}</label>
                    <input type="number" step="0.01" min="0"
                        name="federal_withholding_rate"
                        value="{{ settings['federal_withholding_rate'] if settings and settings['federal_withholding_rate'] is not none else '' }}"
                        placeholder="{_t("Auto", "Automático")}">
                </div>

                <div>
                    <label>{_t("State Withholding %", "% de retención estatal")}</label>
                    <input type="number" step="0.01" min="0"
                        name="state_withholding_rate"
                        value="{{ settings['state_withholding_rate'] if settings and settings['state_withholding_rate'] is not none else '' }}"
                        placeholder="{_t("Auto", "Automático")}">
                </div>

                <div>
                    <label>{_t("Social Security %", "% de Seguro Social")}</label>
                    <input type='number' step='0.01' min='0' name='social_security_rate'
                           value='{float(settings["social_security_rate"]) if settings and settings["social_security_rate"] is not None else 6.20:.2f}'>
                </div>

                <div>
                    <label>{_t("Medicare %", "% de Medicare")}</label>
                    <input type='number' step='0.01' min='0' name='medicare_rate'
                           value='{float(settings["medicare_rate"]) if settings and settings["medicare_rate"] is not None else 1.45:.2f}'>
                </div>

                <div>
                    <label>{_t("Local Tax %", "% de impuesto local")}</label>
                    <input type='number' step='0.01' min='0' name='local_tax_rate'
                           value='{float(settings["local_tax_rate"]) if settings and settings["local_tax_rate"] is not None else 0:.2f}'>
                </div>

                <div>
                    <label>{_t("Unemployment %", "% de desempleo")}</label>
                    <input type='number' step='0.01' min='0' name='unemployment_rate'
                           value='{float(settings["unemployment_rate"]) if settings and settings["unemployment_rate"] is not None else 0:.2f}'>
                </div>

                <div>
                    <label>{_t("Workers Comp %", "% de compensación laboral")}</label>
                    <input type='number' step='0.01' min='0' name='workers_comp_rate'
                           value='{float(settings["workers_comp_rate"]) if settings and settings["workers_comp_rate"] is not None else 0:.2f}'>
                </div>

            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>{_t("Save Tax Settings", "Guardar configuración de impuestos")}</button>
            </div>
        </form>
    </div>
    """

    return render_page(
        render_template_string(tax_default_html, settings=settings),
        _t("Tax Defaults", "Impuestos predeterminados"),
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
        employee_name = escape(str(row.get("employee_name") or _t("Unnamed Employee", "Empleado sin nombre")))
        gross_pay = float(row.get("gross_pay", 0) or 0)
        federal_withholding = float(row.get("federal_withholding", 0) or 0)
        social_security_tax = float(row.get("social_security_tax", 0) or 0)
        medicare_tax = float(row.get("medicare_tax", 0) or 0)
        state_withholding = float(row.get("state_withholding", 0) or 0)
        local_tax = float(row.get("local_tax", 0) or 0)
        has_payroll_data = bool(row.get("has_payroll_data"))

        print_button = (
            f"<a class='btn secondary small' target='_blank' href='{url_for('settings.print_w2_summary', employee_id=employee_id, year=year)}'>{_t('Employee Copies', 'Copias del empleado')}</a>"
            if has_payroll_data and employee_id
            else f"<span class='muted'>{_t('No data', 'Sin datos')}</span>"
        )

        mobile_button = (
            f"<a class='btn secondary small' target='_blank' href='{url_for('settings.print_w2_summary', employee_id=employee_id, year=year)}'>{_t('Employee Copies', 'Copias del empleado')}</a>"
            if has_payroll_data and employee_id
            else f"<span class='muted'>{_t('No data', 'Sin datos')}</span>"
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
                <div><span>{_t("Wages", "Salarios")}</span><strong>${gross_pay:,.2f}</strong></div>
                <div><span>{_t("Federal", "Federal")}</span><strong>${federal_withholding:,.2f}</strong></div>
                <div><span>{_t("SS", "SS")}</span><strong>${social_security_tax:,.2f}</strong></div>
                <div><span>{_t("Medicare", "Medicare")}</span><strong>${medicare_tax:,.2f}</strong></div>
                <div><span>{_t("State", "Estatal")}</span><strong>${state_withholding:,.2f}</strong></div>
                <div><span>{_t("Local", "Local")}</span><strong>${local_tax:,.2f}</strong></div>
            </div>

            <div class='mobile-list-actions'>
                {mobile_button}
            </div>
        </div>
        """

    if not rows:
        rows = f"""
        <tr>
            <td colspan="8" class="muted" style="text-align:center; padding:18px;">
                {_t("No employee W-2 data found for this year.", "No se encontraron datos W-2 de empleados para este año.")}
            </td>
        </tr>
        """

    if not mobile_cards:
        mobile_cards = f"<div class='mobile-list-card muted'>{_t('No employee W-2 data found for this year.', 'No se encontraron datos W-2 de empleados para este año.')}</div>"

    if company_readiness.get("missing"):
        missing_html = "".join(
            f"<li>{escape(str(item))}</li>" for item in company_readiness["missing"]
        )
        readiness_card = f"""
        <div class='card' style='border:1px solid #f59e0b; background:#fffaf0;'>
            <h2>{_t("W-2 Filing Readiness", "Preparación de presentación W-2")}</h2>
            <ul>{missing_html}</ul>
            <a class='btn warning' href='{url_for("settings.settings_w2_company")}'>{_t("Fix Issues", "Corregir problemas")}</a>
        </div>
        """
    else:
        readiness_card = f"""
        <div class='card' style='border:1px solid #16a34a; background:#f0fdf4;'>
            <h2>{_t("W-2 Filing Readiness", "Preparación de presentación W-2")}</h2>
            <p style='color:#166534; font-weight:700;'>{_t("Ready to file", "Listo para presentar")}</p>
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
        <h1>{_t("Year-End Center", "Centro de cierre de año")}</h1>
        <p class='muted'>{_t("Manage W-2s, W-3s, 1099s, filings, and year-end payroll reports.", "Administra W-2, W-3, 1099, presentaciones e informes de nómina de fin de año.")}</p>
    </div>

    {readiness_card}

    <div class='card'>
        <form method='get' style='display:flex; gap:10px; flex-wrap:wrap; align-items:end;'>

            <div>
                <label>{_t("Year", "Año")}</label>
                <input name='year' value='{year}' style='width:100px;'>
            </div>

            <div class='w2-tools-row' style='position:relative;'>

                <button type='submit' class='btn secondary'>{_t("Load Year", "Cargar año")}</button>

                <a target='_blank'
                   href='{url_for("settings.print_all_w2_summaries", year=year)}'
                   class='btn success'>
                   {_t("Print W-2 Totals", "Imprimir totales W-2")}
                </a>

                <div style='position:relative;'>

                    <button type='button'
                            class='btn warning'
                            onclick="toggleW2Dropdown()">
                        {_t("Filing Tools", "Herramientas de presentación")} ▾
                    </button>

                    <div id='w2Dropdown'
                         style='display:none; position:absolute; right:0; top:110%; background:white; border:1px solid #e5e7eb; border-radius:10px; box-shadow:0 10px 25px rgba(0,0,0,.08); min-width:200px; z-index:1000;'>

                        <a target='_blank'
                           href='{url_for("settings.print_w3", year=year)}'
                           style='display:block; padding:10px;'>{_t("Print W-3", "Imprimir W-3")}</a>

                        <a target='_blank'
                           href='{url_for("settings.export_ssa", year=year)}'
                           style='display:block; padding:10px;'>{_t("Export SSA File", "Exportar archivo SSA")}</a>

                        <a href='{url_for("settings.settings_w2_company")}'
                           style='display:block; padding:10px;'>{_t("Company Profile", "Perfil de la empresa")}</a>

                        <a href='{url_for("settings.settings_1099", year=year)}'
                           style='display:block; padding:10px;'>{_t("Open 1099 Center", "Abrir centro 1099")}</a>

                    </div>
                </div>

            </div>
        </form>

        <div class='w2-summary-grid'>
            <div class='w2-summary-card'><div class='label'>{_t("Wages", "Salarios")}</div><div class='value'>${total_wages:,.2f}</div></div>
            <div class='w2-summary-card'><div class='label'>{_t("Federal", "Federal")}</div><div class='value'>${total_federal:,.2f}</div></div>
            <div class='w2-summary-card'><div class='label'>{_t("SS", "SS")}</div><div class='value'>${total_ss:,.2f}</div></div>
            <div class='w2-summary-card'><div class='label'>{_t("Medicare", "Medicare")}</div><div class='value'>${total_medicare:,.2f}</div></div>
            <div class='w2-summary-card'><div class='label'>{_t("State", "Estatal")}</div><div class='value'>${total_state:,.2f}</div></div>
            <div class='w2-summary-card'><div class='label'>{_t("Local", "Local")}</div><div class='value'>${total_local:,.2f}</div></div>
        </div>
    </div>

    <div class='card'>
        <h2>{_t("Employees", "Empleados")}</h2>

        <div class='table-wrap desktop-only'>
            <table>
                <thead>
                    <tr>
                        <th>{_t("Name", "Nombre")}</th>
                        <th>{_t("Wages", "Salarios")}</th>
                        <th>{_t("Federal", "Federal")}</th>
                        <th>{_t("SS", "SS")}</th>
                        <th>{_t("Medicare", "Medicare")}</th>
                        <th>{_t("State", "Estatal")}</th>
                        <th>{_t("Local", "Local")}</th>
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

    return render_page(content, _t("Year-End Center", "Centro de cierre de año"))


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
        flash(_t("Company W-2 profile saved.", "Perfil W-2 de la empresa guardado."))
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
            <h2>{_t("Missing Items", "Elementos faltantes")}</h2>
            <ul>{missing_html}</ul>
        </div>
        """
    else:
        readiness_block = f"""
        <div class='card' style='border:1px solid #16a34a; background:#f0fdf4;'>
            <h2>{_t("Ready", "Listo")}</h2>
            <p style='margin:0; color:#166534; font-weight:700;'>{_t("Company W-2 profile looks complete.", "El perfil W-2 de la empresa parece completo.")}</p>
        </div>
        """

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>{_t("Company W-2 Profile", "Perfil W-2 de la empresa")}</h1>
                <p class='muted' style='margin:0;'>{_t("Store the company filing details needed for year-end W-2 preparation.", "Guarda los detalles de presentación de la empresa necesarios para la preparación de W-2 de fin de año.")}</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("settings.settings_w2")}' class='btn secondary'>{_t("Back to W-2 Center", "Volver al centro W-2")}</a>
            </div>
        </div>
    </div>

    {readiness_block}

    <div class='card'>
        <h2>{_t("W-2 Filing Details", "Detalles de presentación W-2")}</h2>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{csrf_token}">
            <div class='grid'>
                <div>
                    <label>{_t("Legal Business Name", "Nombre legal del negocio")}</label>
                    <input name='legal_name' value='{escape(values["legal_name"])}' placeholder='{_t("Your legal business name", "Nombre legal de tu negocio")}'>
                </div>

                <div>
                    <label>EIN</label>
                    <input name='ein' value='{escape(values["ein"])}' placeholder='12-3456789'>
                </div>

                <div>
                    <label>{_t("State Employer ID", "ID estatal del empleador")}</label>
                    <input name='state_employer_id' value='{escape(values["state_employer_id"])}' placeholder='{_t("State employer account ID", "ID de cuenta estatal del empleador")}'>
                </div>

                <div>
                    <label>{_t("Address Line 1", "Dirección línea 1")}</label>
                    <input name='address_line_1' value='{escape(values["address_line_1"])}' placeholder='{_t("Street address", "Dirección")}'>
                </div>

                <div>
                    <label>{_t("Address Line 2", "Dirección línea 2")}</label>
                    <input name='address_line_2' value='{escape(values["address_line_2"])}' placeholder='{_t("Suite / unit / additional details", "Suite / unidad / detalles adicionales")}'>
                </div>

                <div>
                    <label>{_t("City", "Ciudad")}</label>
                    <input name='city' value='{escape(values["city"])}' placeholder='{_t("City", "Ciudad")}'>
                </div>

                <div>
                    <label>{_t("State", "Estado")}</label>
                    <input name='state' value='{escape(values["state"])}' placeholder='IN' maxlength='2'>
                </div>

                <div>
                    <label>{_t("ZIP Code", "Código postal")}</label>
                    <input name='zip_code' value='{escape(values["zip_code"])}' placeholder='47905'>
                </div>

                <div>
                    <label>{_t("W-2 Contact Name", "Nombre de contacto W-2")}</label>
                    <input name='w2_contact_name' value='{escape(values["w2_contact_name"])}' placeholder='{_t("Contact person for W-2 filing", "Persona de contacto para presentación W-2")}'>
                </div>

                <div>
                    <label>{_t("W-2 Contact Phone", "Teléfono de contacto W-2")}</label>
                    <input name='w2_contact_phone' value='{escape(values["w2_contact_phone"])}' placeholder='(765) 555-1234'>
                </div>

                <div>
                    <label>{_t("W-2 Contact Email", "Correo de contacto W-2")}</label>
                    <input name='w2_contact_email' value='{escape(values["w2_contact_email"])}' placeholder='payroll@yourcompany.com'>
                </div>
            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>{_t("Save Company W-2 Profile", "Guardar perfil W-2 de la empresa")}</button>
            </div>
        </form>
    </div>
    """

    return render_page(content, _t("Company W-2 Profile", "Perfil W-2 de la empresa"))


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
        flash(_t("Employee not found.", "Empleado no encontrado."))
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
        f"{_t('Employee', 'Empleado')} #{employee_id}",
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
            or f"{_t('Employee', 'Empleado')} #{e['id']}"
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
                flash(_t("Invalid logo file type. Please upload PNG, JPG, JPEG, GIF, WEBP, or SVG.", "Tipo de archivo de logotipo no válido. Sube PNG, JPG, JPEG, GIF, WEBP o SVG."))
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
        flash(_t("Branding saved.", "Marca guardada."))
        return redirect(url_for("settings.settings_branding"))

    conn.close()

    values = get_company_profile_values(existing)
    csrf_token = generate_csrf()

    company_logo_preview = (
        f"<img src='{escape(values['logo_url'])}' alt='{escape(_t('Company Logo Preview', 'Vista previa del logotipo de la empresa'))}' style='max-height:84px; max-width:240px; object-fit:contain; border-radius:10px;'>"
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
                <h1 style='margin-bottom:6px;'>{_t("Branding", "Marca")}</h1>
                <p class='muted' style='margin:0;'>{_t("Manage your logo, company branding, and document branding.", "Administra tu logotipo, la marca de tu empresa y la marca de documentos.")}</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("settings.settings")}' class='btn secondary'>{_t("Back to Settings", "Volver a Configuración")}</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>{_t("Company Document Branding Preview", "Vista previa de la marca de documentos de la empresa")}</h2>
        <div style='display:grid; grid-template-columns:110px 1fr; gap:20px; align-items:center;'>
            <div>{company_logo_preview}</div>
            <div>
                <div style='font-size:1.35rem; font-weight:800; color:#334155;'>{escape(values["display_name"] or _t("Your Company Name", "Nombre de tu empresa"))}</div>
                <div style='margin-top:8px; color:#555;'>{escape(values["legal_name"]) if values["legal_name"] else ''}</div>
                <div style='margin-top:10px; color:#666; line-height:1.55;'>
                    {address_preview if address_preview else f"<span class='muted'>{_t('No business address set yet.', 'Aún no se ha configurado la dirección del negocio.')}</span>"}
                </div>
                <div style='margin-top:10px; color:#666; line-height:1.55;'>
                    {contact_lines if contact_lines else f"<span class='muted'>{_t('No phone, email, or website set yet.', 'Aún no se han configurado teléfono, correo ni sitio web.')}</span>"}
                </div>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>{_t("Branding Details", "Detalles de marca")}</h2>
        <form method='post' enctype='multipart/form-data'>
            <input type="hidden" name="csrf_token" value="{csrf_token}">
            <div style='display:grid; grid-template-columns:repeat(auto-fit, minmax(240px, 1fr)); gap:16px;'>

                <div>
                    <label>{_t("Display Name", "Nombre para mostrar")}</label>
                    <input name='display_name' value='{escape(values["display_name"])}' placeholder='{_t("Wrede & Sons Lafayette", "Wrede & Sons Lafayette")}'>
                </div>

                <div>
                    <label>{_t("Legal Business Name", "Nombre legal del negocio")}</label>
                    <input name='legal_name' value='{escape(values["legal_name"])}' placeholder='{_t("Wrede & Sons of Lafayette, Inc.", "Wrede & Sons of Lafayette, Inc.")}'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>{_t("Upload Company Logo", "Subir logotipo de la empresa")}</label>
                    <input type='file' name='logo_file' accept='.png,.jpg,.jpeg,.gif,.webp,.svg'>
                    <div class='muted' style='margin-top:6px;'>{_t("Upload a company logo from your computer.", "Sube un logotipo de la empresa desde tu computadora.")}</div>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>{_t("Or Use Company Logo URL", "O usa la URL del logotipo de la empresa")}</label>
                    <input name='logo_url' value='{escape(values["logo_url"])}' placeholder='https://yourdomain.com/logo.png'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label style='display:flex; align-items:center; gap:8px;'>
                        <input type='checkbox' name='remove_logo' value='1'>
                        {_t("Remove current company logo", "Eliminar logotipo actual de la empresa")}
                    </label>
                </div>

                <div>
                    <label>{_t("Phone", "Teléfono")}</label>
                    <input name='phone' value='{escape(values["phone"])}' placeholder='(765) 555-1234'>
                </div>

                <div>
                    <label>{_t("Email", "Correo")}</label>
                    <input name='email' value='{escape(values["email"])}' placeholder='office@yourcompany.com'>
                </div>

                <div>
                    <label>{_t("Website", "Sitio web")}</label>
                    <input name='website' value='{escape(values["website"])}' placeholder='https://yourcompany.com'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>{_t("Address Line 1", "Dirección línea 1")}</label>
                    <input name='address_line_1' value='{escape(values["address_line_1"])}' placeholder='{_t("123 Main Street", "123 Main Street")}'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>{_t("Address Line 2", "Dirección línea 2")}</label>
                    <input name='address_line_2' value='{escape(values["address_line_2"])}' placeholder='{_t("Suite, building, or additional details", "Suite, edificio o detalles adicionales")}'>
                </div>

                <div>
                    <label>{_t("City", "Ciudad")}</label>
                    <input name='city' value='{escape(values["city"])}' placeholder='{_t("Lafayette", "Lafayette")}'>
                </div>

                <div>
                    <label>{_t("State", "Estado")}</label>
                    <input name='state' value='{escape(values["state"])}' placeholder='IN' maxlength='2'>
                </div>

                <div>
                    <label>{_t("County", "Condado")}</label>
                    <input name='county' value='{escape(values["county"])}' placeholder='Tippecanoe'>
                </div>

                <div>
                    <label>{_t("ZIP Code", "Código postal")}</label>
                    <input name='zip_code' value='{escape(values["zip_code"])}' placeholder='47905'>
                </div>

                <div>
                    <label>{_t("Invoice Header Name", "Nombre de encabezado de factura")}</label>
                    <input name='invoice_header_name' value='{escape(values["invoice_header_name"])}' placeholder='{_t("Name shown at top of invoices", "Nombre que se muestra en la parte superior de las facturas")}'>
                </div>

                <div>
                    <label>{_t("Quote Header Name", "Nombre de encabezado de cotización")}</label>
                    <input name='quote_header_name' value='{escape(values["quote_header_name"])}' placeholder='{_t("Name shown at top of quotes", "Nombre que se muestra en la parte superior de las cotizaciones")}'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>{_t("Invoice Footer Note", "Nota al pie de factura")}</label>
                    <textarea name='invoice_footer_note' placeholder='{_t("Thank you for your business.", "Gracias por su preferencia.")}'>{escape(values["invoice_footer_note"])}</textarea>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>{_t("Quote Footer Note", "Nota al pie de cotización")}</label>
                    <textarea name='quote_footer_note' placeholder='{_t("Pricing valid for 30 days unless otherwise stated.", "Los precios son válidos por 30 días a menos que se indique lo contrario.")}'>{escape(values["quote_footer_note"])}</textarea>
                </div>

            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>{_t("Save Branding", "Guardar marca")}</button>
            </div>
        </form>
    </div>
    """

    return render_page(content, _t("Branding", "Marca"))


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
        flash(_t("Email settings saved.", "Configuración de correo guardada."))
        return redirect(url_for("settings.settings_email"))

    conn.close()

    values = get_company_profile_values(existing)
    csrf_token = generate_csrf()

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>{_t("Email Settings", "Configuración de correo")}</h1>
                <p class='muted' style='margin:0;'>{_t("Manage how quote and invoice emails appear to your customers.", "Administra cómo aparecen los correos de cotizaciones y facturas para tus clientes.")}</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("settings.settings")}' class='btn secondary'>{_t("Back to Settings", "Volver a Configuración")}</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>{_t("Email Delivery Identity", "Identidad de envío de correo")}</h2>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{csrf_token}">
            <div style='display:grid; grid-template-columns:repeat(auto-fit, minmax(240px, 1fr)); gap:16px;'>

                <div>
                    <label>{_t("Email From Name", "Nombre del remitente")}</label>
                    <input name='email_from_name' value='{escape(values["email_from_name"])}' placeholder='Wrede & Sons Lafayette'>
                    <div class='muted' style='margin-top:6px;'>{_t("This is the company name customers will recognize when emails are sent.", "Este es el nombre de la empresa que los clientes reconocerán cuando se envíen correos.")}</div>
                </div>

                <div>
                    <label>{_t("Reply-To Email", "Correo de respuesta")}</label>
                    <input name='reply_to_email' value='{escape(values["reply_to_email"])}' placeholder='sales@yourcompany.com'>
                    <div class='muted' style='margin-top:6px;'>{_t("When customers reply to emailed quotes or invoices, replies go here.", "Cuando los clientes respondan a cotizaciones o facturas enviadas por correo, las respuestas llegarán aquí.")}</div>
                </div>

                <div>
                    <label>{_t("Platform Email Sending", "Envío de correo de la plataforma")}</label>
                    <select name='platform_sender_enabled'>
                        <option value='1' {"selected" if values["platform_sender_enabled"] == 1 else ""}>{_t("Enabled", "Habilitado")}</option>
                        <option value='0' {"selected" if values["platform_sender_enabled"] == 0 else ""}>{_t("Disabled", "Deshabilitado")}</option>
                    </select>
                    <div class='muted' style='margin-top:6px;'>{_t("Uses TerraLedger's sending mailbox while keeping your company reply-to address.", "Usa el buzón de envío de TerraLedger mientras mantiene la dirección de respuesta de tu empresa.")}</div>
                </div>

                <div>
                    <label>{_t("Reply-To Behavior", "Comportamiento de respuesta")}</label>
                    <select name='reply_to_mode'>
                        <option value='company' {"selected" if values["reply_to_mode"] == "company" else ""}>{_t("Company Email", "Correo de la empresa")}</option>
                        <option value='logged_in_user' {"selected" if values["reply_to_mode"] == "logged_in_user" else ""}>{_t("Logged-In User", "Usuario con sesión iniciada")}</option>
                    </select>
                    <div class='muted' style='margin-top:6px;'>{_t("Choose whether replies go to the company email or the user who sent the email.", "Elige si las respuestas van al correo de la empresa o al usuario que envió el correo.")}</div>
                </div>

            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>{_t("Save Email Settings", "Guardar configuración de correo")}</button>
            </div>
        </form>
    </div>

    <div class='card'>
        <h2>{_t("Send Test Email", "Enviar correo de prueba")}</h2>
        <p class='muted'>{_t("Use this to confirm your platform email sending is working before testing quote or invoice emails.", "Usa esto para confirmar que el envío de correo de la plataforma funciona antes de probar correos de cotizaciones o facturas.")}</p>

        <form method='post' action='{url_for("settings.test_email")}'>
            <input type="hidden" name="csrf_token" value="{csrf_token}">
            <div class='grid'>
                <div>
                    <label>{_t("Send Test To", "Enviar prueba a")}</label>
                    <input type='email' name='test_email' placeholder='you@example.com' required>
                </div>
            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn' type='submit'>{_t("Send Test Email", "Enviar correo de prueba")}</button>
            </div>
        </form>
    </div>
    """

    return render_page(content, _t("Email Settings", "Configuración de correo"))


@settings_bp.route("/settings/test_email", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def test_email():
    ensure_company_profile_table()
    ensure_w2_company_profile_columns()

    cid = session.get("company_id")
    if not cid:
        flash(_t("No company is associated with this account.", "No hay una empresa asociada con esta cuenta."))
        return redirect(url_for("settings.settings"))

    test_email_address = (request.form.get("test_email") or "").strip()

    if not test_email_address:
        flash(_t("Please enter a test email address.", "Ingresa una dirección de correo de prueba."))
        return redirect(url_for("settings.settings_email"))

    try:
        send_company_email(
            company_id=cid,
            to_email=test_email_address,
            subject=_t("TerraLedger Test Email", "Correo de prueba de TerraLedger"),
            body=(
                _t(
                    "This is a TerraLedger test email.\n\nIf you received this message, your company email settings and platform sender are working.",
                    "Este es un correo de prueba de TerraLedger.\n\nSi recibiste este mensaje, la configuración de correo de tu empresa y el remitente de la plataforma están funcionando."
                )
            ),
            user_id=session.get("user_id"),
        )
        flash(_t("Test email sent successfully.", "Correo de prueba enviado correctamente."))
    except Exception as e:
        flash(f"{_t('Test email failed:', 'Falló el correo de prueba:')} {e}")

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
            flash(_t("Please choose a backup file.", "Selecciona un archivo de respaldo."))
            return redirect(url_for("settings.restore_backup"))

        if not uploaded_file.filename.lower().endswith(".json"):
            flash(_t("Please upload a valid JSON backup file.", "Sube un archivo de respaldo JSON válido."))
            return redirect(url_for("settings.restore_backup"))

        try:
            backup_data = load_backup_file(uploaded_file)
            result = restore_company_backup(cid, backup_data)

            flash(
                _t(
                    "Backup restored successfully. A pre-restore backup was also saved locally at: ",
                    "Respaldo restaurado correctamente. También se guardó localmente un respaldo previo a la restauración en: "
                ) + f"{result['pre_restore_backup_path']}"
            )
            return redirect(url_for("settings.settings"))

        except Exception as e:
            flash(f"{_t('Restore failed:', 'La restauración falló:')} {e}")
            return redirect(url_for("settings.restore_backup"))

    csrf_token = generate_csrf()

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>{_t("Restore Backup", "Restaurar respaldo")}</h1>
                <p class='muted' style='margin:0;'>
                    {_t("Upload a TerraLedger backup file to replace your current company data.", "Sube un archivo de respaldo de TerraLedger para reemplazar los datos actuales de tu empresa.")}
                </p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("settings.settings")}' class='btn secondary'>{_t("Back to Settings", "Volver a Configuración")}</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>{_t("Important", "Importante")}</h2>
        <p class='muted'>
            {_t("Restoring a backup will replace your current company data. TerraLedger will create a local pre-restore backup automatically before the restore begins.", "Restaurar un respaldo reemplazará los datos actuales de tu empresa. TerraLedger creará automáticamente un respaldo local previo a la restauración antes de comenzar.")}
        </p>

        <form method='post' enctype='multipart/form-data'>
            <input type="hidden" name="csrf_token" value="{csrf_token}">
            <div class='grid'>
                <div>
                    <label>{_t("Backup File (.json)", "Archivo de respaldo (.json)")}</label>
                    <input type='file' name='backup_file' accept='.json' required>
                </div>
            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn warning' type='submit'>{_t("Restore Backup", "Restaurar respaldo")}</button>
            </div>
        </form>
    </div>
    """

    return render_page(content, _t("Restore Backup", "Restaurar respaldo"))


@settings_bp.route("/settings/language", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def settings_language():
    conn = get_db_connection()
    cid = session["company_id"]

    try:
        try:
            conn.execute(
                """
                ALTER TABLE company_profile
                ADD COLUMN IF NOT EXISTS language_preference TEXT DEFAULT 'en'
                """
            )
            conn.commit()
        except Exception:
            conn.rollback()

        company = conn.execute(
            """
            SELECT *
            FROM company_profile
            WHERE company_id = %s
            """,
            (cid,),
        ).fetchone()

        current_language = "en"
        if company and "language_preference" in company.keys() and company["language_preference"]:
            current_language = str(company["language_preference"]).strip().lower()
            if current_language not in {"en", "es"}:
                current_language = "en"

        if request.method == "POST":
            language_preference = (request.form.get("language_preference") or "en").strip().lower()
            if language_preference not in {"en", "es"}:
                language_preference = "en"

            if company:
                conn.execute(
                    """
                    UPDATE company_profile
                    SET language_preference = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE company_id = %s
                    """,
                    (language_preference, cid),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO company_profile (company_id, language_preference)
                    VALUES (%s, %s)
                    """,
                    (cid, language_preference),
                )

            conn.commit()

            session["language"] = language_preference

            flash(
                _t("Language setting updated.", "Configuración de idioma actualizada.")
            )
            return redirect(url_for("settings.settings_language"))

        is_es = current_language == "es"

        content = f"""
        <div class="card">
            <div style="display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;">
                <div>
                    <h1 style="margin-bottom:6px;">{_t("Language Settings", "Configuración de idioma")}</h1>
                    <div class="muted">{_t("Choose the default language used across your company account.", "Elige el idioma predeterminado usado en toda la cuenta de tu empresa.")}</div>
                </div>
                <div class="row-actions">
                    <a class="btn secondary" href="{url_for('settings.settings')}">{_t("Back to Settings", "Volver a Configuración")}</a>
                </div>
            </div>
        </div>

        <div class="card">
            <form method="post">
                {csrf_input()}
                <div class="grid">
                    <div>
                        <label>{_t("Default Language", "Idioma predeterminado")}</label>
                        <select name="language_preference">
                            <option value="en" {"selected" if current_language == "en" else ""}>English</option>
                            <option value="es" {"selected" if current_language == "es" else ""}>Español</option>
                        </select>
                    </div>
                </div>

                <div class="row-actions" style="margin-top:16px;">
                    <button type="submit" class="btn success">{_t("Save Language", "Guardar idioma")}</button>
                </div>
            </form>
        </div>
        """
        return render_page(content, _t("Language Settings", "Configuración de idioma"))

    finally:
        conn.close()