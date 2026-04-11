from flask import Blueprint, request, redirect, url_for, session, flash, make_response, jsonify
from flask_wtf.csrf import generate_csrf
from datetime import date, datetime
import io
import csv
from decimal import Decimal, ROUND_HALF_UP

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch

from db import (
    get_db_connection,
    ensure_employee_payroll_columns,
    ensure_bookkeeping_history_table,
    ensure_payroll_table_structure,
    get_company_profile_row,
)
from decorators import login_required, require_permission, subscription_required
from utils.payroll_tax_service import calculate_payroll_taxes_for_employee
from utils.time_clock import get_company_time_clock_start_day, get_current_pay_period
from page_helpers import render_page

payroll_bp = Blueprint("payroll", __name__)


def safe_float(value, default=0.0):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return default


def clean_text_input(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"none", "null", "n/a"}:
        return ""
    return text


def clean_text_display(value, fallback="-"):
    text = clean_text_input(value)
    return text if text else fallback


def money(value):
    return float(Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def html_escape(value):
    text = "" if value is None else str(value)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def get_salary_period_amount(annual_salary, pay_frequency):
    annual_salary = safe_float(annual_salary, 0)
    pay_frequency = (pay_frequency or "Biweekly").strip()

    if pay_frequency == "Weekly":
        return annual_salary / 52
    if pay_frequency == "Biweekly":
        return annual_salary / 26
    if pay_frequency in ("Semi-Monthly", "Semimonthly"):
        return annual_salary / 24
    if pay_frequency == "Monthly":
        return annual_salary / 12
    if pay_frequency == "Quarterly":
        return annual_salary / 4
    if pay_frequency == "Yearly":
        return annual_salary

    return annual_salary / 26


def build_gross_pay(employee, hours_regular, hours_overtime, rate_regular, rate_overtime):
    pay_type = clean_text_input(employee["pay_type"]) or "Hourly"
    pay_frequency = clean_text_input(employee["pay_frequency"]) or "Biweekly"

    if pay_type == "Salary":
        annual_salary = safe_float(employee["salary_amount"], 0)
        gross_pay = get_salary_period_amount(annual_salary, pay_frequency)

        return {
            "pay_type": pay_type,
            "pay_frequency": pay_frequency,
            "gross_pay": round(max(gross_pay, 0), 2),
            "hours_regular": 1,
            "hours_overtime": 0,
            "rate_regular": 0,
            "rate_overtime": 0,
        }

    if rate_regular <= 0:
        rate_regular = safe_float(employee["hourly_rate"], 0)
    if rate_overtime <= 0:
        rate_overtime = safe_float(employee["overtime_rate"], 0)

    gross_pay = (hours_regular * rate_regular) + (hours_overtime * rate_overtime)

    return {
        "pay_type": pay_type,
        "pay_frequency": pay_frequency,
        "gross_pay": round(max(gross_pay, 0), 2),
        "hours_regular": hours_regular,
        "hours_overtime": hours_overtime,
        "rate_regular": round(rate_regular, 2),
        "rate_overtime": round(rate_overtime, 2),
    }


def get_employee_time_clock_hours(conn, company_id, employee_id, start_date, end_date):
    row = conn.execute(
        """
        SELECT COALESCE(SUM(total_hours), 0) AS total_hours
        FROM employee_time_entries
        WHERE company_id = %s
          AND employee_id = %s
          AND DATE(clock_in) >= %s
          AND DATE(clock_in) <= %s
        """,
        (company_id, employee_id, start_date, end_date),
    ).fetchone()

    total_hours = float(row["total_hours"] or 0)
    regular = min(total_hours, 40.0)
    overtime = max(total_hours - 40.0, 0.0)

    return round(regular, 2), round(overtime, 2)


def ensure_payroll_check_structure():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS checks (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            check_number INTEGER NOT NULL,
            check_date DATE NOT NULL,
            payee_name TEXT NOT NULL,
            amount NUMERIC(12,2) NOT NULL DEFAULT 0,
            amount_written TEXT,
            memo TEXT,
            source_type TEXT NOT NULL,
            source_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'Printed',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            printed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute("ALTER TABLE payroll_entries ADD COLUMN IF NOT EXISTS payment_method TEXT")
    cur.execute("ALTER TABLE payroll_entries ADD COLUMN IF NOT EXISTS check_id INTEGER")
    cur.execute("ALTER TABLE payroll_entries ADD COLUMN IF NOT EXISTS check_number INTEGER")
    cur.execute("ALTER TABLE payroll_entries ADD COLUMN IF NOT EXISTS check_printed_at TIMESTAMP")

    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS next_check_number INTEGER DEFAULT 1001")
    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS company_check_name TEXT")
    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS check_address_line_1 TEXT")
    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS check_address_line_2 TEXT")
    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS check_city TEXT")
    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS check_state TEXT")
    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS check_zip TEXT")

    conn.commit()
    conn.close()


def number_to_words_under_1000(n):
    ones = [
        "Zero", "One", "Two", "Three", "Four", "Five", "Six", "Seven",
        "Eight", "Nine", "Ten", "Eleven", "Twelve", "Thirteen", "Fourteen",
        "Fifteen", "Sixteen", "Seventeen", "Eighteen", "Nineteen"
    ]
    tens = ["", "", "Twenty", "Thirty", "Forty", "Fifty", "Sixty", "Seventy", "Eighty", "Ninety"]

    n = int(n)
    if n < 20:
        return ones[n]
    if n < 100:
        if n % 10 == 0:
            return tens[n // 10]
        return f"{tens[n // 10]} {ones[n % 10]}"
    if n % 100 == 0:
        return f"{ones[n // 100]} Hundred"
    return f"{ones[n // 100]} Hundred {number_to_words_under_1000(n % 100)}"


def number_to_words(n):
    n = int(n)
    if n == 0:
        return "Zero"

    parts = []

    billions = n // 1_000_000_000
    if billions:
        parts.append(f"{number_to_words_under_1000(billions)} Billion")
        n %= 1_000_000_000

    millions = n // 1_000_000
    if millions:
        parts.append(f"{number_to_words_under_1000(millions)} Million")
        n %= 1_000_000

    thousands = n // 1000
    if thousands:
        parts.append(f"{number_to_words_under_1000(thousands)} Thousand")
        n %= 1000

    if n:
        parts.append(number_to_words_under_1000(n))

    return " ".join(parts)


def amount_to_words(amount):
    amount = Decimal(str(amount or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    dollars = int(amount)
    cents = int((amount - Decimal(dollars)) * 100)
    return f"{number_to_words(dollars)} and {cents:02d}/100"


def get_company_check_info(profile):
    profile = profile or {}

    company_name = (
        clean_text_input(profile.get("company_check_name"))
        or clean_text_input(profile.get("company_name"))
        or clean_text_input(profile.get("name"))
        or "Company"
    )

    address_line_1 = (
        clean_text_input(profile.get("check_address_line_1"))
        or clean_text_input(profile.get("address_line_1"))
        or clean_text_input(profile.get("address"))
    )
    address_line_2 = clean_text_input(profile.get("check_address_line_2"))
    city = clean_text_input(profile.get("check_city")) or clean_text_input(profile.get("city"))
    state = clean_text_input(profile.get("check_state")) or clean_text_input(profile.get("state"))
    zip_code = clean_text_input(profile.get("check_zip")) or clean_text_input(profile.get("zip"))

    city_state_zip = " ".join(part for part in [city, state, zip_code] if part).strip()

    next_check_number = int(profile.get("next_check_number") or 1001)

    return {
        "company_name": company_name,
        "address_line_1": address_line_1,
        "address_line_2": address_line_2,
        "city_state_zip": city_state_zip,
        "next_check_number": next_check_number,
    }


def _fit_text_to_width(c, text, font_name, start_font_size, max_width, min_font_size=7):
    text = clean_text_input(text)
    font_size = start_font_size
    while font_size > min_font_size and c.stringWidth(text, font_name, font_size) > max_width:
        font_size -= 0.25
    return text, font_size


def _draw_money_row(c, x_label, x_value, y, label, value, width=1.2 * inch, font_size=8.7):
    c.setFont("Helvetica", font_size)
    c.drawString(x_label, y, label)
    c.drawRightString(x_value + width, y, f"${money(value):,.2f}")


def _draw_stub_section(c, x, y_top, width, height, title, payroll_row, employee_name):
    pad_x = 0.14 * inch
    pad_y = 0.11 * inch

    left = x + pad_x
    right = x + width - pad_x
    top = y_top - pad_y

    c.setLineWidth(0.7)
    c.rect(x, y_top - height, width, height, stroke=1, fill=0)

    c.setFont("Helvetica-Bold", 10)
    c.drawString(left, top, title)

    c.setLineWidth(0.4)
    c.line(left, top - 0.08 * inch, right, top - 0.08 * inch)

    info_y = top - 0.23 * inch
    col1 = left
    col2 = x + width * 0.50

    pay_date = clean_text_input(payroll_row["pay_date"]) or date.today().isoformat()
    pay_period = f"{clean_text_input(payroll_row['pay_period_start'])} to {clean_text_input(payroll_row['pay_period_end'])}"
    payment_method = clean_text_input(payroll_row.get("payment_method")) or "Check"
    check_number = payroll_row["check_number"] if "check_number" in payroll_row.keys() and payroll_row["check_number"] else "-"

    c.setFont("Helvetica", 8.6)
    c.drawString(col1, info_y, f"Employee: {employee_name[:40]}")
    c.drawString(col2, info_y, f"Check #: {check_number}")

    info_y -= 0.17 * inch
    c.drawString(col1, info_y, f"Pay Date: {pay_date}")
    c.drawString(col2, info_y, f"Method: {payment_method}")

    info_y -= 0.17 * inch
    c.drawString(col1, info_y, f"Pay Type: {clean_text_display(payroll_row['pay_type'])}")
    c.drawString(col2, info_y, f"Period: {pay_period[:34]}")

    info_y -= 0.24 * inch

    section1_x = left
    section2_x = x + width * 0.34
    section3_x = x + width * 0.68

    c.setFont("Helvetica-Bold", 8.8)
    c.drawString(section1_x, info_y, "Earnings")
    c.drawString(section2_x, info_y, "Deductions")
    c.drawString(section3_x, info_y, "Totals")

    info_y -= 0.16 * inch
    c.setLineWidth(0.3)
    c.line(section1_x, info_y + 0.04 * inch, x + width * 0.31, info_y + 0.04 * inch)
    c.line(section2_x, info_y + 0.04 * inch, x + width * 0.65, info_y + 0.04 * inch)
    c.line(section3_x, info_y + 0.04 * inch, right, info_y + 0.04 * inch)

    money_width = 0.90 * inch
    row_gap = 0.15 * inch
    row_y = info_y - 0.02 * inch

    _draw_money_row(c, section1_x, x + width * 0.31 - money_width, row_y, "Regular Hours", payroll_row["hours_regular"], width=money_width)
    _draw_money_row(c, section2_x, x + width * 0.65 - money_width, row_y, "Federal", payroll_row["federal_withholding"], width=money_width)
    _draw_money_row(c, section3_x, right - money_width, row_y, "Gross", payroll_row["gross_pay"], width=money_width)

    row_y -= row_gap
    _draw_money_row(c, section1_x, x + width * 0.31 - money_width, row_y, "OT Hours", payroll_row["hours_overtime"], width=money_width)
    _draw_money_row(c, section2_x, x + width * 0.65 - money_width, row_y, "State", payroll_row["state_withholding"], width=money_width)
    _draw_money_row(c, section3_x, right - money_width, row_y, "Net", payroll_row["net_pay"], width=money_width)

    row_y -= row_gap
    _draw_money_row(c, section1_x, x + width * 0.31 - money_width, row_y, "Reg Rate", payroll_row["rate_regular"], width=money_width)
    _draw_money_row(c, section2_x, x + width * 0.65 - money_width, row_y, "Social Sec.", payroll_row["social_security"], width=money_width)

    row_y -= row_gap
    _draw_money_row(c, section1_x, x + width * 0.31 - money_width, row_y, "OT Rate", payroll_row["rate_overtime"], width=money_width)
    _draw_money_row(c, section2_x, x + width * 0.65 - money_width, row_y, "Medicare", payroll_row["medicare"], width=money_width)

    row_y -= row_gap
    _draw_money_row(c, section2_x, x + width * 0.65 - money_width, row_y, "Local Tax", payroll_row["local_tax"], width=money_width)

    row_y -= row_gap
    _draw_money_row(c, section2_x, x + width * 0.65 - money_width, row_y, "Other Ded.", payroll_row["other_deductions"], width=money_width)

    notes = clean_text_input(payroll_row.get("notes"))
    if notes:
        notes_y = y_top - height + 0.16 * inch
        c.setFont("Helvetica", 7.9)
        notes_text = notes[:110]
        c.drawString(left, notes_y, f"Notes: {notes_text}")


def build_payroll_check_pdf(company_info, payroll_row, employee_name, check_number):
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    amount = money(payroll_row["net_pay"])
    amount_written = amount_to_words(amount)
    pay_date = clean_text_input(payroll_row["pay_date"]) or date.today().isoformat()
    memo = f"Payroll {clean_text_input(payroll_row['pay_period_start'])} to {clean_text_input(payroll_row['pay_period_end'])}"

    # Top section: designed for pre-printed check stock.
    # Do NOT print company name/address or check number.
    check_top = height
    check_height = 3.48 * inch

    # Date
    c.setFont("Helvetica", 10)
    c.drawString(8.12 * inch, check_top - 0.92 * inch, pay_date)

    # Payee line
    payee_x = 1.74 * inch
    payee_y = check_top - 1.73 * inch
    payee_text, payee_font = _fit_text_to_width(c, employee_name, "Helvetica-Bold", 11, 4.95 * inch, 8.5)
    c.setFont("Helvetica-Bold", payee_font)
    c.drawString(payee_x, payee_y, payee_text)

    # Numeric amount box
    amount_box_center_x = 8.62 * inch
    amount_box_y = check_top - 1.77 * inch
    amount_text = f"${amount:,.2f}"
    amount_text, amount_font = _fit_text_to_width(c, amount_text, "Helvetica-Bold", 11, 1.12 * inch, 8.0)
    c.setFont("Helvetica-Bold", amount_font)
    c.drawCentredString(amount_box_center_x, amount_box_y, amount_text)

    # Written amount line
    words_x = 0.72 * inch
    words_y = check_top - 2.16 * inch
    max_words_width = 7.35 * inch
    words_text, words_font = _fit_text_to_width(c, amount_written, "Helvetica", 10, max_words_width, 8.0)
    c.setFont("Helvetica", words_font)
    c.drawString(words_x, words_y, words_text)

    # Memo line
    memo_x = 0.88 * inch
    memo_y = check_top - 2.75 * inch
    memo_text, memo_font = _fit_text_to_width(c, memo, "Helvetica", 9, 2.55 * inch, 7.5)
    c.setFont("Helvetica", memo_font)
    c.drawString(memo_x, memo_y, memo_text)

    # Optional signature label only
    c.setFont("Helvetica", 7.5)
    c.drawString(7.12 * inch, check_top - 2.76 * inch, "Authorized Signature")

    # Stub areas
    stub_width = width - (0.48 * inch)
    stub_x = 0.24 * inch

    first_stub_top = height - 3.72 * inch
    stub_height = 2.02 * inch

    second_stub_top = 1.96 * inch

    _draw_stub_section(
        c,
        x=stub_x,
        y_top=first_stub_top,
        width=stub_width,
        height=stub_height,
        title="Payroll Check Stub - Employee Copy",
        payroll_row=payroll_row,
        employee_name=employee_name,
    )

    _draw_stub_section(
        c,
        x=stub_x,
        y_top=second_stub_top,
        width=stub_width,
        height=stub_height,
        title="Payroll Payment Record - Company Copy",
        payroll_row=payroll_row,
        employee_name=employee_name,
    )

    c.showPage()
    c.save()

    pdf_data = buffer.getvalue()
    buffer.close()
    return pdf_data


def create_or_get_payroll_check(conn, company_id, payroll_row, employee_name):
    existing_check_id = payroll_row["check_id"] if "check_id" in payroll_row.keys() else None
    existing_check_number = payroll_row["check_number"] if "check_number" in payroll_row.keys() else None

    if existing_check_id and existing_check_number:
        check = conn.execute(
            """
            SELECT id, check_number
            FROM checks
            WHERE id = %s AND company_id = %s
            """,
            (existing_check_id, company_id),
        ).fetchone()
        if check:
            return int(check["id"]), int(check["check_number"])

    profile = get_company_profile_row(company_id) or {}
    company_info = get_company_check_info(profile)
    check_number = int(company_info["next_check_number"])

    amount = money(payroll_row["net_pay"])
    amount_written = amount_to_words(amount)
    memo = f"Payroll {clean_text_input(payroll_row['pay_period_start'])} to {clean_text_input(payroll_row['pay_period_end'])}"
    check_date = clean_text_input(payroll_row["pay_date"]) or date.today().isoformat()

    inserted = conn.execute(
        """
        INSERT INTO checks (
            company_id, check_number, check_date, payee_name, amount,
            amount_written, memo, source_type, source_id, status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'payroll', %s, 'Printed')
        RETURNING id
        """,
        (
            company_id,
            check_number,
            check_date,
            employee_name,
            amount,
            amount_written,
            memo,
            payroll_row["id"],
        ),
    ).fetchone()

    conn.execute(
        """
        UPDATE payroll_entries
        SET payment_method = 'Check',
            check_id = %s,
            check_number = %s,
            check_printed_at = CURRENT_TIMESTAMP
        WHERE id = %s AND company_id = %s
        """,
        (inserted["id"], check_number, payroll_row["id"], company_id),
    )

    conn.execute(
        """
        UPDATE company_profile
        SET next_check_number = %s
        WHERE company_id = %s
        """,
        (check_number + 1, company_id),
    )

    return int(inserted["id"]), check_number


@payroll_bp.route("/api/time-clock/hours", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_payroll")
def get_time_clock_hours_api():
    ensure_employee_payroll_columns()
    ensure_bookkeeping_history_table()
    ensure_payroll_table_structure()
    ensure_payroll_check_structure()

    data = request.get_json(silent=True) or {}

    employee_id_raw = clean_text_input(data.get("employee_id", ""))
    start_date = clean_text_input(data.get("start_date", ""))
    end_date = clean_text_input(data.get("end_date", ""))

    if not employee_id_raw.isdigit() or not start_date or not end_date:
        return jsonify({
            "ok": False,
            "message": "Missing employee or pay period.",
        }), 400

    employee_id = int(employee_id_raw)

    conn = get_db_connection()
    cid = session["company_id"]

    employee = conn.execute(
        """
        SELECT id
        FROM employees
        WHERE id = %s AND company_id = %s AND is_active = TRUE
        """,
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        return jsonify({
            "ok": False,
            "message": "Employee not found.",
        }), 404

    regular, overtime = get_employee_time_clock_hours(
        conn=conn,
        company_id=cid,
        employee_id=employee_id,
        start_date=start_date,
        end_date=end_date,
    )

    conn.close()

    return jsonify({
        "ok": True,
        "regular": regular,
        "overtime": overtime,
    })


@payroll_bp.route("/employees/payroll/preview", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_payroll")
def payroll_preview():
    ensure_employee_payroll_columns()
    ensure_bookkeeping_history_table()
    ensure_payroll_table_structure()
    ensure_payroll_check_structure()

    conn = get_db_connection()
    cid = session["company_id"]

    employee_id_raw = clean_text_input(request.form.get("employee_id", ""))
    if not employee_id_raw.isdigit():
        conn.close()
        return jsonify({
            "ok": False,
            "message": "Please select an employee.",
        }), 400

    employee_id = int(employee_id_raw)

    employee = conn.execute(
        """
        SELECT
            id,
            first_name,
            last_name,
            full_name,
            pay_type,
            hourly_rate,
            overtime_rate,
            salary_amount,
            pay_frequency,
            federal_filing_status,
            w4_filing_status,
            w4_step2_checked,
            w4_step3_amount,
            w4_step4a_other_income,
            w4_step4b_deductions,
            w4_step4c_extra_withholding,
            state,
            is_indiana_resident,
            county_of_residence,
            county_of_principal_employment,
            county_tax_effective_year
        FROM employees
        WHERE id = %s AND company_id = %s
        """,
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        return jsonify({
            "ok": False,
            "message": "Employee not found.",
        }), 404

    employee = dict(employee)
    employee["w4_step2_checked"] = 1 if employee.get("w4_step2_checked") else 0
    employee["is_indiana_resident"] = 1 if employee.get("is_indiana_resident") else 0

    hours_regular = safe_float(request.form.get("hours_regular"), 0)
    hours_overtime = safe_float(request.form.get("hours_overtime"), 0)
    rate_regular = safe_float(request.form.get("rate_regular"), 0)
    rate_overtime = safe_float(request.form.get("rate_overtime"), 0)
    other_deductions = safe_float(request.form.get("other_deductions"), 0)

    gross_data = build_gross_pay(
        employee=employee,
        hours_regular=hours_regular,
        hours_overtime=hours_overtime,
        rate_regular=rate_regular,
        rate_overtime=rate_overtime,
    )

    gross_pay = gross_data["gross_pay"]

    taxes = calculate_payroll_taxes_for_employee(
        employee=employee,
        gross_pay=gross_pay,
        company_id=cid,
        conn=conn,
    )

    federal_withholding = round(float(taxes.get("federal_withholding", taxes.get("federal_tax", 0)) or 0), 2)
    state_withholding = round(float(taxes.get("state_withholding", taxes.get("state_tax", 0)) or 0), 2)
    social_security = round(float(taxes.get("social_security", 0) or 0), 2)
    medicare = round(float(taxes.get("medicare", 0) or 0), 2)
    local_tax = round(float(taxes.get("local_tax", taxes.get("local_withholding", 0)) or 0), 2)

    net_pay = round(
        gross_pay
        - federal_withholding
        - state_withholding
        - social_security
        - medicare
        - local_tax
        - other_deductions,
        2,
    )

    conn.close()

    return jsonify({
        "ok": True,
        "pay_type": gross_data["pay_type"],
        "pay_frequency": gross_data["pay_frequency"],
        "gross_pay": gross_pay,
        "federal_withholding": federal_withholding,
        "state_withholding": state_withholding,
        "social_security": social_security,
        "medicare": medicare,
        "local_tax": local_tax,
        "other_deductions": round(other_deductions, 2),
        "net_pay": net_pay,
        "provider": clean_text_input(taxes.get("provider", "internal")) or "internal",
        "state_name": clean_text_display(taxes.get("state_name", ""), "-"),
        "local_name": clean_text_display(taxes.get("local_name", ""), "-"),
        "county_used": clean_text_display(taxes.get("county_used", ""), "-"),
        "county_source": clean_text_display(taxes.get("county_source", ""), "-"),
        "local_tax_rate": float(taxes.get("local_tax_rate", 0) or 0),
        "hours_regular": gross_data["hours_regular"],
        "hours_overtime": gross_data["hours_overtime"],
        "rate_regular": gross_data["rate_regular"],
        "rate_overtime": gross_data["rate_overtime"],
        "w4_filing_status": clean_text_input(employee.get("w4_filing_status")) or clean_text_input(employee.get("federal_filing_status")) or "Single",
        "w4_step2_checked": 1 if (employee.get("w4_step2_checked") or 0) else 0,
        "w4_step3_amount": float(employee.get("w4_step3_amount") or 0),
        "w4_step4a_other_income": float(employee.get("w4_step4a_other_income") or 0),
        "w4_step4b_deductions": float(employee.get("w4_step4b_deductions") or 0),
        "w4_step4c_extra_withholding": float(employee.get("w4_step4c_extra_withholding") or 0),
    })


@payroll_bp.route("/employees/payroll", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_payroll")
def employee_payroll():
    ensure_employee_payroll_columns()
    ensure_bookkeeping_history_table()
    ensure_payroll_table_structure()
    ensure_payroll_check_structure()

    conn = get_db_connection()
    cid = session["company_id"]

    profile = get_company_profile_row(cid) or {}
    start_day = get_company_time_clock_start_day(profile)
    pay_period_start, pay_period_end = get_current_pay_period(start_day)
    pay_period_start_default = pay_period_start.isoformat()
    pay_period_end_default = pay_period_end.isoformat()

    if request.method == "POST":
        employee_id_raw = clean_text_input(request.form.get("employee_id", ""))
        if not employee_id_raw.isdigit():
            flash("Please select an employee.")
            conn.close()
            return redirect(url_for("payroll.employee_payroll"))

        employee_id = int(employee_id_raw)
        pay_date = clean_text_input(request.form.get("pay_date", "")) or date.today().isoformat()
        pay_period_start = clean_text_input(request.form.get("pay_period_start", "")) or pay_period_start_default
        pay_period_end = clean_text_input(request.form.get("pay_period_end", "")) or pay_period_end_default
        hours_regular = safe_float(request.form.get("hours_regular"), 0)
        hours_overtime = safe_float(request.form.get("hours_overtime"), 0)
        rate_regular = safe_float(request.form.get("rate_regular"), 0)
        rate_overtime = safe_float(request.form.get("rate_overtime"), 0)
        other_deductions = safe_float(request.form.get("other_deductions"), 0)
        notes = clean_text_input(request.form.get("notes", ""))
        payment_method = clean_text_input(request.form.get("payment_method", "")) or "Direct Deposit"

        employee = conn.execute(
            """
            SELECT
                id,
                first_name,
                last_name,
                full_name,
                pay_type,
                hourly_rate,
                overtime_rate,
                salary_amount,
                pay_frequency,
                federal_filing_status,
                w4_filing_status,
                w4_step2_checked,
                w4_step3_amount,
                w4_step4a_other_income,
                w4_step4b_deductions,
                w4_step4c_extra_withholding,
                state,
                is_indiana_resident,
                county_of_residence,
                county_of_principal_employment,
                county_tax_effective_year
            FROM employees
            WHERE id = %s AND company_id = %s
            """,
            (employee_id, cid),
        ).fetchone()

        if not employee:
            flash("Employee not found.")
            conn.close()
            return redirect(url_for("payroll.employee_payroll"))

        employee = dict(employee)
        employee["w4_step2_checked"] = 1 if employee.get("w4_step2_checked") else 0
        employee["is_indiana_resident"] = 1 if employee.get("is_indiana_resident") else 0

        gross_data = build_gross_pay(
            employee=employee,
            hours_regular=hours_regular,
            hours_overtime=hours_overtime,
            rate_regular=rate_regular,
            rate_overtime=rate_overtime,
        )

        pay_type = gross_data["pay_type"]
        pay_frequency = gross_data["pay_frequency"]
        gross_pay = gross_data["gross_pay"]
        hours_regular = gross_data["hours_regular"]
        hours_overtime = gross_data["hours_overtime"]
        rate_regular = gross_data["rate_regular"]
        rate_overtime = gross_data["rate_overtime"]

        taxes = calculate_payroll_taxes_for_employee(
            employee=employee,
            gross_pay=gross_pay,
            company_id=cid,
            conn=conn,
        )

        federal_withholding = round(float(taxes.get("federal_withholding", taxes.get("federal_tax", 0)) or 0), 2)
        state_withholding = round(float(taxes.get("state_withholding", taxes.get("state_tax", 0)) or 0), 2)
        social_security = round(float(taxes.get("social_security", 0) or 0), 2)
        medicare = round(float(taxes.get("medicare", 0) or 0), 2)
        local_tax = round(float(taxes.get("local_tax", taxes.get("local_withholding", 0)) or 0), 2)

        net_pay = round(
            gross_pay
            - federal_withholding
            - state_withholding
            - social_security
            - medicare
            - local_tax
            - other_deductions,
            2,
        )

        inserted = conn.execute(
            """
            INSERT INTO payroll_entries (
                company_id, employee_id, pay_date, pay_period_start, pay_period_end,
                pay_type, hours_regular, hours_overtime, rate_regular, rate_overtime,
                gross_pay, federal_withholding, state_withholding, social_security,
                medicare, local_tax, other_deductions, net_pay, notes, payment_method
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                cid,
                employee_id,
                pay_date,
                pay_period_start,
                pay_period_end,
                pay_type,
                hours_regular,
                hours_overtime,
                rate_regular,
                rate_overtime,
                gross_pay,
                federal_withholding,
                state_withholding,
                social_security,
                medicare,
                local_tax,
                other_deductions,
                net_pay,
                notes,
                payment_method,
            ),
        ).fetchone()

        conn.commit()

        flash(
            f"Payroll entry saved. Gross: ${gross_pay:.2f} | Federal: ${federal_withholding:.2f} | Net: ${net_pay:.2f}"
        )

        if payment_method == "Check":
            payroll_id = int(inserted["id"])
            conn.close()
            return redirect(url_for("payroll.view_payroll_entry", payroll_id=payroll_id))

        conn.close()
        return redirect(url_for("payroll.employee_payroll"))

    employees = conn.execute(
        """
        SELECT
            id,
            first_name,
            last_name,
            pay_type,
            pay_frequency,
            hourly_rate,
            overtime_rate,
            salary_amount,
            is_active,
            federal_filing_status,
            w4_filing_status,
            w4_step2_checked,
            w4_step3_amount,
            w4_step4a_other_income,
            w4_step4b_deductions,
            w4_step4c_extra_withholding,
            state,
            is_indiana_resident,
            county_of_residence,
            county_of_principal_employment,
            county_tax_effective_year
        FROM employees
        WHERE company_id = %s AND COALESCE(is_active, 1) = 1
        ORDER BY first_name, last_name
        """,
        (cid,),
    ).fetchall()

    rows = conn.execute(
        """
        SELECT
            p.*,
            e.first_name,
            e.last_name
        FROM payroll_entries p
        JOIN employees e ON p.employee_id = e.id
        WHERE p.company_id = %s
        ORDER BY p.pay_date DESC, p.id DESC
        """,
        (cid,),
    ).fetchall()

    preview_taxes = calculate_payroll_taxes_for_employee(
        employee={
            "pay_frequency": "Biweekly",
            "federal_filing_status": "Single",
            "w4_filing_status": "Single",
            "w4_step2_checked": 0,
            "w4_step3_amount": 0,
            "w4_step4a_other_income": 0,
            "w4_step4b_deductions": 0,
            "w4_step4c_extra_withholding": 0,
            "is_indiana_resident": 1,
            "county_of_residence": "Tippecanoe",
            "county_of_principal_employment": "Tippecanoe",
            "state": "IN",
        },
        gross_pay=0,
        company_id=cid,
        conn=conn,
    )

    company_check_info = get_company_check_info(profile)
    next_check_number = company_check_info["next_check_number"]

    conn.close()

    employee_options = "".join(
        f"""
        <option
            value='{e["id"]}'
            data-pay-type='{html_escape(clean_text_input(e["pay_type"]) or "Hourly")}'
            data-pay-frequency='{html_escape(clean_text_input(e["pay_frequency"]) or "Biweekly")}'
            data-hourly-rate='{e["hourly_rate"] or 0}'
            data-overtime-rate='{e["overtime_rate"] or 0}'
            data-salary-amount='{e["salary_amount"] or 0}'
            data-filing-status='{html_escape(clean_text_input(e["w4_filing_status"]) or clean_text_input(e["federal_filing_status"]) or "Single")}'
            data-step2-checked='{1 if (e["w4_step2_checked"] or 0) else 0}'
            data-step3-amount='{e["w4_step3_amount"] or 0}'
            data-step4a='{e["w4_step4a_other_income"] or 0}'
            data-step4b='{e["w4_step4b_deductions"] or 0}'
            data-step4c='{e["w4_step4c_extra_withholding"] or 0}'
            data-state='{html_escape(clean_text_input(e["state"]) or "IN")}'
            data-is-indiana-resident='{1 if (e["is_indiana_resident"] or 0) else 0}'
            data-county-of-residence='{html_escape(clean_text_input(e["county_of_residence"]) or "")}'
            data-county-of-principal-employment='{html_escape(clean_text_input(e["county_of_principal_employment"]) or "")}'
            data-county-tax-effective-year='{e["county_tax_effective_year"] or ""}'
        >
            {html_escape((clean_text_display(e["first_name"], "").strip() + " " + clean_text_display(e["last_name"], "").strip()).strip())}
        </option>
        """
        for e in employees
    )

    payroll_rows = ""
    payroll_mobile_cards = ""

    for r in rows:
        payment_method = clean_text_input(r["payment_method"]) or "-"
        check_number = r["check_number"] if "check_number" in r.keys() else None
        employee_name = (clean_text_input(r["first_name"]) + " " + clean_text_input(r["last_name"])).strip() or "-"
        row_csrf = generate_csrf()

        actions_html = []

        actions_html.append(
            f"<a class='btn secondary small' href='{url_for('payroll.view_payroll_entry', payroll_id=r['id'])}'>View</a>"
        )

        delete_form = f"""
        <form method='post'
              action='{url_for("payroll.delete_payroll_entry", payroll_id=r["id"])}'
              onsubmit="return confirm('Delete this payroll entry?');"
              style='margin:0;'>
            <input type="hidden" name="csrf_token" value="{row_csrf}">
            <button class='btn danger small' type='submit'>Delete</button>
        </form>
        """
        actions_html.append(delete_form)

        payroll_rows += f"""
        <tr>
            <td>{html_escape(clean_text_display(r['pay_date']))}</td>
            <td class='wrap'>{html_escape(employee_name)}</td>
            <td>{html_escape(clean_text_display(r['pay_type']))}</td>
            <td>{html_escape(payment_method)}</td>
            <td class='center'>{html_escape(str(check_number) if check_number else '-')}</td>
            <td class='money'>${float(r['gross_pay'] or 0):.2f}</td>
            <td class='money'>${float(r['federal_withholding'] or 0):.2f}</td>
            <td class='money'>${float(r['state_withholding'] or 0):.2f}</td>
            <td class='money'>${float(r['social_security'] or 0):.2f}</td>
            <td class='money'>${float(r['medicare'] or 0):.2f}</td>
            <td class='money'>${float(r['local_tax'] or 0):.2f}</td>
            <td class='money'>${float(r['other_deductions'] or 0):.2f}</td>
            <td class='money payroll-net'>${float(r['net_pay'] or 0):.2f}</td>
            <td class='wrap'>
                <div class='static-actions'>
                    {''.join(actions_html)}
                </div>
            </td>
        </tr>
        """

        payroll_mobile_cards += f"""
        <div class='mobile-list-card payroll-simple-card'>
            <div class='mobile-list-top'>
                <div>
                    <div class='mobile-list-title'>{html_escape(employee_name)}</div>
                    <div class='mobile-list-subtitle'>{html_escape(clean_text_display(r['pay_date']))}</div>
                </div>
                <div class='mobile-badge'>{html_escape(payment_method)}</div>
            </div>

            <div class='mobile-pay-summary'>
                <div><span>Net Pay</span><strong class='mobile-net-pay'>${float(r['net_pay'] or 0):.2f}</strong></div>
                <div><span>Check #</span><strong>{html_escape(str(check_number) if check_number else '-')}</strong></div>
            </div>

            <div class='mobile-list-actions'>
                <a class='btn secondary small' href='{url_for("payroll.view_payroll_entry", payroll_id=r["id"])}'>View</a>
                {delete_form}
            </div>
        </div>
        """

    tax_defaults_html = f"""
    <div class='card'>
        <h2>Current Tax Defaults</h2>
        <div class='grid'>
            <div><strong>Provider</strong><br>{html_escape(clean_text_display(preview_taxes.get('provider', 'internal'), 'internal'))}</div>
            <div><strong>State</strong><br>{html_escape(clean_text_display(preview_taxes.get('state_name', '-'), '-'))}</div>
            <div><strong>Social Security</strong><br>6.20%</div>
            <div><strong>Medicare</strong><br>1.45%</div>
            <div><strong>Local</strong><br>{html_escape(clean_text_display(preview_taxes.get('local_name', '-'), '-'))}</div>
            <div><strong>Next Check #</strong><br>{next_check_number}</div>
        </div>
    </div>
    """

    form_csrf = generate_csrf()

    content = f"""
    <style>
        .static-table-wrap {{
            width: 100%;
            overflow-x: auto;
        }}

        .static-table {{
            width: 100%;
            table-layout: fixed;
            border-collapse: collapse;
        }}

        .static-table th,
        .static-table td {{
            padding: 10px 8px;
            vertical-align: top;
            font-size: 0.88rem;
            line-height: 1.25;
            border-bottom: 1px solid rgba(0,0,0,0.06);
        }}

        .static-table th {{
            text-align: left;
            font-weight: 700;
        }}

        .static-table td.money,
        .static-table th.money {{
            text-align: right;
            white-space: nowrap;
            font-variant-numeric: tabular-nums;
        }}

        .static-table td.center,
        .static-table th.center {{
            text-align: center;
        }}

        .static-table td.wrap,
        .static-table th.wrap {{
            white-space: normal;
            word-break: break-word;
        }}

        .static-actions {{
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            align-items: center;
            justify-content: flex-start;
        }}

        .static-actions form {{
            margin: 0;
        }}

        .static-actions .btn {{
            white-space: nowrap;
        }}

        .payroll-net {{
            font-weight: 700;
        }}

        .desktop-only {{
            display: block;
        }}

        .mobile-only {{
            display: none;
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
        }}

        .mobile-list-subtitle {{
            margin-top: 4px;
            font-size: .9rem;
            color: #64748b;
            line-height: 1.25;
            word-break: break-word;
        }}

        .mobile-badge {{
            font-size: .85rem;
            font-weight: 700;
            color: #334155;
            background: #f1f5f9;
            padding: 6px 10px;
            border-radius: 999px;
            white-space: nowrap;
        }}

        .payroll-simple-card {{
            display: grid;
            gap: 12px;
        }}

        .mobile-pay-summary {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px 12px;
        }}

        .mobile-pay-summary span {{
            display: block;
            font-size: .78rem;
            color: #64748b;
            margin-bottom: 3px;
        }}

        .mobile-pay-summary strong {{
            display: block;
            color: #0f172a;
            font-size: .95rem;
            line-height: 1.25;
            word-break: break-word;
        }}

        .mobile-net-pay {{
            color: #2f4f1f;
            font-weight: 700;
        }}

        .mobile-list-actions {{
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            align-items: center;
        }}

        .mobile-list-actions form {{
            margin: 0;
        }}

        @media (max-width: 640px) {{
            .desktop-only {{
                display: none !important;
            }}

            .mobile-only {{
                display: block !important;
            }}

            .mobile-pay-summary {{
                grid-template-columns: 1fr;
            }}

            .mobile-list-actions .btn,
            .mobile-list-actions button,
            .mobile-list-actions form {{
                flex: 1 1 auto;
            }}

            .mobile-list-actions .btn,
            .mobile-list-actions button {{
                width: 100%;
                text-align: center;
            }}
        }}
    </style>

    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Payroll</h1>
                <p class='muted' style='margin:0;'>Track employee pay, payroll deductions, and printable payroll checks.</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("payroll.export_payroll")}' class='btn secondary'>Export CSV</a>
                <a href='/settings/taxes' class='btn warning'>Tax Defaults</a>
                <a href='{url_for("employees.employees")}' class='btn secondary'>Back to Employees</a>
            </div>
        </div>
    </div>

    {tax_defaults_html}

    <div class='card'>
        <h2>New Payroll Entry</h2>
        <form method='post' id='payroll_form'>
            <input type="hidden" name="csrf_token" value="{form_csrf}">
            <div class='grid'>
                <div>
                    <label>Employee</label>
                    <select name='employee_id' id='employee_id' required onchange='fillEmployeePayrollInfo(); triggerPayrollPreview();'>
                        <option value=''>Select employee</option>
                        {employee_options}
                    </select>
                </div>

                <div>
                    <label>Pay Date</label>
                    <input type='date' name='pay_date' value='{date.today().isoformat()}' required>
                </div>

                <div>
                    <label>Pay Period Start</label>
                    <input type='date' name='pay_period_start' id='pay_period_start' value='{pay_period_start_default}'>
                </div>

                <div>
                    <label>Pay Period End</label>
                    <input type='date' name='pay_period_end' id='pay_period_end' value='{pay_period_end_default}'>
                </div>

                <div>
                    <label>Payment Method</label>
                    <select name='payment_method' id='payment_method'>
                        <option value='Direct Deposit'>Direct Deposit</option>
                        <option value='Check'>Check</option>
                        <option value='Cash'>Cash</option>
                        <option value='Other'>Other</option>
                    </select>
                </div>

                <div>
                    <label>Pay Type</label>
                    <input type='text' id='pay_type_display' readonly placeholder='Auto-filled from employee'>
                </div>

                <div>
                    <label>Pay Frequency</label>
                    <input type='text' id='pay_frequency_display' readonly placeholder='Auto-filled from employee'>
                </div>

                <div id='hourly_rate_wrap'>
                    <label>Regular Rate</label>
                    <input type='number' step='0.01' min='0' name='rate_regular' id='rate_regular' value='0' oninput='triggerPayrollPreview()'>
                </div>

                <div id='overtime_rate_wrap'>
                    <label>Overtime Rate</label>
                    <input type='number' step='0.01' min='0' name='rate_overtime' id='rate_overtime' value='0' oninput='triggerPayrollPreview()'>
                </div>

                <div id='salary_amount_wrap' style='display:none;'>
                    <label>Annual Salary</label>
                    <input type='number' step='0.01' min='0' id='salary_amount_display' readonly value='0'>
                </div>

                <div id='salary_per_period_wrap' style='display:none;'>
                    <label>Gross Pay</label>
                    <input type='number' step='0.01' min='0' id='salary_per_period_display' readonly value='0'>
                </div>

                <div id='regular_hours_wrap'>
                    <label>Regular Hours</label>
                    <input type='number' step='0.01' min='0' name='hours_regular' id='hours_regular' value='0' oninput='triggerPayrollPreview()'>
                </div>

                <div id='overtime_hours_wrap'>
                    <label>Overtime Hours</label>
                    <input type='number' step='0.01' min='0' name='hours_overtime' id='hours_overtime' value='0' oninput='triggerPayrollPreview()'>
                </div>

                <div>
                    <label>Other Deductions</label>
                    <input type='number' step='0.01' min='0' name='other_deductions' id='other_deductions' value='0' oninput='triggerPayrollPreview()'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>W-4 / Tax Snapshot</label>
                    <div class='card' style='padding:12px; margin-bottom:0; background:#f8fbff; border:1px solid #d7e6ff; box-shadow:none;'>
                        <div class='grid'>
                            <div><strong>Filing Status</strong><br><span id='w4_filing_status_display'>-</span></div>
                            <div><strong>Step 2 Checked</strong><br><span id='w4_step2_display'>-</span></div>
                            <div><strong>Step 3</strong><br><span id='w4_step3_display'>$0.00</span></div>
                            <div><strong>Step 4a</strong><br><span id='w4_step4a_display'>$0.00</span></div>
                            <div><strong>Step 4b</strong><br><span id='w4_step4b_display'>$0.00</span></div>
                            <div><strong>Step 4c</strong><br><span id='w4_step4c_display'>$0.00</span></div>
                        </div>
                    </div>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>Notes</label>
                    <textarea name='notes'></textarea>
                </div>
            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>Save Payroll Entry</button>
                <div class='muted'>Choosing <strong>Check</strong> will save the payroll entry and open the payroll entry details page.</div>
            </div>
        </form>
    </div>

    <div class='card' id='payroll_preview_card'>
        <h2>Payroll Preview</h2>
        <div class='grid'>
            <div><strong>Gross Pay</strong><br><span id='preview_gross'>$0.00</span></div>
            <div><strong>Federal</strong><br><span id='preview_federal'>$0.00</span></div>
            <div><strong>State</strong><br><span id='preview_state'>$0.00</span></div>
            <div><strong>Social Security</strong><br><span id='preview_ss'>$0.00</span></div>
            <div><strong>Medicare</strong><br><span id='preview_medicare'>$0.00</span></div>
            <div><strong>Local Tax</strong><br><span id='preview_local'>$0.00</span></div>
            <div><strong>Other Deductions</strong><br><span id='preview_other'>$0.00</span></div>
            <div><strong>Estimated Net Pay</strong><br><span id='preview_net'>$0.00</span></div>
        </div>
        <div class='muted' id='preview_meta' style='margin-top:14px;'>Select an employee to preview payroll.</div>
    </div>

    <div class='card'>
        <h2>Payroll History</h2>

        <div class='static-table-wrap desktop-only'>
            <table class='static-table'>
                <colgroup>
                    <col style='width:8%;'>
                    <col style='width:11%;'>
                    <col style='width:8%;'>
                    <col style='width:9%;'>
                    <col style='width:7%;'>
                    <col style='width:7%;'>
                    <col style='width:7%;'>
                    <col style='width:7%;'>
                    <col style='width:7%;'>
                    <col style='width:7%;'>
                    <col style='width:7%;'>
                    <col style='width:7%;'>
                    <col style='width:8%;'>
                    <col style='width:17%;'>
                </colgroup>
                <tr>
                    <th>Date</th>
                    <th class='wrap'>Employee</th>
                    <th>Pay Type</th>
                    <th>Method</th>
                    <th class='center'>Check #</th>
                    <th class='money'>Gross</th>
                    <th class='money'>Federal</th>
                    <th class='money'>State</th>
                    <th class='money'>SS</th>
                    <th class='money'>Medicare</th>
                    <th class='money'>Local</th>
                    <th class='money'>Other</th>
                    <th class='money'>Net</th>
                    <th class='wrap'>Actions</th>
                </tr>
                {payroll_rows or "<tr><td colspan='14' class='muted'>No payroll entries yet.</td></tr>"}
            </table>
        </div>

        <div class='mobile-only'>
            <div class='mobile-list'>
                {payroll_mobile_cards or "<div class='mobile-list-card muted'>No payroll entries yet.</div>"}
            </div>
        </div>
    </div>

<script>
let payrollPreviewTimeout = null;

function getCsrfToken() {{
    const tokenInput = document.querySelector("input[name='csrf_token']");
    return tokenInput ? tokenInput.value : "";
}}

function formatMoney(value) {{
    const num = parseFloat(value || 0);
    return '$' + num.toFixed(2);
}}

async function autoFillHoursFromTimeClock() {{
    const employeeId = document.getElementById('employee_id').value;
    const start = document.getElementById('pay_period_start').value;
    const end = document.getElementById('pay_period_end').value;
    const payType = (document.getElementById('pay_type_display').value || '').trim();

    if (!employeeId || !start || !end) return;
    if (payType === 'Salary') return;

    try {{
        const response = await fetch("{url_for('payroll.get_time_clock_hours_api')}", {{
            method: "POST",
            headers: {{
                "Content-Type": "application/json",
                "X-CSRFToken": getCsrfToken()
            }},
            body: JSON.stringify({{
                employee_id: employeeId,
                start_date: start,
                end_date: end
            }})
        }});

        const data = await response.json();

        if (data.ok) {{
            document.getElementById('hours_regular').value = data.regular;
            document.getElementById('hours_overtime').value = data.overtime;
            triggerPayrollPreview();
        }}
    }} catch (err) {{
        console.log("Auto-fill failed", err);
    }}
}}

function fillEmployeePayrollInfo() {{
    const sel = document.getElementById('employee_id');
    const opt = sel.options[sel.selectedIndex];

    const payTypeDisplay = document.getElementById('pay_type_display');
    const payFrequencyDisplay = document.getElementById('pay_frequency_display');
    const rateRegular = document.getElementById('rate_regular');
    const rateOvertime = document.getElementById('rate_overtime');
    const salaryDisplay = document.getElementById('salary_amount_display');
    const salaryPerPeriodDisplay = document.getElementById('salary_per_period_display');

    const hourlyWrap = document.getElementById('hourly_rate_wrap');
    const overtimeWrap = document.getElementById('overtime_rate_wrap');
    const overtimeHoursWrap = document.getElementById('overtime_hours_wrap');
    const regularHoursWrap = document.getElementById('regular_hours_wrap');
    const salaryWrap = document.getElementById('salary_amount_wrap');
    const salaryPerPeriodWrap = document.getElementById('salary_per_period_wrap');

    const hoursRegular = document.getElementById('hours_regular');
    const hoursOvertime = document.getElementById('hours_overtime');

    const w4FilingStatusDisplay = document.getElementById('w4_filing_status_display');
    const w4Step2Display = document.getElementById('w4_step2_display');
    const w4Step3Display = document.getElementById('w4_step3_display');
    const w4Step4aDisplay = document.getElementById('w4_step4a_display');
    const w4Step4bDisplay = document.getElementById('w4_step4b_display');
    const w4Step4cDisplay = document.getElementById('w4_step4c_display');

    if (!opt || !opt.value) {{
        payTypeDisplay.value = '';
        payFrequencyDisplay.value = '';
        rateRegular.value = 0;
        rateOvertime.value = 0;
        salaryDisplay.value = 0;
        salaryPerPeriodDisplay.value = 0;
        hoursRegular.value = 0;
        hoursOvertime.value = 0;

        w4FilingStatusDisplay.innerText = '-';
        w4Step2Display.innerText = '-';
        w4Step3Display.innerText = '$0.00';
        w4Step4aDisplay.innerText = '$0.00';
        w4Step4bDisplay.innerText = '$0.00';
        w4Step4cDisplay.innerText = '$0.00';

        salaryWrap.style.display = 'none';
        salaryPerPeriodWrap.style.display = 'none';
        hourlyWrap.style.display = 'block';
        overtimeWrap.style.display = 'block';
        overtimeHoursWrap.style.display = 'block';
        regularHoursWrap.style.display = 'block';
        return;
    }}

    const payType = opt.getAttribute('data-pay-type') || 'Hourly';
    const payFrequency = opt.getAttribute('data-pay-frequency') || 'Biweekly';
    const hourlyRate = parseFloat(opt.getAttribute('data-hourly-rate') || '0');
    const overtimeRate = parseFloat(opt.getAttribute('data-overtime-rate') || '0');
    const salaryAmount = parseFloat(opt.getAttribute('data-salary-amount') || '0');

    const filingStatus = opt.getAttribute('data-filing-status') || 'Single';
    const step2Checked = parseInt(opt.getAttribute('data-step2-checked') || '0', 10) === 1;
    const step3Amount = parseFloat(opt.getAttribute('data-step3-amount') || '0');
    const step4a = parseFloat(opt.getAttribute('data-step4a') || '0');
    const step4b = parseFloat(opt.getAttribute('data-step4b') || '0');
    const step4c = parseFloat(opt.getAttribute('data-step4c') || '0');

    payTypeDisplay.value = payType;
    payFrequencyDisplay.value = payFrequency;

    w4FilingStatusDisplay.innerText = filingStatus;
    w4Step2Display.innerText = step2Checked ? 'Yes' : 'No';
    w4Step3Display.innerText = '$' + step3Amount.toFixed(2);
    w4Step4aDisplay.innerText = '$' + step4a.toFixed(2);
    w4Step4bDisplay.innerText = '$' + step4b.toFixed(2);
    w4Step4cDisplay.innerText = '$' + step4c.toFixed(2);

    let perPeriod = 0;
    if (payFrequency === 'Weekly') {{
        perPeriod = salaryAmount / 52;
    }} else if (payFrequency === 'Biweekly') {{
        perPeriod = salaryAmount / 26;
    }} else if (payFrequency === 'Semi-Monthly' || payFrequency === 'Semimonthly') {{
        perPeriod = salaryAmount / 24;
    }} else if (payFrequency === 'Monthly') {{
        perPeriod = salaryAmount / 12;
    }} else if (payFrequency === 'Quarterly') {{
        perPeriod = salaryAmount / 4;
    }} else if (payFrequency === 'Yearly') {{
        perPeriod = salaryAmount;
    }} else {{
        perPeriod = salaryAmount / 26;
    }}

    if (payType === 'Salary') {{
        salaryWrap.style.display = 'block';
        salaryPerPeriodWrap.style.display = 'block';
        hourlyWrap.style.display = 'none';
        overtimeWrap.style.display = 'none';
        overtimeHoursWrap.style.display = 'none';
        regularHoursWrap.style.display = 'none';

        salaryDisplay.value = salaryAmount.toFixed(2);
        salaryPerPeriodDisplay.value = perPeriod.toFixed(2);
        rateRegular.value = 0;
        rateOvertime.value = 0;
        hoursOvertime.value = 0;
        hoursRegular.value = 1;
    }} else {{
        salaryWrap.style.display = 'none';
        salaryPerPeriodWrap.style.display = 'none';
        hourlyWrap.style.display = 'block';
        overtimeWrap.style.display = 'block';
        overtimeHoursWrap.style.display = 'block';
        regularHoursWrap.style.display = 'block';

        rateRegular.value = hourlyRate.toFixed(2);
        rateOvertime.value = overtimeRate.toFixed(2);
    }}

    autoFillHoursFromTimeClock();
}}

function resetPayrollPreview(message) {{
    document.getElementById('preview_gross').innerText = '$0.00';
    document.getElementById('preview_federal').innerText = '$0.00';
    document.getElementById('preview_state').innerText = '$0.00';
    document.getElementById('preview_ss').innerText = '$0.00';
    document.getElementById('preview_medicare').innerText = '$0.00';
    document.getElementById('preview_local').innerText = '$0.00';
    document.getElementById('preview_other').innerText = '$0.00';
    document.getElementById('preview_net').innerText = '$0.00';
    document.getElementById('preview_meta').innerText = message || 'Select an employee to preview payroll.';
}}

async function runPayrollPreview() {{
    const form = document.getElementById('payroll_form');
    const employeeId = document.getElementById('employee_id').value;

    if (!employeeId) {{
        resetPayrollPreview('Select an employee to preview payroll.');
        return;
    }}

    const formData = new FormData(form);

    try {{
        const response = await fetch("{url_for('payroll.payroll_preview')}", {{
            method: 'POST',
            headers: {{
                "X-CSRFToken": getCsrfToken()
            }},
            body: formData
        }});

        const data = await response.json();

        if (!response.ok || !data.ok) {{
            resetPayrollPreview(data.message || 'Unable to preview payroll.');
            return;
        }}

        document.getElementById('preview_gross').innerText = formatMoney(data.gross_pay);
        document.getElementById('preview_federal').innerText = formatMoney(data.federal_withholding);
        document.getElementById('preview_state').innerText = formatMoney(data.state_withholding);
        document.getElementById('preview_ss').innerText = formatMoney(data.social_security);
        document.getElementById('preview_medicare').innerText = formatMoney(data.medicare);
        document.getElementById('preview_local').innerText = formatMoney(data.local_tax);
        document.getElementById('preview_other').innerText = formatMoney(data.other_deductions);
        document.getElementById('preview_net').innerText = formatMoney(data.net_pay);

        document.getElementById('preview_meta').innerText =
            'Provider: ' + (data.provider || 'internal') +
            ' | State: ' + (data.state_name || '-') +
            ' | Local: ' + (data.local_name || '-') +
            ' | County Used: ' + (data.county_used || '-') +
            ' | Source: ' + (data.county_source || '-') +
            ' | Local Rate: ' + (((parseFloat(data.local_tax_rate || 0)) * 100).toFixed(3)) + '%' +
            ' | Pay Type: ' + (data.pay_type || '-') +
            ' | Frequency: ' + (data.pay_frequency || '-') +
            ' | W-4 Step 3: ' + formatMoney(data.w4_step3_amount || 0);
    }} catch (error) {{
        resetPayrollPreview('Unable to preview payroll.');
    }}
}}

function triggerPayrollPreview() {{
    if (payrollPreviewTimeout) {{
        clearTimeout(payrollPreviewTimeout);
    }}

    payrollPreviewTimeout = setTimeout(runPayrollPreview, 250);
}}

document.addEventListener('DOMContentLoaded', function() {{
    const payPeriodStart = document.getElementById('pay_period_start');
    const payPeriodEnd = document.getElementById('pay_period_end');

    if (payPeriodStart) {{
        payPeriodStart.addEventListener('change', function() {{
            autoFillHoursFromTimeClock();
            triggerPayrollPreview();
        }});
    }}

    if (payPeriodEnd) {{
        payPeriodEnd.addEventListener('change', function() {{
            autoFillHoursFromTimeClock();
            triggerPayrollPreview();
        }});
    }}

    fillEmployeePayrollInfo();
    triggerPayrollPreview();
}});
</script>
"""
    return render_page(content, "Employee Payroll")


@payroll_bp.route("/employees/payroll/<int:payroll_id>")
@login_required
@subscription_required
@require_permission("can_manage_payroll")
def view_payroll_entry(payroll_id):
    ensure_employee_payroll_columns()
    ensure_bookkeeping_history_table()
    ensure_payroll_table_structure()
    ensure_payroll_check_structure()

    conn = get_db_connection()
    cid = session["company_id"]

    row = conn.execute(
        """
        SELECT
            p.*,
            e.first_name,
            e.last_name,
            e.full_name
        FROM payroll_entries p
        JOIN employees e ON p.employee_id = e.id
        WHERE p.id = %s AND p.company_id = %s
        """,
        (payroll_id, cid),
    ).fetchone()

    conn.close()

    if not row:
        flash("Payroll entry not found.")
        return redirect(url_for("payroll.employee_payroll"))

    employee_name = (
        clean_text_input(row["full_name"])
        or f"{clean_text_input(row['first_name'])} {clean_text_input(row['last_name'])}".strip()
        or "Employee"
    )

    payment_method = clean_text_input(row["payment_method"]) or "-"
    check_number = row["check_number"] if "check_number" in row.keys() else None
    can_print_check = payment_method == "Check" or bool(check_number)

    row_csrf = generate_csrf()

    print_button = ""
    if can_print_check:
        print_button = f"""
        <a class='btn warning' href='{url_for("payroll.print_payroll_check", payroll_id=payroll_id)}' target='_blank'>Print Check</a>
        """

    content = f"""
    <style>
        .payroll-view-page {{
            display: grid;
            gap: 18px;
        }}

        .payroll-view-head {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 12px;
            flex-wrap: wrap;
        }}

        .payroll-summary-grid {{
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 12px;
        }}

        .payroll-summary-card {{
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 14px;
            padding: 14px;
            background: #fff;
        }}

        .payroll-summary-card span {{
            display: block;
            font-size: .8rem;
            color: #64748b;
            margin-bottom: 4px;
        }}

        .payroll-summary-card strong {{
            display: block;
            color: #0f172a;
            line-height: 1.3;
            word-break: break-word;
        }}

        .payroll-notes-box {{
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 14px;
            padding: 14px;
            background: #fff;
            color: #0f172a;
            line-height: 1.5;
        }}

        @media (max-width: 900px) {{
            .payroll-summary-grid {{
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }}
        }}

        @media (max-width: 640px) {{
            .payroll-summary-grid {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>

    <div class='payroll-view-page'>
        <div class='card'>
            <div class='payroll-view-head'>
                <div>
                    <h1 style='margin-bottom:6px;'>Payroll Entry</h1>
                    <p class='muted' style='margin:0;'>{html_escape(employee_name)} · {html_escape(clean_text_display(row["pay_date"]))}</p>
                </div>
                <div class='row-actions'>
                    {print_button}
                    <a class='btn secondary' href='{url_for("payroll.employee_payroll")}'>Back to Payroll</a>
                </div>
            </div>
        </div>

        <div class='card'>
            <h2>Payroll Summary</h2>
            <div class='payroll-summary-grid'>
                <div class='payroll-summary-card'><span>Employee</span><strong>{html_escape(employee_name)}</strong></div>
                <div class='payroll-summary-card'><span>Pay Date</span><strong>{html_escape(clean_text_display(row["pay_date"]))}</strong></div>
                <div class='payroll-summary-card'><span>Pay Type</span><strong>{html_escape(clean_text_display(row["pay_type"]))}</strong></div>
                <div class='payroll-summary-card'><span>Payment Method</span><strong>{html_escape(payment_method)}</strong></div>

                <div class='payroll-summary-card'><span>Check #</span><strong>{html_escape(str(check_number) if check_number else "-")}</strong></div>
                <div class='payroll-summary-card'><span>Pay Period Start</span><strong>{html_escape(clean_text_display(row["pay_period_start"]))}</strong></div>
                <div class='payroll-summary-card'><span>Pay Period End</span><strong>{html_escape(clean_text_display(row["pay_period_end"]))}</strong></div>
                <div class='payroll-summary-card'><span>Regular Hours</span><strong>{float(row["hours_regular"] or 0):.2f}</strong></div>

                <div class='payroll-summary-card'><span>Overtime Hours</span><strong>{float(row["hours_overtime"] or 0):.2f}</strong></div>
                <div class='payroll-summary-card'><span>Regular Rate</span><strong>${float(row["rate_regular"] or 0):.2f}</strong></div>
                <div class='payroll-summary-card'><span>Overtime Rate</span><strong>${float(row["rate_overtime"] or 0):.2f}</strong></div>
                <div class='payroll-summary-card'><span>Gross Pay</span><strong>${float(row["gross_pay"] or 0):.2f}</strong></div>

                <div class='payroll-summary-card'><span>Federal</span><strong>${float(row["federal_withholding"] or 0):.2f}</strong></div>
                <div class='payroll-summary-card'><span>State</span><strong>${float(row["state_withholding"] or 0):.2f}</strong></div>
                <div class='payroll-summary-card'><span>Social Security</span><strong>${float(row["social_security"] or 0):.2f}</strong></div>
                <div class='payroll-summary-card'><span>Medicare</span><strong>${float(row["medicare"] or 0):.2f}</strong></div>

                <div class='payroll-summary-card'><span>Local Tax</span><strong>${float(row["local_tax"] or 0):.2f}</strong></div>
                <div class='payroll-summary-card'><span>Other Deductions</span><strong>${float(row["other_deductions"] or 0):.2f}</strong></div>
                <div class='payroll-summary-card'><span>Net Pay</span><strong>${float(row["net_pay"] or 0):.2f}</strong></div>
            </div>
        </div>

        <div class='card'>
            <h2>Notes</h2>
            <div class='payroll-notes-box'>{html_escape(clean_text_display(row["notes"], "-"))}</div>
        </div>

        <div class='card'>
            <div class='row-actions'>
                {print_button}
                <form method='post'
                      action='{url_for("payroll.delete_payroll_entry", payroll_id=payroll_id)}'
                      onsubmit="return confirm('Delete this payroll entry?');"
                      style='margin:0;'>
                    <input type="hidden" name="csrf_token" value="{row_csrf}">
                    <button class='btn danger' type='submit'>Delete</button>
                </form>
            </div>
        </div>
    </div>
    """
    return render_page(content, f"Payroll Entry #{payroll_id}")


@payroll_bp.route("/employees/payroll/<int:payroll_id>/print-check")
@login_required
@subscription_required
@require_permission("can_manage_payroll")
def print_payroll_check(payroll_id):
    ensure_employee_payroll_columns()
    ensure_bookkeeping_history_table()
    ensure_payroll_table_structure()
    ensure_payroll_check_structure()

    conn = get_db_connection()
    cid = session["company_id"]

    row = conn.execute(
        """
        SELECT
            p.*,
            e.first_name,
            e.last_name,
            e.full_name
        FROM payroll_entries p
        JOIN employees e ON p.employee_id = e.id
        WHERE p.id = %s AND p.company_id = %s
        """,
        (payroll_id, cid),
    ).fetchone()

    if not row:
        conn.close()
        flash("Payroll entry not found.")
        return redirect(url_for("payroll.employee_payroll"))

    employee_name = (
        clean_text_input(row["full_name"])
        or f"{clean_text_input(row['first_name'])} {clean_text_input(row['last_name'])}".strip()
        or "Employee"
    )

    if money(row["net_pay"]) <= 0:
        conn.close()
        flash("Cannot print a check for a payroll entry with zero or negative net pay.")
        return redirect(url_for("payroll.employee_payroll"))

    _, check_number = create_or_get_payroll_check(conn, cid, row, employee_name)
    conn.commit()

    profile = get_company_profile_row(cid) or {}
    company_info = get_company_check_info(profile)

    refreshed_row = conn.execute(
        """
        SELECT
            p.*,
            e.first_name,
            e.last_name,
            e.full_name
        FROM payroll_entries p
        JOIN employees e ON p.employee_id = e.id
        WHERE p.id = %s AND p.company_id = %s
        """,
        (payroll_id, cid),
    ).fetchone()

    pdf_data = build_payroll_check_pdf(
        company_info=company_info,
        payroll_row=refreshed_row,
        employee_name=employee_name,
        check_number=check_number,
    )

    conn.close()

    response = make_response(pdf_data)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f"inline; filename=payroll_check_{check_number}.pdf"
    return response


@payroll_bp.route("/employees/payroll/export")
@login_required
@subscription_required
@require_permission("can_manage_payroll")
def export_payroll():
    ensure_employee_payroll_columns()
    ensure_bookkeeping_history_table()
    ensure_payroll_table_structure()
    ensure_payroll_check_structure()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT
            p.*,
            e.first_name,
            e.last_name
        FROM payroll_entries p
        JOIN employees e ON p.employee_id = e.id
        WHERE p.company_id = %s
        ORDER BY p.pay_date DESC, p.id DESC
        """,
        (cid,),
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Payroll ID",
        "Pay Date",
        "Employee",
        "Pay Type",
        "Payment Method",
        "Check Number",
        "Pay Period Start",
        "Pay Period End",
        "Regular Hours",
        "Overtime Hours",
        "Regular Rate",
        "Overtime Rate",
        "Gross Pay",
        "Federal Withholding",
        "State Withholding",
        "Social Security",
        "Medicare",
        "Local Tax",
        "Other Deductions",
        "Net Pay",
        "Notes",
    ])

    for r in rows:
        employee_name = f"{clean_text_input(r['first_name'])} {clean_text_input(r['last_name'])}".strip()

        writer.writerow([
            r["id"] or "",
            clean_text_input(r["pay_date"]),
            employee_name,
            clean_text_input(r["pay_type"]),
            clean_text_input(r["payment_method"]) if "payment_method" in r.keys() else "",
            r["check_number"] if "check_number" in r.keys() and r["check_number"] else "",
            clean_text_input(r["pay_period_start"]),
            clean_text_input(r["pay_period_end"]),
            float(r["hours_regular"] or 0),
            float(r["hours_overtime"] or 0),
            float(r["rate_regular"] or 0),
            float(r["rate_overtime"] or 0),
            float(r["gross_pay"] or 0),
            float(r["federal_withholding"] or 0),
            float(r["state_withholding"] or 0),
            float(r["social_security"] or 0),
            float(r["medicare"] or 0),
            float(r["local_tax"] or 0),
            float(r["other_deductions"] or 0),
            float(r["net_pay"] or 0),
            clean_text_input(r["notes"]),
        ])

    conn.close()

    csv_data = output.getvalue()
    output.close()

    filename = f"payroll_export_{date.today().isoformat()}.csv"

    response = make_response(csv_data)
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response


@payroll_bp.route("/payroll/add", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_payroll")
def add_payroll():
    return redirect(url_for("payroll.employee_payroll"))


@payroll_bp.route("/employees/payroll/<int:payroll_id>/delete", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_payroll")
def delete_payroll_entry(payroll_id):
    ensure_payroll_check_structure()

    conn = get_db_connection()
    cid = session["company_id"]

    row = conn.execute(
        """
        SELECT id, ledger_entry_id, check_id
        FROM payroll_entries
        WHERE id = %s AND company_id = %s
        """,
        (payroll_id, cid),
    ).fetchone()

    if not row:
        conn.close()
        flash("Payroll entry not found.")
        return redirect(url_for("payroll.employee_payroll"))

    if "ledger_entry_id" in row.keys() and row["ledger_entry_id"]:
        conn.execute(
            "DELETE FROM ledger_entries WHERE id = %s AND company_id = %s",
            (row["ledger_entry_id"], cid),
        )

    if "check_id" in row.keys() and row["check_id"]:
        conn.execute(
            "DELETE FROM checks WHERE id = %s AND company_id = %s",
            (row["check_id"], cid),
        )

    conn.execute(
        """
        DELETE FROM payroll_entries
        WHERE id = %s AND company_id = %s
        """,
        (payroll_id, cid),
    )
    conn.commit()
    conn.close()

    flash("Payroll entry deleted.")
    return redirect(url_for("payroll.employee_payroll"))