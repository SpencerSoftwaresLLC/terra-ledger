from flask import Blueprint, session, url_for, request, redirect, flash, make_response
from flask_wtf.csrf import generate_csrf
from html import escape
from datetime import date, datetime
import csv
import io
from decimal import Decimal, ROUND_HALF_UP

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch

from db import get_db_connection, table_columns
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page
from helpers import get_period_range

bookkeeping_bp = Blueprint("bookkeeping", __name__)


EXPENSE_TYPES = {
    "expense",
    "expenses",
    "cost",
    "job cost",
    "material",
    "materials",
    "mulch",
    "stone",
    "dump fee",
    "dump_fee",
    "plants",
    "trees",
    "soil",
    "fertilizer",
    "hardscape material",
    "hardscape_material",
    "labor",
    "labour",
    "fuel",
    "equipment",
    "delivery",
    "misc",
    "payroll",
    "hand tools",
    "office supplies",
    "maintenance",
    "power equipment",
    "vehicles",
    "insurance",
    "marketing",
    "office and admin",
    "safety gear",
    "licensing & certifications",
    "licensing and certifications",
}

JOB_COST_CATEGORY_MAP = {
    "material": "Material",
    "materials": "Material",
    "mulch": "Mulch",
    "stone": "Stone",
    "dump fee": "Dump Fee",
    "dump_fee": "Dump Fee",
    "plants": "Plants",
    "trees": "Trees",
    "soil": "Soil",
    "fertilizer": "Fertilizer",
    "hardscape material": "Hardscape Material",
    "hardscape_material": "Hardscape Material",
    "labor": "Labor",
    "labour": "Labor",
    "fuel": "Fuel",
    "equipment": "Equipment",
    "delivery": "Delivery",
    "misc": "Misc",
    "payroll": "Payroll",
    "hand tools": "Hand Tools",
    "office supplies": "Office Supplies",
    "maintenance": "Maintenance",
    "power equipment": "Power Equipment",
    "vehicles": "Vehicles",
    "insurance": "Insurance",
    "marketing": "Marketing",
    "office and admin": "Office and Admin",
    "safety gear": "Safety Gear",
    "licensing & certifications": "Licensing & Certifications",
    "licensing and certifications": "Licensing & Certifications",
}

SERVICE_TYPE_LABELS = {
    "mowing": "Mowing",
    "mulch": "Mulch",
    "cleanup": "Cleanup",
    "installation": "Installation",
    "hardscape": "Hardscape",
    "snow_removal": "Snow Removal",
    "fertilizing": "Fertilizing",
    "other": "Other",
}


def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _fmt_money(value, show_plus=False):
    amount = _safe_float(value)
    if amount < 0:
        return f"-${abs(amount):,.2f}"
    if show_plus and amount > 0:
        return f"+${amount:,.2f}"
    return f"${amount:,.2f}"


def _normalize_text(value):
    return str(value or "").strip().lower()


def _clean_text(value):
    text = str(value or "").strip()
    if text.lower() in {"none", "null", "n/a"}:
        return ""
    return text


def _money(value):
    return float(Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _normalize_service_type(value):
    key = _normalize_text(value).replace("-", "_").replace(" ", "_")
    return key if key in SERVICE_TYPE_LABELS else ""


def _display_service_type(value, fallback="-"):
    key = _normalize_service_type(value)
    if not key:
        return fallback
    return SERVICE_TYPE_LABELS.get(key, fallback)


def _service_chip_class(value):
    key = _normalize_service_type(value)
    if key == "mowing":
        return "mowing"
    if key in {"mulch", "installation", "hardscape"}:
        return "material"
    if key in {"cleanup", "snow_removal"}:
        return "seasonal"
    return "default"


def _table_exists(conn, table_name):
    try:
        return len(table_columns(conn, table_name)) > 0
    except Exception:
        return False


def _has_col(conn, table_name, col_name):
    try:
        return col_name in table_columns(conn, table_name)
    except Exception:
        return False


def _safe_get(row, key, default=None):
    try:
        if row is not None and key in row.keys():
            value = row[key]
            return default if value is None else value
    except Exception:
        pass
    return default


def _canonicalize_category(value):
    v = _normalize_text(value)
    if v in {"dump_fee", "dump fee"}:
        return "Dump Fee"
    if v in {"hardscape_material", "hardscape material"}:
        return "Hardscape Material"
    if v == "mulch":
        return "Mulch"
    if v == "stone":
        return "Stone"
    if v == "plants":
        return "Plants"
    if v == "trees":
        return "Trees"
    if v == "soil":
        return "Soil"
    if v == "fertilizer":
        return "Fertilizer"
    if v in {"labor", "labour"}:
        return "Labor"
    if v == "fuel":
        return "Fuel"
    if v == "equipment":
        return "Equipment"
    if v == "delivery":
        return "Delivery"
    if v == "misc":
        return "Misc"
    if v == "payroll":
        return "Payroll"
    if v == "hand tools":
        return "Hand Tools"
    if v == "office supplies":
        return "Office Supplies"
    if v == "maintenance":
        return "Maintenance"
    if v == "power equipment":
        return "Power Equipment"
    if v == "vehicles":
        return "Vehicles"
    if v == "insurance":
        return "Insurance"
    if v == "marketing":
        return "Marketing"
    if v == "office and admin":
        return "Office and Admin"
    if v == "safety gear":
        return "Safety Gear"
    if v in {"licensing & certifications", "licensing and certifications"}:
        return "Licensing & Certifications"
    if v == "bank deposits":
        return "Bank Deposits"
    if "invoice payment" in v:
        return "Invoice Payments"
    if v == "income":
        return "Income"
    if v in {"material", "materials"}:
        return "Material"
    if not v:
        return ""
    return v.replace("_", " ").title()


def _is_expense_entry(entry_type, description="", category="", source_type="", reference_type=""):
    et = _normalize_text(entry_type)
    desc = _normalize_text(description)
    cat = _normalize_text(category)
    src = _normalize_text(source_type)
    ref = _normalize_text(reference_type)

    if et in EXPENSE_TYPES or cat in EXPENSE_TYPES:
        return True

    for keyword in EXPENSE_TYPES:
        if keyword and (keyword in et or keyword in cat):
            return True

    if src in {"job_item", "payroll"} or ref in {"job_item", "payroll"}:
        return True

    desc_keywords = {
        "labor",
        "labour",
        "mulch",
        "stone",
        "dump fee",
        "dump_fee",
        "plants",
        "trees",
        "soil",
        "fertilizer",
        "hardscape",
        "fuel",
        "equipment",
        "delivery",
        "payroll",
        "material",
        "materials",
        "hand tools",
        "office supplies",
        "maintenance",
        "power equipment",
        "vehicles",
        "insurance",
        "marketing",
        "office and admin",
        "safety gear",
        "licensing",
        "certifications",
    }
    for keyword in desc_keywords:
        if keyword in desc:
            return True

    return False


def _normalize_ledger_type(raw_type, source_type, amount):
    raw = (raw_type or "").strip().lower()
    source = (source_type or "").strip().lower()
    amt = _safe_float(amount, 0)

    if raw in ("income", "payment"):
        return "Income"
    if raw in ("expense", "cost"):
        return "Expense"

    if source in {"job_item", "job_line", "job_material", "job_labor", "job_cost", "payroll"}:
        return "Expense"

    if source in {"invoice_payment", "invoice_paid", "invoice_mark_paid", "payment"}:
        return "Income"

    return "Expense" if amt < 0 else "Income"


def _guess_job_cost_category(entry_type="", description="", category="", source_type="", reference_type=""):
    et = _normalize_text(entry_type)
    desc = _normalize_text(description)
    cat = _normalize_text(category)
    src = _normalize_text(source_type)
    ref = _normalize_text(reference_type)

    for key, label in JOB_COST_CATEGORY_MAP.items():
        if cat == key or et == key:
            return label

    for key, label in JOB_COST_CATEGORY_MAP.items():
        if key in cat or key in et:
            return label

    if src == "job_item" or ref == "job_item":
        if "dump fee" in desc or "dump_fee" in desc:
            return "Dump Fee"
        if "mulch" in desc:
            return "Mulch"
        if "stone" in desc:
            return "Stone"
        if "plant" in desc:
            return "Plants"
        if "tree" in desc:
            return "Trees"
        if "soil" in desc:
            return "Soil"
        if "fertilizer" in desc:
            return "Fertilizer"
        if "hardscape" in desc:
            return "Hardscape Material"
        if "labor" in desc or "labour" in desc or "hour" in desc or "hours" in desc or "hr" in desc or "hrs" in desc:
            return "Labor"
        if "fuel" in desc:
            return "Fuel"
        if "equipment" in desc:
            return "Equipment"
        if "delivery" in desc:
            return "Delivery"
        if "misc" in desc:
            return "Misc"
        return "Material"

    if src == "payroll" or ref == "payroll" or "payroll" in desc:
        return "Payroll"

    return ""


def _get_pl_bucket(entry_type="", description="", category="", source_type="", reference_type=""):
    is_expense = _is_expense_entry(
        entry_type=entry_type,
        description=description,
        category=category,
        source_type=source_type,
        reference_type=reference_type,
    )

    if not is_expense:
        normalized_category = _normalize_text(category)
        normalized_entry_type = _normalize_text(entry_type)
        normalized_desc = _normalize_text(description)

        if "invoice payment" in normalized_category or "invoice payment" in normalized_entry_type:
            return "Invoice Payments"
        if "bank deposit" in normalized_category or "bank deposit" in normalized_entry_type:
            return "Bank Deposits"
        if "income" in normalized_category:
            return "Income"
        if "invoice" in normalized_desc and "payment" in normalized_desc:
            return "Invoice Payments"
        return "Income"

    job_cost_bucket = _guess_job_cost_category(
        entry_type=entry_type,
        description=description,
        category=category,
        source_type=source_type,
        reference_type=reference_type,
    )
    if job_cost_bucket:
        return job_cost_bucket

    normalized_category = _normalize_text(category)
    normalized_entry_type = _normalize_text(entry_type)

    if normalized_category:
        return _canonicalize_category(normalized_category)
    if normalized_entry_type:
        return _canonicalize_category(normalized_entry_type)

    return "Expense"


def _ensure_bookkeeping_check_structure():
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

    cur.execute("ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS payee_name TEXT")
    cur.execute("ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS payment_method TEXT")
    cur.execute("ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS check_id INTEGER")
    cur.execute("ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS check_number INTEGER")

    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS next_check_number INTEGER DEFAULT 1001")
    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS company_check_name TEXT")
    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS check_address_line_1 TEXT")
    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS check_address_line_2 TEXT")
    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS check_city TEXT")
    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS check_state TEXT")
    cur.execute("ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS check_zip TEXT")

    conn.commit()
    conn.close()


def _number_to_words_under_1000(n):
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
    return f"{ones[n // 100]} Hundred {_number_to_words_under_1000(n % 100)}"


def _number_to_words(n):
    n = int(n)
    if n == 0:
        return "Zero"

    parts = []

    billions = n // 1_000_000_000
    if billions:
        parts.append(f"{_number_to_words_under_1000(billions)} Billion")
        n %= 1_000_000_000

    millions = n // 1_000_000
    if millions:
        parts.append(f"{_number_to_words_under_1000(millions)} Million")
        n %= 1_000_000

    thousands = n // 1000
    if thousands:
        parts.append(f"{_number_to_words_under_1000(thousands)} Thousand")
        n %= 1000

    if n:
        parts.append(_number_to_words_under_1000(n))

    return " ".join(parts)


def _amount_to_words(amount):
    amount = Decimal(str(amount or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    dollars = int(amount)
    cents = int((amount - Decimal(dollars)) * 100)
    return f"{_number_to_words(dollars)} and {cents:02d}/100"


def _get_company_profile_row(company_id):
    conn = get_db_connection()
    try:
        return conn.execute(
            "SELECT * FROM company_profile WHERE company_id = %s",
            (company_id,),
        ).fetchone()
    finally:
        conn.close()


def _get_company_check_info(company_id):
    profile = _get_company_profile_row(company_id) or {}

    company_name = (
        _clean_text(profile["company_check_name"]) if "company_check_name" in profile.keys() else ""
    ) or (
        _clean_text(profile["company_name"]) if "company_name" in profile.keys() else ""
    ) or (
        _clean_text(profile["name"]) if "name" in profile.keys() else ""
    ) or "Company"

    address_line_1 = (
        _clean_text(profile["check_address_line_1"]) if "check_address_line_1" in profile.keys() else ""
    ) or (
        _clean_text(profile["address_line_1"]) if "address_line_1" in profile.keys() else ""
    ) or (
        _clean_text(profile["address"]) if "address" in profile.keys() else ""
    )
    address_line_2 = _clean_text(profile["check_address_line_2"]) if "check_address_line_2" in profile.keys() else ""
    city = (_clean_text(profile["check_city"]) if "check_city" in profile.keys() else "") or (_clean_text(profile["city"]) if "city" in profile.keys() else "")
    state = (_clean_text(profile["check_state"]) if "check_state" in profile.keys() else "") or (_clean_text(profile["state"]) if "state" in profile.keys() else "")
    zip_code = (_clean_text(profile["check_zip"]) if "check_zip" in profile.keys() else "") or (_clean_text(profile["zip"]) if "zip" in profile.keys() else "")

    city_state_zip = " ".join(part for part in [city, state, zip_code] if part).strip()
    next_check_number = int(profile["next_check_number"] or 1001) if "next_check_number" in profile.keys() else 1001

    return {
        "company_name": company_name,
        "address_line_1": address_line_1,
        "address_line_2": address_line_2,
        "city_state_zip": city_state_zip,
        "next_check_number": next_check_number,
    }


def _build_ledger_check_pdf(company_info, ledger_row, check_number):
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    amount = _money(_safe_get(ledger_row, "amount", 0))
    amount_written = _amount_to_words(amount)
    check_date = _clean_text(_safe_get(ledger_row, "entry_date", "")) or date.today().isoformat()
    payee_name = _clean_text(_safe_get(ledger_row, "payee_name", "")) or _clean_text(_safe_get(ledger_row, "description", "")) or "Payee"
    memo = _clean_text(_safe_get(ledger_row, "description", "")) or (_clean_text(_safe_get(ledger_row, "category", "")) or "Bookkeeping Entry")

    top_y = height - 0.75 * inch
    c.setFont("Helvetica-Bold", 13)
    c.drawString(0.7 * inch, top_y, company_info["company_name"])

    c.setFont("Helvetica", 9)
    line_y = top_y - 0.18 * inch
    if company_info["address_line_1"]:
        c.drawString(0.7 * inch, line_y, company_info["address_line_1"])
        line_y -= 0.15 * inch
    if company_info["address_line_2"]:
        c.drawString(0.7 * inch, line_y, company_info["address_line_2"])
        line_y -= 0.15 * inch
    if company_info["city_state_zip"]:
        c.drawString(0.7 * inch, line_y, company_info["city_state_zip"])

    c.setFont("Helvetica-Bold", 11)
    c.drawRightString(width - 0.7 * inch, top_y, f"Check #{check_number}")
    c.setFont("Helvetica", 10)
    c.drawRightString(width - 0.7 * inch, top_y - 0.24 * inch, f"Date: {check_date}")

    c.setFont("Helvetica", 10)
    c.drawString(0.7 * inch, height - 1.75 * inch, "Pay to the Order of:")
    c.line(1.95 * inch, height - 1.79 * inch, width - 1.65 * inch, height - 1.79 * inch)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2.05 * inch, height - 1.72 * inch, payee_name[:60])

    c.rect(width - 1.85 * inch, height - 1.97 * inch, 1.15 * inch, 0.33 * inch)
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(width - 1.275 * inch, height - 1.84 * inch, f"${amount:,.2f}")

    c.setFont("Helvetica", 10)
    c.drawString(0.7 * inch, height - 2.35 * inch, amount_written)
    c.line(0.7 * inch, height - 2.42 * inch, width - 1.05 * inch, height - 2.42 * inch)

    c.setFont("Helvetica", 9)
    c.drawString(0.7 * inch, height - 2.78 * inch, "Memo")
    c.line(1.05 * inch, height - 2.82 * inch, 3.8 * inch, height - 2.82 * inch)
    c.drawString(1.12 * inch, height - 2.75 * inch, memo[:40])

    c.drawString(width - 2.55 * inch, height - 2.78 * inch, "Authorized Signature")
    c.line(width - 2.65 * inch, height - 2.82 * inch, width - 0.7 * inch, height - 2.82 * inch)

    divider_y = height - 3.45 * inch
    c.setDash(4, 3)
    c.line(0.5 * inch, divider_y, width - 0.5 * inch, divider_y)
    c.setDash()

    c.setFont("Helvetica-Bold", 12)
    c.drawString(0.7 * inch, divider_y - 0.35 * inch, "Check Stub")

    c.setFont("Helvetica", 10)
    stub_y = divider_y - 0.65 * inch
    c.drawString(0.7 * inch, stub_y, f"Payee: {payee_name[:55]}")
    c.drawString(4.2 * inch, stub_y, f"Check #: {check_number}")

    stub_y -= 0.22 * inch
    c.drawString(0.7 * inch, stub_y, f"Date: {check_date}")
    c.drawString(4.2 * inch, stub_y, f"Method: Check")

    stub_y -= 0.22 * inch
    c.drawString(0.7 * inch, stub_y, f"Category: {_clean_text(_safe_get(ledger_row, 'category', '')) or '-'}")

    stub_y -= 0.22 * inch
    c.drawString(0.7 * inch, stub_y, f"Description: {memo[:80]}")

    stub_y -= 0.22 * inch
    c.drawString(0.7 * inch, stub_y, f"Amount: ${amount:,.2f}")

    notes = _clean_text(_safe_get(ledger_row, "notes", ""))
    if notes:
        stub_y -= 0.30 * inch
        c.setFont("Helvetica-Bold", 10)
        c.drawString(0.7 * inch, stub_y, "Notes:")
        c.setFont("Helvetica", 10)
        c.drawString(1.1 * inch, stub_y, notes[:95])

    c.showPage()
    c.save()

    pdf_data = buffer.getvalue()
    buffer.close()
    return pdf_data


def _fetch_ledger_entry_by_id(conn, cid, entry_id):
    ledger_cols = table_columns(conn, "ledger_entries")

    def col_or(expr, alias):
        return f"{expr} AS {alias}"

    date_select = "le.entry_date" if "entry_date" in ledger_cols else ("le.date" if "date" in ledger_cols else "NULL")
    entry_type_select = "le.entry_type" if "entry_type" in ledger_cols else "''"
    category_select = "le.category" if "category" in ledger_cols else "''"
    amount_select = "le.amount" if "amount" in ledger_cols else "0"

    if "description" in ledger_cols and "memo" in ledger_cols:
        description_select = "COALESCE(le.description, le.memo, '')"
    elif "description" in ledger_cols:
        description_select = "COALESCE(le.description, '')"
    elif "memo" in ledger_cols:
        description_select = "COALESCE(le.memo, '')"
    else:
        description_select = "''"

    notes_select = "le.notes" if "notes" in ledger_cols else "''"
    source_type_select = "le.source_type" if "source_type" in ledger_cols else "''"
    reference_type_select = "le.reference_type" if "reference_type" in ledger_cols else "''"
    source_id_select = "le.source_id" if "source_id" in ledger_cols else "NULL"
    customer_id_select = "le.customer_id" if "customer_id" in ledger_cols else "NULL"
    invoice_id_select = "le.invoice_id" if "invoice_id" in ledger_cols else "NULL"
    job_id_select = "le.job_id" if "job_id" in ledger_cols else "NULL"
    payee_name_select = "le.payee_name" if "payee_name" in ledger_cols else "''"
    payment_method_select = "le.payment_method" if "payment_method" in ledger_cols else "''"
    check_id_select = "le.check_id" if "check_id" in ledger_cols else "NULL"
    check_number_select = "le.check_number" if "check_number" in ledger_cols else "NULL"

    job_join = "LEFT JOIN jobs j ON le.job_id = j.id"
    service_type_select = "COALESCE(j.service_type, '')"

    return conn.execute(
        f"""
        SELECT
            le.id,
            le.company_id,
            {col_or(date_select, "entry_date")},
            {col_or(entry_type_select, "entry_type")},
            {col_or(category_select, "category")},
            {col_or(amount_select, "amount")},
            {col_or(description_select, "description")},
            {col_or(notes_select, "notes")},
            {col_or(source_type_select, "source_type")},
            {col_or(reference_type_select, "reference_type")},
            {col_or(source_id_select, "source_id")},
            {col_or(customer_id_select, "customer_id")},
            {col_or(invoice_id_select, "invoice_id")},
            {col_or(job_id_select, "job_id")},
            {col_or(payee_name_select, "payee_name")},
            {col_or(payment_method_select, "payment_method")},
            {col_or(check_id_select, "check_id")},
            {col_or(check_number_select, "check_number")},
            {col_or(service_type_select, "service_type")}
        FROM ledger_entries le
        {job_join}
        WHERE le.id = %s AND le.company_id = %s
        """,
        (entry_id, cid),
    ).fetchone()


def _create_or_get_ledger_check(conn, company_id, ledger_row):
    existing_check_id = _safe_get(ledger_row, "check_id")
    existing_check_number = _safe_get(ledger_row, "check_number")

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

    profile = conn.execute(
        """
        SELECT *
        FROM company_profile
        WHERE company_id = %s
        FOR UPDATE
        """,
        (company_id,),
    ).fetchone()

    current_next_check_number = 1001
    if profile and "next_check_number" in profile.keys() and profile["next_check_number"] is not None:
        current_next_check_number = int(profile["next_check_number"])

    amount = abs(_money(_safe_get(ledger_row, "amount", 0)))
    amount_written = _amount_to_words(amount)
    memo = _clean_text(_safe_get(ledger_row, "description", "")) or (_clean_text(_safe_get(ledger_row, "category", "")) or "Bookkeeping Entry")
    payee_name = _clean_text(_safe_get(ledger_row, "payee_name", "")) or _clean_text(_safe_get(ledger_row, "description", "")) or "Payee"
    check_date = _clean_text(_safe_get(ledger_row, "entry_date", "")) or date.today().isoformat()

    inserted = conn.execute(
        """
        INSERT INTO checks (
            company_id, check_number, check_date, payee_name, amount,
            amount_written, memo, source_type, source_id, status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'ledger', %s, 'Printed')
        RETURNING id
        """,
        (
            company_id,
            current_next_check_number,
            check_date,
            payee_name,
            amount,
            amount_written,
            memo,
            _safe_get(ledger_row, "id"),
        ),
    ).fetchone()

    conn.execute(
        """
        UPDATE ledger_entries
        SET payee_name = %s,
            payment_method = 'Check',
            check_id = %s,
            check_number = %s
        WHERE id = %s AND company_id = %s
        """,
        (
            payee_name,
            inserted["id"],
            current_next_check_number,
            _safe_get(ledger_row, "id"),
            company_id,
        ),
    )

    conn.execute(
        """
        UPDATE company_profile
        SET next_check_number = %s
        WHERE company_id = %s
        """,
        (current_next_check_number + 1, company_id),
    )

    return int(inserted["id"]), int(current_next_check_number)


def _get_ledger_date_column(conn):
    ledger_cols = table_columns(conn, "ledger_entries")
    for possible in ["entry_date", "date", "posted_at", "created_at"]:
        if possible in ledger_cols:
            return possible
    return None


def _get_ledger_select_parts(conn):
    ledger_cols = table_columns(conn, "ledger_entries")

    date_col = None
    for possible in ["entry_date", "date", "posted_at", "created_at"]:
        if possible in ledger_cols:
            date_col = possible
            break

    description_expr = []
    if "description" in ledger_cols:
        description_expr.append("le.description")
    if "memo" in ledger_cols:
        description_expr.append("le.memo")
    if "notes" in ledger_cols:
        description_expr.append("le.notes")
    if "source_type" in ledger_cols:
        description_expr.append("le.source_type")
    if "reference_type" in ledger_cols:
        description_expr.append("le.reference_type")

    desc_sql = "COALESCE(" + ", ".join(description_expr) + ", '')" if description_expr else "''"
    entry_type_expr = "le.entry_type" if "entry_type" in ledger_cols else "'Entry'"
    amount_expr = "le.amount" if "amount" in ledger_cols else "0"
    category_expr = "le.category" if "category" in ledger_cols else "NULL"
    source_type_expr = "le.source_type" if "source_type" in ledger_cols else "NULL"
    reference_type_expr = "le.reference_type" if "reference_type" in ledger_cols else "NULL"
    source_id_expr = "le.source_id" if "source_id" in ledger_cols else "NULL"
    customer_id_expr = "le.customer_id" if "customer_id" in ledger_cols else "NULL"
    invoice_id_expr = "le.invoice_id" if "invoice_id" in ledger_cols else "NULL"
    job_id_expr = "le.job_id" if "job_id" in ledger_cols else "NULL"
    notes_expr = "le.notes" if "notes" in ledger_cols else "''"
    service_type_expr = "COALESCE(j.service_type, '')"

    return {
        "ledger_cols": ledger_cols,
        "date_col": date_col,
        "desc_sql": desc_sql,
        "entry_type_expr": entry_type_expr,
        "amount_expr": amount_expr,
        "category_expr": category_expr,
        "source_type_expr": source_type_expr,
        "reference_type_expr": reference_type_expr,
        "source_id_expr": source_id_expr,
        "customer_id_expr": customer_id_expr,
        "invoice_id_expr": invoice_id_expr,
        "job_id_expr": job_id_expr,
        "notes_expr": notes_expr,
        "service_type_expr": service_type_expr,
    }


def _build_job_item_query(conn, start_date, end_date):
    if not _table_exists(conn, "job_items") or not _table_exists(conn, "jobs"):
        return None, None

    cols = table_columns(conn, "job_items")

    date_col = None
    for candidate in ("entry_date", "item_date", "created_date", "date_created", "created_at", "date"):
        if candidate in cols:
            date_col = candidate
            break

    job_id_col = "job_id" if "job_id" in cols else "id"

    category_expr = "'Material'"
    if "item_type" in cols:
        category_expr = """
            CASE LOWER(COALESCE(NULLIF(TRIM(ji.item_type), ''), 'material'))
                WHEN 'labor' THEN 'Labor'
                WHEN 'fuel' THEN 'Fuel'
                WHEN 'equipment' THEN 'Equipment'
                WHEN 'delivery' THEN 'Delivery'
                WHEN 'misc' THEN 'Misc'
                WHEN 'mulch' THEN 'Mulch'
                WHEN 'stone' THEN 'Stone'
                WHEN 'dump_fee' THEN 'Dump Fee'
                WHEN 'plants' THEN 'Plants'
                WHEN 'trees' THEN 'Trees'
                WHEN 'soil' THEN 'Soil'
                WHEN 'fertilizer' THEN 'Fertilizer'
                WHEN 'hardscape_material' THEN 'Hardscape Material'
                ELSE 'Material'
            END
        """
    else:
        for candidate in ("category", "cost_category", "item_category", "type"):
            if candidate in cols:
                category_expr = f"COALESCE(NULLIF(TRIM(ji.{candidate}), ''), 'Material')"
                break

    description_expr = "'Job item'"
    for candidate in ("description", "name", "item_name", "title"):
        if candidate in cols:
            description_expr = f"COALESCE(NULLIF(TRIM(ji.{candidate}), ''), 'Job item')"
            break

    amount_expr = None
    for candidate in (
        "cost_amount",
        "line_total_cost",
        "total_cost",
        "cost_total",
        "extended_cost",
        "amount",
        "cost",
        "unit_cost",
        "price",
        "line_total",
        "total",
    ):
        if candidate in cols:
            amount_expr = f"ABS(COALESCE(ji.{candidate}, 0))"
            break

    if amount_expr is None:
        qty_col = None
        unit_cost_col = None

        for candidate in ("quantity", "qty", "units"):
            if candidate in cols:
                qty_col = candidate
                break

        for candidate in ("unit_cost", "cost", "price"):
            if candidate in cols:
                unit_cost_col = candidate
                break

        if qty_col and unit_cost_col:
            amount_expr = f"ABS(COALESCE(ji.{qty_col}, 0) * COALESCE(ji.{unit_cost_col}, 0))"
        else:
            amount_expr = "0"

    customer_expr = "j.customer_id"
    service_type_expr = "COALESCE(j.service_type, '')"

    ledger_entry_expr = "NULL"
    if "ledger_entry_id" in cols:
        ledger_entry_expr = "ji.ledger_entry_id"

    where_parts = ["j.company_id = %s"]
    params = [start_date, end_date] if date_col else []

    if date_col:
        where_parts.append(f"ji.{date_col} BETWEEN %s AND %s")

    where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT
            ji.id,
            ji.{job_id_col} AS job_id,
            {customer_expr} AS customer_id,
            {service_type_expr} AS service_type,
            {ledger_entry_expr} AS ledger_entry_id,
            {category_expr} AS category,
            {description_expr} AS description,
            {amount_expr} AS amount,
            {f"ji.{date_col}" if date_col else "NULL"} AS entry_date
        FROM job_items ji
        JOIN jobs j ON ji.job_id = j.id
        WHERE {where_sql}
        ORDER BY {f"ji.{date_col}" if date_col else "ji.id"} DESC, ji.id DESC
    """

    return sql, params


def _insert_manual_ledger_entry(conn, company_id, entry_date, entry_type, category, description, amount, notes, payee_name=""):
    ledger_cols = table_columns(conn, "ledger_entries")

    values = {}

    if "company_id" in ledger_cols:
        values["company_id"] = company_id
    if "entry_date" in ledger_cols:
        values["entry_date"] = entry_date
    elif "date" in ledger_cols:
        values["date"] = entry_date
    if "entry_type" in ledger_cols:
        values["entry_type"] = entry_type
    if "category" in ledger_cols:
        values["category"] = category
    if "description" in ledger_cols:
        values["description"] = description
    elif "memo" in ledger_cols:
        values["memo"] = description
    if "amount" in ledger_cols:
        values["amount"] = amount
    if "notes" in ledger_cols:
        values["notes"] = notes
    if "payee_name" in ledger_cols:
        values["payee_name"] = payee_name or description
    if "source_type" in ledger_cols:
        values["source_type"] = "manual"
    if "reference_type" in ledger_cols:
        values["reference_type"] = "manual"
    if "source_id" in ledger_cols:
        values["source_id"] = None
    if "status" in ledger_cols:
        values["status"] = "posted"

    if not values:
        return

    cols_sql = ", ".join(values.keys())
    placeholders = ", ".join(["%s"] * len(values))
    conn.execute(
        f"INSERT INTO ledger_entries ({cols_sql}) VALUES ({placeholders})",
        tuple(values.values()),
    )


def _fetch_ledger_rows(conn, cid, start_date, end_date):
    select_parts = _get_ledger_select_parts(conn)
    date_col = select_parts["date_col"]

    if not date_col:
        return []

    rows = conn.execute(
        f"""
        SELECT
            le.id,
            {select_parts["entry_type_expr"]} AS entry_type,
            {select_parts["amount_expr"]} AS amount,
            {select_parts["desc_sql"]} AS description,
            {select_parts["category_expr"]} AS category,
            {select_parts["source_type_expr"]} AS source_type,
            {select_parts["reference_type_expr"]} AS reference_type,
            {select_parts["source_id_expr"]} AS source_id,
            {select_parts["customer_id_expr"]} AS customer_id,
            {select_parts["invoice_id_expr"]} AS invoice_id,
            {select_parts["job_id_expr"]} AS job_id,
            {select_parts["notes_expr"]} AS notes,
            {select_parts["service_type_expr"]} AS service_type,
            le.{date_col} AS entry_date
        FROM ledger_entries le
        LEFT JOIN jobs j ON le.job_id = j.id
        WHERE le.company_id = %s
          AND le.{date_col} BETWEEN %s AND %s
        ORDER BY le.{date_col} DESC, le.id DESC
        """,
        (cid, start_date, end_date),
    ).fetchall()

    return rows


def _fetch_payroll_rows(conn, cid, start_date, end_date):
    if not _table_exists(conn, "payroll_entries") or not _table_exists(conn, "employees"):
        return []

    return conn.execute(
        """
        SELECT p.*, e.first_name, e.last_name
        FROM payroll_entries p
        JOIN employees e ON p.employee_id = e.id
        WHERE p.company_id = %s
          AND p.pay_date BETWEEN %s AND %s
        ORDER BY p.pay_date DESC, p.id DESC
        """,
        (cid, start_date, end_date),
    ).fetchall()


def _fetch_invoice_payment_rows(conn, cid, start_date, end_date):
    if not _table_exists(conn, "invoice_payments") or not _table_exists(conn, "invoices"):
        return []

    customer_join = ""
    customer_name_select = "NULL AS customer_name"
    if _table_exists(conn, "customers"):
        customer_join = "LEFT JOIN customers c ON i.customer_id = c.id"
        customer_name_select = "c.name AS customer_name"

    jobs_join = ""
    service_type_select = "'' AS service_type"
    if _table_exists(conn, "jobs") and _has_col(conn, "invoices", "job_id"):
        jobs_join = "LEFT JOIN jobs j ON i.job_id = j.id"
        service_type_select = "COALESCE(j.service_type, '') AS service_type"

    return conn.execute(
        f"""
        SELECT ip.*, i.customer_id, i.invoice_number, i.total AS invoice_total,
               i.status AS invoice_status, {customer_name_select},
               {service_type_select}
        FROM invoice_payments ip
        JOIN invoices i ON ip.invoice_id = i.id
        {customer_join}
        {jobs_join}
        WHERE ip.company_id = %s
          AND ip.payment_date BETWEEN %s AND %s
        ORDER BY ip.payment_date DESC, ip.id DESC
        """,
        (cid, start_date, end_date),
    ).fetchall()


def _fetch_job_item_rows(conn, cid, start_date, end_date):
    sql, extra_params = _build_job_item_query(conn, start_date, end_date)
    if not sql:
        return []

    params = [cid] + list(extra_params)
    return conn.execute(sql, params).fetchall()


def _normalize_ledger_rows(ledger_rows):
    normalized = []

    for r in ledger_rows:
        source_type = (_safe_get(r, "source_type", "manual") or "manual").strip().lower()
        reference_type = (_safe_get(r, "reference_type", "") or "").strip().lower()
        entry_type_raw = (_safe_get(r, "entry_type", "") or "").strip().lower()
        category_raw = (_safe_get(r, "category", "") or "").strip().lower()
        description_raw = (_safe_get(r, "description", "") or "").strip().lower()

        raw_amount = _safe_float(_safe_get(r, "amount", 0))

        if (
            source_type in {"invoice_payment", "invoice_paid", "invoice_mark_paid", "payment"}
            or reference_type in {"invoice_payment", "invoice_paid", "invoice_mark_paid", "payment"}
            or "invoice payment" in entry_type_raw
            or "invoice payment" in category_raw
            or ("invoice" in description_raw and "payment" in description_raw)
        ):
            continue

        entry_type = _normalize_ledger_type(
            _safe_get(r, "entry_type", ""),
            source_type,
            raw_amount,
        )

        normalized.append({
            "id": _safe_get(r, "id"),
            "entry_date": _safe_get(r, "entry_date", ""),
            "entry_type": entry_type,
            "category": (_safe_get(r, "category", "Uncategorized") or "Uncategorized").strip() or "Uncategorized",
            "description": _safe_get(r, "description", "") or "",
            "amount": abs(raw_amount),
            "source_type": source_type,
            "reference_type": reference_type,
            "source_id": _safe_get(r, "source_id"),
            "customer_id": _safe_get(r, "customer_id"),
            "invoice_id": _safe_get(r, "invoice_id"),
            "job_id": _safe_get(r, "job_id"),
            "service_type": _normalize_service_type(_safe_get(r, "service_type", "")),
            "notes": _safe_get(r, "notes", "") or "",
            "can_delete": _normalize_text(source_type) == "manual" or _normalize_text(reference_type) == "manual",
        })

    return normalized


def _normalize_payroll_rows(payroll_rows):
    normalized = []

    for r in payroll_rows:
        employee_name = f"{_safe_get(r, 'first_name', '') or ''} {_safe_get(r, 'last_name', '') or ''}".strip()
        normalized.append({
            "id": f"payroll-{_safe_get(r, 'id')}",
            "entry_date": _safe_get(r, "pay_date", ""),
            "entry_type": "Expense",
            "category": "Payroll",
            "description": f"Payroll - {employee_name}" + (
                f" ({_safe_get(r, 'notes', '')})" if _safe_get(r, "notes") else ""
            ),
            "amount": abs(_safe_float(_safe_get(r, "gross_pay", 0))),
            "source_type": "payroll",
            "reference_type": "payroll",
            "source_id": _safe_get(r, "id"),
            "customer_id": None,
            "invoice_id": None,
            "job_id": None,
            "service_type": "",
            "employee_id": _safe_get(r, "employee_id"),
            "employee_name": employee_name,
            "notes": _safe_get(r, "notes", "") or "",
            "can_delete": False,
        })

    return normalized


def _normalize_invoice_payment_rows(payment_rows):
    normalized = []
    grouped_paid = {}

    for r in payment_rows:
        invoice_status = (_safe_get(r, "invoice_status", "") or "").strip()
        service_type = _normalize_service_type(_safe_get(r, "service_type", ""))

        if invoice_status == "Paid":
            key = _safe_get(r, "invoice_id")

            if key not in grouped_paid:
                grouped_paid[key] = {
                    "id": f"payment-group-{_safe_get(r, 'invoice_id')}",
                    "entry_date": _safe_get(r, "payment_date", ""),
                    "entry_type": "Income",
                    "category": "Invoice Payments",
                    "description": f"Invoice #{_safe_get(r, 'invoice_number') or _safe_get(r, 'invoice_id')} paid in full" + (
                        f" ({_safe_get(r, 'customer_name')})" if _safe_get(r, "customer_name") else ""
                    ),
                    "amount": abs(_safe_float(_safe_get(r, "invoice_total", 0))),
                    "source_type": "invoice_payment",
                    "reference_type": "invoice_payment",
                    "source_id": _safe_get(r, "invoice_id"),
                    "customer_id": _safe_get(r, "customer_id"),
                    "invoice_id": _safe_get(r, "invoice_id"),
                    "job_id": None,
                    "service_type": service_type,
                    "notes": "",
                    "can_delete": False,
                }

            if (_safe_get(r, "payment_date", "") or "") > (grouped_paid[key]["entry_date"] or ""):
                grouped_paid[key]["entry_date"] = _safe_get(r, "payment_date", "")

        else:
            normalized.append({
                "id": f"payment-{_safe_get(r, 'id')}",
                "entry_date": _safe_get(r, "payment_date", ""),
                "entry_type": "Income",
                "category": "Invoice Payments",
                "description": f"Partial payment for Invoice #{_safe_get(r, 'invoice_number') or _safe_get(r, 'invoice_id')}" + (
                    f" ({_safe_get(r, 'customer_name')})" if _safe_get(r, "customer_name") else ""
                ),
                "amount": abs(_safe_float(_safe_get(r, "amount", 0))),
                "source_type": "invoice_payment",
                "reference_type": "invoice_payment",
                "source_id": _safe_get(r, "id"),
                "customer_id": _safe_get(r, "customer_id"),
                "invoice_id": _safe_get(r, "invoice_id"),
                "job_id": None,
                "service_type": service_type,
                "notes": "",
                "can_delete": False,
            })

    normalized.extend(grouped_paid.values())
    return normalized


def _normalize_job_item_rows(job_item_rows, existing_ledger_rows):
    normalized = []

    existing_pairs = set()
    existing_ledger_ids = set()

    for r in existing_ledger_rows:
        source_type = (r.get("source_type") or "").strip().lower()
        source_id = r.get("source_id")
        row_id = r.get("id")

        if source_type and source_id not in (None, ""):
            existing_pairs.add((source_type, str(source_id)))

        if row_id not in (None, ""):
            existing_ledger_ids.add(str(row_id))

    for r in job_item_rows:
        item_id = _safe_get(r, "id")
        ledger_entry_id = _safe_get(r, "ledger_entry_id")
        source_pair = ("job_item", str(item_id)) if item_id not in (None, "") else None

        if source_pair and source_pair in existing_pairs:
            continue

        if ledger_entry_id not in (None, "") and str(ledger_entry_id) in existing_ledger_ids:
            continue

        entry_date = _safe_get(r, "entry_date", "") or date.today().isoformat()
        amount = abs(_safe_float(_safe_get(r, "amount", 0)))

        if amount == 0:
            continue

        normalized.append({
            "id": f"job-item-{item_id}",
            "entry_date": entry_date,
            "entry_type": "Expense",
            "category": (_safe_get(r, "category", "Material") or "Material").strip() or "Material",
            "description": _safe_get(r, "description", "Job item") or "Job item",
            "amount": amount,
            "source_type": "job_item",
            "reference_type": "job_item",
            "source_id": item_id,
            "customer_id": _safe_get(r, "customer_id"),
            "invoice_id": None,
            "job_id": _safe_get(r, "job_id"),
            "service_type": _normalize_service_type(_safe_get(r, "service_type", "")),
            "notes": "",
            "can_delete": False,
        })

    return normalized


def _apply_service_type_filter(rows, service_filter):
    service_filter = _normalize_service_type(service_filter)
    if not service_filter:
        return rows

    filtered = []
    for r in rows:
        row_service = _normalize_service_type(r.get("service_type"))
        if row_service == service_filter:
            filtered.append(r)
    return filtered


def _build_combined_rows(conn, cid, start_date, end_date, service_filter=""):
    ledger_rows_db = _fetch_ledger_rows(conn, cid, start_date, end_date)
    payroll_rows_db = _fetch_payroll_rows(conn, cid, start_date, end_date)
    payment_rows_db = _fetch_invoice_payment_rows(conn, cid, start_date, end_date)
    job_item_rows_db = _fetch_job_item_rows(conn, cid, start_date, end_date)

    normalized_ledger = _normalize_ledger_rows(ledger_rows_db)
    normalized_payroll = _normalize_payroll_rows(payroll_rows_db)
    normalized_payments = _normalize_invoice_payment_rows(payment_rows_db)
    normalized_job_items = _normalize_job_item_rows(job_item_rows_db, normalized_ledger)

    rows = normalized_ledger + normalized_payroll + normalized_payments + normalized_job_items
    rows = _apply_service_type_filter(rows, service_filter)
    rows.sort(key=lambda x: (x.get("entry_date") or "", str(x.get("id") or "")), reverse=True)

    return rows


def _service_filter_options(selected_value=""):
    selected_value = _normalize_service_type(selected_value)
    options = [f"<option value='' {'selected' if not selected_value else ''}>All Services</option>"]
    for key, label in SERVICE_TYPE_LABELS.items():
        selected_attr = " selected" if key == selected_value else ""
        options.append(f"<option value='{key}'{selected_attr}>{escape(label)}</option>")
    return "".join(options)


def _render_bookkeeping_page(conn, cid):
    if request.method == "POST":
        entry_date = (request.form.get("entry_date") or "").strip() or date.today().isoformat()
        entry_type = (request.form.get("entry_type") or "expense").strip().lower()
        category = _canonicalize_category(request.form.get("category") or "")
        description = (request.form.get("description") or "").strip()
        amount = abs(_safe_float(request.form.get("amount")))
        notes = (request.form.get("notes") or "").strip()
        payee_name = (request.form.get("payee_name") or "").strip()

        if amount <= 0:
            flash("Amount must be greater than 0.")
            return redirect(url_for("bookkeeping.bookkeeping"))

        if not description:
            flash("Description is required.")
            return redirect(url_for("bookkeeping.bookkeeping"))

        if not category:
            category = "Income" if entry_type == "income" else "Expense"

        _insert_manual_ledger_entry(
            conn=conn,
            company_id=cid,
            entry_date=entry_date,
            entry_type=entry_type.title(),
            category=category,
            description=description,
            amount=amount,
            notes=notes,
            payee_name=payee_name,
        )

        conn.commit()
        flash("Manual bookkeeping entry added.")
        return redirect(url_for("bookkeeping.bookkeeping"))

    view_type = request.args.get("view", "monthly")
    valid_views = ["daily", "weekly", "monthly", "quarterly", "yearly", "yoy"]
    if view_type not in valid_views:
        view_type = "monthly"

    anchor_date = request.args.get("anchor_date", date.today().isoformat())
    service_filter = _normalize_service_type(request.args.get("service_type", ""))

    yoy_html = ""
    if view_type == "yoy":
        anchor_year = datetime.strptime(anchor_date, "%Y-%m-%d").date().year
        current_year = anchor_year
        prior_year = current_year - 1

        current_rows = _build_combined_rows(
            conn, cid, f"{current_year}-01-01", f"{current_year}-12-31", service_filter=service_filter
        )
        prior_rows = _build_combined_rows(
            conn, cid, f"{prior_year}-01-01", f"{prior_year}-12-31", service_filter=service_filter
        )

        rows = current_rows

        current_income = sum(r["amount"] for r in current_rows if r["entry_type"] == "Income")
        current_expense = sum(r["amount"] for r in current_rows if r["entry_type"] == "Expense")
        current_net = current_income - current_expense

        prior_income = sum(r["amount"] for r in prior_rows if r["entry_type"] == "Income")
        prior_expense = sum(r["amount"] for r in prior_rows if r["entry_type"] == "Expense")
        prior_net = prior_income - prior_expense

        income = current_income
        expense = current_expense
        net = current_net
        period_label = f"{current_year} vs {prior_year}"

        if service_filter:
            period_label += f" • {_display_service_type(service_filter)}"

        yoy_html = f"""
        <div class='card'>
            <h2>Year over Year Comparison</h2>
            <div class='static-table-wrap desktop-only'>
                <table class='static-table summary-table'>
                    <colgroup>
                        <col style='width:25%;'>
                        <col style='width:25%;'>
                        <col style='width:25%;'>
                        <col style='width:25%;'>
                    </colgroup>
                    <tr>
                        <th>Year</th>
                        <th class='money'>Income</th>
                        <th class='money'>Expenses</th>
                        <th class='money'>Net</th>
                    </tr>
                    <tr>
                        <td>{prior_year}</td>
                        <td class='money positive'>+${prior_income:.2f}</td>
                        <td class='money negative'>-${prior_expense:.2f}</td>
                        <td class='money {"positive" if prior_net >= 0 else "negative"}'>
                            {'+' if prior_net >= 0 else '-'}${abs(prior_net):.2f}
                        </td>
                    </tr>
                    <tr>
                        <td>{current_year}</td>
                        <td class='money positive'>+${current_income:.2f}</td>
                        <td class='money negative'>-${current_expense:.2f}</td>
                        <td class='money {"positive" if current_net >= 0 else "negative"}'>
                            {'+' if current_net >= 0 else '-'}${abs(current_net):.2f}
                        </td>
                    </tr>
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    <div class='mobile-list-card'>
                        <div class='mobile-list-top'>
                            <div class='mobile-list-title'>{prior_year}</div>
                        </div>
                        <div class='mobile-list-grid'>
                            <div><span>Income</span><strong class='positive'>+${prior_income:.2f}</strong></div>
                            <div><span>Expenses</span><strong class='negative'>-${prior_expense:.2f}</strong></div>
                            <div><span>Net</span><strong class='{"positive" if prior_net >= 0 else "negative"}'>{'+' if prior_net >= 0 else '-'}${abs(prior_net):.2f}</strong></div>
                        </div>
                    </div>

                    <div class='mobile-list-card'>
                        <div class='mobile-list-top'>
                            <div class='mobile-list-title'>{current_year}</div>
                        </div>
                        <div class='mobile-list-grid'>
                            <div><span>Income</span><strong class='positive'>+${current_income:.2f}</strong></div>
                            <div><span>Expenses</span><strong class='negative'>-${current_expense:.2f}</strong></div>
                            <div><span>Net</span><strong class='{"positive" if current_net >= 0 else "negative"}'>{'+' if current_net >= 0 else '-'}${abs(current_net):.2f}</strong></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        """
    else:
        start_date, end_date = get_period_range(view_type, anchor_date)
        rows = _build_combined_rows(conn, cid, start_date, end_date, service_filter=service_filter)

        income = sum(r["amount"] for r in rows if r["entry_type"] == "Income")
        expense = sum(r["amount"] for r in rows if r["entry_type"] == "Expense")
        net = income - expense
        period_label = f"{start_date} to {end_date}"
        if service_filter:
            period_label += f" • {_display_service_type(service_filter)}"

    category_totals = {}
    for r in rows:
        if r["entry_type"] not in ("Income", "Expense"):
            continue

        cat = (r["category"] or "Uncategorized").strip() or "Uncategorized"
        if cat not in category_totals:
            category_totals[cat] = {"Income": 0.0, "Expense": 0.0}

        category_totals[cat][r["entry_type"]] += abs(_safe_float(r["amount"]))

    category_rows = "".join(
        f"""
        <tr>
            <td class='wrap'>{escape(cat)}</td>
            <td class='money positive'>+${vals['Income']:.2f}</td>
            <td class='money negative'>-${vals['Expense']:.2f}</td>
            <td class='money {"positive" if (vals["Income"] - vals["Expense"]) >= 0 else "negative"}'>
                {'+' if (vals['Income'] - vals['Expense']) >= 0 else '-'}${abs(vals['Income'] - vals['Expense']):.2f}
            </td>
        </tr>
        """
        for cat, vals in sorted(category_totals.items())
    )

    mobile_category_cards = "".join(
        f"""
        <div class='mobile-list-card'>
            <div class='mobile-list-top'>
                <div class='mobile-list-title'>{escape(cat)}</div>
            </div>
            <div class='mobile-list-grid'>
                <div><span>Income</span><strong class='positive'>+${vals['Income']:.2f}</strong></div>
                <div><span>Expenses</span><strong class='negative'>-${vals['Expense']:.2f}</strong></div>
                <div><span>Net</span><strong class='{"positive" if (vals["Income"] - vals["Expense"]) >= 0 else "negative"}'>{'+' if (vals['Income'] - vals['Expense']) >= 0 else '-'}${abs(vals['Income'] - vals['Expense']):.2f}</strong></div>
            </div>
        </div>
        """
        for cat, vals in sorted(category_totals.items())
    )

    category_html = f"""
    <div class='card'>
        <h2>P&amp;L by Category</h2>

        <div class='static-table-wrap desktop-only'>
            <table class='static-table summary-table'>
                <colgroup>
                    <col style='width:40%;'>
                    <col style='width:20%;'>
                    <col style='width:20%;'>
                    <col style='width:20%;'>
                </colgroup>
                <tr>
                    <th class='wrap'>Category</th>
                    <th class='money'>Income</th>
                    <th class='money'>Expenses</th>
                    <th class='money'>Net</th>
                </tr>
                {category_rows or '<tr><td colspan="4" class="muted">No category data for this period.</td></tr>'}
            </table>
        </div>

        <div class='mobile-only'>
            <div class='mobile-list'>
                {mobile_category_cards or "<div class='mobile-list-card muted'>No category data for this period.</div>"}
            </div>
        </div>
    </div>
    """

    ledger_row_html = []
    mobile_ledger_cards = []

    for r in rows:
        source_html = escape(str(r.get("source_type") or "-"))
        source_text = escape(str(r.get("source_type") or "-"))

        if r.get("invoice_id"):
            source_html = f"<a class='btn secondary small' href='{url_for('invoices.view_invoice', invoice_id=r.get('invoice_id'))}'>Open Invoice</a>"
            source_text = "Invoice"
        elif r.get("job_id"):
            source_html = f"<a class='btn secondary small' href='{url_for('jobs.view_job', job_id=r.get('job_id'))}'>Open Job</a>"
            source_text = "Job"
        elif r.get("source_type") == "payroll" and r.get("employee_id"):
            try:
                source_html = f"<a class='btn secondary small' href='{url_for('employees.view_employee', employee_id=r.get('employee_id'))}'>Open Employee</a>"
                source_text = "Payroll"
            except Exception:
                source_html = "Payroll"
                source_text = "Payroll"

        actions = [
            f"<a class='btn secondary small' href='{url_for('bookkeeping.view_bookkeeping_entry', entry_id=r.get('id'))}'>View</a>"
            if isinstance(r.get("id"), int) else "<span class='muted small'>Auto</span>"
        ]

        if r.get("can_delete") and isinstance(r.get("id"), int):
            actions.append(
                f"""
                <form method='post'
                      action='{url_for("bookkeeping.delete_bookkeeping_entry", entry_id=r.get("id"))}'
                      onsubmit="return confirm('Delete this bookkeeping entry?');"
                      style='margin:0;'>
                    <input type="hidden" name="csrf_token" value="{generate_csrf()}">
                    <button class='btn danger small' type='submit'>Delete</button>
                </form>
                """
            )

        actions_html = "".join(actions)
        amount_class = "positive" if r.get("entry_type") == "Income" else "negative"
        amount_text = f"{'+' if r.get('entry_type') == 'Income' else '-'}${abs(_safe_float(r.get('amount'))):.2f}"

        service_type = _normalize_service_type(r.get("service_type"))
        service_chip = (
            f"<span class='service-chip {_service_chip_class(service_type)}'>{escape(_display_service_type(service_type))}</span>"
            if service_type else "<span class='muted small'>-</span>"
        )
        service_text = _display_service_type(service_type)

        ledger_row_html.append(
            f"""
            <tr>
                <td>{escape(str(r.get('entry_date') or '-'))}</td>
                <td>{escape(str(r.get('entry_type') or '-'))}</td>
                <td class='center'>{service_chip}</td>
                <td class='wrap'>{escape(str(r.get('category') or '-'))}</td>
                <td class='wrap'>{escape(str(r.get('description') or '-'))}</td>
                <td class='money {amount_class}'>{amount_text}</td>
                <td class='center'>{source_html}</td>
                <td class='wrap'>
                    <div class='static-actions'>{actions_html}</div>
                </td>
            </tr>
            """
        )

        mobile_ledger_cards.append(
            f"""
            <div class='mobile-list-card'>
                <div class='mobile-list-top'>
                    <div>
                        <div class='mobile-list-title'>{escape(str(r.get('description') or '-'))}</div>
                        <div class='mobile-list-subtitle'>{escape(str(r.get('entry_date') or '-'))}</div>
                    </div>
                    <div class='mobile-badge'>{escape(str(r.get('entry_type') or '-'))}</div>
                </div>

                <div style='margin:-2px 0 10px 0;'>
                    {service_chip if service_type else ""}
                </div>

                <div class='mobile-list-grid'>
                    <div>
                        <span>Category</span>
                        <strong>{escape(str(r.get('category') or '-'))}</strong>
                    </div>
                    <div>
                        <span>Amount</span>
                        <strong class='{amount_class}'>{amount_text}</strong>
                    </div>
                    <div>
                        <span>Service</span>
                        <strong>{escape(service_text)}</strong>
                    </div>
                    <div>
                        <span>Source</span>
                        <strong>{source_text}</strong>
                    </div>
                </div>

                <div class='mobile-list-actions'>
                    {source_html if "<a " in source_html else ""}
                    {actions_html}
                </div>
            </div>
            """
        )

    ledger_rows = "".join(ledger_row_html)
    mobile_ledger_html = "".join(mobile_ledger_cards)

    filter_bar = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <h1 style='margin:0;'>Bookkeeping / P&amp;L</h1>
            <div class='row-actions'>
                <a href="{url_for('bookkeeping.bookkeeping_pnl')}" class="btn success">P&amp;L Page</a>
            </div>
        </div>

        <form method='get' style='margin-top:18px;'>
            <div class='grid'>
                <div>
                    <label>View</label>
                    <select name='view'>
                        <option value='daily' {'selected' if view_type == 'daily' else ''}>Daily</option>
                        <option value='weekly' {'selected' if view_type == 'weekly' else ''}>Weekly</option>
                        <option value='monthly' {'selected' if view_type == 'monthly' else ''}>Monthly</option>
                        <option value='quarterly' {'selected' if view_type == 'quarterly' else ''}>Quarterly</option>
                        <option value='yearly' {'selected' if view_type == 'yearly' else ''}>Yearly</option>
                        <option value='yoy' {'selected' if view_type == 'yoy' else ''}>YoY</option>
                    </select>
                </div>
                <div>
                    <label>Anchor Date</label>
                    <input type='date' name='anchor_date' value='{anchor_date}'>
                </div>
                <div>
                    <label>Service Filter</label>
                    <select name='service_type'>
                        {_service_filter_options(service_filter)}
                    </select>
                </div>
            </div>
            <br>
            <button class='btn' type='submit'>Apply</button>
            <a class='btn secondary' href='{url_for("bookkeeping.export_bookkeeping_csv", view=view_type, anchor_date=anchor_date, service_type=service_filter)}'>Export CSV</a>
        </form>

        <div class='muted' style='margin-top:14px;'><strong>Viewing:</strong> {period_label}</div>
    </div>
    """

    manual_entry_form = f"""
    <div class='card'>
        <h2>Add Manual Bookkeeping Entry</h2>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{generate_csrf()}">
            <div class='grid'>
                <div>
                    <label>Date</label>
                    <input type='date' name='entry_date' value='{date.today().isoformat()}' required>
                </div>
                <div>
                    <label>Type</label>
                    <select name='entry_type' id='manual_entry_type' onchange='toggleManualCategories()' required>
                        <option value='expense'>Expense</option>
                        <option value='income'>Income</option>
                    </select>
                </div>
                <div>
                    <label>Category</label>
                    <select name='category' id='manual_category'>
                        <option value='Mulch'>Mulch</option>
                        <option value='Stone'>Stone</option>
                        <option value='Dump Fee'>Dump Fee</option>
                        <option value='Plants'>Plants</option>
                        <option value='Trees'>Trees</option>
                        <option value='Soil'>Soil</option>
                        <option value='Fertilizer'>Fertilizer</option>
                        <option value='Hardscape Material'>Hardscape Material</option>
                        <option value='Labor'>Labor</option>
                        <option value='Fuel'>Fuel</option>
                        <option value='Equipment'>Equipment</option>
                        <option value='Delivery'>Delivery</option>
                        <option value='Misc'>Misc</option>
                        <option value='Payroll'>Payroll</option>
                        <option value='Hand Tools'>Hand Tools</option>
                        <option value='Office Supplies'>Office Supplies</option>
                        <option value='Maintenance'>Maintenance</option>
                        <option value='Power Equipment'>Power Equipment</option>
                        <option value='Vehicles'>Vehicles</option>
                        <option value='Insurance'>Insurance</option>
                        <option value='Marketing'>Marketing</option>
                        <option value='Office and Admin'>Office and Admin</option>
                        <option value='Safety Gear'>Safety Gear</option>
                        <option value='Licensing &amp; Certifications'>Licensing &amp; Certifications</option>
                    </select>
                </div>
                <div>
                    <label>Amount</label>
                    <input type='number' step='0.01' min='0.01' name='amount' placeholder='0.00' required>
                </div>
                <div>
                    <label>Payee Name</label>
                    <input type='text' name='payee_name' placeholder='Who the check would be payable to'>
                </div>
                <div style='grid-column:1 / -1;'>
                    <label>Description</label>
                    <input type='text' name='description' placeholder='Enter description.' required>
                </div>
                <div style='grid-column:1 / -1;'>
                    <label>Notes</label>
                    <textarea name='notes' placeholder='Optional notes.'></textarea>
                </div>
            </div>

            <div class='row-actions' style='margin-top:14px;'>
                <button class='btn success' type='submit'>Add Entry</button>
            </div>
        </form>
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

        .static-table-wrap {{
            width: 100%;
        }}

        .static-table-scroll {{
            max-height: 520px;
            overflow-y: auto;
            overflow-x: hidden;
            border: 1px solid rgba(0,0,0,0.06);
            border-radius: 12px;
        }}

        .static-table-scroll .static-table {{
            margin: 0;
        }}

        .static-table-scroll thead th {{
            position: sticky;
            top: 0;
            z-index: 2;
            background: #ffffff;
            box-shadow: 0 1px 0 rgba(0,0,0,0.06);
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
        }}

        .static-actions form {{
            margin: 0;
        }}

        .static-actions .btn {{
            white-space: nowrap;
        }}

        .summary-table .positive {{
            color: #16a34a;
            font-weight: 700;
        }}

        .summary-table .negative {{
            color: #dc2626;
            font-weight: 700;
        }}

        .ledger-income {{
            color: #16a34a;
            font-weight: 700;
        }}

        .ledger-expense {{
            color: #dc2626;
            font-weight: 700;
        }}

        .positive {{
            color: #16a34a;
            font-weight: 700;
        }}

        .negative {{
            color: #dc2626;
            font-weight: 700;
        }}

        .service-chip {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: .79rem;
            font-weight: 700;
            line-height: 1;
            white-space: nowrap;
            border: 1px solid rgba(15,23,42,.08);
            background: #f8fafc;
            color: #334155;
        }}

        .service-chip.mowing {{
            background: #ecfdf3;
            color: #166534;
            border-color: #bbf7d0;
        }}

        .service-chip.material {{
            background: #fff7ed;
            color: #9a3412;
            border-color: #fed7aa;
        }}

        .service-chip.seasonal {{
            background: #eff6ff;
            color: #1d4ed8;
            border-color: #bfdbfe;
        }}

        .service-chip.default {{
            background: #f8fafc;
            color: #334155;
            border-color: #e2e8f0;
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

        @media (max-width: 900px) {{
            .summary-cards {{
                grid-template-columns: 1fr !important;
            }}
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
        }}
    </style>

    {filter_bar}

    <div class="summary-cards" style="display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:16px; margin-bottom:20px;">
        <div class='card'>
            <h3>Total Income</h3>
            <div style="color:#16a34a; font-weight:700; font-size:1.4rem;">+${income:.2f}</div>
        </div>
        <div class='card'>
            <h3>Total Expenses</h3>
            <div style="color:#dc2626; font-weight:700; font-size:1.4rem;">-${expense:.2f}</div>
        </div>
        <div class='card'>
            <h3>Net</h3>
            <div style="color:{'#16a34a' if net >= 0 else '#dc2626'}; font-weight:700; font-size:1.4rem;">
                {'+' if net >= 0 else '-'}${abs(net):.2f}
            </div>
        </div>
    </div>

    {yoy_html}
    {category_html}
    {manual_entry_form}

    <div class='card'>
        <h2>Ledger Entries</h2>

        <div class='static-table-wrap desktop-only'>
            <div class='static-table-scroll'>
                <table class='static-table'>
                    <colgroup>
                        <col style='width:9%;'>
                        <col style='width:9%;'>
                        <col style='width:11%;'>
                        <col style='width:14%;'>
                        <col style='width:24%;'>
                        <col style='width:10%;'>
                        <col style='width:10%;'>
                        <col style='width:13%;'>
                    </colgroup>
                    <thead>
                        <tr>
                            <th>Date</th>
                            <th>Type</th>
                            <th class='center'>Service</th>
                            <th class='wrap'>Category</th>
                            <th class='wrap'>Description</th>
                            <th class='money'>Amount</th>
                            <th class='center'>Source</th>
                            <th class='wrap'>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {ledger_rows or '<tr><td colspan="8" class="muted">No bookkeeping entries for this period.</td></tr>'}
                    </tbody>
                </table>
            </div>
        </div>

        <div class='mobile-only'>
            <div class='mobile-list'>
                {mobile_ledger_html or "<div class='mobile-list-card muted'>No bookkeeping entries for this period.</div>"}
            </div>
        </div>
    </div>

    <script>
    function toggleManualCategories() {{
        var typeEl = document.getElementById("manual_entry_type");
        var categoryEl = document.getElementById("manual_category");
        if (!typeEl || !categoryEl) return;

        var expenseOptions = [
            "Mulch",
            "Stone",
            "Dump Fee",
            "Plants",
            "Trees",
            "Soil",
            "Fertilizer",
            "Hardscape Material",
            "Labor",
            "Fuel",
            "Equipment",
            "Delivery",
            "Misc",
            "Payroll",
            "Hand Tools",
            "Office Supplies",
            "Maintenance",
            "Power Equipment",
            "Vehicles",
            "Insurance",
            "Marketing",
            "Office and Admin",
            "Safety Gear",
            "Licensing & Certifications"
        ];

        var incomeOptions = [
            "Income",
            "Invoice Payments",
            "Bank Deposits"
        ];

        var selected = typeEl.value === "income" ? incomeOptions : expenseOptions;
        categoryEl.innerHTML = "";

        selected.forEach(function(opt) {{
            var el = document.createElement("option");
            el.value = opt;
            el.textContent = opt;
            categoryEl.appendChild(el);
        }});
    }}

    document.addEventListener("DOMContentLoaded", function() {{
        toggleManualCategories();
    }});
    </script>
    """

    return render_page(content, "Bookkeeping / P&L")

@bookkeeping_bp.route("/bookkeeping", methods=["GET", "POST"])
@bookkeeping_bp.route("/ledger", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_bookkeeping")
def bookkeeping():
    _ensure_bookkeeping_check_structure()
    conn = get_db_connection()
    cid = session["company_id"]
    try:
        return _render_bookkeeping_page(conn, cid)
    finally:
        conn.close()


@bookkeeping_bp.route("/bookkeeping/<int:entry_id>")
@bookkeeping_bp.route("/ledger/<int:entry_id>")
@login_required
@subscription_required
@require_permission("can_manage_bookkeeping")
def view_bookkeeping_entry(entry_id):
    _ensure_bookkeeping_check_structure()
    conn = get_db_connection()
    cid = session["company_id"]

    try:
        row = _fetch_ledger_entry_by_id(conn, cid, entry_id)
        if not row:
            flash("Bookkeeping entry not found.")
            return redirect(url_for("bookkeeping.bookkeeping"))

        entry_type = _normalize_ledger_type(
            _safe_get(row, "entry_type", ""),
            _safe_get(row, "source_type", ""),
            _safe_get(row, "amount", 0),
        )
        amount = abs(_safe_float(_safe_get(row, "amount", 0)))
        payee_name = _clean_text(_safe_get(row, "payee_name", "")) or _clean_text(_safe_get(row, "description", ""))
        can_print_check = (
            entry_type == "Expense"
            and amount > 0
            and _normalize_text(_safe_get(row, "source_type", "")) != "payroll"
        )

        check_action_html = ""
        if can_print_check:
            if _safe_get(row, "check_number"):
                check_action_html = f"""
                <a class='btn success' href='{url_for("bookkeeping.print_bookkeeping_check", entry_id=entry_id)}' target='_blank'>
                    View Check PDF
                </a>
                """
            else:
                check_action_html = f"""
                <a class='btn success' href='{url_for("bookkeeping.print_bookkeeping_check", entry_id=entry_id)}' target='_blank'>
                    Print Check
                </a>
                """

        source_html = escape(_clean_text(_safe_get(row, "source_type", "")) or "-")
        if _safe_get(row, "invoice_id"):
            source_html = f"<a class='btn secondary' href='{url_for('invoices.view_invoice', invoice_id=_safe_get(row, 'invoice_id'))}'>Open Invoice</a>"
        elif _safe_get(row, "job_id"):
            source_html = f"<a class='btn secondary' href='{url_for('jobs.view_job', job_id=_safe_get(row, 'job_id'))}'>Open Job</a>"

        service_type = _normalize_service_type(_safe_get(row, "service_type", "")) or ""
        service_html = (
            f"<span class='service-chip {_service_chip_class(service_type)}'>{escape(_display_service_type(service_type))}</span>"
            if service_type else "-"
        )

        content = f"""
        <style>
            .service-chip {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                padding: 6px 10px;
                border-radius: 999px;
                font-size: .79rem;
                font-weight: 700;
                line-height: 1;
                white-space: nowrap;
                border: 1px solid rgba(15,23,42,.08);
                background: #f8fafc;
                color: #334155;
            }}

            .service-chip.mowing {{
                background: #ecfdf3;
                color: #166534;
                border-color: #bbf7d0;
            }}

            .service-chip.material {{
                background: #fff7ed;
                color: #9a3412;
                border-color: #fed7aa;
            }}

            .service-chip.seasonal {{
                background: #eff6ff;
                color: #1d4ed8;
                border-color: #bfdbfe;
            }}

            .service-chip.default {{
                background: #f8fafc;
                color: #334155;
                border-color: #e2e8f0;
            }}
        </style>

        <div class='card'>
            <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
                <div>
                    <h1 style='margin-bottom:6px;'>Ledger Entry Detail</h1>
                    <p class='muted' style='margin:0;'>Review this entry before printing a check.</p>
                </div>
                <div class='row-actions'>
                    <a class='btn secondary' href='{url_for("bookkeeping.bookkeeping")}'>Back to Bookkeeping</a>
                    {check_action_html}
                </div>
            </div>
        </div>

        <div class='card'>
            <div class='grid'>
                <div><strong>Date</strong><br>{escape(str(_safe_get(row, "entry_date", "") or "-"))}</div>
                <div><strong>Type</strong><br>{escape(entry_type)}</div>
                <div><strong>Service</strong><br>{service_html}</div>
                <div><strong>Category</strong><br>{escape(_clean_text(_safe_get(row, "category", "")) or "-")}</div>
                <div><strong>Amount</strong><br>{escape(_fmt_money(amount))}</div>
                <div><strong>Payee</strong><br>{escape(payee_name or "-")}</div>
                <div><strong>Payment Method</strong><br>{escape(_clean_text(_safe_get(row, "payment_method", "")) or "-")}</div>
                <div><strong>Check #</strong><br>{escape(str(_safe_get(row, "check_number", "") or "-"))}</div>
                <div><strong>Source</strong><br>{source_html}</div>
                <div style='grid-column:1 / -1;'><strong>Description</strong><br>{escape(_clean_text(_safe_get(row, "description", "")) or "-")}</div>
                <div style='grid-column:1 / -1;'><strong>Notes</strong><br>{escape(_clean_text(_safe_get(row, "notes", "")) or "-")}</div>
            </div>
        </div>
        """
        return render_page(content, "Ledger Entry Detail")
    finally:
        conn.close()


@bookkeeping_bp.route("/bookkeeping/<int:entry_id>/print-check")
@bookkeeping_bp.route("/ledger/<int:entry_id>/print-check")
@login_required
@subscription_required
@require_permission("can_manage_bookkeeping")
def print_bookkeeping_check(entry_id):
    _ensure_bookkeeping_check_structure()
    conn = get_db_connection()
    cid = session["company_id"]

    try:
        row = _fetch_ledger_entry_by_id(conn, cid, entry_id)
        if not row:
            flash("Bookkeeping entry not found.")
            return redirect(url_for("bookkeeping.bookkeeping"))

        entry_type = _normalize_ledger_type(
            _safe_get(row, "entry_type", ""),
            _safe_get(row, "source_type", ""),
            _safe_get(row, "amount", 0),
        )
        amount = abs(_safe_float(_safe_get(row, "amount", 0)))

        if entry_type != "Expense":
            flash("Only expense-type entries can be printed as checks.")
            return redirect(url_for("bookkeeping.view_bookkeeping_entry", entry_id=entry_id))

        if amount <= 0:
            flash("Cannot print a check for a zero or negative amount.")
            return redirect(url_for("bookkeeping.view_bookkeeping_entry", entry_id=entry_id))

        if _normalize_text(_safe_get(row, "source_type", "")) == "payroll":
            flash("Payroll checks should be printed from the payroll screen.")
            return redirect(url_for("bookkeeping.view_bookkeeping_entry", entry_id=entry_id))

        _, check_number = _create_or_get_ledger_check(conn, cid, row)
        conn.commit()

        refreshed_row = _fetch_ledger_entry_by_id(conn, cid, entry_id)
        company_info = _get_company_check_info(cid)

        pdf_data = _build_ledger_check_pdf(
            company_info=company_info,
            ledger_row=refreshed_row,
            check_number=check_number,
        )

        response = make_response(pdf_data)
        response.headers["Content-Type"] = "application/pdf"
        response.headers["Content-Disposition"] = f"inline; filename=ledger_check_{check_number}.pdf"
        return response
    finally:
        conn.close()


@bookkeeping_bp.route("/bookkeeping/export")
@bookkeeping_bp.route("/ledger/export")
@login_required
@subscription_required
@require_permission("can_manage_bookkeeping")
def export_bookkeeping_csv():
    conn = get_db_connection()
    cid = session["company_id"]

    view_type = request.args.get("view", "monthly")
    valid_views = ["daily", "weekly", "monthly", "quarterly", "yearly", "yoy"]
    if view_type not in valid_views:
        view_type = "monthly"

    anchor_date = request.args.get("anchor_date", date.today().isoformat())
    service_filter = _normalize_service_type(request.args.get("service_type", ""))

    if view_type == "yoy":
        anchor_year = datetime.strptime(anchor_date, "%Y-%m-%d").date().year
        start_date = f"{anchor_year}-01-01"
        end_date = f"{anchor_year}-12-31"
    else:
        start_date, end_date = get_period_range(view_type, anchor_date)

    try:
        rows = _build_combined_rows(conn, cid, start_date, end_date, service_filter=service_filter)
    finally:
        conn.close()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Date",
        "Type",
        "Service Type",
        "Category",
        "Description",
        "Amount",
        "Source Type",
        "Invoice ID",
        "Job ID",
    ])

    for r in rows:
        signed_amount = abs(_safe_float(r.get("amount")))
        if r.get("entry_type") == "Expense":
            signed_amount = -signed_amount

        writer.writerow([
            r.get("entry_date") or "",
            r.get("entry_type") or "",
            _display_service_type(r.get("service_type"), fallback=""),
            r.get("category") or "",
            r.get("description") or "",
            f"{signed_amount:.2f}",
            r.get("source_type") or "",
            r.get("invoice_id") or "",
            r.get("job_id") or "",
        ])

    filename = f"bookkeeping_{view_type}_{anchor_date}.csv"

    response = make_response(output.getvalue())
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers["Content-type"] = "text/csv"
    return response


@bookkeeping_bp.route("/bookkeeping/<int:entry_id>/delete", methods=["POST"])
@bookkeeping_bp.route("/ledger/<int:entry_id>/delete", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_bookkeeping")
def delete_bookkeeping_entry(entry_id):
    conn = get_db_connection()
    cid = session["company_id"]

    try:
        ledger_cols = table_columns(conn, "ledger_entries")

        date_select = "entry_date"
        if "entry_date" in ledger_cols:
            date_select = "entry_date"
        elif "date" in ledger_cols:
            date_select = "date"
        else:
            date_select = "NULL"

        entry_type_select = "entry_type" if "entry_type" in ledger_cols else "''"
        category_select = "category" if "category" in ledger_cols else "''"
        amount_select = "amount" if "amount" in ledger_cols else "0"

        if "description" in ledger_cols and "memo" in ledger_cols:
            description_select = "COALESCE(description, memo, '')"
        elif "description" in ledger_cols:
            description_select = "COALESCE(description, '')"
        elif "memo" in ledger_cols:
            description_select = "COALESCE(memo, '')"
        else:
            description_select = "''"

        notes_select = "notes" if "notes" in ledger_cols else "''"
        source_type_select = "source_type" if "source_type" in ledger_cols else "''"
        reference_type_select = "reference_type" if "reference_type" in ledger_cols else "''"
        source_id_select = "source_id" if "source_id" in ledger_cols else "NULL"
        check_id_select = "check_id" if "check_id" in ledger_cols else "NULL"

        row = conn.execute(
            f"""
            SELECT
                id,
                company_id,
                {date_select} AS entry_date,
                {entry_type_select} AS entry_type,
                {category_select} AS category,
                {amount_select} AS amount,
                {description_select} AS description,
                {notes_select} AS notes,
                {source_type_select} AS source_type,
                {reference_type_select} AS reference_type,
                {source_id_select} AS source_id,
                {check_id_select} AS check_id
            FROM ledger_entries
            WHERE id = %s AND company_id = %s
            """,
            (entry_id, cid),
        ).fetchone()

        if not row:
            flash("Bookkeeping entry not found.")
            return redirect(url_for("bookkeeping.bookkeeping"))

        is_manual = (
            _normalize_text(_safe_get(row, "source_type", "")) == "manual"
            or _normalize_text(_safe_get(row, "reference_type", "")) == "manual"
        )

        if not is_manual:
            flash("Only manual bookkeeping entries can be deleted here.")
            return redirect(url_for("bookkeeping.bookkeeping"))

        if _safe_get(row, "check_id"):
            conn.execute(
                "DELETE FROM checks WHERE id = %s AND company_id = %s",
                (_safe_get(row, "check_id"), cid),
            )

        conn.execute(
            "DELETE FROM ledger_entries WHERE id = %s AND company_id = %s",
            (entry_id, cid),
        )
        conn.commit()

        flash("Bookkeeping entry deleted.")
        return redirect(url_for("bookkeeping.bookkeeping"))
    finally:
        conn.close()


@bookkeeping_bp.route("/bookkeeping/pnl")
@login_required
@subscription_required
@require_permission("can_view_bookkeeping")
def bookkeeping_pnl():
    conn = get_db_connection()
    cid = session["company_id"]

    date_from = (request.args.get("date_from") or "").strip()
    date_to = (request.args.get("date_to") or "").strip()
    service_filter = _normalize_service_type(request.args.get("service_type", ""))

    today = date.today()

    if not date_from:
        date_from = f"{today.year}-01-01"
    if not date_to:
        date_to = today.isoformat()

    try:
        rows = _build_combined_rows(conn, cid, date_from, date_to, service_filter=service_filter)
    finally:
        conn.close()

    total_income = 0.0
    total_expenses = 0.0
    breakdown = {}

    for r in rows:
        amount = abs(_safe_float(r["amount"]))
        entry_type = str(r["entry_type"] or "")
        description = str(r["description"] or "")
        category = str(r["category"] or "")
        source_type = str(r["source_type"] or "")
        reference_type = str(r.get("reference_type") or "")

        bucket = _get_pl_bucket(
            entry_type=entry_type,
            description=description,
            category=category,
            source_type=source_type,
            reference_type=reference_type,
        )

        is_expense = _is_expense_entry(
            entry_type=entry_type,
            description=description,
            category=category,
            source_type=source_type,
            reference_type=reference_type,
        )

        if bucket not in breakdown:
            breakdown[bucket] = 0.0

        if is_expense:
            total_expenses += amount
            breakdown[bucket] -= amount
        else:
            total_income += amount
            breakdown[bucket] += amount

    net_profit = total_income - total_expenses

    rows_html = ""
    for cat, amt in sorted(breakdown.items(), key=lambda x: x[0].lower()):
        color = "#16a34a" if amt >= 0 else "#dc2626"
        rows_html += f"""
        <tr>
            <td class='wrap'>{escape(cat)}</td>
            <td class='money' style="color:{color};">{_fmt_money(amt, show_plus=True)}</td>
        </tr>
        """

    net_color = "#16a34a" if net_profit >= 0 else "#dc2626"

    range_text = f"{escape(date_from)} to {escape(date_to)}"
    if service_filter:
        range_text += f" • {escape(_display_service_type(service_filter))}"

    content = f"""
    <style>
        .static-table-wrap {{
            width: 100%;
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

        .static-table td.wrap,
        .static-table th.wrap {{
            white-space: normal;
            word-break: break-word;
        }}
    </style>

    <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
            <h1 style="margin:0;">Profit &amp; Loss</h1>
            <div class="row-actions">
                <a href="{url_for('bookkeeping.bookkeeping')}" class="btn secondary">Back</a>
            </div>
        </div>

        <form method="get" style="margin-top:18px;">
            <div class="grid">
                <div>
                    <label>From Date</label>
                    <input type="date" name="date_from" value="{escape(date_from)}">
                </div>
                <div>
                    <label>To Date</label>
                    <input type="date" name="date_to" value="{escape(date_to)}">
                </div>
                <div>
                    <label>Service Filter</label>
                    <select name="service_type">
                        {_service_filter_options(service_filter)}
                    </select>
                </div>
            </div>

            <div class="row-actions" style="margin-top:14px;">
                <button class="btn success" type="submit">Apply Date Filter</button>
                <a href="{url_for('bookkeeping.bookkeeping_pnl')}" class="btn secondary">Clear</a>
            </div>
        </form>

        <div class="muted" style="margin-top:14px;">
            <strong>Date Range:</strong> {range_text}
        </div>

        <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:20px;">
            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Total Income</div>
                <div class="stat-value" style="color:#16a34a;">{_fmt_money(total_income)}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Total Expenses</div>
                <div class="stat-value" style="color:#dc2626;">-{abs(total_expenses):,.2f}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Net Profit</div>
                <div class="stat-value" style="color:{net_color};">{_fmt_money(net_profit, show_plus=True)}</div>
            </div>
        </div>

        <div class="card" style="margin-top:20px;">
            <h2>Breakdown</h2>
            <div class="static-table-wrap">
                <table class="static-table">
                    <colgroup>
                        <col style='width:70%;'>
                        <col style='width:30%;'>
                    </colgroup>
                    <thead>
                        <tr>
                            <th class='wrap'>Category</th>
                            <th class='money'>Amount</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html or '<tr><td colspan="2" class="muted">No P&amp;L data for this date range.</td></tr>'}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    """

    return render_page(content, "Profit & Loss")