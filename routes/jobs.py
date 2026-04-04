from flask import Blueprint, request, redirect, url_for, session, flash, make_response, abort
from flask_wtf.csrf import generate_csrf
from datetime import date, datetime, timedelta
from html import escape
import json
import io
import csv

from db import get_db_connection, ensure_job_cost_ledger
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page
from helpers import *
from calculations import recalc_job, recalc_invoice
from utils.emailing import send_company_email

jobs_bp = Blueprint("jobs", __name__)


ITEM_TYPE_LABELS = {
    "mulch": "Mulch",
    "stone": "Stone",
    "dump_fee": "Dump Fee",
    "plants": "Plants",
    "trees": "Trees",
    "soil": "Soil",
    "fertilizer": "Fertilizer",
    "hardscape_material": "Hardscape Material",
    "labor": "Labor",
    "equipment": "Equipment",
    "delivery": "Delivery",
    "fuel": "Fuel",
    "misc": "Misc",
    "material": "Material",
}

JOB_SERVICE_TYPE_LABELS = {
    "mowing": "Mowing",
    "mulch": "Mulch",
    "cleanup": "Cleanup",
    "installation": "Installation",
    "hardscape": "Hardscape",
    "snow_removal": "Snow Removal",
    "fertilizing": "Fertilizing",
    "other": "Other",
}


def ensure_job_schedule_columns():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_start_time TIME")
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_end_time TIME")
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS assigned_to TEXT")
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS service_type TEXT")
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS recurring_schedule_id INTEGER")
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS generated_from_schedule BOOLEAN DEFAULT FALSE")
        conn.commit()
    finally:
        conn.close()

    ensure_recurring_mowing_tables()


def ensure_recurring_mowing_tables():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS recurring_mowing_schedules (
                id SERIAL PRIMARY KEY,
                company_id INTEGER NOT NULL,
                customer_id INTEGER NOT NULL,
                title TEXT,
                service_type TEXT DEFAULT 'mowing',
                interval_weeks INTEGER DEFAULT 1,
                start_date DATE NOT NULL,
                next_run_date DATE,
                end_date DATE,
                scheduled_start_time TIME,
                scheduled_end_time TIME,
                assigned_to TEXT,
                status_default TEXT DEFAULT 'Scheduled',
                address TEXT,
                notes TEXT,
                active BOOLEAN DEFAULT TRUE,
                auto_generate_until_days INTEGER DEFAULT 42,
                last_generated_on TIMESTAMP NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS recurring_mowing_schedule_items (
                id SERIAL PRIMARY KEY,
                schedule_id INTEGER NOT NULL,
                company_id INTEGER NOT NULL,
                item_type TEXT,
                description TEXT NOT NULL,
                quantity NUMERIC(12,2) DEFAULT 0,
                unit TEXT,
                unit_cost NUMERIC(12,2) DEFAULT 0,
                sale_price NUMERIC(12,2) DEFAULT 0,
                billable BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_recurring_schedule_items_schedule
            ON recurring_mowing_schedule_items (company_id, schedule_id)
            """
        )

        cur.execute(
            "ALTER TABLE recurring_mowing_schedule_items ADD COLUMN IF NOT EXISTS item_type TEXT"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedule_items ADD COLUMN IF NOT EXISTS description TEXT"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedule_items ADD COLUMN IF NOT EXISTS quantity NUMERIC(12,2) DEFAULT 0"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedule_items ADD COLUMN IF NOT EXISTS unit TEXT"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedule_items ADD COLUMN IF NOT EXISTS unit_cost NUMERIC(12,2) DEFAULT 0"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedule_items ADD COLUMN IF NOT EXISTS sale_price NUMERIC(12,2) DEFAULT 0"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedule_items ADD COLUMN IF NOT EXISTS billable BOOLEAN DEFAULT TRUE"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedule_items ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_recurring_mowing_schedules_company
            ON recurring_mowing_schedules (company_id, active, next_run_date)
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_jobs_recurring_schedule
            ON jobs (company_id, recurring_schedule_id, scheduled_date)
            """
        )

        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS title TEXT"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS service_type TEXT DEFAULT 'mowing'"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS interval_weeks INTEGER DEFAULT 1"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS start_date DATE"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS next_run_date DATE"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS end_date DATE"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS scheduled_start_time TIME"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS scheduled_end_time TIME"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS assigned_to TEXT"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS status_default TEXT DEFAULT 'Scheduled'"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS address TEXT"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS notes TEXT"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT TRUE"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS auto_generate_until_days INTEGER DEFAULT 42"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS last_generated_on TIMESTAMP NULL"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        )
        cur.execute(
            "ALTER TABLE recurring_mowing_schedules ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        )

        conn.commit()
    finally:
        conn.close()


def clean_text_input(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"none", "null", "n/a", "0", "0.0", "0.00"}:
        return ""
    return text


def clean_text_display(value, fallback="-"):
    text = clean_text_input(value)
    return text if text else fallback


def safe_float(value, default=0.0):
    try:
        return float(value or 0)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        return int(str(value).strip())
    except Exception:
        return default


def parse_iso_date(value):
    text = clean_text_input(value)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except Exception:
        return None


def date_to_iso(value):
    if not value:
        return ""
    if isinstance(value, str):
        return clean_text_input(value)
    try:
        return value.isoformat()
    except Exception:
        return clean_text_input(value)


def normalize_service_type(value):
    key = clean_text_input(value).lower().replace("-", "_").replace(" ", "_")
    return key if key in JOB_SERVICE_TYPE_LABELS else "other"


def display_service_type(value):
    key = normalize_service_type(value)
    return JOB_SERVICE_TYPE_LABELS.get(key, "Other")


def service_type_badge_class(value):
    key = normalize_service_type(value)
    if key == "mowing":
        return "mowing"
    if key in {"mulch", "installation", "hardscape"}:
        return "material"
    if key in {"cleanup", "snow_removal"}:
        return "seasonal"
    return "default"


def service_type_select_options(selected_value="other"):
    selected_value = normalize_service_type(selected_value)
    options = []
    for key, label in JOB_SERVICE_TYPE_LABELS.items():
        selected_attr = " selected" if key == selected_value else ""
        options.append(f"<option value='{key}'{selected_attr}>{escape(label)}</option>")
    return "".join(options)


def interval_mode_from_weeks(interval_weeks):
    weeks = safe_int(interval_weeks, 1)
    if weeks <= 1:
        return "weekly"
    if weeks == 2:
        return "every_2"
    return "custom"


def interval_label(interval_weeks):
    weeks = safe_int(interval_weeks, 1)
    if weeks <= 1:
        return "Weekly"
    if weeks == 2:
        return "Every 2 Weeks"
    return f"Every {weeks} Weeks"


def schedule_status_badge(active):
    return "Active" if active else "Paused"


def schedule_status_class(active):
    return "mowing" if active else "default"


def derive_interval_weeks_from_form(form):
    mode = clean_text_input(form.get("interval_mode", "weekly")).lower()
    if mode == "every_2":
        return 2
    if mode == "custom":
        custom_value = safe_int(form.get("custom_interval_weeks"), 1)
        return custom_value if custom_value > 0 else 1
    return 1


def recurring_schedule_title_default(title):
    title = clean_text_input(title)
    return title or "Recurring Mowing"


def default_mowing_status(value="Scheduled"):
    text = clean_text_input(value)
    return text or "Scheduled"


def _time_to_minutes(value):
    if not value:
        return None
    try:
        parts = str(value).split(":")
        return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        return None


def check_schedule_conflict(conn, company_id, scheduled_date, start_time, end_time, assigned_to, exclude_job_id=None):
    if not scheduled_date or not start_time or not assigned_to:
        return None

    new_start = _time_to_minutes(start_time)
    new_end = _time_to_minutes(end_time) if end_time else (new_start + 60)

    rows = conn.execute(
        """
        SELECT id, title, scheduled_start_time, scheduled_end_time
        FROM jobs
        WHERE company_id = %s
          AND scheduled_date = %s
          AND assigned_to = %s
          AND id != COALESCE(%s, -1)
        """,
        (company_id, scheduled_date, assigned_to, exclude_job_id),
    ).fetchall()

    for r in rows:
        existing_start = _time_to_minutes(r["scheduled_start_time"])
        existing_end = _time_to_minutes(r["scheduled_end_time"]) if r["scheduled_end_time"] else (existing_start + 60)

        if existing_start is None:
            continue

        if new_start < existing_end and new_end > existing_start:
            return {
                "id": r["id"],
                "title": r["title"],
                "start": r["scheduled_start_time"],
                "end": r["scheduled_end_time"],
            }

    return None

def upcoming_schedule_preview(start_date_value, interval_weeks, count=3, end_date_value=None):
    preview = []

    current = parse_iso_date(start_date_value)
    interval_weeks = max(1, safe_int(interval_weeks, 1))
    end_date_value = parse_iso_date(end_date_value)

    loops = 0
    while current and len(preview) < count and loops < 50:
        loops += 1

        if end_date_value and current > end_date_value:
            break

        preview.append(current.isoformat())
        current = current + timedelta(weeks=interval_weeks)

    return ", ".join(preview)

def display_item_type(value):
    key = clean_text_input(value).lower()
    if key in ITEM_TYPE_LABELS:
        return ITEM_TYPE_LABELS[key]
    return key.replace("_", " ").title() if key else "Material"


def default_unit_for_item_type(item_type):
    key = clean_text_input(item_type).lower()

    if key == "mulch":
        return "Yards"
    if key == "stone":
        return "Tons"
    if key == "soil":
        return "Yards"
    if key == "fertilizer":
        return "Bags"
    if key == "hardscape_material":
        return "Tons"
    if key == "plants":
        return "EA"
    if key == "trees":
        return "EA"
    if key == "labor":
        return "hr"
    if key == "dump_fee":
        return "fee"

    return ""


def build_job_update_email(job, update_type):
    company_name = clean_text_input(session.get("company_name")) or "TerraLedger"
    customer_name = clean_text_input(job.get("customer_name")) or "Customer"
    job_title = clean_text_input(job.get("title")) or "your scheduled job"
    scheduled_date = clean_text_input(job.get("scheduled_date"))
    start_time = clean_text_input(job.get("scheduled_start_time"))
    end_time = clean_text_input(job.get("scheduled_end_time"))
    address = clean_text_input(job.get("address"))
    assigned_to = clean_text_input(job.get("assigned_to"))
    service_type_label = display_service_type(job.get("service_type"))

    schedule_line = ""
    if scheduled_date and start_time and end_time:
        schedule_line = f"{scheduled_date} from {start_time} to {end_time}"
    elif scheduled_date and start_time:
        schedule_line = f"{scheduled_date} at {start_time}"
    elif scheduled_date:
        schedule_line = scheduled_date

    if update_type == "on_the_way":
        subject = f"{company_name}: We are on the way"
        intro = f"Hello {customer_name},<br><br>We are on the way for <strong>{escape(job_title)}</strong>."
    elif update_type == "job_started":
        subject = f"{company_name}: Job started"
        intro = f"Hello {customer_name},<br><br>We have started <strong>{escape(job_title)}</strong>."
    elif update_type == "job_completed":
        subject = f"{company_name}: Job completed"
        intro = f"Hello {customer_name},<br><br>Your job <strong>{escape(job_title)}</strong> has been completed."
    else:
        subject = f"{company_name}: Job update"
        intro = f"Hello {customer_name},<br><br>Here is an update for <strong>{escape(job_title)}</strong>."

    details = []
    if service_type_label:
        details.append(f"<strong>Service Type:</strong> {escape(service_type_label)}")
    if schedule_line:
        details.append(f"<strong>Scheduled:</strong> {escape(schedule_line)}")
    if address:
        details.append(f"<strong>Address:</strong> {escape(address)}")
    if assigned_to:
        details.append(f"<strong>Assigned To:</strong> {escape(assigned_to)}")

    details_html = "<br>".join(details)

    html_body = f"""
    <div style="font-family: Arial, sans-serif; color: #1f2933; line-height: 1.5;">
        {intro}
        {'<br><br>' + details_html if details_html else ''}
        <br><br>
        Thank you,<br>
        {escape(company_name)}
    </div>
    """

    text_parts = [
        f"Hello {customer_name},",
        "",
    ]

    if update_type == "on_the_way":
        text_parts.append(f"We are on the way for {job_title}.")
    elif update_type == "job_started":
        text_parts.append(f"We have started {job_title}.")
    elif update_type == "job_completed":
        text_parts.append(f"Your job {job_title} has been completed.")
    else:
        text_parts.append(f"Here is an update for {job_title}.")

    if service_type_label:
        text_parts.append(f"Service Type: {service_type_label}")
    if schedule_line:
        text_parts.append(f"Scheduled: {schedule_line}")
    if address:
        text_parts.append(f"Address: {address}")
    if assigned_to:
        text_parts.append(f"Assigned To: {assigned_to}")

    text_parts.extend([
        "",
        "Thank you,",
        company_name,
    ])

    text_body = "\n".join(text_parts)

    return subject, html_body, text_body


def send_job_update_email(company_id, customer_email, job, update_type, user_id=None):
    subject, html_body, text_body = build_job_update_email(job, update_type)

    try:
        send_company_email(
            company_id=company_id,
            user_id=user_id,
            to_email=customer_email,
            subject=subject,
            html=html_body,
            body=text_body,
        )
        return True, None
    except Exception as e:
        return False, str(e)


def get_schedule_job_title(schedule_row):
    title = clean_text_input(schedule_row.get("title")) if hasattr(schedule_row, "get") else clean_text_input(schedule_row["title"])
    return title or "Recurring Mowing"

def get_recurring_schedule_items(conn, company_id, schedule_id):
    return conn.execute(
        """
        SELECT *
        FROM recurring_mowing_schedule_items
        WHERE company_id = %s
          AND schedule_id = %s
        ORDER BY id ASC
        """,
        (company_id, schedule_id),
    ).fetchall()


def add_default_recurring_mowing_items(conn, company_id, schedule_id):
    existing = conn.execute(
        """
        SELECT id
        FROM recurring_mowing_schedule_items
        WHERE company_id = %s
          AND schedule_id = %s
        LIMIT 1
        """,
        (company_id, schedule_id),
    ).fetchone()

    if existing:
        return

    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO recurring_mowing_schedule_items (
            schedule_id,
            company_id,
            item_type,
            description,
            quantity,
            unit,
            unit_cost,
            sale_price,
            billable
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            schedule_id,
            company_id,
            "labor",
            "Mowing Service",
            1.00,
            "Hours",
            0.00,
            0.00,
            True,
        ),
    )


def copy_recurring_schedule_items_to_job(conn, schedule_id, company_id, job_id):
    items = get_recurring_schedule_items(conn, company_id, schedule_id)

    if not items:
        return

    created_item_ids = []

    for item in items:
        item_type = clean_text_input(item["item_type"]).lower()
        description = clean_text_input(item["description"])
        qty = safe_float(item["quantity"])
        unit = clean_text_input(item["unit"])
        sale_price = safe_float(item["sale_price"])
        unit_cost = safe_float(item["unit_cost"])
        billable = 1 if item["billable"] else 0

        if not description:
            continue

        if item_type == "mulch" and not unit:
            unit = "Yards"
        elif item_type == "stone" and not unit:
            unit = "Tons"
        elif item_type == "soil" and not unit:
            unit = "Yards"
        elif item_type == "hardscape_material" and not unit:
            unit = "Tons"
        elif item_type == "fuel" and not unit:
            unit = "Gallons"
        elif item_type == "delivery" and not unit:
            unit = "Miles"
        elif item_type == "labor" and not unit:
            unit = "Hours"
        elif item_type == "equipment" and not unit:
            unit = "Rentals"
        elif item_type in ["plants", "trees", "misc", "dump_fee"]:
            unit = ""

        if item_type == "labor":
            unit_cost = 0.0

        if item_type == "dump_fee":
            unit = ""
            if qty <= 0:
                qty = 1
            unit_cost = 0.0

        line_total = qty * sale_price
        cost_amount = qty * unit_cost

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO job_items (
                job_id,
                item_type,
                description,
                quantity,
                unit,
                unit_cost,
                unit_price,
                sale_price,
                cost_amount,
                line_total,
                billable
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                job_id,
                item_type,
                description,
                qty,
                unit,
                unit_cost,
                sale_price,
                sale_price,
                cost_amount,
                line_total,
                billable,
            ),
        )
        row = cur.fetchone()
        if row and "id" in row:
            created_item_ids.append(row["id"])

    for job_item_id in created_item_ids:
        ensure_job_cost_ledger(conn, job_item_id)

    recalc_job(conn, job_id)


def create_job_from_recurring_schedule(conn, schedule_row, scheduled_date):
    scheduled_date_iso = date_to_iso(scheduled_date)

    existing = conn.execute(
        """
        SELECT id
        FROM jobs
        WHERE company_id = %s
          AND recurring_schedule_id = %s
          AND scheduled_date = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (schedule_row["company_id"], schedule_row["id"], scheduled_date_iso),
    ).fetchone()

    if existing:
        return existing["id"], False

    title = get_schedule_job_title(schedule_row)
    service_type = normalize_service_type(schedule_row["service_type"] or "mowing")
    notes = clean_text_input(schedule_row["notes"])
    schedule_note = f"Auto-generated from recurring mowing schedule #{schedule_row['id']}."
    notes_final = schedule_note if not notes else f"{schedule_note}\n\n{notes}"

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO jobs (
            company_id,
            customer_id,
            title,
            service_type,
            scheduled_date,
            scheduled_start_time,
            scheduled_end_time,
            assigned_to,
            status,
            address,
            notes,
            recurring_schedule_id,
            generated_from_schedule
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            schedule_row["company_id"],
            schedule_row["customer_id"],
            title,
            service_type,
            scheduled_date_iso or None,
            clean_text_input(schedule_row["scheduled_start_time"]) or None,
            clean_text_input(schedule_row["scheduled_end_time"]) or None,
            clean_text_input(schedule_row["assigned_to"]) or None,
            default_mowing_status(schedule_row["status_default"]),
            clean_text_input(schedule_row["address"]),
            notes_final,
            schedule_row["id"],
            True,
        ),
    )
    row = cur.fetchone()
    job_id = row["id"] if row and "id" in row else None

    if not job_id:
        return None, False

    recurring_items = conn.execute(
        """
        SELECT *
        FROM recurring_mowing_schedule_items
        WHERE company_id = %s
          AND schedule_id = %s
        ORDER BY id ASC
        """,
        (schedule_row["company_id"], schedule_row["id"]),
    ).fetchall()

    for item in recurring_items:
        item_type = clean_text_input(item["item_type"]).lower()
        description = clean_text_input(item["description"])
        qty = safe_float(item["quantity"], 0)
        unit = clean_text_input(item["unit"])
        sale_price = safe_float(item["sale_price"], 0)
        unit_cost = safe_float(item["unit_cost"], 0)
        billable_value = 1 if item["billable"] else 0

        if qty <= 0:
            qty = 1.0

        if item_type == "mulch" and not unit:
            unit = "Yards"
        elif item_type == "stone" and not unit:
            unit = "Tons"
        elif item_type == "soil" and not unit:
            unit = "Yards"
        elif item_type == "hardscape_material" and not unit:
            unit = "Tons"
        elif item_type == "fuel" and not unit:
            unit = "Gallons"
        elif item_type == "delivery" and not unit:
            unit = "Miles"
        elif item_type == "labor" and not unit:
            unit = "Hours"
        elif item_type == "equipment" and not unit:
            unit = "Rentals"
        elif item_type == "fertilizer" and not unit:
            unit = "Bags"
        elif item_type in ["plants", "trees", "misc", "dump_fee"]:
            unit = ""

        if item_type == "labor":
            unit_cost = 0.0

        if item_type == "dump_fee":
            unit = ""
            qty = 1.0
            unit_cost = 0.0

        line_total = qty * sale_price
        cost_amount = qty * unit_cost

        cur.execute(
            """
            INSERT INTO job_items (
                job_id,
                item_type,
                description,
                quantity,
                unit,
                unit_cost,
                unit_price,
                sale_price,
                cost_amount,
                line_total,
                billable
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                job_id,
                item_type,
                description,
                qty,
                unit,
                unit_cost,
                sale_price,
                sale_price,
                cost_amount,
                line_total,
                billable_value,
            ),
        )
        item_row = cur.fetchone()
        job_item_id = item_row["id"] if item_row and "id" in item_row else None

        if job_item_id:
            ensure_job_cost_ledger(conn, job_item_id)

    recalc_job(conn, job_id)
    return job_id, True


def auto_generate_recurring_jobs(conn, company_id, through_date=None):
    today = date.today()
    if through_date is None:
        through_date = today + timedelta(days=42)

    schedules = conn.execute(
        """
        SELECT *
        FROM recurring_mowing_schedules
        WHERE company_id = %s
          AND COALESCE(active, TRUE) = TRUE
          AND next_run_date IS NOT NULL
        ORDER BY next_run_date ASC, id ASC
        """,
        (company_id,),
    ).fetchall()

    created_count = 0

    for schedule in schedules:
        interval_weeks = safe_int(schedule["interval_weeks"], 1)
        if interval_weeks <= 0:
            interval_weeks = 1

        next_run = parse_iso_date(schedule["next_run_date"])
        start_date_value = parse_iso_date(schedule["start_date"])
        end_date_value = parse_iso_date(schedule["end_date"])
        horizon_days = safe_int(schedule["auto_generate_until_days"], 42)

        if end_date_value:
            schedule_through = end_date_value
        else:
            schedule_through = today + timedelta(days=horizon_days if horizon_days > 0 else 42)

        if through_date:
            target_through = schedule_through if schedule_through <= through_date else through_date
        else:
            target_through = schedule_through

        if next_run is None:
            next_run = start_date_value or today

        safety = 0
        while next_run and next_run <= target_through and safety < 250:
            safety += 1

            if end_date_value and next_run > end_date_value:
                break

            _, created = create_job_from_recurring_schedule(conn, schedule, next_run)
            if created:
                created_count += 1

            next_run = next_run + timedelta(weeks=interval_weeks)

        conn.execute(
            """
            UPDATE recurring_mowing_schedules
            SET next_run_date = %s,
                last_generated_on = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s AND company_id = %s
            """,
            (
                date_to_iso(next_run) if next_run else None,
                schedule["id"],
                company_id,
            ),
        )

    return created_count


@jobs_bp.route("/jobs", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def jobs():
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    try:
        auto_generate_recurring_jobs(conn, cid)
        conn.commit()
    except Exception:
        conn.rollback()

    customers = conn.execute(
        """
        SELECT id, name, company, email
        FROM customers
        WHERE company_id = %s
        ORDER BY name
        """,
        (cid,),
    ).fetchall()

    customer_list = [
        {
            "id": c["id"],
            "name": clean_text_input(c["name"]),
            "company": clean_text_input(c["company"]),
            "email": clean_text_input(c["email"]),
        }
        for c in customers
    ]

    if request.method == "POST":
        customer_id = request.form.get("customer_id", type=int)

        if not customer_id:
            conn.close()
            flash("Please select a customer from the search results.")
            return redirect(url_for("jobs.jobs"))

        title = clean_text_input(request.form.get("title", ""))
        service_type = normalize_service_type(request.form.get("service_type", "other"))
        scheduled_date = clean_text_input(request.form.get("scheduled_date", ""))
        scheduled_start_time = clean_text_input(request.form.get("scheduled_start_time", ""))
        scheduled_end_time = clean_text_input(request.form.get("scheduled_end_time", ""))
        assigned_to = clean_text_input(request.form.get("assigned_to", ""))
        status = clean_text_input(request.form.get("status", "Scheduled")) or "Scheduled"
        address = clean_text_input(request.form.get("address", ""))
        notes = clean_text_input(request.form.get("notes", ""))

        if not title:
            conn.close()
            flash("Job title is required.")
            return redirect(url_for("jobs.jobs"))

        conflict = check_schedule_conflict(
            conn,
            cid,
            scheduled_date,
            scheduled_start_time,
            scheduled_end_time,
            assigned_to,
        )

        if conflict:
            conn.close()
            flash(
                f"Schedule conflict: '{conflict['title']}' is already scheduled for {assigned_to} "
                f"from {conflict['start']} to {conflict['end']}."
            )
            return redirect(url_for("jobs.jobs"))

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO jobs (
                company_id,
                customer_id,
                title,
                service_type,
                scheduled_date,
                scheduled_start_time,
                scheduled_end_time,
                assigned_to,
                status,
                address,
                notes,
                recurring_schedule_id,
                generated_from_schedule
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                cid,
                customer_id,
                title,
                service_type,
                scheduled_date or None,
                scheduled_start_time or None,
                scheduled_end_time or None,
                assigned_to or None,
                status,
                address,
                notes,
                None,
                False,
            ),
        )
        row = cur.fetchone()
        job_id = row["id"] if row and "id" in row else None

        conn.commit()
        conn.close()

        if not job_id:
            flash("Could not create job.")
            return redirect(url_for("jobs.jobs"))

        flash("Job created.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    rows = conn.execute(
        """
        SELECT
            j.*,
            c.name AS customer_name,
            rms.title AS recurring_title
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        LEFT JOIN recurring_mowing_schedules rms
          ON j.recurring_schedule_id = rms.id
         AND rms.company_id = j.company_id
        WHERE j.company_id = %s
          AND COALESCE(j.status, '') != 'Finished'
          AND COALESCE(j.generated_from_schedule, FALSE) = FALSE
        ORDER BY
            j.scheduled_date NULLS LAST,
            j.scheduled_start_time NULLS LAST,
            j.id DESC
        """,
        (cid,),
    ).fetchall()

    recurring_rows = conn.execute(
        """
        SELECT
            rms.*,
            c.name AS customer_name,
            (
                SELECT COUNT(*)
                FROM jobs j
                WHERE j.company_id = rms.company_id
                  AND j.recurring_schedule_id = rms.id
            ) AS generated_jobs_count,
            (
                SELECT COUNT(*)
                FROM jobs j
                WHERE j.company_id = rms.company_id
                  AND j.recurring_schedule_id = rms.id
                  AND COALESCE(j.status, '') != 'Finished'
            ) AS active_jobs_count,
            (
                SELECT COALESCE(SUM(j.revenue), 0)
                FROM jobs j
                WHERE j.company_id = rms.company_id
                  AND j.recurring_schedule_id = rms.id
            ) AS total_revenue,
            (
                SELECT COALESCE(SUM(j.cost_total), 0)
                FROM jobs j
                WHERE j.company_id = rms.company_id
                  AND j.recurring_schedule_id = rms.id
            ) AS total_cost,
            (
                SELECT COALESCE(SUM(j.profit), 0)
                FROM jobs j
                WHERE j.company_id = rms.company_id
                  AND j.recurring_schedule_id = rms.id
            ) AS total_profit
        FROM recurring_mowing_schedules rms
        JOIN customers c ON rms.customer_id = c.id
        WHERE rms.company_id = %s
        ORDER BY COALESCE(rms.active, TRUE) DESC, rms.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    job_row_list = []
    job_mobile_card_list = []

    for r in rows:
        delete_csrf = generate_csrf()
        service_type_label = display_service_type(r["service_type"])
        service_type_class = service_type_badge_class(r["service_type"])

        recurring_link_html = ""
        if r["recurring_schedule_id"]:
            recurring_link_html = (
                f"<div class='small muted' style='margin-top:4px;'>"
                f"Recurring: <a href='/jobs/recurring/{r['recurring_schedule_id']}/edit'>"
                f"Schedule #{r['recurring_schedule_id']}</a></div>"
            )

        job_row_list.append(
            f"""
            <tr>
                <td>#{r['id']}</td>
                <td class='wrap'>
                    {escape(clean_text_display(r['title']))}
                    {recurring_link_html}
                </td>
                <td><span class='service-chip {service_type_class}'>{escape(service_type_label)}</span></td>
                <td class='wrap'>{escape(clean_text_display(r['customer_name']))}</td>
                <td>{escape(clean_text_display(r['scheduled_date']))}</td>
                <td>{escape(clean_text_display(r['scheduled_start_time']))}</td>
                <td>{escape(clean_text_display(r['scheduled_end_time']))}</td>
                <td class='wrap'>{escape(clean_text_display(r['assigned_to']))}</td>
                <td>{escape(clean_text_display(r['status']))}</td>
                <td class='money'>${safe_float(r['revenue']):.2f}</td>
                <td class='money'>${safe_float(r['cost_total']):.2f}</td>
                <td class='money jobs-profit'>${safe_float(r['profit']):.2f}</td>
                <td class='wrap'>
                    <div class='static-actions'>
                        <a class='btn secondary small' href='{url_for("jobs.view_job", job_id=r["id"])}'>View</a>
                        <a class='btn warning small' href='{url_for("jobs.edit_job", job_id=r["id"])}'>Edit Job</a>
                        <a class='btn success small' href='{url_for("jobs.convert_job_to_invoice", job_id=r["id"])}'>Convert to Invoice</a>
                        <form method='post'
                              action='{url_for("jobs.delete_job", job_id=r["id"])}'
                              style='margin:0;'
                              onsubmit="return confirm('Delete this job and all items?');">
                            <input type="hidden" name="csrf_token" value="{delete_csrf}">
                            <button class='btn danger small' type='submit'>Delete Job</button>
                        </form>
                    </div>
                </td>
            </tr>
            """
        )

        mobile_recurring = ""
        if r["recurring_schedule_id"]:
            mobile_recurring = (
                f"<div style='margin-top:6px;' class='muted small'>"
                f"Recurring: <a href='/jobs/recurring/{r['recurring_schedule_id']}/edit'>"
                f"Schedule #{r['recurring_schedule_id']}</a></div>"
            )

        job_mobile_card_list.append(
            f"""
            <div class='mobile-list-card'>
                <div class='mobile-list-top'>
                    <div class='mobile-list-title'>#{r['id']} - {escape(clean_text_display(r['title']))}{mobile_recurring}</div>
                    <div class='mobile-badge'>{escape(clean_text_display(r['status']))}</div>
                </div>

                <div style='margin:-2px 0 10px 0; display:flex; flex-wrap:wrap; gap:8px;'>
                    <span class='service-chip {service_type_class}'>{escape(service_type_label)}</span>
                    {'<span class="service-chip mowing">Recurring</span>' if r["recurring_schedule_id"] else ''}
                </div>

                <div class='mobile-list-grid'>
                    <div><span>Customer</span><strong>{escape(clean_text_display(r['customer_name']))}</strong></div>
                    <div><span>Date</span><strong>{escape(clean_text_display(r['scheduled_date']))}</strong></div>
                    <div><span>Start</span><strong>{escape(clean_text_display(r['scheduled_start_time']))}</strong></div>
                    <div><span>End</span><strong>{escape(clean_text_display(r['scheduled_end_time']))}</strong></div>
                    <div><span>Assigned To</span><strong>{escape(clean_text_display(r['assigned_to']))}</strong></div>
                    <div><span>Revenue</span><strong>${safe_float(r['revenue']):.2f}</strong></div>
                    <div><span>Costs</span><strong>${safe_float(r['cost_total']):.2f}</strong></div>
                    <div><span>Profit/Loss</span><strong>${safe_float(r['profit']):.2f}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <a class='btn secondary small' href='{url_for("jobs.view_job", job_id=r["id"])}'>View</a>
                    <a class='btn warning small' href='{url_for("jobs.edit_job", job_id=r["id"])}'>Edit Job</a>
                    <a class='btn success small' href='{url_for("jobs.convert_job_to_invoice", job_id=r["id"])}'>Convert to Invoice</a>
                    <form method='post'
                          action='{url_for("jobs.delete_job", job_id=r["id"])}'
                          style='margin:0;'
                          onsubmit="return confirm('Delete this job and all items?');">
                        <input type="hidden" name="csrf_token" value="{delete_csrf}">
                        <button class='btn danger small' type='submit'>Delete Job</button>
                    </form>
                </div>
            </div>
            """
        )

    recurring_row_list = []
    recurring_mobile_list = []

    for r in recurring_rows:
        edit_url = f"/jobs/recurring/{r['id']}/edit"
        toggle_url = f"/jobs/recurring/{r['id']}/toggle"
        generate_url = f"/jobs/recurring/{r['id']}/generate"
        convert_url = f"/jobs/recurring/{r['id']}/convert_to_invoice"
        delete_url = f"/jobs/recurring/{r['id']}/delete"

        toggle_csrf = generate_csrf()
        generate_now_csrf = generate_csrf()
        convert_csrf = generate_csrf()
        delete_csrf = generate_csrf()

        next_preview = upcoming_schedule_preview(
            r["next_run_date"] or r["start_date"],
            r["interval_weeks"],
            3,
            r["end_date"],
        )

        active_chip = f"<span class='service-chip {schedule_status_class(r['active'])}'>{escape(schedule_status_badge(r['active']))}</span>"

        recurring_row_list.append(
            f"""
            <tr>
                <td>#{r['id']}</td>
                <td class='wrap'>
                    {escape(clean_text_display(r['title'], 'Recurring Mowing'))}
                    <div class='small muted' style='margin-top:4px;'>Mowing Default</div>
                </td>
                <td><span class='service-chip mowing'>Mowing</span></td>
                <td class='wrap'>{escape(clean_text_display(r['customer_name']))}</td>
                <td>{escape(interval_label(r['interval_weeks']))}</td>
                <td>{escape(clean_text_display(r['next_run_date']))}</td>
                <td class='wrap'>{escape(clean_text_display(r['assigned_to']))}</td>
                <td>{active_chip}</td>
                <td class='center'>{safe_int(r['generated_jobs_count'], 0)}</td>
                <td class='money'>${safe_float(r['total_revenue']):.2f}</td>
                <td class='money'>${safe_float(r['total_cost']):.2f}</td>
                <td class='money jobs-profit'>${safe_float(r['total_profit']):.2f}</td>
                <td class='wrap'>
                    <div class='static-actions'>
                        <a class='btn secondary small' href='{edit_url}'>Edit Schedule</a>

                        <form method='post' action='{generate_url}' style='margin:0;'>
                            <input type="hidden" name="csrf_token" value="{generate_now_csrf}">
                            <button class='btn success small' type='submit'>Generate Now</button>
                        </form>

                        <form method='post' action='{convert_url}' style='margin:0;'>
                            <input type="hidden" name="csrf_token" value="{convert_csrf}">
                            <button class='btn success small' type='submit'>Convert to Invoice</button>
                        </form>

                        <form method='post' action='{toggle_url}' style='margin:0;'>
                            <input type="hidden" name="csrf_token" value="{toggle_csrf}">
                            <button class='btn warning small' type='submit'>
                                {"Pause" if r["active"] else "Resume"}
                            </button>
                        </form>

                        <form method='post'
                              action='{delete_url}'
                              style='margin:0;'
                              onsubmit="return confirm('Delete this recurring mowing schedule? Existing jobs will stay, but future auto-generation will stop.');">
                            <input type="hidden" name="csrf_token" value="{delete_csrf}">
                            <button class='btn danger small' type='submit'>Delete</button>
                        </form>
                    </div>
                </td>
            </tr>
            """
        )

        recurring_mobile_list.append(
            f"""
            <div class='mobile-list-card'>
                <div class='mobile-list-top'>
                    <div class='mobile-list-title'>#{r['id']} - {escape(clean_text_display(r['title'], 'Recurring Mowing'))}</div>
                    <div>{active_chip}</div>
                </div>

                <div style='margin:-2px 0 10px 0; display:flex; gap:8px; flex-wrap:wrap;'>
                    <span class='service-chip mowing'>Mowing</span>
                    <span class='service-chip default'>{escape(interval_label(r['interval_weeks']))}</span>
                </div>

                <div class='mobile-list-grid'>
                    <div><span>Customer</span><strong>{escape(clean_text_display(r['customer_name']))}</strong></div>
                    <div><span>Next Run</span><strong>{escape(clean_text_display(r['next_run_date']))}</strong></div>
                    <div><span>Assigned To</span><strong>{escape(clean_text_display(r['assigned_to']))}</strong></div>
                    <div><span>Jobs Generated</span><strong>{safe_int(r['generated_jobs_count'], 0)}</strong></div>
                    <div><span>Total Revenue</span><strong>${safe_float(r['total_revenue']):.2f}</strong></div>
                    <div><span>Total Costs</span><strong>${safe_float(r['total_cost']):.2f}</strong></div>
                    <div><span>Total Profit</span><strong>${safe_float(r['total_profit']):.2f}</strong></div>
                    <div><span>Upcoming</span><strong>{escape(next_preview or '-')}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <a class='btn secondary small' href='{edit_url}'>Edit Schedule</a>

                    <form method='post' action='{generate_url}' style='margin:0;'>
                        <input type="hidden" name="csrf_token" value="{generate_now_csrf}">
                        <button class='btn success small' type='submit'>Generate</button>
                    </form>

                    <form method='post' action='{convert_url}' style='margin:0;'>
                        <input type="hidden" name="csrf_token" value="{convert_csrf}">
                        <button class='btn success small' type='submit'>Convert Invoice</button>
                    </form>

                    <form method='post' action='{toggle_url}' style='margin:0;'>
                        <input type="hidden" name="csrf_token" value="{toggle_csrf}">
                        <button class='btn warning small' type='submit'>{"Pause" if r["active"] else "Resume"}</button>
                    </form>

                    <form method='post'
                          action='{delete_url}'
                          style='margin:0;'
                          onsubmit="return confirm('Delete this recurring mowing schedule?');">
                        <input type="hidden" name="csrf_token" value="{delete_csrf}">
                        <button class='btn danger small' type='submit'>Delete</button>
                    </form>
                </div>
            </div>
            """
        )

    job_rows = "".join(job_row_list)
    job_mobile_cards = "".join(job_mobile_card_list)
    recurring_rows_html = "".join(recurring_row_list)
    recurring_mobile_cards = "".join(recurring_mobile_list)

    create_job_csrf = generate_csrf()
    create_schedule_csrf = generate_csrf()

    content = f"""
    <style>
        .customer-search-wrap {{
            position: relative;
        }}

        .customer-search-wrap label {{
            display: block;
            margin-bottom: 6px;
        }}

        .customer-search-input-wrap {{
            position: relative;
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

        .service-help {{
            margin-top: 6px;
            font-size: .8rem;
            color: #64748b;
            line-height: 1.35;
        }}

        .service-chip {{
            display: inline-flex;
            align-items: center;
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

        .quick-fill-row {{
            display:flex;
            flex-wrap:wrap;
            gap:8px;
            margin-top:10px;
        }}

        .quick-fill-chip {{
            appearance:none;
            border:1px solid #d8e2d0;
            background:#fff;
            color:#1f2933;
            border-radius:999px;
            padding:7px 11px;
            font-size:.82rem;
            font-weight:700;
            cursor:pointer;
        }}

        .quick-fill-chip:hover {{
            background:#f7f7f5;
        }}

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

        .jobs-profit {{
            font-weight: 700;
        }}

        .jobs-page {{
            display: grid;
            gap: 18px;
        }}

        .jobs-section-head {{
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:12px;
            flex-wrap:wrap;
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
            align-items:center;
        }}

        .recurring-card-head {{
            display:flex;
            justify-content:space-between;
            align-items:flex-start;
            gap:12px;
            flex-wrap:wrap;
            margin-bottom:14px;
        }}

        .recurring-default-note {{
            margin-top:8px;
            padding:10px 12px;
            border-radius:12px;
            background:#f0fdf4;
            border:1px solid #bbf7d0;
            color:#166534;
            font-size:.88rem;
            line-height:1.35;
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

            .jobs-section-head {{
                align-items:flex-start;
            }}

            .static-actions .btn,
            .mobile-list-actions .btn,
            .btn.small {{
                padding:8px 10px !important;
                font-size:0.84rem !important;
                line-height:1.2 !important;
            }}

            .customer-results {{
                max-height: 220px;
            }}
        }}
    </style>

    <div class='jobs-page'>
        <div class='card'>
            <div class='jobs-section-head'>
                <h1 style='margin:0;'>Jobs</h1>
                <div class='row-actions'>
                    <a class='btn secondary' href='{url_for("jobs.export_jobs")}'>Export CSV</a>
                    <a class='btn warning' href='{url_for("jobs.finished_jobs")}'>Finished Jobs</a>
                </div>
            </div>

            <form method='post' style='margin-top:18px;'>
                <input type="hidden" name="csrf_token" value="{create_job_csrf}">
                <div class='grid'>
                    <div class='customer-search-wrap'>
                        <label>Customer</label>

                        <div class='customer-search-input-wrap'>
                            <input type='text'
                                id='job_customer_search'
                                placeholder='Search customer name, company, or email...'
                                autocomplete='off'
                                required>
                            <input type='hidden' name='customer_id' id='job_customer_id' required>
                            <div id='job_customer_results' class='customer-results'></div>
                        </div>
                    </div>

                    <div>
                        <label>Title</label>
                        <input name='title' id='title' required placeholder='Example: Weekly Front Yard Mowing'>
                    </div>

                    <div>
                        <label>Service Type</label>
                        <select name='service_type' id='service_type'>
                            {service_type_select_options("mowing")}
                        </select>
                        <div class='service-help'>Mowing defaults are built in, and mowing jobs get a green mowing badge across Jobs and Calendar.</div>
                    </div>

                    <div>
                        <label>Scheduled Date</label>
                        <input type='date' name='scheduled_date'>
                    </div>

                    <div>
                        <label>Start Time</label>
                        <input type='time' name='scheduled_start_time'>
                    </div>

                    <div>
                        <label>End Time</label>
                        <input type='time' name='scheduled_end_time'>
                    </div>

                    <div>
                        <label>Assigned To</label>
                        <input name='assigned_to' placeholder='Crew / Employee'>
                    </div>

                    <div>
                        <label>Status</label>
                        <select name='status'>
                            <option>Scheduled</option>
                            <option>In Progress</option>
                            <option>Completed</option>
                            <option>Invoiced</option>
                        </select>
                    </div>

                    <div>
                        <label>Address</label>
                        <input name='address'>
                    </div>
                </div>

                <div class='quick-fill-row'>
                    <button class='quick-fill-chip' type='button' onclick="applyJobTemplate('mowing')">Use Mowing</button>
                    <button class='quick-fill-chip' type='button' onclick="applyJobTemplate('mulch')">Use Mulch</button>
                    <button class='quick-fill-chip' type='button' onclick="applyJobTemplate('cleanup')">Use Cleanup</button>
                    <button class='quick-fill-chip' type='button' onclick="applyJobTemplate('installation')">Use Installation</button>
                </div>

                <br>
                <label>Notes</label>
                <textarea name='notes'></textarea>
                <br>
                <button class='btn'>Create Job</button>
            </form>
        </div>

        <div class='card'>
            <div class='recurring-card-head'>
                <div>
                    <h2 style='margin:0;'>Recurring Mowing Schedules</h2>
                    <p class='muted' style='margin:6px 0 0 0;'>Weekly, every 2 weeks, or your own custom week interval. Upcoming jobs are auto-generated into Jobs and your Calendar.</p>
                </div>
                <div style='display:flex; gap:8px; flex-wrap:wrap;'>
                    <span class='service-chip mowing'>Mowing Default</span>
                    <span class='service-chip default'>Auto-Generates Jobs</span>
                </div>
            </div>

            <div class='recurring-default-note'>
                Recurring mowing schedules default to <strong>Mowing</strong>, create future jobs automatically, and each generated job links back to its parent recurring schedule.
            </div>

            <form method='post' action='{url_for("jobs.create_recurring_schedule")}' style='margin-top:16px;'>
                <input type="hidden" name="csrf_token" value="{create_schedule_csrf}">

                <div class='grid'>
                    <div class='customer-search-wrap'>
                        <label>Customer</label>

                        <div class='customer-search-input-wrap'>
                            <input type='text'
                                id='recurring_customer_search'
                                placeholder='Search customer name, company, or email...'
                                autocomplete='off'
                                required>
                            <input type='hidden' name='customer_id' id='recurring_customer_id' required>
                            <div id='recurring_customer_results' class='customer-results'></div>
                        </div>
                    </div>

                    <div>
                        <label>Schedule Title</label>
                        <input name='title' id='recurring_title' value='Recurring Mowing' required>
                    </div>

                    <div>
                        <label>Service Type</label>
                        <select name='service_type'>
                            {service_type_select_options("mowing")}
                        </select>
                    </div>

                    <div>
                        <label>Start Date</label>
                        <input type='date' name='start_date' value='{date.today().isoformat()}' required>
                    </div>

                    <div>
                        <label>Interval</label>
                        <select name='interval_mode' id='interval_mode' onchange='toggleCustomInterval()'>
                            <option value='weekly'>Weekly</option>
                            <option value='every_2'>Every 2 Weeks</option>
                            <option value='custom'>Custom Week Interval</option>
                        </select>
                    </div>

                    <div id='custom_interval_wrap' style='display:none;'>
                        <label>Custom Weeks</label>
                        <input type='number' name='custom_interval_weeks' id='custom_interval_weeks' min='1' step='1' value='3'>
                    </div>

                    <div>
                        <label>End Date</label>
                        <input type='date' name='end_date'>
                    </div>

                    <div>
                        <label>Start Time</label>
                        <input type='time' name='scheduled_start_time'>
                    </div>

                    <div>
                        <label>End Time</label>
                        <input type='time' name='scheduled_end_time'>
                    </div>

                    <div>
                        <label>Assigned To</label>
                        <input name='assigned_to' placeholder='Crew / Employee'>
                    </div>

                    <div>
                        <label>Default Job Status</label>
                        <select name='status_default'>
                            <option selected>Scheduled</option>
                            <option>In Progress</option>
                            <option>Completed</option>
                            <option>Invoiced</option>
                        </select>
                    </div>

                    <div>
                        <label>Address</label>
                        <input name='address'>
                    </div>
                </div>

                <br>
                <label>Notes</label>
                <textarea name='notes' placeholder='Notes that should carry onto each generated mowing job'></textarea>
                <br>
                <button class='btn success'>Create Recurring Mowing Schedule</button>
            </form>
        </div>

        <div class='card'>
            <h2>Recurring Schedule List</h2>

            <div class='static-table-wrap desktop-only'>
                <table class='static-table'>
                    <colgroup>
                        <col style='width:5%;'>
                        <col style='width:12%;'>
                        <col style='width:8%;'>
                        <col style='width:12%;'>
                        <col style='width:9%;'>
                        <col style='width:9%;'>
                        <col style='width:9%;'>
                        <col style='width:8%;'>
                        <col style='width:6%;'>
                        <col style='width:8%;'>
                        <col style='width:8%;'>
                        <col style='width:8%;'>
                        <col style='width:15%;'>
                    </colgroup>
                    <tr>
                        <th>ID</th>
                        <th class='wrap'>Title</th>
                        <th>Service</th>
                        <th class='wrap'>Customer</th>
                        <th>Interval</th>
                        <th>Next Run</th>
                        <th class='wrap'>Assigned To</th>
                        <th>Status</th>
                        <th class='center'>Jobs</th>
                        <th class='money'>Revenue</th>
                        <th class='money'>Costs</th>
                        <th class='money'>Profit</th>
                        <th class='wrap'>Actions</th>
                    </tr>
                    {recurring_rows_html or '<tr><td colspan="13" class="muted">No recurring mowing schedules yet.</td></tr>'}
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {recurring_mobile_cards or "<div class='mobile-list-card'>No recurring mowing schedules yet.</div>"}
                </div>
            </div>
        </div>

        <div class='card'>
            <h2>Job List</h2>

            <div class='static-table-wrap desktop-only'>
                <table class='static-table'>
                    <colgroup>
                        <col style='width:6%;'>
                        <col style='width:13%;'>
                        <col style='width:9%;'>
                        <col style='width:13%;'>
                        <col style='width:9%;'>
                        <col style='width:7%;'>
                        <col style='width:7%;'>
                        <col style='width:10%;'>
                        <col style='width:9%;'>
                        <col style='width:7%;'>
                        <col style='width:7%;'>
                        <col style='width:9%;'>
                        <col style='width:19%;'>
                    </colgroup>
                    <tr>
                        <th>ID</th>
                        <th class='wrap'>Title</th>
                        <th>Service</th>
                        <th class='wrap'>Customer</th>
                        <th>Date</th>
                        <th>Start</th>
                        <th>End</th>
                        <th class='wrap'>Assigned To</th>
                        <th>Status</th>
                        <th class='money'>Revenue</th>
                        <th class='money'>Costs</th>
                        <th class='money'>Profit/Loss</th>
                        <th class='wrap'>Actions</th>
                    </tr>
                    {job_rows or '<tr><td colspan="13" class="muted">No jobs yet.</td></tr>'}
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {job_mobile_cards or "<div class='mobile-list-card'>No jobs yet.</div>"}
                </div>
            </div>
        </div>
    </div>

    <script>
        const customers = {json.dumps(customer_list)};

        function escapeHtml(text) {{
            return String(text || "")
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }}

        function bindCustomerLookup(searchId, hiddenId, resultsId) {{
            const searchInput = document.getElementById(searchId);
            const customerIdInput = document.getElementById(hiddenId);
            const resultsBox = document.getElementById(resultsId);

            if (!searchInput || !customerIdInput || !resultsBox) return;

            function hideResults() {{
                resultsBox.innerHTML = "";
                resultsBox.classList.remove("show");
            }}

            function showResults() {{
                resultsBox.classList.add("show");
            }}

            function renderResults(matches) {{
                if (!matches.length) {{
                    resultsBox.innerHTML = "<div class='customer-result-item muted'>No customers found</div>";
                    showResults();
                    return;
                }}

                resultsBox.innerHTML = matches.map(c => `
                    <div class="customer-result-item" data-id="${{c.id}}">
                        <strong>${{escapeHtml(c.name || "Unnamed Customer")}}</strong>
                        ${{c.company ? `<div class="muted small">${{escapeHtml(c.company)}}</div>` : ""}}
                        ${{c.email ? `<div class="muted small">${{escapeHtml(c.email)}}</div>` : ""}}
                    </div>
                `).join("");

                showResults();

                resultsBox.querySelectorAll(".customer-result-item[data-id]").forEach(item => {{
                    item.addEventListener("click", function () {{
                        const id = this.dataset.id;
                        const customer = customers.find(x => String(x.id) === String(id));
                        if (!customer) return;

                        customerIdInput.value = customer.id;
                        searchInput.value = customer.company
                            ? `${{customer.name}} - ${{customer.company}}`
                            : (customer.name || "Unnamed Customer");

                        hideResults();
                    }});
                }});
            }}

            searchInput.addEventListener("input", function () {{
                const q = this.value.trim().toLowerCase();
                customerIdInput.value = "";

                if (!q) {{
                    hideResults();
                    return;
                }}

                const matches = customers.filter(c =>
                    (c.name && c.name.toLowerCase().includes(q)) ||
                    (c.company && c.company.toLowerCase().includes(q)) ||
                    (c.email && c.email.toLowerCase().includes(q))
                ).slice(0, 8);

                renderResults(matches);
            }});

            document.addEventListener("click", function (e) {{
                if (!e.target.closest(".customer-search-wrap")) {{
                    hideResults();
                }}
            }});
        }}

        const serviceTypeInput = document.getElementById("service_type");
        const titleInput = document.getElementById("title");

        function maybeFillTitleFromService() {{
            if (!titleInput || !serviceTypeInput) return;
            if ((titleInput.value || "").trim()) return;

            const templates = {{
                mowing: "Weekly Mowing",
                mulch: "Mulch Delivery / Install",
                cleanup: "Property Cleanup",
                installation: "Landscape Installation",
                hardscape: "Hardscape Work",
                snow_removal: "Snow Removal",
                fertilizing: "Fertilizing Service",
                other: ""
            }};

            const value = serviceTypeInput.value || "other";
            if (templates[value]) {{
                titleInput.value = templates[value];
            }}
        }}

        function applyJobTemplate(serviceType) {{
            if (!serviceTypeInput) return;
            serviceTypeInput.value = serviceType;
            maybeFillTitleFromService();

            if (serviceType === "mowing" && titleInput && !(titleInput.value || "").trim()) {{
                titleInput.value = "Weekly Mowing";
            }}
        }}

        function toggleCustomInterval() {{
            const mode = document.getElementById("interval_mode");
            const wrap = document.getElementById("custom_interval_wrap");
            if (!mode || !wrap) return;
            wrap.style.display = mode.value === "custom" ? "block" : "none";
        }}

        if (serviceTypeInput) {{
            serviceTypeInput.addEventListener("change", function() {{
                maybeFillTitleFromService();
            }});
        }}

        bindCustomerLookup("job_customer_search", "job_customer_id", "job_customer_results");
        bindCustomerLookup("recurring_customer_search", "recurring_customer_id", "recurring_customer_results");
        toggleCustomInterval();
    </script>
    """
    return render_page(content, "Jobs")


@jobs_bp.route("/jobs/recurring/create", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def create_recurring_schedule():
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    customer_id = request.form.get("customer_id", type=int)
    title = recurring_schedule_title_default(request.form.get("title", "Recurring Mowing"))
    service_type = normalize_service_type(request.form.get("service_type", "mowing"))
    start_date = clean_text_input(request.form.get("start_date", ""))
    end_date = clean_text_input(request.form.get("end_date", ""))
    scheduled_start_time = clean_text_input(request.form.get("scheduled_start_time", ""))
    scheduled_end_time = clean_text_input(request.form.get("scheduled_end_time", ""))
    assigned_to = clean_text_input(request.form.get("assigned_to", ""))
    status_default = default_mowing_status(request.form.get("status_default", "Scheduled"))
    address = clean_text_input(request.form.get("address", ""))
    notes = clean_text_input(request.form.get("notes", ""))
    interval_weeks = derive_interval_weeks_from_form(request.form)

    if not customer_id:
        conn.close()
        flash("Please select a customer for the recurring mowing schedule.")
        return redirect(url_for("jobs.jobs"))

    if not start_date:
        conn.close()
        flash("Start date is required.")
        return redirect(url_for("jobs.jobs"))

    start_date_value = parse_iso_date(start_date)
    end_date_value = parse_iso_date(end_date)

    if not start_date_value:
        conn.close()
        flash("Invalid start date.")
        return redirect(url_for("jobs.jobs"))

    if end_date and not end_date_value:
        conn.close()
        flash("Invalid end date.")
        return redirect(url_for("jobs.jobs"))

    if end_date_value and end_date_value < start_date_value:
        conn.close()
        flash("End date cannot be before the start date.")
        return redirect(url_for("jobs.jobs"))

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO recurring_mowing_schedules (
            company_id,
            customer_id,
            title,
            service_type,
            interval_weeks,
            start_date,
            next_run_date,
            end_date,
            scheduled_start_time,
            scheduled_end_time,
            assigned_to,
            status_default,
            address,
            notes,
            active,
            auto_generate_until_days,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        RETURNING id
        """,
        (
            cid,
            customer_id,
            title,
            service_type or "mowing",
            interval_weeks,
            start_date,
            start_date,
            end_date or None,
            scheduled_start_time or None,
            scheduled_end_time or None,
            assigned_to or None,
            status_default,
            address,
            notes,
            True,
            42,
        ),
    )
    row = cur.fetchone()
    schedule_id = row["id"] if row and "id" in row else None

    if schedule_id:
        add_default_recurring_mowing_items(conn, cid, schedule_id)

    try:
        auto_generate_recurring_jobs(conn, cid)
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f"Could not create recurring mowing schedule: {e}")
        return redirect(url_for("jobs.jobs"))

    conn.close()

    if schedule_id:
        flash("Recurring mowing schedule created and upcoming jobs generated.")
        return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

    flash("Recurring mowing schedule created.")
    return redirect(url_for("jobs.jobs"))

@jobs_bp.route("/jobs/recurring/<int:schedule_id>/generate", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def generate_recurring_schedule_jobs(schedule_id):
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    schedule = conn.execute(
        """
        SELECT *
        FROM recurring_mowing_schedules
        WHERE id = %s AND company_id = %s
        """,
        (schedule_id, cid),
    ).fetchone()

    if not schedule:
        conn.close()
        flash("Recurring mowing schedule not found.")
        return redirect(url_for("jobs.jobs"))

    try:
        horizon_days = safe_int(schedule["auto_generate_until_days"], 42)
        through_date = date.today() + timedelta(days=horizon_days if horizon_days > 0 else 42)
        created_count = auto_generate_recurring_jobs(conn, cid, through_date=through_date)
        conn.commit()
        flash(f"Recurring generation complete. {created_count} job(s) created.")
    except Exception as e:
        conn.rollback()
        flash(f"Could not generate recurring jobs: {e}")
    finally:
        conn.close()

    return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))


@jobs_bp.route("/jobs/recurring/<int:schedule_id>/edit", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def edit_recurring_schedule(schedule_id):
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    schedule = conn.execute(
        """
        SELECT rms.*, c.name AS customer_name
        FROM recurring_mowing_schedules rms
        JOIN customers c ON rms.customer_id = c.id
        WHERE rms.id = %s AND rms.company_id = %s
        """,
        (schedule_id, cid),
    ).fetchone()

    if not schedule:
        conn.close()
        flash("Recurring mowing schedule not found.")
        return redirect(url_for("jobs.jobs"))

    customers = conn.execute(
        "SELECT id, name FROM customers WHERE company_id = %s ORDER BY name",
        (cid,),
    ).fetchall()

    if request.method == "POST":
        customer_id = request.form.get("customer_id", type=int)
        title = recurring_schedule_title_default(request.form.get("title", "Recurring Mowing"))
        service_type = normalize_service_type(request.form.get("service_type", "mowing"))
        start_date = clean_text_input(request.form.get("start_date", ""))
        end_date = clean_text_input(request.form.get("end_date", ""))
        scheduled_start_time = clean_text_input(request.form.get("scheduled_start_time", ""))
        scheduled_end_time = clean_text_input(request.form.get("scheduled_end_time", ""))
        assigned_to = clean_text_input(request.form.get("assigned_to", ""))
        status_default = default_mowing_status(request.form.get("status_default", "Scheduled"))
        address = clean_text_input(request.form.get("address", ""))
        notes = clean_text_input(request.form.get("notes", ""))
        interval_weeks = derive_interval_weeks_from_form(request.form)

        start_date_value = parse_iso_date(start_date)
        end_date_value = parse_iso_date(end_date)
        old_next_run = parse_iso_date(schedule["next_run_date"])
        old_start_date = parse_iso_date(schedule["start_date"])

        if not customer_id:
            conn.close()
            flash("Customer is required.")
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        if not start_date_value:
            conn.close()
            flash("Valid start date is required.")
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        if end_date and not end_date_value:
            conn.close()
            flash("Invalid end date.")
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        if end_date_value and end_date_value < start_date_value:
            conn.close()
            flash("End date cannot be before start date.")
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        new_next_run = old_next_run
        if not new_next_run:
            new_next_run = start_date_value

        if old_start_date and start_date_value and old_next_run == old_start_date:
            new_next_run = start_date_value

        if new_next_run and new_next_run < start_date_value:
            new_next_run = start_date_value

        conn.execute(
            """
            UPDATE recurring_mowing_schedules
            SET customer_id = %s,
                title = %s,
                service_type = %s,
                interval_weeks = %s,
                start_date = %s,
                next_run_date = %s,
                end_date = %s,
                scheduled_start_time = %s,
                scheduled_end_time = %s,
                assigned_to = %s,
                status_default = %s,
                address = %s,
                notes = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s AND company_id = %s
            """,
            (
                customer_id,
                title,
                service_type or "mowing",
                interval_weeks,
                start_date,
                date_to_iso(new_next_run) if new_next_run else start_date,
                end_date or None,
                scheduled_start_time or None,
                scheduled_end_time or None,
                assigned_to or None,
                status_default,
                address,
                notes,
                schedule_id,
                cid,
            ),
        )

        try:
            auto_generate_recurring_jobs(conn, cid)
            conn.commit()
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f"Could not update recurring mowing schedule: {e}")
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        conn.close()
        flash("Recurring mowing schedule updated.")
        return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

    generated_jobs = conn.execute(
        """
        SELECT id, title, scheduled_date, scheduled_start_time, scheduled_end_time, status
        FROM jobs
        WHERE company_id = %s
          AND recurring_schedule_id = %s
        ORDER BY scheduled_date DESC, id DESC
        LIMIT 25
        """,
        (cid, schedule_id),
    ).fetchall()

    recurring_items = conn.execute(
        """
        SELECT *
        FROM recurring_mowing_schedule_items
        WHERE company_id = %s
          AND schedule_id = %s
        ORDER BY id ASC
        """,
        (cid, schedule_id),
    ).fetchall()

    customer_opts = "".join(
        f"<option value='{c['id']}' {'selected' if c['id'] == schedule['customer_id'] else ''}>{escape(clean_text_display(c['name'], 'Customer #' + str(c['id'])))}</option>"
        for c in customers
    )

    edit_csrf = generate_csrf()
    generate_csrf_token = generate_csrf()
    toggle_csrf = generate_csrf()
    convert_csrf = generate_csrf()

    jobs_rows = []
    for j in generated_jobs:
        jobs_rows.append(
            f"""
            <tr>
                <td>#{j['id']}</td>
                <td class='wrap'><a href='{url_for("jobs.view_job", job_id=j["id"])}'>{escape(clean_text_display(j['title']))}</a></td>
                <td>{escape(clean_text_display(j['scheduled_date']))}</td>
                <td>{escape(clean_text_display(j['scheduled_start_time']))}</td>
                <td>{escape(clean_text_display(j['scheduled_end_time']))}</td>
                <td>{escape(clean_text_display(j['status']))}</td>
            </tr>
            """
        )

    generated_jobs_table = "".join(jobs_rows)

    recurring_item_rows = []
    recurring_item_mobile_cards = []

    for item in recurring_items:
        delete_item_csrf = generate_csrf()

        unit_cost_display = f"${safe_float(item['unit_cost']):.2f}"
        sale_price_display = f"${safe_float(item['sale_price']):.2f}"
        total_cost_display = safe_float(item["quantity"]) * safe_float(item["unit_cost"])
        total_revenue_display = safe_float(item["quantity"]) * safe_float(item["sale_price"])

        recurring_item_rows.append(
            f"""
            <tr>
                <td>{escape(display_item_type(item['item_type']))}</td>
                <td class='wrap'>{escape(clean_text_display(item['description']))}</td>
                <td class='money'>{safe_float(item['quantity']):g}</td>
                <td>{escape(clean_text_display(item['unit']))}</td>
                <td class='money'>{sale_price_display}</td>
                <td class='money'>{unit_cost_display}</td>
                <td class='money'>${total_cost_display:.2f}</td>
                <td class='center'>{'Yes' if item['billable'] else 'No'}</td>
                <td class='money'>${total_revenue_display:.2f}</td>
                <td class='wrap'>
                    <form method='post'
                          action='{url_for("jobs.delete_recurring_schedule_item", schedule_id=schedule_id, item_id=item["id"])}'
                          style='margin:0;'
                          onsubmit="return confirm('Delete this recurring schedule item?');">
                        <input type="hidden" name="csrf_token" value="{delete_item_csrf}">
                        <button class='btn danger small' type='submit'>Delete</button>
                    </form>
                </td>
            </tr>
            """
        )

        recurring_item_mobile_cards.append(
            f"""
            <div class='mobile-list-card'>
                <div class='mobile-list-top'>
                    <div class='mobile-list-title'>{escape(display_item_type(item['item_type']))} - {escape(clean_text_display(item['description']))}</div>
                    <div class='mobile-badge'>{'Billable' if item['billable'] else 'Non-Billable'}</div>
                </div>

                <div class='mobile-list-grid'>
                    <div><span>Qty</span><strong>{safe_float(item['quantity']):g}</strong></div>
                    <div><span>Unit</span><strong>{escape(clean_text_display(item['unit']))}</strong></div>
                    <div><span>Sale Price</span><strong>{sale_price_display}</strong></div>
                    <div><span>Unit Cost</span><strong>{unit_cost_display}</strong></div>
                    <div><span>Total Cost</span><strong>${total_cost_display:.2f}</strong></div>
                    <div><span>Total Revenue</span><strong>${total_revenue_display:.2f}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <form method='post'
                          action='{url_for("jobs.delete_recurring_schedule_item", schedule_id=schedule_id, item_id=item["id"])}'
                          style='margin:0;'
                          onsubmit="return confirm('Delete this recurring schedule item?');">
                        <input type="hidden" name="csrf_token" value="{delete_item_csrf}">
                        <button class='btn danger small' type='submit'>Delete</button>
                    </form>
                </div>
            </div>
            """
        )

    recurring_item_rows_html = "".join(recurring_item_rows)
    recurring_item_mobile_html = "".join(recurring_item_mobile_cards)

    add_recurring_item_csrf = generate_csrf()

    interval_mode = interval_mode_from_weeks(schedule["interval_weeks"])
    custom_wrap_display = "block" if interval_mode == "custom" else "none"

    content = f"""
    <style>
        .service-chip {{
            display: inline-flex;
            align-items: center;
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
        .service-chip.default {{
            background: #f8fafc;
            color: #334155;
            border-color: #e2e8f0;
        }}
        .static-table {{
            width:100%;
            border-collapse:collapse;
            table-layout:fixed;
        }}
        .static-table th,
        .static-table td {{
            padding:10px 8px;
            border-bottom:1px solid rgba(0,0,0,.06);
            vertical-align:top;
        }}
        .static-table td.wrap,
        .static-table th.wrap {{
            word-break:break-word;
            white-space:normal;
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
            align-items:center;
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

    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:flex-start; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin:0;'>Edit Recurring Mowing Schedule #{schedule['id']}</h1>
                <p class='muted' style='margin:6px 0 0 0;'>This schedule auto-generates mowing jobs and keeps them linked back here.</p>
            </div>
            <div style='display:flex; gap:8px; flex-wrap:wrap;'>
                <span class='service-chip mowing'>Mowing</span>
                <span class='service-chip default'>{escape(interval_label(schedule['interval_weeks']))}</span>
                <span class='service-chip {schedule_status_class(schedule["active"])}'>{escape(schedule_status_badge(schedule["active"]))}</span>
            </div>
        </div>

        <div class='row-actions' style='margin-top:14px;'>
            <a class='btn secondary' href='{url_for("jobs.jobs")}'>Back to Jobs</a>

            <form method='post' action='{url_for("jobs.generate_recurring_schedule_jobs", schedule_id=schedule["id"])}' style='margin:0;'>
                <input type="hidden" name="csrf_token" value="{generate_csrf_token}">
                <button class='btn success' type='submit'>Generate Upcoming Jobs Now</button>
            </form>

            <form method='post' action='{url_for("jobs.convert_recurring_schedule_to_invoice", schedule_id=schedule["id"])}' style='margin:0;'>
                <input type="hidden" name="csrf_token" value="{convert_csrf}">
                <button class='btn success' type='submit'>Convert Schedule to Invoice</button>
            </form>

            <form method='post' action='{url_for("jobs.toggle_recurring_schedule", schedule_id=schedule["id"])}' style='margin:0;'>
                <input type="hidden" name="csrf_token" value="{toggle_csrf}">
                <button class='btn warning' type='submit'>{"Pause Schedule" if schedule["active"] else "Resume Schedule"}</button>
            </form>
        </div>
    </div>

    <div class='card'>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{edit_csrf}">
            <div class='grid'>
                <div>
                    <label>Customer</label>
                    <select name='customer_id' required>
                        <option value=''>Select customer</option>
                        {customer_opts}
                    </select>
                </div>

                <div>
                    <label>Schedule Title</label>
                    <input name='title' value="{escape(clean_text_input(schedule['title']) or 'Recurring Mowing')}" required>
                </div>

                <div>
                    <label>Service Type</label>
                    <select name='service_type'>
                        {service_type_select_options(schedule['service_type'] or 'mowing')}
                    </select>
                </div>

                <div>
                    <label>Start Date</label>
                    <input type='date' name='start_date' value="{escape(date_to_iso(schedule['start_date']))}" required>
                </div>

                <div>
                    <label>Interval</label>
                    <select name='interval_mode' id='edit_interval_mode' onchange='toggleEditCustomInterval()'>
                        <option value='weekly' {'selected' if interval_mode == 'weekly' else ''}>Weekly</option>
                        <option value='every_2' {'selected' if interval_mode == 'every_2' else ''}>Every 2 Weeks</option>
                        <option value='custom' {'selected' if interval_mode == 'custom' else ''}>Custom Week Interval</option>
                    </select>
                </div>

                <div id='edit_custom_interval_wrap' style='display:{custom_wrap_display};'>
                    <label>Custom Weeks</label>
                    <input type='number' name='custom_interval_weeks' min='1' step='1' value='{safe_int(schedule["interval_weeks"], 1)}'>
                </div>

                <div>
                    <label>Next Run Date</label>
                    <input type='date' value="{escape(date_to_iso(schedule['next_run_date']))}" disabled>
                    <div class='muted small' style='margin-top:4px;'>Auto-managed after generation.</div>
                </div>

                <div>
                    <label>End Date</label>
                    <input type='date' name='end_date' value="{escape(date_to_iso(schedule['end_date']))}">
                </div>

                <div>
                    <label>Start Time</label>
                    <input type='time' name='scheduled_start_time' value="{escape(clean_text_input(schedule['scheduled_start_time']))}">
                </div>

                <div>
                    <label>End Time</label>
                    <input type='time' name='scheduled_end_time' value="{escape(clean_text_input(schedule['scheduled_end_time']))}">
                </div>

                <div>
                    <label>Assigned To</label>
                    <input name='assigned_to' value="{escape(clean_text_input(schedule['assigned_to']))}">
                </div>

                <div>
                    <label>Default Job Status</label>
                    <select name='status_default'>
                        <option {'selected' if clean_text_input(schedule['status_default']) == 'Scheduled' else ''}>Scheduled</option>
                        <option {'selected' if clean_text_input(schedule['status_default']) == 'In Progress' else ''}>In Progress</option>
                        <option {'selected' if clean_text_input(schedule['status_default']) == 'Completed' else ''}>Completed</option>
                        <option {'selected' if clean_text_input(schedule['status_default']) == 'Invoiced' else ''}>Invoiced</option>
                    </select>
                </div>

                <div>
                    <label>Address</label>
                    <input name='address' value="{escape(clean_text_input(schedule['address']))}">
                </div>
            </div>

            <br>
            <label>Notes</label>
            <textarea name='notes'>{escape(clean_text_input(schedule['notes']))}</textarea>
            <br>
            <button class='btn'>Save Schedule Changes</button>
        </form>
    </div>

    <div class='card'>
        <h2>Recurring Schedule Items</h2>
        <p class='muted'>These items will be copied into each newly generated recurring job. This is where you set the mowing price and costs.</p>

        <form method='post' action='{url_for("jobs.add_recurring_schedule_item", schedule_id=schedule_id)}'>
            <input type="hidden" name="csrf_token" value="{add_recurring_item_csrf}">
            <div class='grid'>
                <div>
                    <label>Type</label>
                    <select name='item_type' id='recurring_item_type' onchange='toggleRecurringItemMode()'>
                        <option value='labor'>Labor</option>
                        <option value='fuel'>Fuel</option>
                        <option value='misc'>Misc</option>
                        <option value='dump_fee'>Dump Fee</option>
                        <option value='equipment'>Equipment</option>
                        <option value='delivery'>Delivery</option>
                        <option value='mulch'>Mulch</option>
                        <option value='stone'>Stone</option>
                        <option value='soil'>Soil</option>
                        <option value='fertilizer'>Fertilizer</option>
                        <option value='plants'>Plants</option>
                        <option value='trees'>Trees</option>
                        <option value='hardscape_material'>Hardscape Material</option>
                    </select>
                </div>

                <div>
                    <label>Description</label>
                    <input name='description' value='Mowing Service' required>
                </div>

                <div>
                    <label id='recurring_quantity_label'>Quantity</label>
                    <input type='number' step='0.01' name='quantity' id='recurring_quantity' value='1' required>
                </div>

                <div>
                    <label>Unit</label>
                    <input name='unit' id='recurring_unit' value='Hours'>
                </div>

                <div>
                    <label id='recurring_sale_price_label'>Sale Price</label>
                    <input type='number' step='0.01' name='sale_price' id='recurring_sale_price' value='0' required>
                </div>

                <div id="unit_cost_wrap">
                    <label id='recurring_cost_label'>Unit Cost</label>
                    <input type='number' step='0.01' name='unit_cost' id='recurring_unit_cost' value='0'>
                </div>

                <div>
                    <label>Billable?</label>
                    <select name='billable'>
                        <option value='1'>Yes</option>
                        <option value='0'>No</option>
                    </select>
                </div>
            </div>

            <br>
            <button class='btn success' type='submit'>Add Recurring Item</button>
        </form>
    </div>

    <div class='card'>
        <h2>Recurring Item List</h2>

        <div class='static-table-wrap desktop-only'>
            <table class='static-table'>
                <colgroup>
                    <col style='width:10%;'>
                    <col style='width:22%;'>
                    <col style='width:7%;'>
                    <col style='width:8%;'>
                    <col style='width:10%;'>
                    <col style='width:10%;'>
                    <col style='width:10%;'>
                    <col style='width:8%;'>
                    <col style='width:10%;'>
                    <col style='width:15%;'>
                </colgroup>
                <tr>
                    <th>Type</th>
                    <th class='wrap'>Description</th>
                    <th class='money'>Qty</th>
                    <th>Unit</th>
                    <th class='money'>Sale Price</th>
                    <th class='money'>Unit Cost</th>
                    <th class='money'>Total Cost</th>
                    <th class='center'>Billable</th>
                    <th class='money'>Revenue</th>
                    <th class='wrap'>Actions</th>
                </tr>
                {recurring_item_rows_html or '<tr><td colspan="10" class="muted">No recurring items yet.</td></tr>'}
            </table>
        </div>

        <div class='mobile-only'>
            <div class='mobile-list'>
                {recurring_item_mobile_html or "<div class='mobile-list-card'>No recurring items yet.</div>"}
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Generated Jobs Linked to This Schedule</h2>
        <table class='static-table'>
            <colgroup>
                <col style='width:10%;'>
                <col style='width:30%;'>
                <col style='width:16%;'>
                <col style='width:14%;'>
                <col style='width:14%;'>
                <col style='width:16%;'>
            </colgroup>
            <tr>
                <th>ID</th>
                <th class='wrap'>Title</th>
                <th>Date</th>
                <th>Start</th>
                <th>End</th>
                <th>Status</th>
            </tr>
            {generated_jobs_table or '<tr><td colspan="6" class="muted">No generated jobs yet.</td></tr>'}
        </table>
    </div>

    <script>
        function toggleEditCustomInterval() {{
            const mode = document.getElementById("edit_interval_mode");
            const wrap = document.getElementById("edit_custom_interval_wrap");
            if (!mode || !wrap) return;
            wrap.style.display = mode.value === "custom" ? "block" : "none";
        }}

        function toggleRecurringItemMode() {{
            const type = document.getElementById('recurring_item_type').value;

            const quantityLabel = document.getElementById('recurring_quantity_label');
            const salePriceLabel = document.getElementById('recurring_sale_price_label');
            const costLabel = document.getElementById('recurring_cost_label');

            const unitInput = document.getElementById('recurring_unit');
            const quantityInput = document.getElementById('recurring_quantity');
            const unitCostInput = document.getElementById('recurring_unit_cost');
            const unitCostWrap = unitCostInput ? unitCostInput.closest("div") : null;

            if (quantityLabel) quantityLabel.innerText = 'Quantity';
            if (salePriceLabel) salePriceLabel.innerText = 'Sale Price';
            if (costLabel) costLabel.innerText = 'Unit Cost';

            if (quantityInput) {{
                quantityInput.readOnly = false;
                quantityInput.step = '0.01';
            }}

            if (unitCostWrap) unitCostWrap.style.display = 'block';

            if (type === 'labor') {{
                if (quantityLabel) quantityLabel.innerText = 'Billable Hours';
                if (salePriceLabel) salePriceLabel.innerText = 'Hourly Rate';
                if (unitInput) unitInput.value = 'Hours';

                if (unitCostWrap) unitCostWrap.style.display = 'none';
                if (unitCostInput) unitCostInput.value = '0';
            }}
            else if (type === 'mulch') {{
                if (quantityLabel) quantityLabel.innerText = 'Yards';
                if (unitInput) unitInput.value = 'Yards';
            }}
            else if (type === 'stone') {{
                if (quantityLabel) quantityLabel.innerText = 'Tons';
                if (unitInput) unitInput.value = 'Tons';
            }}
            else if (type === 'soil') {{
                if (quantityLabel) quantityLabel.innerText = 'Yards';
                if (unitInput) unitInput.value = 'Yards';
            }}
            else if (type === 'hardscape_material') {{
                if (quantityLabel) quantityLabel.innerText = 'Tons';
                if (unitInput) unitInput.value = 'Tons';
            }}
            else if (type === 'fuel') {{
                if (quantityLabel) quantityLabel.innerText = 'Gallons';
                if (unitInput) unitInput.value = 'Gallons';
            }}
            else if (type === 'delivery') {{
                if (quantityLabel) quantityLabel.innerText = 'Miles';
                if (unitInput) unitInput.value = 'Miles';
            }}
            else if (type === 'equipment') {{
                if (quantityLabel) quantityLabel.innerText = 'Rentals';
                if (unitInput) unitInput.value = 'Rentals';
            }}
            else if (type === 'dump_fee') {{
                if (quantityLabel) quantityLabel.innerText = 'Fee';
                if (salePriceLabel) salePriceLabel.innerText = 'Fee Amount';
                if (unitInput) unitInput.value = '';

                if (unitCostWrap) unitCostWrap.style.display = 'none';
                if (unitCostInput) unitCostInput.value = '0';

                if (quantityInput) {{
                    quantityInput.value = '1';
                    quantityInput.readOnly = true;
                }}
            }}
            else if (type === 'fertilizer') {{
                if (unitInput) unitInput.value = 'Bags';
            }}
            else {{
                if (unitInput) unitInput.value = '';
            }}
        }}

        toggleEditCustomInterval();
        toggleRecurringItemMode();
    </script>
    """
    conn.close()
    return render_page(content, f"Recurring Schedule #{schedule_id}")

@jobs_bp.route("/jobs/recurring/<int:schedule_id>/items/add", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def add_recurring_schedule_item(schedule_id):
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    schedule = conn.execute(
        """
        SELECT id, company_id
        FROM recurring_mowing_schedules
        WHERE id = %s AND company_id = %s
        """,
        (schedule_id, cid),
    ).fetchone()

    if not schedule:
        conn.close()
        flash("Recurring mowing schedule not found.")
        return redirect(url_for("jobs.jobs"))

    item_type = clean_text_input(request.form.get("item_type", "")).lower()
    description = clean_text_input(request.form.get("description", ""))
    qty = safe_float(request.form.get("quantity"))
    unit = clean_text_input(request.form.get("unit", ""))
    sale_price = safe_float(request.form.get("sale_price"))
    unit_cost = safe_float(request.form.get("unit_cost"))
    billable = True if request.form.get("billable") == "1" else False

    if not description:
        conn.close()
        flash("Description is required.")
        return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

    if qty <= 0:
        qty = 1.0

    if item_type == "mulch" and not unit:
        unit = "Yards"
    elif item_type == "stone" and not unit:
        unit = "Tons"
    elif item_type == "soil" and not unit:
        unit = "Yards"
    elif item_type == "hardscape_material" and not unit:
        unit = "Tons"
    elif item_type == "fuel" and not unit:
        unit = "Gallons"
    elif item_type == "delivery" and not unit:
        unit = "Miles"
    elif item_type == "labor" and not unit:
        unit = "Hours"
    elif item_type == "equipment" and not unit:
        unit = "Rentals"
    elif item_type == "fertilizer" and not unit:
        unit = "Bags"
    elif item_type in ["plants", "trees", "misc", "dump_fee"]:
        unit = ""

    if item_type == "labor":
        unit_cost = 0.0

    if item_type == "dump_fee":
        unit = ""
        qty = 1.0
        unit_cost = 0.0

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO recurring_mowing_schedule_items (
            company_id,
            schedule_id,
            item_type,
            description,
            quantity,
            unit,
            unit_cost,
            sale_price,
            billable
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            cid,
            schedule_id,
            item_type,
            description,
            qty,
            unit,
            unit_cost,
            sale_price,
            billable,
        ),
    )
    row = cur.fetchone()
    item_id = row["id"] if row and "id" in row else None

    if not item_id:
        conn.rollback()
        conn.close()
        flash("Could not add recurring schedule item.")
        return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

    conn.commit()
    conn.close()

    flash("Recurring schedule item added.")
    return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))


@jobs_bp.route("/jobs/recurring/<int:schedule_id>/delete", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def delete_recurring_schedule(schedule_id):
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    schedule = conn.execute(
        """
        SELECT id, title
        FROM recurring_mowing_schedules
        WHERE id = %s AND company_id = %s
        """,
        (schedule_id, cid),
    ).fetchone()

    if not schedule:
        conn.close()
        flash("Recurring mowing schedule not found.")
        return redirect(url_for("jobs.jobs"))

    try:
        jobs_to_delete = conn.execute(
            """
            SELECT id, title, status
            FROM jobs
            WHERE company_id = %s
              AND recurring_schedule_id = %s
            ORDER BY scheduled_date ASC, id ASC
            """,
            (cid, schedule_id),
        ).fetchall()

        job_ids = [row["id"] for row in jobs_to_delete]

        if job_ids:
            invoiced_jobs = conn.execute(
                """
                SELECT
                    j.id,
                    j.title,
                    j.status,
                    i.id AS invoice_id,
                    i.invoice_number
                FROM jobs j
                LEFT JOIN invoices i
                  ON i.job_id = j.id
                 AND i.company_id = j.company_id
                WHERE j.company_id = %s
                  AND j.id = ANY(%s)
                  AND (
                        COALESCE(j.status, '') = 'Invoiced'
                        OR i.id IS NOT NULL
                  )
                ORDER BY j.id ASC
                """,
                (cid, job_ids),
            ).fetchall()

            if invoiced_jobs:
                sample = invoiced_jobs[0]
                invoice_label = clean_text_input(sample["invoice_number"]) or f"Invoice #{sample['invoice_id']}"
                flash(
                    f"Cannot delete this recurring mowing schedule because generated job #{sample['id']} "
                    f"is already tied to {invoice_label}. Remove or handle the invoice first."
                )
                conn.close()
                return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

            ledger_rows = conn.execute(
                """
                SELECT ledger_entry_id
                FROM job_items
                WHERE job_id = ANY(%s)
                  AND ledger_entry_id IS NOT NULL
                """,
                (job_ids,),
            ).fetchall()

            ledger_ids = [row["ledger_entry_id"] for row in ledger_rows if row["ledger_entry_id"]]

            if ledger_ids:
                conn.execute(
                    """
                    DELETE FROM ledger_entries
                    WHERE id = ANY(%s)
                    """,
                    (ledger_ids,),
                )

            conn.execute(
                """
                DELETE FROM job_items
                WHERE job_id = ANY(%s)
                """,
                (job_ids,),
            )

            conn.execute(
                """
                DELETE FROM jobs
                WHERE id = ANY(%s)
                  AND company_id = %s
                """,
                (job_ids, cid),
            )

        conn.execute(
            """
            DELETE FROM recurring_mowing_schedule_items
            WHERE company_id = %s
              AND schedule_id = %s
            """,
            (cid, schedule_id),
        )

        conn.execute(
            """
            DELETE FROM recurring_mowing_schedules
            WHERE id = %s AND company_id = %s
            """,
            (schedule_id, cid),
        )

        conn.commit()
        flash("Recurring mowing schedule and all non-invoiced generated jobs were deleted.")
    except Exception as e:
        conn.rollback()
        flash(f"Could not delete recurring mowing schedule: {e}")
    finally:
        conn.close()

    return redirect(url_for("jobs.jobs"))


@jobs_bp.route("/jobs/recurring/<int:schedule_id>/items/<int:item_id>/delete", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def delete_recurring_schedule_item(schedule_id, item_id):
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    item = conn.execute(
        """
        SELECT id
        FROM recurring_mowing_schedule_items
        WHERE id = %s
          AND schedule_id = %s
          AND company_id = %s
        """,
        (item_id, schedule_id, cid),
    ).fetchone()

    if not item:
        conn.close()
        flash("Recurring schedule item not found.")
        return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

    conn.execute(
        """
        DELETE FROM recurring_mowing_schedule_items
        WHERE id = %s
          AND schedule_id = %s
          AND company_id = %s
        """,
        (item_id, schedule_id, cid),
    )
    conn.commit()
    conn.close()

    flash("Recurring schedule item deleted.")
    return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))


@jobs_bp.route("/jobs/recurring/<int:schedule_id>/toggle", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def toggle_recurring_schedule(schedule_id):
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    schedule = conn.execute(
        """
        SELECT id, active
        FROM recurring_mowing_schedules
        WHERE id = %s AND company_id = %s
        """,
        (schedule_id, cid),
    ).fetchone()

    if not schedule:
        conn.close()
        flash("Recurring mowing schedule not found.")
        return redirect(url_for("jobs.jobs"))

    new_active = not bool(schedule["active"])

    conn.execute(
        """
        UPDATE recurring_mowing_schedules
        SET active = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s AND company_id = %s
        """,
        (new_active, schedule_id, cid),
    )
    conn.commit()
    conn.close()

    flash("Recurring mowing schedule resumed." if new_active else "Recurring mowing schedule paused.")
    return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

@jobs_bp.route("/jobs/recurring/<int:schedule_id>/convert_to_invoice", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def convert_recurring_schedule_to_invoice(schedule_id):
    conn = get_db_connection()
    cid = session["company_id"]

    try:
        schedule = conn.execute(
            """
            SELECT
                rms.*,
                c.name AS customer_name,
                c.email AS customer_email
            FROM recurring_mowing_schedules rms
            JOIN customers c ON rms.customer_id = c.id
            WHERE rms.id = %s AND rms.company_id = %s
            """,
            (schedule_id, cid),
        ).fetchone()

        if not schedule:
            flash("Recurring mowing schedule not found.")
            return redirect(url_for("jobs.jobs"))

        jobs = conn.execute(
            """
            SELECT
                j.*,
                (
                    SELECT COUNT(*)
                    FROM job_items ji
                    WHERE ji.job_id = j.id
                      AND COALESCE(ji.billable, 1) = 1
                ) AS billable_item_count,
                (
                    SELECT COALESCE(SUM(ji.line_total), 0)
                    FROM job_items ji
                    WHERE ji.job_id = j.id
                      AND COALESCE(ji.billable, 1) = 1
                ) AS billable_total
            FROM jobs j
            WHERE j.company_id = %s
              AND j.recurring_schedule_id = %s
              AND COALESCE(j.generated_from_schedule, FALSE) = TRUE
              AND COALESCE(j.status, '') != 'Invoiced'
            ORDER BY j.scheduled_date ASC, j.id ASC
            """,
            (cid, schedule_id),
        ).fetchall()

        if not jobs:
            flash("There are no eligible recurring jobs to invoice for this schedule.")
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        invoice_lines = []
        invoiced_job_ids = []

        for job in jobs:
            billable_total = safe_float(job["billable_total"])
            revenue_total = safe_float(job["revenue"])

            line_total = 0.0
            if billable_total > 0:
                line_total = billable_total
            elif revenue_total > 0:
                line_total = revenue_total

            if line_total <= 0:
                continue

            scheduled_date = clean_text_input(job["scheduled_date"])
            title = clean_text_input(job["title"]) or clean_text_input(schedule["title"]) or "Recurring Service"
            service_label = display_service_type(job["service_type"] or schedule["service_type"] or "mowing")

            desc_parts = [title]
            if scheduled_date:
                desc_parts.append(f"({scheduled_date})")
            if service_label:
                desc_parts.append(f"- {service_label}")

            description = " ".join(desc_parts)

            invoice_lines.append(
                {
                    "description": description.strip(),
                    "quantity": 1,
                    "unit": "visit",
                    "unit_price": line_total,
                    "line_total": line_total,
                    "job_id": job["id"],
                }
            )
            invoiced_job_ids.append(job["id"])

        if not invoice_lines:
            flash("No billable recurring job totals were found to invoice.")
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        invoice_date = date.today().isoformat()
        due_date = invoice_date
        invoice_number = f"INV-{int(datetime.now().timestamp())}"

        first_date = clean_text_input(jobs[0]["scheduled_date"]) if jobs else ""
        last_date = clean_text_input(jobs[-1]["scheduled_date"]) if jobs else ""

        schedule_title = clean_text_input(schedule["title"]) or "Recurring Mowing"
        notes_lines = [
            f"Recurring schedule invoice for Schedule #{schedule_id} - {schedule_title}"
        ]
        if first_date and last_date:
            notes_lines.append(f"Service dates: {first_date} to {last_date}")
        notes_lines.append(f"Included visits: {len(invoice_lines)}")
        invoice_notes = "\n".join(notes_lines)

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO invoices (
                company_id,
                customer_id,
                job_id,
                quote_id,
                invoice_number,
                invoice_date,
                due_date,
                status,
                notes,
                amount_paid,
                balance_due
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                cid,
                schedule["customer_id"],
                None,
                None,
                invoice_number,
                invoice_date,
                due_date,
                "Unpaid",
                invoice_notes,
                0,
                0,
            ),
        )

        row = cur.fetchone()
        if not row or "id" not in row:
            raise Exception("Failed to create invoice record.")

        invoice_id = row["id"]

        for line in invoice_lines:
            cur.execute(
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
                    line["description"],
                    line["quantity"],
                    line["unit"],
                    line["unit_price"],
                    line["line_total"],
                ),
            )

        recalc_invoice(conn, invoice_id)

        cur.execute(
            """
            UPDATE jobs
            SET status = 'Invoiced'
            WHERE company_id = %s
              AND id = ANY(%s)
            """,
            (cid, invoiced_job_ids),
        )

        conn.commit()
        flash(f"Recurring mowing schedule converted to invoice. {len(invoice_lines)} visit(s) included.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    except Exception as e:
        conn.rollback()
        flash(f"Could not convert recurring mowing schedule to invoice: {e}")
        return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

    finally:
        conn.close()


@jobs_bp.route("/jobs/export")
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def export_jobs():
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT
            j.id,
            j.title,
            j.service_type,
            j.scheduled_date,
            j.scheduled_start_time,
            j.scheduled_end_time,
            j.assigned_to,
            j.status,
            j.address,
            j.notes,
            j.revenue,
            j.cost_total,
            j.profit,
            j.recurring_schedule_id,
            j.generated_from_schedule,
            c.name AS customer_name,
            c.email AS customer_email
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.company_id = %s
        ORDER BY j.id DESC
        """,
        (cid,),
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Job ID",
        "Title",
        "Service Type",
        "Customer",
        "Customer Email",
        "Scheduled Date",
        "Start Time",
        "End Time",
        "Assigned To",
        "Status",
        "Address",
        "Revenue",
        "Costs",
        "Profit/Loss",
        "Recurring Schedule ID",
        "Generated From Schedule",
        "Notes",
    ])

    for r in rows:
        writer.writerow([
            r["id"] or "",
            clean_text_input(r["title"]),
            display_service_type(r["service_type"]),
            clean_text_input(r["customer_name"]),
            clean_text_input(r["customer_email"]),
            clean_text_input(r["scheduled_date"]),
            clean_text_input(r["scheduled_start_time"]),
            clean_text_input(r["scheduled_end_time"]),
            clean_text_input(r["assigned_to"]),
            clean_text_input(r["status"]),
            clean_text_input(r["address"]),
            safe_float(r["revenue"]),
            safe_float(r["cost_total"]),
            safe_float(r["profit"]),
            r["recurring_schedule_id"] or "",
            "Yes" if r["generated_from_schedule"] else "No",
            clean_text_input(r["notes"]),
        ])

    conn.close()

    csv_data = output.getvalue()
    output.close()

    filename = f"jobs_export_{date.today().isoformat()}.csv"

    response = make_response(csv_data)
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response

@jobs_bp.route("/jobs/<int:job_id>", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def view_job(job_id):
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    job = conn.execute(
        """
        SELECT
            j.*,
            c.name AS customer_name,
            c.email AS customer_email,
            rms.title AS recurring_title
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        LEFT JOIN recurring_mowing_schedules rms
          ON j.recurring_schedule_id = rms.id
         AND rms.company_id = j.company_id
        WHERE j.id = %s AND j.company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    if not job:
        conn.close()
        abort(404)

    if request.method == "POST":
        item_type = clean_text_input(request.form.get("item_type", "")).lower()
        description = clean_text_input(request.form.get("description", ""))
        qty = safe_float(request.form.get("quantity"))
        unit = clean_text_input(request.form.get("unit", ""))
        sale_price = safe_float(request.form.get("sale_price"))
        unit_cost = safe_float(request.form.get("unit_cost"))
        billable = 1 if request.form.get("billable") == "1" else 0

        if not description:
            conn.close()
            flash("Description is required.")
            return redirect(url_for("jobs.view_job", job_id=job_id))

        if item_type == "mulch" and not unit:
            unit = "Yards"
        elif item_type == "stone" and not unit:
            unit = "Tons"
        elif item_type == "soil" and not unit:
            unit = "Yards"
        elif item_type == "hardscape_material" and not unit:
            unit = "Tons"
        elif item_type == "fuel" and not unit:
            unit = "Gallons"
        elif item_type == "delivery" and not unit:
            unit = "Miles"
        elif item_type == "labor" and not unit:
            unit = "Hours"
        elif item_type == "equipment" and not unit:
            unit = "Rentals"
        elif item_type in ["plants", "trees", "misc", "dump_fee"]:
            unit = ""

        if item_type == "labor":
            unit_cost = 0.0

        if item_type == "dump_fee":
            unit = ""
            if qty <= 0:
                qty = 1
            unit_cost = 0.0

        line_total = qty * sale_price
        cost_amount = qty * unit_cost

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO job_items (
                job_id, item_type, description, quantity, unit,
                unit_cost, unit_price, sale_price, cost_amount, line_total, billable
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                job_id,
                item_type,
                description,
                qty,
                unit,
                unit_cost,
                sale_price,
                sale_price,
                cost_amount,
                line_total,
                billable,
            ),
        )
        row = cur.fetchone()
        job_item_id = row["id"] if row and "id" in row else None

        if not job_item_id:
            conn.rollback()
            conn.close()
            flash("Could not add job item.")
            return redirect(url_for("jobs.view_job", job_id=job_id))

        ensure_job_cost_ledger(conn, job_item_id)
        recalc_job(conn, job_id)
        conn.commit()
        conn.close()

        flash("Job item added and bookkeeping updated.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    items = conn.execute(
        "SELECT * FROM job_items WHERE job_id = %s ORDER BY id",
        (job_id,),
    ).fetchall()

    conn.close()

    item_row_list = []
    item_mobile_card_list = []
    for i in items:
        delete_item_csrf = generate_csrf()

        unit_cost_display = "-"
        if clean_text_input(i["item_type"]).lower() not in ["dump_fee", "labor"]:
            unit_cost_display = f"${((safe_float(i['cost_amount']) / safe_float(i['quantity'])) if safe_float(i['quantity']) else 0):.2f}"

        item_row_list.append(
            f"""
            <tr>
                <td>{escape(display_item_type(i['item_type']))}</td>
                <td class='wrap'>{escape(clean_text_display(i['description']))}</td>
                <td class='money'>{safe_float(i['quantity']):g}</td>
                <td>{escape(clean_text_display(i['unit']))}</td>
                <td class='money'>${safe_float(i['sale_price']):.2f}</td>
                <td class='money'>{unit_cost_display}</td>
                <td class='money'>${safe_float(i['cost_amount']):.2f}</td>
                <td class='center'>{'Yes' if i['billable'] else 'No'}</td>
                <td class='money job-items-revenue'>${safe_float(i['line_total']):.2f}</td>
                <td class='wrap'>
                    <div class='static-actions'>
                        <a class='btn secondary small' href='{url_for("jobs.edit_job_item", job_id=job_id, item_id=i["id"])}#job-items-section'>Edit</a>
                        <form method='post'
                              action='{url_for("jobs.delete_job_item", job_id=job_id, item_id=i["id"])}'
                              style='margin:0;'
                              onsubmit="saveJobsScrollPosition('job-items-section'); return confirm('Delete this job item?');">
                            <input type="hidden" name="csrf_token" value="{delete_item_csrf}">
                            <button class='btn danger small' type='submit'>Delete</button>
                        </form>
                    </div>
                </td>
            </tr>
            """
        )

        item_mobile_card_list.append(
            f"""
            <div class='mobile-list-card'>
                <div class='mobile-list-top'>
                    <div class='mobile-list-title'>{escape(display_item_type(i['item_type']))} - {escape(clean_text_display(i['description']))}</div>
                    <div class='mobile-badge'>{'Billable' if i['billable'] else 'Non-Billable'}</div>
                </div>

                <div class='mobile-list-grid'>
                    <div><span>Qty</span><strong>{safe_float(i['quantity']):g}</strong></div>
                    <div><span>Unit</span><strong>{escape(clean_text_display(i['unit']))}</strong></div>
                    <div><span>Sale Price</span><strong>${safe_float(i['sale_price']):.2f}</strong></div>
                    <div><span>Unit Cost</span><strong>{unit_cost_display}</strong></div>
                    <div><span>Total Cost</span><strong>${safe_float(i['cost_amount']):.2f}</strong></div>
                    <div><span>Revenue</span><strong>${safe_float(i['line_total']):.2f}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <a class='btn secondary small' href='{url_for("jobs.edit_job_item", job_id=job_id, item_id=i["id"])}#job-items-section'>Edit</a>
                    <form method='post'
                          action='{url_for("jobs.delete_job_item", job_id=job_id, item_id=i["id"])}'
                          style='margin:0;'
                          onsubmit="saveJobsScrollPosition('job-items-section'); return confirm('Delete this job item?');">
                        <input type="hidden" name="csrf_token" value="{delete_item_csrf}">
                        <button class='btn danger small' type='submit'>Delete</button>
                    </form>
                </div>
            </div>
            """
        )

    item_rows = "".join(item_row_list)
    item_mobile_cards = "".join(item_mobile_card_list)

    schedule_bits = []
    if clean_text_input(job["scheduled_date"]):
        schedule_bits.append(f"<strong>Date:</strong> {escape(clean_text_display(job['scheduled_date']))}")
    if clean_text_input(job["scheduled_start_time"]):
        if clean_text_input(job["scheduled_end_time"]):
            schedule_bits.append(
                f"<strong>Time:</strong> {escape(clean_text_display(job['scheduled_start_time']))} - {escape(clean_text_display(job['scheduled_end_time']))}"
            )
        else:
            schedule_bits.append(f"<strong>Start:</strong> {escape(clean_text_display(job['scheduled_start_time']))}")
    if clean_text_input(job["assigned_to"]):
        schedule_bits.append(f"<strong>Assigned To:</strong> {escape(clean_text_display(job['assigned_to']))}")

    schedule_html = "<br>".join(schedule_bits) if schedule_bits else "<strong>Schedule:</strong> -"

    customer_email = clean_text_input(job["customer_email"])
    service_type_label = display_service_type(job["service_type"])
    service_type_class = service_type_badge_class(job["service_type"])

    recurring_link_block = ""
    if job["recurring_schedule_id"]:
        recurring_link_block = f"""
        <div class='job-summary-card'>
            <span>Recurring Schedule</span>
            <strong>
                <a href='{url_for("jobs.edit_recurring_schedule", schedule_id=job["recurring_schedule_id"])}'>
                    Schedule #{job["recurring_schedule_id"]}
                </a>
            </strong>
        </div>
        """

    if customer_email:
        email_csrf_1 = generate_csrf()
        email_csrf_2 = generate_csrf()
        email_csrf_3 = generate_csrf()
        email_csrf_custom = generate_csrf()

        email_buttons = f"""
        <style>
            .updates-menu-wrap {{
                position: relative;
                display: inline-block;
            }}
            .updates-menu {{
                display: none;
                position: absolute;
                top: calc(100% + 6px);
                left: 0;
                min-width: 240px;
                background: #fff;
                border: 1px solid #d8e2d0;
                border-radius: 10px;
                box-shadow: 0 10px 24px rgba(0,0,0,0.10);
                z-index: 1000;
                overflow: hidden;
            }}
            .updates-menu form {{
                margin: 0;
            }}
            .updates-menu button {{
                width: 100%;
                text-align: left;
                background: #fff;
                border: none;
                padding: 12px 14px;
                cursor: pointer;
                font-weight: 600;
                color: #1f2933;
            }}
            .updates-menu button:hover {{
                background: #f7f7f5;
            }}
            .custom-update-card {{
                display: none;
                margin-top: 14px;
            }}
        </style>

        <div class="updates-menu-wrap">
            <button class="btn secondary" type="button" onclick="toggleUpdatesMenu(event)">Updates ▼</button>

            <div id="updatesMenu" class="updates-menu">
                <form method="post" action="{url_for("jobs.send_update_email", job_id=job_id)}">
                    <input type="hidden" name="csrf_token" value="{email_csrf_1}">
                    <input type="hidden" name="update_type" value="on_the_way">
                    <button type="submit">Send On The Way Email</button>
                </form>

                <form method="post" action="{url_for("jobs.send_update_email", job_id=job_id)}">
                    <input type="hidden" name="csrf_token" value="{email_csrf_2}">
                    <input type="hidden" name="update_type" value="job_started">
                    <button type="submit">Send Job Started Email</button>
                </form>

                <form method="post" action="{url_for("jobs.send_update_email", job_id=job_id)}">
                    <input type="hidden" name="csrf_token" value="{email_csrf_3}">
                    <input type="hidden" name="update_type" value="job_completed">
                    <button type="submit">Send Job Finished Email</button>
                </form>

                <div style="border-top:1px solid #e8ece7;"></div>

                <button type="button" onclick="toggleCustomUpdateCard()">Compose Custom Update</button>
            </div>
        </div>

        <div id="customUpdateCard" class="card custom-update-card">
            <h3>Custom Job Update</h3>

            <form method="post" action="{url_for("jobs.send_custom_email", job_id=job_id)}">
                <input type="hidden" name="csrf_token" value="{email_csrf_custom}">
                <div class="grid">
                    <div>
                        <label>To Email</label>
                        <input
                            type="email"
                            name="to_email"
                            value="{escape(customer_email)}"
                            placeholder="Enter customer email"
                            required
                        >
                    </div>

                    <div>
                        <label>Subject</label>
                        <input
                            type="text"
                            name="subject"
                            value="Job Update - {escape(clean_text_display(job['title']))}"
                            required
                        >
                    </div>
                </div>

                <div style="margin-top:14px;">
                    <label>Message</label>
                    <textarea name="message" required>Hello {escape(clean_text_display(job['customer_name']))},

This is an update regarding your job "{escape(clean_text_display(job['title']))}" ({escape(service_type_label)}).

Thank you,
{escape(session.get("company_name") or "Your Company")}</textarea>
                </div>

                <div class="row-actions" style="margin-top:12px;">
                    <button class="btn success" type="submit">Send Email</button>
                    <button class="btn secondary" type="button" onclick="toggleCustomUpdateCard()">Cancel</button>
                </div>
            </form>
        </div>
        """
    else:
        email_csrf_custom_empty = generate_csrf()
        email_buttons = """
        <div class='muted small'>Add a customer email address to send job updates.</div>
        <div id="customUpdateCard" class="card custom-update-card" style="display:block; margin-top:14px;">
            <h3>Custom Job Update</h3>
            <div class="muted small" style="margin-bottom:12px;">No customer email is on file, but you can still enter one manually below.</div>

            <form method="post" action="{send_custom_url}">
                <input type="hidden" name="csrf_token" value="{csrf_token_value}">
                <div class="grid">
                    <div>
                        <label>To Email</label>
                        <input
                            type="email"
                            name="to_email"
                            value=""
                            placeholder="Enter recipient email"
                            required
                        >
                    </div>

                    <div>
                        <label>Subject</label>
                        <input
                            type="text"
                            name="subject"
                            value="Job Update - {job_title}"
                            required
                        >
                    </div>
                </div>

                <div style="margin-top:14px;">
                    <label>Message</label>
                    <textarea name="message" required>Hello {customer_name},

This is an update regarding your job "{job_title}" ({service_type_label}).

Thank you,
{company_name}</textarea>
                </div>

                <div class="row-actions" style="margin-top:12px;">
                    <button class="btn success" type="submit">Send Email</button>
                </div>
            </form>
        </div>
        """.format(
            send_custom_url=url_for("jobs.send_custom_email", job_id=job_id),
            csrf_token_value=email_csrf_custom_empty,
            job_title=escape(clean_text_display(job["title"])),
            customer_name=escape(clean_text_display(job["customer_name"])),
            service_type_label=escape(service_type_label),
            company_name=escape(session.get("company_name") or "Your Company"),
        )

    add_item_csrf = generate_csrf()

    content = f"""
        <style>
            .service-chip {{
                display: inline-flex;
                align-items: center;
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

            .job-items-revenue {{
                font-weight: 700;
            }}

            .job-view-page {{
                display:grid;
                gap:18px;
            }}

            .job-summary-grid {{
                display:grid;
                grid-template-columns:repeat(5, minmax(0, 1fr));
                gap:12px;
                margin-top:16px;
            }}

            .job-summary-card {{
                border:1px solid rgba(15, 23, 42, 0.08);
                border-radius:12px;
                padding:12px;
                background:#fff;
            }}

            .job-summary-card span {{
                display:block;
                font-size:.8rem;
                color:#64748b;
                margin-bottom:4px;
            }}

            .job-summary-card strong {{
                display:block;
                color:#0f172a;
                line-height:1.3;
                word-break:break-word;
            }}

            .job-financials-grid {{
                display:grid;
                grid-template-columns:repeat(3, minmax(0, 1fr));
                gap:12px;
                margin-top:14px;
            }}

            .job-financial-card {{
                border:1px solid rgba(15, 23, 42, 0.08);
                border-radius:12px;
                padding:12px;
                background:#fff;
            }}

            .job-financial-card span {{
                display:block;
                font-size:.8rem;
                color:#64748b;
                margin-bottom:4px;
            }}

            .job-financial-card strong {{
                font-size:1.05rem;
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
                align-items:center;
            }}

            @media (max-width: 900px) {{
                .job-summary-grid {{
                    grid-template-columns:repeat(2, minmax(0, 1fr));
                }}

                .job-financials-grid {{
                    grid-template-columns:1fr;
                }}
            }}

            @media (max-width: 640px) {{
                .desktop-only {{
                    display:none !important;
                }}

                .mobile-only {{
                    display:block !important;
                }}

                .job-summary-grid {{
                    grid-template-columns:1fr;
                }}

                .mobile-list-grid {{
                    grid-template-columns:1fr;
                }}

                .static-actions .btn,
                .mobile-list-actions .btn,
                .btn.small {{
                    padding:8px 10px !important;
                    font-size:0.84rem !important;
                    line-height:1.2 !important;
                }}
            }}
        </style>

        <div class='job-view-page'>
            <div class='card'>
                <h1>Job #{job['id']} - {escape(clean_text_display(job['title']))}</h1>

                <div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
                    <span class='service-chip {service_type_class}'>{escape(service_type_label)}</span>
                    {'<span class="service-chip mowing">Recurring</span>' if job["recurring_schedule_id"] else ''}
                </div>

                <div class='job-summary-grid'>
                    <div class='job-summary-card'>
                        <span>Customer</span>
                        <strong>{escape(clean_text_display(job['customer_name']))}</strong>
                    </div>
                    <div class='job-summary-card'>
                        <span>Email</span>
                        <strong>{escape(clean_text_display(job['customer_email']))}</strong>
                    </div>
                    <div class='job-summary-card'>
                        <span>Status</span>
                        <strong>{escape(clean_text_display(job['status']))}</strong>
                    </div>
                    <div class='job-summary-card'>
                        <span>Service Type</span>
                        <strong>{escape(service_type_label)}</strong>
                    </div>
                    <div class='job-summary-card'>
                        <span>Schedule</span>
                        <strong>{schedule_html.replace("<br>", " | ")}</strong>
                    </div>
                    {recurring_link_block}
                </div>

                <div class='job-financials-grid'>
                    <div class='job-financial-card'>
                        <span>Revenue</span>
                        <strong>${safe_float(job['revenue']):.2f}</strong>
                    </div>
                    <div class='job-financial-card'>
                        <span>Costs</span>
                        <strong>${safe_float(job['cost_total']):.2f}</strong>
                    </div>
                    <div class='job-financial-card'>
                        <span>Profit/Loss</span>
                        <strong>${safe_float(job['profit']):.2f}</strong>
                    </div>
                </div>

                <div class="row-actions" style="margin-top:14px;">
                    <a class='btn secondary' href='{url_for("jobs.jobs")}'>Done Editing</a>
                    <a class='btn warning' href='{url_for("jobs.edit_job", job_id=job_id)}'>Edit Job</a>
                    <a class='btn success' href='{url_for("jobs.convert_job_to_invoice", job_id=job_id)}'>Convert to Invoice</a>
                </div>

                <div class="row-actions" style="margin-top:12px;">
                    {email_buttons}
                </div>
            </div>

            <div class='card' id='add-job-item-section'>
                <h2>Add Job Item</h2>
                <p class='muted'>Any cost you enter here is automatically pushed into bookkeeping as an expense.</p>

                <form method='post' onsubmit="saveJobsScrollPosition('job-items-section');">
                    <input type="hidden" name="csrf_token" value="{add_item_csrf}">
                    <div class='grid'>

                        <div>
                            <label>Type</label>
                            <select name='item_type' id='item_type' onchange='toggleJobItemMode()'>
                                <option value='mulch'>Mulch</option>
                                <option value='stone'>Stone</option>
                                <option value='dump_fee'>Dump Fee</option>
                                <option value='plants'>Plants</option>
                                <option value='trees'>Trees</option>
                                <option value='soil'>Soil</option>
                                <option value='fertilizer'>Fertilizer</option>
                                <option value='hardscape_material'>Hardscape Material</option>
                                <option value='labor'>Labor</option>
                                <option value='equipment'>Equipment</option>
                                <option value='delivery'>Delivery</option>
                                <option value='fuel'>Fuel</option>
                                <option value='misc'>Misc</option>
                            </select>
                        </div>

                        <div>
                            <label>Description</label>
                            <input name='description' required>
                        </div>

                        <div>
                            <label id='quantity_label'>Quantity</label>
                            <input type='number' step='0.01' name='quantity' id='quantity' required>
                        </div>

                        <div>
                            <label>Unit</label>
                            <input name='unit' id='unit' placeholder='Unit'>
                        </div>

                        <div id='sale_price_wrap'>
                            <label id='sale_price_label'>Sale Price</label>
                            <input type='number' step='0.01' name='sale_price' id='sale_price' value='0' required>
                        </div>

                        <div id='unit_cost_wrap'>
                            <label id='cost_label'>Unit Cost</label>
                            <input type='number' step='0.01' name='unit_cost' id='unit_cost' value='0'>
                        </div>

                        <div>
                            <label>Billable?</label>
                            <select name='billable'>
                                <option value='1'>Yes</option>
                                <option value='0'>No</option>
                            </select>
                        </div>

                    </div>

                    <br>
                    <button class='btn' type='submit'>Add Job Item</button>
                </form>
            </div>

            <script>
            function saveJobsScrollPosition(targetId) {{
                try {{
                    const target = document.getElementById(targetId);
                    const scrollY = window.scrollY || window.pageYOffset || 0;
                    sessionStorage.setItem("jobs_view_scroll_y", String(scrollY));
                    if (targetId) {{
                        sessionStorage.setItem("jobs_view_scroll_target", targetId);
                    }}
                }} catch (e) {{}}
            }}

            function restoreJobsScrollPosition() {{
                try {{
                    const savedTargetId = sessionStorage.getItem("jobs_view_scroll_target");
                    const savedY = sessionStorage.getItem("jobs_view_scroll_y");

                    if (savedTargetId) {{
                        const target = document.getElementById(savedTargetId);
                        if (target) {{
                            setTimeout(function() {{
                                target.scrollIntoView({{ behavior: "auto", block: "start" }});
                                if (savedY) {{
                                    window.scrollTo(0, parseInt(savedY, 10) || 0);
                                }}
                            }}, 40);
                        }}
                    }} else if (savedY) {{
                        setTimeout(function() {{
                            window.scrollTo(0, parseInt(savedY, 10) || 0);
                        }}, 40);
                    }}

                    sessionStorage.removeItem("jobs_view_scroll_target");
                    sessionStorage.removeItem("jobs_view_scroll_y");
                }} catch (e) {{}}
            }}

            function toggleJobItemMode() {{
                const type = document.getElementById('item_type').value;

                const quantityLabel = document.getElementById('quantity_label');
                const costLabel = document.getElementById('cost_label');
                const salePriceLabel = document.getElementById('sale_price_label');
                const salePriceWrap = document.getElementById('sale_price_wrap');
                const unitCostWrap = document.getElementById('unit_cost_wrap');
                const unitInput = document.getElementById('unit');
                const quantityInput = document.getElementById('quantity');
                const unitCostInput = document.getElementById('unit_cost');

                quantityLabel.innerText = 'Quantity';
                salePriceLabel.innerText = 'Sale Price';
                costLabel.innerText = 'Unit Cost';
                if (salePriceWrap) salePriceWrap.style.display = 'block';
                if (unitCostWrap) unitCostWrap.style.display = 'block';

                if (quantityInput) {{
                    quantityInput.readOnly = false;
                    quantityInput.step = '0.01';
                }}

                if (unitInput) unitInput.value = '';

                if (type === 'mulch') {{
                    quantityLabel.innerText = 'Yards';
                    unitInput.value = 'Yards';
                }} else if (type === 'stone') {{
                    quantityLabel.innerText = 'Tons';
                    unitInput.value = 'Tons';
                }} else if (type === 'soil') {{
                    quantityLabel.innerText = 'Yards';
                    unitInput.value = 'Yards';
                }} else if (type === 'hardscape_material') {{
                    quantityLabel.innerText = 'Tons';
                    unitInput.value = 'Tons';
                }} else if (type === 'fuel') {{
                    quantityLabel.innerText = 'Gallons';
                    unitInput.value = 'Gallons';
                }} else if (type === 'delivery') {{
                    quantityLabel.innerText = 'Miles';
                    unitInput.value = 'Miles';
                }} else if (type === 'labor') {{
                    quantityLabel.innerText = 'Billable Hours';
                    salePriceLabel.innerText = 'Hourly Rate';
                    unitInput.value = 'Hours';
                    if (unitCostWrap) unitCostWrap.style.display = 'none';
                    if (unitCostInput) unitCostInput.value = '0';
                }} else if (type === 'equipment') {{
                    quantityLabel.innerText = 'Rentals';
                    unitInput.value = 'Rentals';
                }} else if (type === 'plants' || type === 'trees' || type === 'misc') {{
                    quantityLabel.innerText = 'Quantity';
                    unitInput.value = '';
                }} else if (type === 'dump_fee') {{
                    quantityLabel.innerText = 'Fee';
                    salePriceLabel.innerText = 'Fee Amount';
                    unitInput.value = '';
                    if (unitCostWrap) unitCostWrap.style.display = 'none';
                    if (unitCostInput) unitCostInput.value = '0';
                    if (quantityInput) {{
                        quantityInput.value = '1';
                        quantityInput.readOnly = true;
                    }}
                }} else if (type === 'fertilizer') {{
                    quantityLabel.innerText = 'Quantity';
                    unitInput.value = '';
                }}
            }}

            function toggleUpdatesMenu(event) {{
                event.stopPropagation();
                const menu = document.getElementById('updatesMenu');
                if (!menu) return;
                menu.style.display = menu.style.display === 'block' ? 'none' : 'block';
            }}

            function toggleCustomUpdateCard() {{
                const card = document.getElementById('customUpdateCard');
                const menu = document.getElementById('updatesMenu');
                if (menu) menu.style.display = 'none';
                if (!card) return;
                card.style.display = card.style.display === 'block' ? 'none' : 'block';
            }}

            document.addEventListener('DOMContentLoaded', function() {{
                toggleJobItemMode();
                restoreJobsScrollPosition();
            }});

            document.addEventListener('click', function() {{
                const menu = document.getElementById('updatesMenu');
                if (menu) {{
                    menu.style.display = 'none';
                }}
            }});
            </script>

            <div class='card' id='job-items-section'>
                <h2>Job Items</h2>

                <div class='static-table-wrap desktop-only'>
                    <table class='static-table'>
                        <colgroup>
                            <col style='width:10%;'>
                            <col style='width:22%;'>
                            <col style='width:7%;'>
                            <col style='width:8%;'>
                            <col style='width:10%;'>
                            <col style='width:10%;'>
                            <col style='width:10%;'>
                            <col style='width:8%;'>
                            <col style='width:10%;'>
                            <col style='width:15%;'>
                        </colgroup>
                        <tr>
                            <th>Type</th>
                            <th class='wrap'>Description</th>
                            <th class='money'>Qty</th>
                            <th>Unit</th>
                            <th class='money'>Sale Price</th>
                            <th class='money'>Unit Cost</th>
                            <th class='money'>Total Cost</th>
                            <th class='center'>Billable</th>
                            <th class='money'>Revenue</th>
                            <th class='wrap'>Actions</th>
                        </tr>
                        {item_rows or '<tr><td colspan="10" class="muted">No job items yet.</td></tr>'}
                    </table>
                </div>

                <div class='mobile-only'>
                    <div class='mobile-list'>
                        {item_mobile_cards or "<div class='mobile-list-card'>No job items yet.</div>"}
                    </div>
                </div>
            </div>
        </div>
        """
    return render_page(content, f"Job #{job_id}")

@jobs_bp.route("/jobs/<int:job_id>/send_update_email", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def send_update_email(job_id):
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]
    uid = session.get("user_id")

    job = conn.execute(
        """
        SELECT
            j.*,
            c.name AS customer_name,
            c.email AS customer_email
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.id = %s AND j.company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    conn.close()

    if not job:
        abort(404)

    customer_email = clean_text_input(job["customer_email"])
    if not customer_email:
        flash("This customer does not have an email address.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    update_type = clean_text_input(request.form.get("update_type", ""))
    if update_type not in {"on_the_way", "job_started", "job_completed"}:
        flash("Invalid email update type.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    success, error_message = send_job_update_email(
        company_id=cid,
        customer_email=customer_email,
        job=job,
        update_type=update_type,
        user_id=uid,
    )

    if success:
        if update_type == "on_the_way":
            flash("On the way email sent.")
        elif update_type == "job_started":
            flash("Job started email sent.")
        elif update_type == "job_completed":
            flash("Job completed email sent.")
        else:
            flash("Job update email sent.")
    else:
        flash(f"Could not send email: {error_message}")

    return redirect(url_for("jobs.view_job", job_id=job_id))


@jobs_bp.route("/jobs/<int:job_id>/send_custom_email", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def send_custom_email(job_id):
    conn = get_db_connection()
    cid = session["company_id"]
    uid = session.get("user_id")

    job = conn.execute(
        """
        SELECT j.id
        FROM jobs j
        WHERE j.id = %s AND j.company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    conn.close()

    if not job:
        abort(404)

    to_email = clean_text_input(request.form.get("to_email", ""))
    subject = clean_text_input(request.form.get("subject", ""))
    message = (request.form.get("message", "") or "").strip()

    if not to_email:
        flash("Recipient email is required.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    if not subject:
        flash("Email subject is required.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    if not message:
        flash("Email message is required.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    try:
        send_company_email(
            company_id=cid,
            user_id=uid,
            to_email=to_email,
            subject=subject,
            html=message.replace("\n", "<br>"),
            body=message,
        )
        flash("Custom job update email sent.")
    except Exception as e:
        flash(f"Could not send email: {e}")

    return redirect(url_for("jobs.view_job", job_id=job_id))

@jobs_bp.route("/jobs/<int:job_id>/edit", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def edit_job(job_id):
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    job = conn.execute(
        """
        SELECT *
        FROM jobs
        WHERE id = %s AND company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    if not job:
        conn.close()
        flash("Job not found.")
        return redirect(url_for("jobs.jobs"))

    customers = conn.execute(
        "SELECT id, name FROM customers WHERE company_id = %s ORDER BY name",
        (cid,),
    ).fetchall()

    if request.method == "POST":
        customer_id = request.form.get("customer_id", type=int)
        title = clean_text_input(request.form.get("title", ""))
        service_type = normalize_service_type(request.form.get("service_type", "other"))
        scheduled_date = clean_text_input(request.form.get("scheduled_date", ""))
        scheduled_start_time = clean_text_input(request.form.get("scheduled_start_time", ""))
        scheduled_end_time = clean_text_input(request.form.get("scheduled_end_time", ""))
        assigned_to = clean_text_input(request.form.get("assigned_to", ""))
        status = clean_text_input(request.form.get("status", ""))
        address = clean_text_input(request.form.get("address", ""))
        notes = clean_text_input(request.form.get("notes", ""))

        if not customer_id or not title:
            conn.close()
            flash("Customer and title are required.")
            return redirect(url_for("jobs.edit_job", job_id=job_id))

        conflict = check_schedule_conflict(
            conn,
            cid,
            scheduled_date,
            scheduled_start_time,
            scheduled_end_time,
            assigned_to,
            exclude_job_id=job_id,
        )

        if conflict:
            conn.close()
            flash(
                f"Schedule conflict: '{conflict['title']}' already scheduled for {assigned_to} "
                f"from {conflict['start']} to {conflict['end']}."
            )
            return redirect(url_for("jobs.edit_job", job_id=job_id))

        conn.execute(
            """
            UPDATE jobs
            SET customer_id = %s,
                title = %s,
                service_type = %s,
                scheduled_date = %s,
                scheduled_start_time = %s,
                scheduled_end_time = %s,
                assigned_to = %s,
                status = %s,
                address = %s,
                notes = %s
            WHERE id = %s AND company_id = %s
            """,
            (
                customer_id,
                title,
                service_type,
                scheduled_date or None,
                scheduled_start_time or None,
                scheduled_end_time or None,
                assigned_to or None,
                status,
                address,
                notes,
                job_id,
                cid,
            ),
        )
        conn.commit()
        conn.close()

        flash("Job updated.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    customer_opts = "".join(
        f"<option value='{c['id']}' {'selected' if c['id'] == job['customer_id'] else ''}>{escape(clean_text_display(c['name'], 'Customer #' + str(c['id'])))}</option>"
        for c in customers
    )

    edit_job_csrf = generate_csrf()

    recurring_note = ""
    if job["recurring_schedule_id"]:
        recurring_note = f"""
        <div class="card" style="margin-bottom:16px;">
            <strong>This job was generated from recurring schedule #{job["recurring_schedule_id"]}.</strong><br>
            <a href="{url_for("jobs.edit_recurring_schedule", schedule_id=job["recurring_schedule_id"])}">Edit recurring schedule</a>
        </div>
        """

    content = f"""
    {recurring_note}
    <div class='card'>
        <h1>Edit Job #{job['id']}</h1>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{edit_job_csrf}">
            <div class='grid'>
                <div>
                    <label>Customer</label>
                    <select name='customer_id' required>
                        <option value=''>Select customer</option>
                        {customer_opts}
                    </select>
                </div>
                <div>
                    <label>Title</label>
                    <input name='title' value="{escape(clean_text_input(job['title']))}" required>
                </div>
                <div>
                    <label>Service Type</label>
                    <select name='service_type'>
                        {service_type_select_options(job['service_type'])}
                    </select>
                </div>
                <div>
                    <label>Scheduled Date</label>
                    <input type='date' name='scheduled_date' value="{escape(clean_text_input(job['scheduled_date']))}">
                </div>
                <div>
                    <label>Start Time</label>
                    <input type='time' name='scheduled_start_time' value="{escape(clean_text_input(job['scheduled_start_time']))}">
                </div>
                <div>
                    <label>End Time</label>
                    <input type='time' name='scheduled_end_time' value="{escape(clean_text_input(job['scheduled_end_time']))}">
                </div>
                <div>
                    <label>Assigned To</label>
                    <input name='assigned_to' value="{escape(clean_text_input(job['assigned_to']))}">
                </div>
                <div>
                    <label>Status</label>
                    <select name='status'>
                        <option {'selected' if job['status'] == 'Scheduled' else ''}>Scheduled</option>
                        <option {'selected' if job['status'] == 'In Progress' else ''}>In Progress</option>
                        <option {'selected' if job['status'] == 'Completed' else ''}>Completed</option>
                        <option {'selected' if job['status'] == 'Invoiced' else ''}>Invoiced</option>
                        <option {'selected' if job['status'] == 'Finished' else ''}>Finished</option>
                    </select>
                </div>
                <div>
                    <label>Address</label>
                    <input name='address' value="{escape(clean_text_input(job['address']))}">
                </div>
            </div>
            <br>
            <label>Notes</label>
            <textarea name='notes'>{escape(clean_text_input(job['notes']))}</textarea>
            <br>
            <button class='btn'>Save Changes</button>
            <a class='btn secondary' href='{url_for("jobs.view_job", job_id=job_id)}'>Cancel</a>
        </form>
    </div>
    """
    conn.close()
    return render_page(content, f"Edit Job #{job['id']}")

@jobs_bp.route("/jobs/<int:job_id>/items/<int:item_id>/edit", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def edit_job_item(job_id, item_id):
    conn = get_db_connection()
    cid = session["company_id"]

    job = conn.execute(
        """
        SELECT j.*, c.name AS customer_name
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.id = %s AND j.company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    if not job:
        conn.close()
        abort(404)

    item = conn.execute(
        """
        SELECT ji.*
        FROM job_items ji
        JOIN jobs j ON ji.job_id = j.id
        WHERE ji.id = %s AND ji.job_id = %s AND j.company_id = %s
        """,
        (item_id, job_id, cid),
    ).fetchone()

    if not item:
        conn.close()
        flash("Job item not found.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    if request.method == "POST":
        item_type = clean_text_input(request.form.get("item_type", "")).lower()
        description = clean_text_input(request.form.get("description", ""))
        unit = clean_text_input(request.form.get("unit", ""))
        qty = safe_float(request.form.get("quantity"))
        sale_price = safe_float(request.form.get("sale_price"))
        unit_cost = safe_float(request.form.get("unit_cost"))
        billable = 1 if request.form.get("billable") == "1" else 0

        if not description:
            conn.close()
            flash("Description is required.")
            return redirect(url_for("jobs.edit_job_item", job_id=job_id, item_id=item_id))

        if item_type == "mulch" and not unit:
            unit = "Yards"
        elif item_type == "stone" and not unit:
            unit = "Tons"
        elif item_type == "soil" and not unit:
            unit = "Yards"
        elif item_type == "hardscape_material" and not unit:
            unit = "Tons"
        elif item_type == "fuel" and not unit:
            unit = "Gallons"
        elif item_type == "delivery" and not unit:
            unit = "Miles"
        elif item_type == "labor" and not unit:
            unit = "Hours"
        elif item_type == "equipment" and not unit:
            unit = "Rentals"
        elif item_type in ["plants", "trees", "misc", "dump_fee"]:
            unit = ""

        if item_type == "labor":
            unit_cost = 0.0

        if item_type == "dump_fee":
            unit = ""
            if qty <= 0:
                qty = 1
            unit_cost = 0.0

        line_total = qty * sale_price
        cost_amount = qty * unit_cost

        conn.execute(
            """
            UPDATE job_items
            SET item_type = %s,
                description = %s,
                quantity = %s,
                unit = %s,
                unit_cost = %s,
                unit_price = %s,
                sale_price = %s,
                cost_amount = %s,
                line_total = %s,
                billable = %s
            WHERE id = %s AND job_id = %s
            """,
            (
                item_type,
                description,
                qty,
                unit,
                unit_cost,
                sale_price,
                sale_price,
                cost_amount,
                line_total,
                billable,
                item_id,
                job_id,
            ),
        )

        ensure_job_cost_ledger(conn, item_id)
        recalc_job(conn, job_id)
        conn.commit()
        conn.close()

        flash("Job item updated.")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    item_type_val = clean_text_input(item["item_type"]).lower()
    qty_val = safe_float(item["quantity"])
    sale_price_val = safe_float(item["sale_price"])
    unit_cost_val = (safe_float(item["cost_amount"]) / safe_float(item["quantity"])) if safe_float(item["quantity"]) else 0
    hide_cost = item_type_val in ["dump_fee", "labor"]
    edit_item_csrf = generate_csrf()

    content = f"""
    <div class='card'>
        <h1>Edit Job Item</h1>
        <p>
            <strong>Job:</strong> #{job['id']} - {escape(clean_text_display(job['title']))}<br>
            <strong>Customer:</strong> {escape(clean_text_display(job['customer_name']))}
        </p>

        <form method='post'>
            <input type="hidden" name="csrf_token" value="{edit_item_csrf}">
            <div class='grid'>
                <div>
                    <label>Type</label>
                    <select name='item_type' id='edit_item_type' onchange='toggleEditJobItemMode()'>
                        <option value='mulch' {'selected' if item_type_val == 'mulch' else ''}>Mulch</option>
                        <option value='stone' {'selected' if item_type_val == 'stone' else ''}>Stone</option>
                        <option value='dump_fee' {'selected' if item_type_val == 'dump_fee' else ''}>Dump Fee</option>
                        <option value='plants' {'selected' if item_type_val == 'plants' else ''}>Plants</option>
                        <option value='trees' {'selected' if item_type_val == 'trees' else ''}>Trees</option>
                        <option value='soil' {'selected' if item_type_val == 'soil' else ''}>Soil</option>
                        <option value='fertilizer' {'selected' if item_type_val == 'fertilizer' else ''}>Fertilizer</option>
                        <option value='hardscape_material' {'selected' if item_type_val == 'hardscape_material' else ''}>Hardscape Material</option>
                        <option value='labor' {'selected' if item_type_val == 'labor' else ''}>Labor</option>
                        <option value='equipment' {'selected' if item_type_val == 'equipment' else ''}>Equipment</option>
                        <option value='delivery' {'selected' if item_type_val == 'delivery' else ''}>Delivery</option>
                        <option value='fuel' {'selected' if item_type_val == 'fuel' else ''}>Fuel</option>
                        <option value='misc' {'selected' if item_type_val == 'misc' else ''}>Misc</option>
                    </select>
                </div>

                <div>
                    <label>Description</label>
                    <input name='description' value="{escape(clean_text_input(item['description']))}" required>
                </div>

                <div>
                    <label id='edit_quantity_label'>Quantity</label>
                    <input type='number' step='0.01' name='quantity' id='edit_quantity' value="{qty_val:.2f}" required>
                </div>

                <div>
                    <label>Unit</label>
                    <input name='unit' id='edit_unit' value="{escape(clean_text_input(item['unit']))}">
                </div>

                <div id='edit_sale_price_wrap'>
                    <label id='edit_sale_price_label'>Sale Price</label>
                    <input type='number' step='0.01' name='sale_price' id='edit_sale_price' value="{sale_price_val:.2f}">
                </div>

                <div id='edit_unit_cost_wrap' style="display:{'none' if hide_cost else 'block'};">
                    <label id='edit_cost_label'>Unit Cost</label>
                    <input type='number' step='0.01' name='unit_cost' id='edit_unit_cost' value="{unit_cost_val:.2f}">
                </div>

                <div>
                    <label>Billable?</label>
                    <select name='billable'>
                        <option value='1' {'selected' if item['billable'] else ''}>Yes</option>
                        <option value='0' {'selected' if not item['billable'] else ''}>No</option>
                    </select>
                </div>
            </div>

            <br>
            <button class='btn'>Save Changes</button>
            <a class='btn secondary' href='{url_for("jobs.view_job", job_id=job_id)}'>Cancel</a>
        </form>
    </div>

    <script>
    function toggleEditJobItemMode() {{
        const type = document.getElementById('edit_item_type').value;

        const quantityLabel = document.getElementById('edit_quantity_label');
        const costLabel = document.getElementById('edit_cost_label');
        const salePriceLabel = document.getElementById('edit_sale_price_label');
        const salePriceWrap = document.getElementById('edit_sale_price_wrap');
        const unitCostWrap = document.getElementById('edit_unit_cost_wrap');
        const unitInput = document.getElementById('edit_unit');
        const quantityInput = document.getElementById('edit_quantity');
        const unitCostInput = document.getElementById('edit_unit_cost');

        quantityLabel.innerText = 'Quantity';
        salePriceLabel.innerText = 'Sale Price';
        costLabel.innerText = 'Unit Cost';
        if (salePriceWrap) salePriceWrap.style.display = 'block';
        if (unitCostWrap) unitCostWrap.style.display = 'block';

        if (quantityInput) {{
            quantityInput.readOnly = false;
            quantityInput.step = '0.01';
        }}

        if (unitInput) unitInput.value = '';

        if (type === 'mulch') {{
            quantityLabel.innerText = 'Yards';
            unitInput.value = 'Yards';
        }} else if (type === 'stone') {{
            quantityLabel.innerText = 'Tons';
            unitInput.value = 'Tons';
        }} else if (type === 'soil') {{
            quantityLabel.innerText = 'Yards';
            unitInput.value = 'Yards';
        }} else if (type === 'hardscape_material') {{
            quantityLabel.innerText = 'Tons';
            unitInput.value = 'Tons';
        }} else if (type === 'fuel') {{
            quantityLabel.innerText = 'Gallons';
            unitInput.value = 'Gallons';
        }} else if (type === 'delivery') {{
            quantityLabel.innerText = 'Miles';
            unitInput.value = 'Miles';
        }} else if (type === 'labor') {{
            quantityLabel.innerText = 'Billable Hours';
            salePriceLabel.innerText = 'Hourly Rate';
            unitInput.value = 'Hours';
            if (unitCostWrap) unitCostWrap.style.display = 'none';
            if (unitCostInput) unitCostInput.value = '0';
        }} else if (type === 'equipment') {{
            quantityLabel.innerText = 'Rentals';
            unitInput.value = 'Rentals';
        }} else if (type === 'plants' || type === 'trees' || type === 'misc') {{
            quantityLabel.innerText = 'Quantity';
            unitInput.value = '';
        }} else if (type === 'dump_fee') {{
            quantityLabel.innerText = 'Fee';
            salePriceLabel.innerText = 'Fee Amount';
            unitInput.value = '';
            if (unitCostWrap) unitCostWrap.style.display = 'none';
            if (unitCostInput) unitCostInput.value = '0';
            if (quantityInput) {{
                quantityInput.value = '1';
                quantityInput.readOnly = true;
            }}
        }} else if (type === 'fertilizer') {{
            quantityLabel.innerText = 'Quantity';
            unitInput.value = '';
        }}
    }}

    document.addEventListener('DOMContentLoaded', function() {{
        toggleEditJobItemMode();
    }});
    </script>
    """

    conn.close()
    return render_page(content, f"Edit Job Item #{item_id}")

@jobs_bp.route("/jobs/<int:job_id>/items/<int:item_id>/delete", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def delete_job_item(job_id, item_id):
    conn = get_db_connection()

    item = conn.execute(
        """
        SELECT ji.*, j.company_id
        FROM job_items ji
        JOIN jobs j ON ji.job_id = j.id
        WHERE ji.id = %s AND ji.job_id = %s AND j.company_id = %s
        """,
        (item_id, job_id, session["company_id"]),
    ).fetchone()

    if not item:
        conn.close()
        abort(404)

    if "ledger_entry_id" in item.keys() and item["ledger_entry_id"]:
        conn.execute("DELETE FROM ledger_entries WHERE id = %s", (item["ledger_entry_id"],))

    conn.execute("DELETE FROM job_items WHERE id = %s", (item_id,))
    recalc_job(conn, job_id)
    conn.commit()
    conn.close()
    flash("Job item deleted.")
    return redirect(url_for("jobs.view_job", job_id=job_id))


@jobs_bp.route("/jobs/<int:job_id>/convert_to_invoice")
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def convert_job_to_invoice(job_id):
    conn = get_db_connection()
    cid = session["company_id"]

    try:
        job = conn.execute(
            """
            SELECT *
            FROM jobs
            WHERE id = %s AND company_id = %s
            """,
            (job_id, cid),
        ).fetchone()

        if not job:
            abort(404)

        existing_invoice = conn.execute(
            """
            SELECT id
            FROM invoices
            WHERE job_id = %s AND company_id = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (job_id, cid),
        ).fetchone()

        if existing_invoice:
            flash("This job has already been converted to an invoice.")
            return redirect(url_for("invoices.view_invoice", invoice_id=existing_invoice["id"]))

        items = conn.execute(
            """
            SELECT *
            FROM job_items
            WHERE job_id = %s AND COALESCE(billable, 1) = 1
            ORDER BY id
            """,
            (job_id,),
        ).fetchall()

        if not items:
            flash("This job has no billable items to invoice.")
            return redirect(url_for("jobs.view_job", job_id=job_id))

        invoice_date = date.today().isoformat()
        due_date = invoice_date
        notes = clean_text_input(job["notes"]) if "notes" in job.keys() and job["notes"] else ""
        invoice_number = f"INV-{int(datetime.now().timestamp())}"

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO invoices (
                company_id,
                customer_id,
                job_id,
                quote_id,
                invoice_number,
                invoice_date,
                due_date,
                status,
                notes,
                amount_paid,
                balance_due
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                job["company_id"],
                job["customer_id"],
                job_id,
                job["quote_id"] if "quote_id" in job.keys() else None,
                invoice_number,
                invoice_date,
                due_date,
                "Unpaid",
                notes,
                0,
                0,
            ),
        )

        row = cur.fetchone()
        if not row or "id" not in row:
            raise Exception("Failed to create invoice record.")

        invoice_id = row["id"]

        for i in items:
            description = clean_text_input(i["description"]) if i["description"] else ""
            quantity = safe_float(i["quantity"])
            unit = clean_text_input(i["unit"]) if i["unit"] else ""
            unit_price = safe_float(i["sale_price"] if i["sale_price"] is not None else i["unit_price"])
            line_total = safe_float(i["line_total"])

            cur.execute(
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
                    description,
                    quantity,
                    unit,
                    unit_price,
                    line_total,
                ),
            )

        recalc_invoice(conn, invoice_id)

        cur.execute(
            """
            UPDATE jobs
            SET status = %s
            WHERE id = %s AND company_id = %s
            """,
            ("Invoiced", job_id, cid),
        )

        conn.commit()
        flash("Job converted to invoice.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    except Exception as e:
        conn.rollback()
        flash(f"Could not convert job to invoice: {e}")
        return redirect(url_for("jobs.view_job", job_id=job_id))

    finally:
        conn.close()


@jobs_bp.route("/jobs/<int:job_id>/delete", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def delete_job(job_id):
    conn = get_db_connection()
    job = conn.execute(
        "SELECT id FROM jobs WHERE id = %s AND company_id = %s",
        (job_id, session["company_id"]),
    ).fetchone()

    if not job:
        conn.close()
        abort(404)

    ledger_ids = conn.execute(
        "SELECT ledger_entry_id FROM job_items WHERE job_id = %s AND ledger_entry_id IS NOT NULL",
        (job_id,),
    ).fetchall()

    for row in ledger_ids:
        conn.execute("DELETE FROM ledger_entries WHERE id = %s", (row["ledger_entry_id"],))

    conn.execute("DELETE FROM job_items WHERE job_id = %s", (job_id,))
    conn.execute("DELETE FROM jobs WHERE id = %s AND company_id = %s", (job_id, session["company_id"]))
    conn.commit()
    conn.close()
    flash("Job deleted.")
    return redirect(url_for("jobs.jobs"))

@jobs_bp.route("/jobs/finished")
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def finished_jobs():
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT j.*, c.name AS customer_name
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.company_id = %s
          AND j.status = 'Finished'
        ORDER BY
            j.scheduled_date NULLS LAST,
            j.scheduled_start_time NULLS LAST,
            j.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    table_rows = []
    mobile_cards = []

    for r in rows:
        service_type_label = display_service_type(r["service_type"])
        service_type_class = service_type_badge_class(r["service_type"])

        recurring_note = ""
        if r["recurring_schedule_id"]:
            recurring_note = f"<div class='small muted' style='margin-top:4px;'>Recurring: Schedule #{r['recurring_schedule_id']}</div>"

        table_rows.append(
            f"""
            <tr>
                <td>#{r['id']}</td>
                <td class='wrap'>{escape(clean_text_display(r['title']))}{recurring_note}</td>
                <td><span class='service-chip {service_type_class}'>{escape(service_type_label)}</span></td>
                <td class='wrap'>{escape(clean_text_display(r['customer_name']))}</td>
                <td>{escape(clean_text_display(r['scheduled_date']))}</td>
                <td>{escape(clean_text_display(r['scheduled_start_time']))}</td>
                <td>{escape(clean_text_display(r['scheduled_end_time']))}</td>
                <td class='wrap'>{escape(clean_text_display(r['assigned_to']))}</td>
                <td>{escape(clean_text_display(r['status']))}</td>
                <td class='money'>${safe_float(r['revenue']):.2f}</td>
                <td class='money'>${safe_float(r['cost_total']):.2f}</td>
                <td class='money jobs-profit'>${safe_float(r['profit']):.2f}</td>
                <td class='wrap'>
                    <div class='static-actions'>
                        <a class='btn secondary small' href='{url_for("jobs.view_job", job_id=r["id"])}'>View</a>
                        <a class='btn warning small' href='{url_for("jobs.reopen_job", job_id=r["id"])}'>Reopen</a>
                    </div>
                </td>
            </tr>
            """
        )

        mobile_cards.append(
            f"""
            <div class='mobile-list-card'>
                <div class='mobile-list-top'>
                    <div class='mobile-list-title'>#{r['id']} - {escape(clean_text_display(r['title']))}</div>
                    <div class='mobile-badge'>{escape(clean_text_display(r['status']))}</div>
                </div>

                <div style='margin:-2px 0 10px 0; display:flex; gap:8px; flex-wrap:wrap;'>
                    <span class='service-chip {service_type_class}'>{escape(service_type_label)}</span>
                    {'<span class="service-chip mowing">Recurring</span>' if r["recurring_schedule_id"] else ''}
                </div>

                <div class='mobile-list-grid'>
                    <div><span>Customer</span><strong>{escape(clean_text_display(r['customer_name']))}</strong></div>
                    <div><span>Date</span><strong>{escape(clean_text_display(r['scheduled_date']))}</strong></div>
                    <div><span>Start</span><strong>{escape(clean_text_display(r['scheduled_start_time']))}</strong></div>
                    <div><span>End</span><strong>{escape(clean_text_display(r['scheduled_end_time']))}</strong></div>
                    <div><span>Assigned To</span><strong>{escape(clean_text_display(r['assigned_to']))}</strong></div>
                    <div><span>Revenue</span><strong>${safe_float(r['revenue']):.2f}</strong></div>
                    <div><span>Costs</span><strong>${safe_float(r['cost_total']):.2f}</strong></div>
                    <div><span>Profit/Loss</span><strong>${safe_float(r['profit']):.2f}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <a class='btn secondary small' href='{url_for("jobs.view_job", job_id=r["id"])}'>View</a>
                    <a class='btn warning small' href='{url_for("jobs.reopen_job", job_id=r["id"])}'>Reopen</a>
                </div>
            </div>
            """
        )

    job_rows = "".join(table_rows)
    mobile_cards_html = "".join(mobile_cards)

    content = f"""
    <style>
        .service-chip {{
            display: inline-flex;
            align-items: center;
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

        .jobs-profit {{
            font-weight: 700;
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

    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin:0;'>Finished Jobs</h1>
                <p class='muted' style='margin:6px 0 0 0;'>Completed and fully paid jobs.</p>
            </div>
            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("jobs.jobs")}'>Back to Active Jobs</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <div class='static-table-wrap desktop-only'>
            <table class='static-table'>
                <colgroup>
                    <col style='width:6%;'>
                    <col style='width:13%;'>
                    <col style='width:9%;'>
                    <col style='width:13%;'>
                    <col style='width:9%;'>
                    <col style='width:7%;'>
                    <col style='width:7%;'>
                    <col style='width:10%;'>
                    <col style='width:9%;'>
                    <col style='width:7%;'>
                    <col style='width:7%;'>
                    <col style='width:9%;'>
                    <col style='width:19%;'>
                </colgroup>
                <tr>
                    <th>ID</th>
                    <th class='wrap'>Title</th>
                    <th>Service</th>
                    <th class='wrap'>Customer</th>
                    <th>Date</th>
                    <th>Start</th>
                    <th>End</th>
                    <th class='wrap'>Assigned To</th>
                    <th>Status</th>
                    <th class='money'>Revenue</th>
                    <th class='money'>Costs</th>
                    <th class='money'>Profit/Loss</th>
                    <th class='wrap'>Actions</th>
                </tr>
                {job_rows or '<tr><td colspan="13" class="muted">No finished jobs yet.</td></tr>'}
            </table>
        </div>

        <div class='mobile-only'>
            <div class='mobile-list'>
                {mobile_cards_html or "<div class='mobile-list-card'>No finished jobs yet.</div>"}
            </div>
        </div>
    </div>
    """
    return render_page(content, "Finished Jobs")


@jobs_bp.route("/jobs/<int:job_id>/reopen")
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def reopen_job(job_id):
    conn = get_db_connection()
    cid = session["company_id"]

    job = conn.execute(
        """
        SELECT id
        FROM jobs
        WHERE id = %s AND company_id = %s
        """,
        (job_id, cid),
    ).fetchone()

    if not job:
        conn.close()
        flash("Job not found.")
        return redirect(url_for("jobs.finished_jobs"))

    conn.execute(
        """
        UPDATE jobs
        SET status = 'Invoiced'
        WHERE id = %s AND company_id = %s
        """,
        (job_id, cid),
    )

    conn.commit()
    conn.close()

    flash("Job reopened.")
    return redirect(url_for("jobs.view_job", job_id=job_id))