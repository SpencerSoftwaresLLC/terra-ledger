from flask import Blueprint, request, redirect, url_for, session, flash, make_response, abort
from flask_wtf.csrf import generate_csrf
from datetime import date, datetime, timedelta
from html import escape
import json
import io
import csv

from db import get_db_connection, ensure_job_cost_ledger, table_columns
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page
from helpers import *
from calculations import recalc_job, recalc_invoice, recalc_all_recurring_jobs
from utils.emailing import send_company_email
from utils.recurring import auto_generate_recurring_jobs

jobs_bp = Blueprint("jobs", __name__)


def _is_es():
    return str(session.get("language") or "en").strip().lower() == "es"


def _t(en_text, es_text):
    return es_text if _is_es() else en_text


ITEM_TYPE_LABELS = {
    "mulch": {"en": "Mulch", "es": "Mantillo"},
    "stone": {"en": "Stone", "es": "Piedra"},
    "dump_fee": {"en": "Dump Fee", "es": "Tarifa de vertedero"},
    "plants": {"en": "Plants", "es": "Plantas"},
    "trees": {"en": "Trees", "es": "Árboles"},
    "soil": {"en": "Soil", "es": "Tierra"},
    "fertilizer": {"en": "Fertilizer", "es": "Fertilizante"},
    "hardscape_material": {"en": "Hardscape Material", "es": "Material de paisajismo duro"},
    "labor": {"en": "Labor", "es": "Mano de obra"},
    "equipment": {"en": "Equipment", "es": "Equipo"},
    "delivery": {"en": "Delivery", "es": "Entrega"},
    "fuel": {"en": "Fuel", "es": "Combustible"},
    "misc": {"en": "Misc", "es": "Varios"},
    "material": {"en": "Material", "es": "Material"},
}

JOB_SERVICE_TYPE_LABELS = {
    "mowing": {"en": "Mowing", "es": "Corte de césped"},
    "mulch": {"en": "Mulch", "es": "Mantillo"},
    "cleanup": {"en": "Cleanup", "es": "Limpieza"},
    "installation": {"en": "Installation", "es": "Instalación"},
    "hardscape": {"en": "Hardscape", "es": "Paisajismo duro"},
    "snow_removal": {"en": "Snow Removal", "es": "Remoción de nieve"},
    "fertilizing": {"en": "Fertilizing", "es": "Fertilización"},
    "other": {"en": "Other", "es": "Otro"},
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

        try:
            cur.execute(
                """
                ALTER TABLE jobs
                ALTER COLUMN generated_from_schedule TYPE BOOLEAN
                USING CASE
                    WHEN generated_from_schedule IN (1, '1', TRUE, 'true', 't', 'yes', 'on') THEN TRUE
                    ELSE FALSE
                END
                """
            )
        except Exception:
            conn.rollback()
            cur = conn.cursor()
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

        try:
            cur.execute(
                """
                ALTER TABLE recurring_mowing_schedule_items
                ALTER COLUMN billable TYPE BOOLEAN
                USING CASE
                    WHEN billable IN (1, '1', TRUE, 'true', 't', 'yes', 'on') THEN TRUE
                    ELSE FALSE
                END
                """
            )
        except Exception:
            conn.rollback()
            cur = conn.cursor()

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

        try:
            cur.execute(
                """
                ALTER TABLE recurring_mowing_schedules
                ALTER COLUMN active TYPE BOOLEAN
                USING CASE
                    WHEN active IN (1, '1', TRUE, 'true', 't', 'yes', 'on') THEN TRUE
                    ELSE FALSE
                END
                """
            )
        except Exception:
            conn.rollback()
            cur = conn.cursor()

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
    label = JOB_SERVICE_TYPE_LABELS.get(key, JOB_SERVICE_TYPE_LABELS["other"])
    return label["es"] if _is_es() else label["en"]


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
        display_label = label["es"] if _is_es() else label["en"]
        options.append(f"<option value='{key}'{selected_attr}>{escape(display_label)}</option>")
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
        return _t("Weekly", "Semanal")
    if weeks == 2:
        return _t("Every 2 Weeks", "Cada 2 semanas")
    return _t(f"Every {weeks} Weeks", f"Cada {weeks} semanas")


def schedule_status_badge(active):
    return _t("Active", "Activo") if active else _t("Paused", "Pausado")


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
    return title or _t("Recurring Mowing", "Corte recurrente de césped")


def default_mowing_status(value="Scheduled"):
    text = clean_text_input(value)
    return text or _t("Scheduled", "Programado")


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
        label = ITEM_TYPE_LABELS[key]
        return label["es"] if _is_es() else label["en"]
    return key.replace("_", " ").title() if key else _t("Material", "Material")


def _safe_float(value, default=0.0):
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _table_exists(conn, table_name):
    try:
        return len(table_columns(conn, table_name)) > 0
    except Exception:
        return False


def default_unit_for_item_type(item_type):
    key = clean_text_input(item_type).lower()

    if key == "mulch":
        return _t("Yards", "Yardas")
    if key == "stone":
        return _t("Tons", "Toneladas")
    if key == "soil":
        return _t("Yards", "Yardas")
    if key == "fertilizer":
        return _t("Bags", "Bolsas")
    if key == "hardscape_material":
        return _t("Tons", "Toneladas")
    if key == "plants":
        return "EA"
    if key == "trees":
        return "EA"
    if key == "labor":
        return "hr"
    if key == "dump_fee":
        return _t("fee", "tarifa")

    return ""


def build_job_update_email(job, update_type):
    company_name = clean_text_input(session.get("company_name")) or "TerraLedger"
    customer_name = clean_text_input(job.get("customer_name")) or _t("Customer", "Cliente")
    job_title = clean_text_input(job.get("title")) or _t("your scheduled job", "tu trabajo programado")
    scheduled_date = clean_text_input(job.get("scheduled_date"))
    start_time = clean_text_input(job.get("scheduled_start_time"))
    end_time = clean_text_input(job.get("scheduled_end_time"))
    address = clean_text_input(job.get("address"))
    assigned_to = clean_text_input(job.get("assigned_to"))
    service_type_label = display_service_type(job.get("service_type"))

    schedule_line = ""
    if scheduled_date and start_time and end_time:
        schedule_line = _t(
            f"{scheduled_date} from {start_time} to {end_time}",
            f"{scheduled_date} de {start_time} a {end_time}",
        )
    elif scheduled_date and start_time:
        schedule_line = _t(
            f"{scheduled_date} at {start_time}",
            f"{scheduled_date} a las {start_time}",
        )
    elif scheduled_date:
        schedule_line = scheduled_date

    if update_type == "on_the_way":
        subject = _t(f"{company_name}: We are on the way", f"{company_name}: Vamos en camino")
        intro = _t(
            f"Hello {customer_name},<br><br>We are on the way for <strong>{escape(job_title)}</strong>.",
            f"Hola {customer_name},<br><br>Vamos en camino para <strong>{escape(job_title)}</strong>.",
        )
    elif update_type == "job_started":
        subject = _t(f"{company_name}: Job started", f"{company_name}: Trabajo iniciado")
        intro = _t(
            f"Hello {customer_name},<br><br>We have started <strong>{escape(job_title)}</strong>.",
            f"Hola {customer_name},<br><br>Hemos comenzado <strong>{escape(job_title)}</strong>.",
        )
    elif update_type == "job_completed":
        subject = _t(f"{company_name}: Job completed", f"{company_name}: Trabajo completado")
        intro = _t(
            f"Hello {customer_name},<br><br>Your job <strong>{escape(job_title)}</strong> has been completed.",
            f"Hola {customer_name},<br><br>Tu trabajo <strong>{escape(job_title)}</strong> ha sido completado.",
        )
    else:
        subject = _t(f"{company_name}: Job update", f"{company_name}: Actualización del trabajo")
        intro = _t(
            f"Hello {customer_name},<br><br>Here is an update for <strong>{escape(job_title)}</strong>.",
            f"Hola {customer_name},<br><br>Aquí tienes una actualización para <strong>{escape(job_title)}</strong>.",
        )

    details = []
    if service_type_label:
        details.append(f"<strong>{escape(_t('Service Type:', 'Tipo de servicio:'))}</strong> {escape(service_type_label)}")
    if schedule_line:
        details.append(f"<strong>{escape(_t('Scheduled:', 'Programado:'))}</strong> {escape(schedule_line)}")
    if address:
        details.append(f"<strong>{escape(_t('Address:', 'Dirección:'))}</strong> {escape(address)}")
    if assigned_to:
        details.append(f"<strong>{escape(_t('Assigned To:', 'Asignado a:'))}</strong> {escape(assigned_to)}")

    details_html = "<br>".join(details)

    html_body = f"""
    <div style="font-family: Arial, sans-serif; color: #1f2933; line-height: 1.5;">
        {intro}
        {'<br><br>' + details_html if details_html else ''}
        <br><br>
        {escape(_t('Thank you,', 'Gracias,'))}<br>
        {escape(company_name)}
    </div>
    """

    text_parts = [
        _t(f"Hello {customer_name},", f"Hola {customer_name},"),
        "",
    ]

    if update_type == "on_the_way":
        text_parts.append(_t(f"We are on the way for {job_title}.", f"Vamos en camino para {job_title}."))
    elif update_type == "job_started":
        text_parts.append(_t(f"We have started {job_title}.", f"Hemos comenzado {job_title}."))
    elif update_type == "job_completed":
        text_parts.append(_t(f"Your job {job_title} has been completed.", f"Tu trabajo {job_title} ha sido completado."))
    else:
        text_parts.append(_t(f"Here is an update for {job_title}.", f"Aquí tienes una actualización para {job_title}."))

    if service_type_label:
        text_parts.append(_t(f"Service Type: {service_type_label}", f"Tipo de servicio: {service_type_label}"))
    if schedule_line:
        text_parts.append(_t(f"Scheduled: {schedule_line}", f"Programado: {schedule_line}"))
    if address:
        text_parts.append(_t(f"Address: {address}", f"Dirección: {address}"))
    if assigned_to:
        text_parts.append(_t(f"Assigned To: {assigned_to}", f"Asignado a: {assigned_to}"))

    text_parts.extend([
        "",
        _t("Thank you,", "Gracias,"),
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
    return title or _t("Recurring Mowing", "Corte recurrente de césped")


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
        billable = True if item["billable"] else False

        if not description:
            continue

        if item_type == "mulch" and not unit:
            unit = _t("Yards", "Yardas")
        elif item_type == "stone" and not unit:
            unit = _t("Tons", "Toneladas")
        elif item_type == "soil" and not unit:
            unit = _t("Yards", "Yardas")
        elif item_type == "hardscape_material" and not unit:
            unit = _t("Tons", "Toneladas")
        elif item_type == "fuel" and not unit:
            unit = _t("Gallons", "Galones")
        elif item_type == "delivery" and not unit:
            unit = _t("Miles", "Millas")
        elif item_type == "labor" and not unit:
            unit = _t("Hours", "Horas")
        elif item_type == "equipment" and not unit:
            unit = _t("Rentals", "Alquileres")
        elif item_type in ["plants", "trees", "misc", "dump_fee"]:
            unit = ""

        if item_type == "dump_fee":
            unit = ""
            if qty <= 0:
                qty = 1
            unit_cost = 0.0

        line_total = qty * sale_price if billable else 0.0
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
    schedule_note = _t(
        f"Auto-generated from recurring mowing schedule #{schedule_row['id']}.",
        f"Generado automáticamente desde el programa recurrente de corte #{schedule_row['id']}.",
    )
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
        unit_price = safe_float(item["sale_price"], 0)
        unit_cost = safe_float(item["unit_cost"], 0)
        billable_value = True if item["billable"] else False

        if qty <= 0:
            qty = 1.0

        if item_type == "mulch" and not unit:
            unit = _t("Yards", "Yardas")
        elif item_type == "stone" and not unit:
            unit = _t("Tons", "Toneladas")
        elif item_type == "soil" and not unit:
            unit = _t("Yards", "Yardas")
        elif item_type == "hardscape_material" and not unit:
            unit = _t("Tons", "Toneladas")
        elif item_type == "fuel" and not unit:
            unit = _t("Gallons", "Galones")
        elif item_type == "delivery" and not unit:
            unit = _t("Miles", "Millas")
        elif item_type == "labor" and not unit:
            unit = _t("Hours", "Horas")
        elif item_type == "equipment" and not unit:
            unit = _t("Rentals", "Alquileres")
        elif item_type == "fertilizer" and not unit:
            unit = _t("Bags", "Bolsas")
        elif item_type in ["plants", "trees", "misc", "dump_fee"]:
            unit = ""

        if item_type == "dump_fee":
            unit = ""
            qty = 1.0

        line_total = qty * unit_price if billable_value else 0.0
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
                unit_price,
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
        recalc_all_recurring_jobs(conn, cid)
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
            flash(_t("Please select a customer from the search results.", "Selecciona un cliente de los resultados de búsqueda."))
            return redirect(url_for("jobs.jobs"))

        title = clean_text_input(request.form.get("title", ""))
        service_type = normalize_service_type(request.form.get("service_type", "other"))
        scheduled_date = clean_text_input(request.form.get("scheduled_date", ""))
        scheduled_start_time = clean_text_input(request.form.get("scheduled_start_time", ""))
        scheduled_end_time = clean_text_input(request.form.get("scheduled_end_time", ""))
        assigned_to = clean_text_input(request.form.get("assigned_to", ""))
        status = clean_text_input(request.form.get("status", _t("Scheduled", "Programado"))) or _t("Scheduled", "Programado")
        address = clean_text_input(request.form.get("address", ""))
        notes = clean_text_input(request.form.get("notes", ""))

        if not title:
            conn.close()
            flash(_t("Job title is required.", "El título del trabajo es obligatorio."))
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
                _t(
                    f"Schedule conflict: '{conflict['title']}' is already scheduled for {assigned_to} from {conflict['start']} to {conflict['end']}.",
                    f"Conflicto de horario: '{conflict['title']}' ya está programado para {assigned_to} de {conflict['start']} a {conflict['end']}.",
                )
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
            flash(_t("Could not create job.", "No se pudo crear el trabajo."))
            return redirect(url_for("jobs.jobs"))

        flash(_t("Job created.", "Trabajo creado."))
        return redirect(url_for("jobs.view_job", job_id=job_id))

    rows = conn.execute(
        """
        SELECT
            j.*,
            c.name AS customer_name,
            rms.title AS recurring_title,
            i.id AS invoice_id,
            i.invoice_number
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        LEFT JOIN recurring_mowing_schedules rms
          ON j.recurring_schedule_id = rms.id
         AND rms.company_id = j.company_id
        LEFT JOIN invoices i
          ON i.job_id = j.id
         AND i.company_id = j.company_id
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

    today = date.today().isoformat()

    recurring_rows = conn.execute(
        """
        SELECT
            rms.*,
            c.name AS customer_name,
            COALESCE(stats.generated_jobs_count, 0) AS generated_jobs_count,
            COALESCE(stats.active_jobs_count, 0) AS active_jobs_count,
            COALESCE(stats.total_revenue, 0) AS total_revenue,
            COALESCE(stats.total_cost, 0) AS total_cost,
            COALESCE(stats.total_profit, 0) AS total_profit,
            (
                SELECT j2.scheduled_date
                FROM jobs j2
                WHERE j2.company_id = rms.company_id
                  AND j2.recurring_schedule_id = rms.id
                  AND j2.scheduled_date IS NOT NULL
                  AND j2.scheduled_date >= %s
                ORDER BY j2.scheduled_date ASC, j2.id ASC
                LIMIT 1
            ) AS computed_next_run
        FROM recurring_mowing_schedules rms
        JOIN customers c
          ON rms.customer_id = c.id
        LEFT JOIN (
            SELECT
                j.company_id,
                j.recurring_schedule_id,
                COUNT(DISTINCT j.id) AS generated_jobs_count,
                COUNT(DISTINCT CASE WHEN COALESCE(j.status, '') != 'Finished' THEN j.id END) AS active_jobs_count,
                COALESCE(SUM(COALESCE(j.revenue, 0)), 0) AS total_revenue,
                COALESCE(SUM(COALESCE(j.cost_total, 0)), 0) AS total_cost,
                COALESCE(SUM(COALESCE(j.profit, 0)), 0) AS total_profit
            FROM jobs j
            WHERE j.recurring_schedule_id IS NOT NULL
            GROUP BY j.company_id, j.recurring_schedule_id
        ) stats
          ON stats.company_id = rms.company_id
         AND stats.recurring_schedule_id = rms.id
        WHERE rms.company_id = %s
        ORDER BY COALESCE(rms.active, TRUE) DESC, rms.id DESC
        """,
        (today, cid),
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
                f"{escape(_t('Recurring:', 'Recurrente:'))} "
                f"<a href='/jobs/recurring/{r['recurring_schedule_id']}/edit'>"
                f"{escape(_t('Schedule', 'Programa'))} #{r['recurring_schedule_id']}</a></div>"
            )

        invoice_action_html = ""
        if r["invoice_id"]:
            invoice_label = clean_text_input(r["invoice_number"]) or f"{_t('Invoice', 'Factura')} #{r['invoice_id']}"
            invoice_action_html = (
                f"<a class='btn secondary small' href='{url_for('invoices.view_invoice', invoice_id=r['invoice_id'])}'>"
                f"{escape(_t('View', 'Ver'))} {escape(invoice_label)}</a>"
            )
        else:
            invoice_action_html = (
                f"<a class='btn success small' href='{url_for('jobs.convert_job_to_invoice', job_id=r['id'])}'>"
                f"{escape(_t('Convert to Invoice', 'Convertir en factura'))}</a>"
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
                        <a class='btn secondary small' href='{url_for("jobs.view_job", job_id=r["id"])}'>{_t("View", "Ver")}</a>
                        <a class='btn warning small' href='{url_for("jobs.edit_job", job_id=r["id"])}'>{_t("Edit Job", "Editar trabajo")}</a>
                        {invoice_action_html}
                        <form method='post'
                              action='{url_for("jobs.delete_job", job_id=r["id"])}'
                              style='margin:0;'
                              onsubmit="return confirm('{_t("Delete this job and all items?", "¿Eliminar este trabajo y todos sus artículos?")}');">
                            <input type="hidden" name="csrf_token" value="{delete_csrf}">
                            <button class='btn danger small' type='submit'>{_t("Delete Job", "Eliminar trabajo")}</button>
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
                f"{escape(_t('Recurring:', 'Recurrente:'))} "
                f"<a href='/jobs/recurring/{r['recurring_schedule_id']}/edit'>"
                f"{escape(_t('Schedule', 'Programa'))} #{r['recurring_schedule_id']}</a></div>"
            )

        mobile_invoice_action_html = ""
        if r["invoice_id"]:
            invoice_label = clean_text_input(r["invoice_number"]) or f"{_t('Invoice', 'Factura')} #{r['invoice_id']}"
            mobile_invoice_action_html = (
                f"<a class='btn secondary small' href='{url_for('invoices.view_invoice', invoice_id=r['invoice_id'])}'>"
                f"{escape(_t('View', 'Ver'))} {escape(invoice_label)}</a>"
            )
        else:
            mobile_invoice_action_html = (
                f"<a class='btn success small' href='{url_for('jobs.convert_job_to_invoice', job_id=r['id'])}'>"
                f"{escape(_t('Convert to Invoice', 'Convertir en factura'))}</a>"
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
                    {'<span class="service-chip mowing">' + escape(_t("Recurring", "Recurrente")) + '</span>' if r["recurring_schedule_id"] else ''}
                </div>

                <div class='mobile-list-grid'>
                    <div><span>{_t("Customer", "Cliente")}</span><strong>{escape(clean_text_display(r['customer_name']))}</strong></div>
                    <div><span>{_t("Date", "Fecha")}</span><strong>{escape(clean_text_display(r['scheduled_date']))}</strong></div>
                    <div><span>{_t("Start", "Inicio")}</span><strong>{escape(clean_text_display(r['scheduled_start_time']))}</strong></div>
                    <div><span>{_t("End", "Fin")}</span><strong>{escape(clean_text_display(r['scheduled_end_time']))}</strong></div>
                    <div><span>{_t("Assigned To", "Asignado a")}</span><strong>{escape(clean_text_display(r['assigned_to']))}</strong></div>
                    <div><span>{_t("Revenue", "Ingresos")}</span><strong>${safe_float(r['revenue']):.2f}</strong></div>
                    <div><span>{_t("Costs", "Costos")}</span><strong>${safe_float(r['cost_total']):.2f}</strong></div>
                    <div><span>{_t("Profit/Loss", "Ganancia/Pérdida")}</span><strong>${safe_float(r['profit']):.2f}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <a class='btn secondary small' href='{url_for("jobs.view_job", job_id=r["id"])}'>{_t("View", "Ver")}</a>
                    <a class='btn warning small' href='{url_for("jobs.edit_job", job_id=r["id"])}'>{_t("Edit Job", "Editar trabajo")}</a>
                    {mobile_invoice_action_html}
                    <form method='post'
                          action='{url_for("jobs.delete_job", job_id=r["id"])}'
                          style='margin:0;'
                          onsubmit="return confirm('{_t("Delete this job and all items?", "¿Eliminar este trabajo y todos sus artículos?")}');">
                        <input type="hidden" name="csrf_token" value="{delete_csrf}">
                        <button class='btn danger small' type='submit'>{_t("Delete Job", "Eliminar trabajo")}</button>
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
        delete_url = f"/jobs/recurring/{r['id']}/delete"

        toggle_csrf = generate_csrf()
        generate_now_csrf = generate_csrf()
        delete_csrf = generate_csrf()

        is_active = bool(r["active"])
        effective_next_run = r["computed_next_run"] or r["next_run_date"] or r["start_date"]

        next_preview = upcoming_schedule_preview(
            effective_next_run,
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
                    {escape(clean_text_display(r['title'], _t('Recurring Mowing', 'Corte recurrente de césped')))}
                    <div class='small muted' style='margin-top:4px;'>{_t("Mowing Default", "Predeterminado de corte de césped")}</div>
                </td>
                <td><span class='service-chip mowing'>{_t("Mowing", "Corte de césped")}</span></td>
                <td class='wrap'>{escape(clean_text_display(r['customer_name']))}</td>
                <td>{escape(interval_label(r['interval_weeks']))}</td>
                <td>{escape(clean_text_display(effective_next_run))}</td>
                <td class='wrap'>{escape(clean_text_display(r['assigned_to']))}</td>
                <td>{active_chip}</td>
                <td class='center'>{safe_int(r['generated_jobs_count'], 0)}</td>
                <td class='money'>${safe_float(r['total_revenue']):.2f}</td>
                <td class='money'>${safe_float(r['total_cost']):.2f}</td>
                <td class='money jobs-profit'>${safe_float(r['total_profit']):.2f}</td>
                <td class='wrap'>
                    <div class='static-actions'>
                        <a class='btn secondary small' href='{edit_url}'>{_t("Edit Schedule", "Editar programa")}</a>

                        <form method='post' action='{generate_url}' style='margin:0;'>
                            <input type="hidden" name="csrf_token" value="{generate_now_csrf}">
                            <button class='btn success small' type='submit' {"disabled" if not is_active else ""}>{_t("Generate Now", "Generar ahora")}</button>
                        </form>

                        <form method='post' action='{toggle_url}' style='margin:0;'>
                            <input type="hidden" name="csrf_token" value="{toggle_csrf}">
                            <button class='btn warning small' type='submit'>
                                {_t("Pause", "Pausar") if is_active else _t("Resume", "Reanudar")}
                            </button>
                        </form>

                        <form method='post'
                              action='{delete_url}'
                              style='margin:0;'
                              onsubmit="return confirm('{_t("Delete this recurring mowing schedule and all non-invoiced generated jobs?", "¿Eliminar este programa recurrente de corte y todos los trabajos generados no facturados?")}');">
                            <input type="hidden" name="csrf_token" value="{delete_csrf}">
                            <button class='btn danger small' type='submit'>{_t("Delete", "Eliminar")}</button>
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
                    <div class='mobile-list-title'>#{r['id']} - {escape(clean_text_display(r['title'], _t('Recurring Mowing', 'Corte recurrente de césped')))}</div>
                    <div>{active_chip}</div>
                </div>

                <div style='margin:-2px 0 10px 0; display:flex; gap:8px; flex-wrap:wrap;'>
                    <span class='service-chip mowing'>{_t("Mowing", "Corte de césped")}</span>
                    <span class='service-chip default'>{escape(interval_label(r['interval_weeks']))}</span>
                </div>

                <div class='mobile-list-grid'>
                    <div><span>{_t("Customer", "Cliente")}</span><strong>{escape(clean_text_display(r['customer_name']))}</strong></div>
                    <div><span>{_t("Next Run", "Próxima ejecución")}</span><strong>{escape(clean_text_display(effective_next_run))}</strong></div>
                    <div><span>{_t("Assigned To", "Asignado a")}</span><strong>{escape(clean_text_display(r['assigned_to']))}</strong></div>
                    <div><span>{_t("Jobs Generated", "Trabajos generados")}</span><strong>{safe_int(r['generated_jobs_count'], 0)}</strong></div>
                    <div><span>{_t("Total Revenue", "Ingresos totales")}</span><strong>${safe_float(r['total_revenue']):.2f}</strong></div>
                    <div><span>{_t("Total Costs", "Costos totales")}</span><strong>${safe_float(r['total_cost']):.2f}</strong></div>
                    <div><span>{_t("Total Profit", "Ganancia total")}</span><strong>${safe_float(r['total_profit']):.2f}</strong></div>
                    <div><span>{_t("Upcoming", "Próximas")}</span><strong>{escape(next_preview or '-')}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <a class='btn secondary small' href='{edit_url}'>{_t("Edit Schedule", "Editar programa")}</a>

                    <form method='post' action='{generate_url}' style='margin:0;'>
                        <input type="hidden" name="csrf_token" value="{generate_now_csrf}">
                        <button class='btn success small' type='submit' {"disabled" if not is_active else ""}>{_t("Generate", "Generar")}</button>
                    </form>

                    <form method='post' action='{toggle_url}' style='margin:0;'>
                        <input type="hidden" name="csrf_token" value="{toggle_csrf}">
                        <button class='btn warning small' type='submit'>{_t("Pause", "Pausar") if is_active else _t("Resume", "Reanudar")}</button>
                    </form>

                    <form method='post'
                          action='{delete_url}'
                          style='margin:0;'
                          onsubmit="return confirm('{_t("Delete this recurring mowing schedule and all non-invoiced generated jobs?", "¿Eliminar este programa recurrente de corte y todos los trabajos generados no facturados?")}');">
                        <input type="hidden" name="csrf_token" value="{delete_csrf}">
                        <button class='btn danger small' type='submit'>{_t("Delete", "Eliminar")}</button>
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
                <h1 style='margin:0;'>{_t("Jobs", "Trabajos")}</h1>
                <div class='row-actions'>
                    <a class='btn secondary' href='{url_for("jobs.export_jobs")}'>{_t("Export CSV", "Exportar CSV")}</a>
                    <a class='btn warning' href='{url_for("jobs.finished_jobs")}'>{_t("Finished Jobs", "Trabajos terminados")}</a>
                </div>
            </div>

            <form method='post' style='margin-top:18px;'>
                <input type="hidden" name="csrf_token" value="{create_job_csrf}">
                <div class='grid'>
                    <div class='customer-search-wrap'>
                        <label>{_t("Customer", "Cliente")}</label>

                        <div class='customer-search-input-wrap'>
                            <input type='text'
                                id='job_customer_search'
                                placeholder='{escape(_t("Search customer name, company, or email...", "Buscar nombre del cliente, empresa o correo..."))}'
                                autocomplete='off'
                                required>
                            <input type='hidden' name='customer_id' id='job_customer_id' required>
                            <div id='job_customer_results' class='customer-results'></div>
                        </div>
                    </div>

                    <div>
                        <label>{_t("Title", "Título")}</label>
                        <input name='title' id='title' required placeholder='{escape(_t("Example: Weekly Front Yard Mowing", "Ejemplo: Corte semanal del jardín delantero"))}'>
                    </div>

                    <div>
                        <label>{_t("Service Type", "Tipo de servicio")}</label>
                        <select name='service_type' id='service_type'>
                            {service_type_select_options("mowing")}
                        </select>
                        <div class='service-help'>{_t("Mowing defaults are built in, and mowing jobs get a green mowing badge across Jobs and Calendar.", "Los valores predeterminados de corte ya están incluidos, y los trabajos de corte muestran una insignia verde en Trabajos y Calendario.")}</div>
                    </div>

                    <div>
                        <label>{_t("Scheduled Date", "Fecha programada")}</label>
                        <input type='date' name='scheduled_date'>
                    </div>

                    <div>
                        <label>{_t("Start Time", "Hora de inicio")}</label>
                        <input type='time' name='scheduled_start_time'>
                    </div>

                    <div>
                        <label>{_t("End Time", "Hora de fin")}</label>
                        <input type='time' name='scheduled_end_time'>
                    </div>

                    <div>
                        <label>{_t("Assigned To", "Asignado a")}</label>
                        <input name='assigned_to' placeholder='{escape(_t("Crew / Employee", "Cuadrilla / Empleado"))}'>
                    </div>

                    <div>
                        <label>{_t("Status", "Estado")}</label>
                        <select name='status'>
                            <option>{_t("Scheduled", "Programado")}</option>
                            <option>{_t("In Progress", "En progreso")}</option>
                            <option>{_t("Completed", "Completado")}</option>
                            <option>{_t("Invoiced", "Facturado")}</option>
                        </select>
                    </div>

                    <div>
                        <label>{_t("Address", "Dirección")}</label>
                        <input name='address'>
                    </div>
                </div>

                <div class='quick-fill-row'>
                    <button class='quick-fill-chip' type='button' onclick="applyJobTemplate('mowing')">{_t("Use Mowing", "Usar corte")}</button>
                    <button class='quick-fill-chip' type='button' onclick="applyJobTemplate('mulch')">{_t("Use Mulch", "Usar mantillo")}</button>
                    <button class='quick-fill-chip' type='button' onclick="applyJobTemplate('cleanup')">{_t("Use Cleanup", "Usar limpieza")}</button>
                    <button class='quick-fill-chip' type='button' onclick="applyJobTemplate('installation')">{_t("Use Installation", "Usar instalación")}</button>
                </div>

                <br>
                <label>{_t("Notes", "Notas")}</label>
                <textarea name='notes'></textarea>
                <br>
                <button class='btn'>{_t("Create Job", "Crear trabajo")}</button>
            </form>
        </div>

        <div class='card'>
            <div class='recurring-card-head'>
                <div>
                    <h2 style='margin:0;'>{_t("Recurring Mowing Schedules", "Programas recurrentes de corte de césped")}</h2>
                    <p class='muted' style='margin:6px 0 0 0;'>{_t("Weekly, every 2 weeks, or your own custom week interval. Jobs continue generating automatically until you pause the schedule.", "Semanal, cada 2 semanas o tu propio intervalo personalizado en semanas. Los trabajos siguen generándose automáticamente hasta que pauses el programa.")}</p>
                </div>
                <div style='display:flex; gap:8px; flex-wrap:wrap;'>
                    <span class='service-chip mowing'>{_t("Mowing Default", "Predeterminado de corte")}</span>
                    <span class='service-chip default'>{_t("Auto-Generates Jobs", "Genera trabajos automáticamente")}</span>
                </div>
            </div>

            <div class='recurring-default-note'>
                {_t("Recurring mowing schedules default to ", "Los programas recurrentes de corte usan por defecto ")}<strong>{_t("Mowing", "Corte de césped")}</strong>{_t(", create future jobs automatically, and each generated job links back to its parent recurring schedule. End date is optional. Leave it blank to keep generating until paused. Generated visits should be invoiced one visit at a time from the visit job itself.", ", crean trabajos futuros automáticamente, y cada trabajo generado queda vinculado a su programa recurrente principal. La fecha final es opcional. Déjala vacía para seguir generando hasta pausar. Las visitas generadas deben facturarse una por una desde el mismo trabajo de la visita.")}
            </div>

            <form method='post' action='{url_for("jobs.create_recurring_schedule")}' style='margin-top:16px;'>
                <input type="hidden" name="csrf_token" value="{create_schedule_csrf}">

                <div class='grid'>
                    <div class='customer-search-wrap'>
                        <label>{_t("Customer", "Cliente")}</label>

                        <div class='customer-search-input-wrap'>
                            <input type='text'
                                id='recurring_customer_search'
                                placeholder='{escape(_t("Search customer name, company, or email...", "Buscar nombre del cliente, empresa o correo..."))}'
                                autocomplete='off'
                                required>
                            <input type='hidden' name='customer_id' id='recurring_customer_id' required>
                            <div id='recurring_customer_results' class='customer-results'></div>
                        </div>
                    </div>

                    <div>
                        <label>{_t("Schedule Title", "Título del programa")}</label>
                        <input name='title' id='recurring_title' value='{escape(_t("Recurring Mowing", "Corte recurrente de césped"))}' required>
                    </div>

                    <div>
                        <label>{_t("Service Type", "Tipo de servicio")}</label>
                        <select name='service_type'>
                            {service_type_select_options("mowing")}
                        </select>
                    </div>

                    <div>
                        <label>{_t("Start Date", "Fecha de inicio")}</label>
                        <input type='date' name='start_date' value='{date.today().isoformat()}' required>
                    </div>

                    <div>
                        <label>{_t("Interval", "Intervalo")}</label>
                        <select name='interval_mode' id='interval_mode' onchange='toggleCustomInterval()'>
                            <option value='weekly'>{_t("Weekly", "Semanal")}</option>
                            <option value='every_2'>{_t("Every 2 Weeks", "Cada 2 semanas")}</option>
                            <option value='custom'>{_t("Custom Week Interval", "Intervalo personalizado en semanas")}</option>
                        </select>
                    </div>

                    <div id='custom_interval_wrap' style='display:none;'>
                        <label>{_t("Custom Weeks", "Semanas personalizadas")}</label>
                        <input type='number' name='custom_interval_weeks' id='custom_interval_weeks' min='1' step='1' value='3'>
                    </div>

                    <div>
                        <label>{_t("End Date (Optional)", "Fecha de fin (opcional)")}</label>
                        <input type='date' name='end_date'>
                    </div>

                    <div>
                        <label>{_t("Start Time", "Hora de inicio")}</label>
                        <input type='time' name='scheduled_start_time'>
                    </div>

                    <div>
                        <label>{_t("End Time", "Hora de fin")}</label>
                        <input type='time' name='scheduled_end_time'>
                    </div>

                    <div>
                        <label>{_t("Assigned To", "Asignado a")}</label>
                        <input name='assigned_to' placeholder='{escape(_t("Crew / Employee", "Cuadrilla / Empleado"))}'>
                    </div>

                    <div>
                        <label>{_t("Default Job Status", "Estado predeterminado del trabajo")}</label>
                        <select name='status_default'>
                            <option selected>{_t("Scheduled", "Programado")}</option>
                            <option>{_t("In Progress", "En progreso")}</option>
                            <option>{_t("Completed", "Completado")}</option>
                            <option>{_t("Invoiced", "Facturado")}</option>
                        </select>
                    </div>

                    <div>
                        <label>{_t("Address", "Dirección")}</label>
                        <input name='address'>
                    </div>
                </div>

                <br>
                <label>{_t("Notes", "Notas")}</label>
                <textarea name='notes' placeholder='{escape(_t("Notes that should carry onto each generated mowing job", "Notas que deben copiarse a cada trabajo de corte generado"))}'></textarea>
                <br>
                <button class='btn success'>{_t("Create Recurring Mowing Schedule", "Crear programa recurrente de corte")}</button>
            </form>
        </div>

        <div class='card'>
            <h2>{_t("Recurring Schedule List", "Lista de programas recurrentes")}</h2>

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
                        <th class='wrap'>{_t("Title", "Título")}</th>
                        <th>{_t("Service", "Servicio")}</th>
                        <th class='wrap'>{_t("Customer", "Cliente")}</th>
                        <th>{_t("Interval", "Intervalo")}</th>
                        <th>{_t("Next Run", "Próxima ejecución")}</th>
                        <th class='wrap'>{_t("Assigned To", "Asignado a")}</th>
                        <th>{_t("Status", "Estado")}</th>
                        <th class='center'>{_t("Jobs", "Trabajos")}</th>
                        <th class='money'>{_t("Revenue", "Ingresos")}</th>
                        <th class='money'>{_t("Costs", "Costos")}</th>
                        <th class='money'>{_t("Profit", "Ganancia")}</th>
                        <th class='wrap'>{_t("Actions", "Acciones")}</th>
                    </tr>
                    {recurring_rows_html or f'<tr><td colspan="13" class="muted">{_t("No recurring mowing schedules yet.", "Todavía no hay programas recurrentes de corte.")}</td></tr>'}
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {recurring_mobile_cards or f"<div class='mobile-list-card'>{_t('No recurring mowing schedules yet.', 'Tod_avía no hay programas recurrentes de corte.')}</div>"}
                </div>
            </div>
        </div>

        <div class='card'>
            <h2>{_t("Job List", "Lista de trabajos")}</h2>

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
                        <th class='wrap'>{_t("Title", "Título")}</th>
                        <th>{_t("Service", "Servicio")}</th>
                        <th class='wrap'>{_t("Customer", "Cliente")}</th>
                        <th>{_t("Date", "Fecha")}</th>
                        <th>{_t("Start", "Inicio")}</th>
                        <th>{_t("End", "Fin")}</th>
                        <th class='wrap'>{_t("Assigned To", "Asignado a")}</th>
                        <th>{_t("Status", "Estado")}</th>
                        <th class='money'>{_t("Revenue", "Ingresos")}</th>
                        <th class='money'>{_t("Costs", "Costos")}</th>
                        <th class='money'>{_t("Profit/Loss", "Ganancia/Pérdida")}</th>
                        <th class='wrap'>{_t("Actions", "Acciones")}</th>
                    </tr>
                    {job_rows or f'<tr><td colspan="13" class="muted">{_t("No jobs yet.", "Todavía no hay trabajos.")}</td></tr>'}
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {job_mobile_cards or f"<div class='mobile-list-card'>{_t('No jobs yet.', 'Todavía no hay trabajos.')}</div>"}
                </div>
            </div>
        </div>
    </div>

    <script>
        const customers = {json.dumps(customer_list)};
        const textNoCustomersFound = {json.dumps(_t("No customers found", "No se encontraron clientes"))};
        const textUnnamedCustomer = {json.dumps(_t("Unnamed Customer", "Cliente sin nombre"))};
        const textRecurring = {json.dumps(_t("Recurring", "Recurrente"))};

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
                    resultsBox.innerHTML = `<div class='customer-result-item muted'>${{escapeHtml(textNoCustomersFound)}}</div>`;
                    showResults();
                    return;
                }}

                resultsBox.innerHTML = matches.map(c => `
                    <div class="customer-result-item" data-id="${{c.id}}">
                        <strong>${{escapeHtml(c.name || textUnnamedCustomer)}}</strong>
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
                            : (customer.name || textUnnamedCustomer);

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
                mowing: {json.dumps(_t("Weekly Mowing", "Corte semanal"))},
                mulch: {json.dumps(_t("Mulch Delivery / Install", "Entrega / instalación de mantillo"))},
                cleanup: {json.dumps(_t("Property Cleanup", "Limpieza de propiedad"))},
                installation: {json.dumps(_t("Landscape Installation", "Instalación de jardinería"))},
                hardscape: {json.dumps(_t("Hardscape Work", "Trabajo de hardscape"))},
                snow_removal: {json.dumps(_t("Snow Removal", "Remoción de nieve"))},
                fertilizing: {json.dumps(_t("Fertilizing Service", "Servicio de fertilización"))},
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
                titleInput.value = {json.dumps(_t("Weekly Mowing", "Corte semanal"))};
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
    return render_page(content, _t("Jobs", "Trabajos"))

@jobs_bp.route("/jobs/recurring/create", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def create_recurring_schedule():
    ensure_job_schedule_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    customer_id = request.form.get("customer_id", type=int)
    title = recurring_schedule_title_default(
        request.form.get("title", _t("Recurring Mowing", "Corte recurrente de césped"))
    )
    service_type = normalize_service_type(request.form.get("service_type", "mowing"))
    start_date = clean_text_input(request.form.get("start_date", ""))
    end_date = clean_text_input(request.form.get("end_date", ""))
    scheduled_start_time = clean_text_input(request.form.get("scheduled_start_time", ""))
    scheduled_end_time = clean_text_input(request.form.get("scheduled_end_time", ""))
    assigned_to = clean_text_input(request.form.get("assigned_to", ""))
    status_default = default_mowing_status(
        request.form.get("status_default", _t("Scheduled", "Programado"))
    )
    address = clean_text_input(request.form.get("address", ""))
    notes = clean_text_input(request.form.get("notes", ""))
    interval_weeks = derive_interval_weeks_from_form(request.form)

    if not customer_id:
        conn.close()
        flash(_t("Please select a customer for the recurring mowing schedule.", "Selecciona un cliente para el programa recurrente de corte de césped."))
        return redirect(url_for("jobs.jobs"))

    if not start_date:
        conn.close()
        flash(_t("Start date is required.", "La fecha de inicio es obligatoria."))
        return redirect(url_for("jobs.jobs"))

    start_date_value = parse_iso_date(start_date)
    end_date_value = parse_iso_date(end_date)

    if not start_date_value:
        conn.close()
        flash(_t("Invalid start date.", "Fecha de inicio no válida."))
        return redirect(url_for("jobs.jobs"))

    if end_date and not end_date_value:
        conn.close()
        flash(_t("Invalid end date.", "Fecha de fin no válida."))
        return redirect(url_for("jobs.jobs"))

    if end_date_value and end_date_value < start_date_value:
        conn.close()
        flash(_t("End date cannot be before the start date.", "La fecha de fin no puede ser anterior a la fecha de inicio."))
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

    conn.commit()
    conn.close()

    if not schedule_id:
        flash(_t("Could not create recurring mowing schedule.", "No se pudo crear el programa recurrente de corte de césped."))
        return redirect(url_for("jobs.jobs"))

    flash(_t(
        "Recurring mowing schedule created. It will continue generating jobs on its interval until you pause it.",
        "Programa recurrente de corte creado. Seguirá generando trabajos según su intervalo hasta que lo pauses."
    ))
    return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))


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
        flash(_t("Recurring mowing schedule not found.", "Programa recurrente de corte no encontrado."))
        return redirect(url_for("jobs.jobs"))

    if not bool(schedule["active"]):
        conn.close()
        flash(_t(
            "This recurring mowing schedule is paused. Resume it before generating upcoming jobs.",
            "Este programa recurrente de corte está en pausa. Reanúdalo antes de generar los próximos trabajos."
        ))
        return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

    try:
        horizon_days = safe_int(schedule["auto_generate_until_days"], 90)
        through_date = date.today() + timedelta(days=horizon_days if horizon_days > 0 else 90)
        created_count = auto_generate_recurring_jobs(conn, cid, through_date=through_date)
        conn.commit()
        flash(_t(
            f"Recurring generation complete. {created_count} job(s) created.",
            f"Generación recurrente completada. Se crearon {created_count} trabajo(s)."
        ))
    except Exception as e:
        conn.rollback()
        flash(_t(f"Could not generate recurring jobs: {e}", f"No se pudieron generar los trabajos recurrentes: {e}"))
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
        flash(_t("Recurring mowing schedule not found.", "Programa recurrente de corte no encontrado."))
        return redirect(url_for("jobs.jobs"))

    computed_next_run_row = conn.execute(
        """
        SELECT scheduled_date
        FROM jobs
        WHERE company_id = %s
          AND recurring_schedule_id = %s
          AND scheduled_date IS NOT NULL
          AND scheduled_date >= %s
        ORDER BY scheduled_date ASC, id ASC
        LIMIT 1
        """,
        (cid, schedule_id, date.today().isoformat()),
    ).fetchone()

    computed_next_run = (
        computed_next_run_row["scheduled_date"]
        if computed_next_run_row and computed_next_run_row["scheduled_date"]
        else None
    )

    customers = conn.execute(
        """
        SELECT id, name
        FROM customers
        WHERE company_id = %s
        ORDER BY name
        """,
        (cid,),
    ).fetchall()

    if request.method == "POST":
        customer_id = request.form.get("customer_id", type=int)
        title = recurring_schedule_title_default(
            request.form.get("title", _t("Recurring Mowing", "Corte recurrente de césped"))
        )
        service_type = normalize_service_type(request.form.get("service_type", "mowing"))
        start_date = clean_text_input(request.form.get("start_date", ""))
        end_date = clean_text_input(request.form.get("end_date", ""))
        scheduled_start_time = clean_text_input(request.form.get("scheduled_start_time", ""))
        scheduled_end_time = clean_text_input(request.form.get("scheduled_end_time", ""))
        assigned_to = clean_text_input(request.form.get("assigned_to", ""))
        status_default = default_mowing_status(
            request.form.get("status_default", _t("Scheduled", "Programado"))
        )
        address = clean_text_input(request.form.get("address", ""))
        notes = clean_text_input(request.form.get("notes", ""))
        interval_weeks = derive_interval_weeks_from_form(request.form)

        start_date_value = parse_iso_date(start_date)
        end_date_value = parse_iso_date(end_date)
        old_next_run = parse_iso_date(schedule["next_run_date"])
        old_start_date = parse_iso_date(schedule["start_date"])

        if not customer_id:
            conn.close()
            flash(_t("Customer is required.", "El cliente es obligatorio."))
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        if not start_date_value:
            conn.close()
            flash(_t("Valid start date is required.", "Se requiere una fecha de inicio válida."))
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        if end_date and not end_date_value:
            conn.close()
            flash(_t("Invalid end date.", "Fecha de fin no válida."))
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        if end_date_value and end_date_value < start_date_value:
            conn.close()
            flash(_t("End date cannot be before start date.", "La fecha de fin no puede ser anterior a la fecha de inicio."))
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        upcoming_generated_row = conn.execute(
            """
            SELECT scheduled_date
            FROM jobs
            WHERE company_id = %s
              AND recurring_schedule_id = %s
              AND scheduled_date IS NOT NULL
              AND scheduled_date >= %s
            ORDER BY scheduled_date ASC, id ASC
            LIMIT 1
            """,
            (cid, schedule_id, date.today().isoformat()),
        ).fetchone()

        upcoming_generated_date = (
            parse_iso_date(upcoming_generated_row["scheduled_date"])
            if upcoming_generated_row and upcoming_generated_row["scheduled_date"]
            else None
        )

        new_next_run = upcoming_generated_date or old_next_run or start_date_value

        if old_start_date and start_date_value and old_next_run == old_start_date:
            new_next_run = start_date_value

        if new_next_run and new_next_run < start_date_value:
            new_next_run = start_date_value

        if end_date_value and new_next_run and new_next_run > end_date_value:
            new_next_run = end_date_value

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
            if bool(schedule["active"]):
                auto_generate_recurring_jobs(conn, cid)
            conn.commit()
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(_t(
                f"Could not update recurring mowing schedule: {e}",
                f"No se pudo actualizar el programa recurrente de corte: {e}"
            ))
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        conn.close()
        flash(_t("Recurring mowing schedule updated.", "Programa recurrente de corte actualizado."))
        return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

    generated_jobs = conn.execute(
        """
        SELECT
            j.id,
            j.title,
            j.scheduled_date,
            j.scheduled_start_time,
            j.scheduled_end_time,
            j.status,
            i.id AS invoice_id,
            i.invoice_number
        FROM jobs j
        LEFT JOIN invoices i
          ON i.job_id = j.id
         AND i.company_id = j.company_id
        WHERE j.company_id = %s
          AND j.recurring_schedule_id = %s
        ORDER BY j.scheduled_date ASC NULLS LAST, j.id ASC
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

    customer_option_list = []
    for c in customers:
        selected_attr = "selected" if c["id"] == schedule["customer_id"] else ""
        label = escape(clean_text_display(c["name"], _t("Customer", "Cliente") + f" #{c['id']}"))
        customer_option_list.append(
            f"<option value='{c['id']}' {selected_attr}>{label}</option>"
        )

    customer_opts = "".join(customer_option_list)
    edit_csrf = generate_csrf()
    generate_jobs_csrf = generate_csrf()
    toggle_csrf = generate_csrf()
    schedule_invoice_csrf = generate_csrf()

    jobs_rows = []
    jobs_mobile_cards = []

    for j in generated_jobs:
        invoice_button_html = ""
        mobile_invoice_button_html = ""

        if j["invoice_id"]:
            invoice_label = clean_text_input(j["invoice_number"]) or f"{_t('Invoice', 'Factura')} #{j['invoice_id']}"
            invoice_button_html = (
                f"<a class='btn secondary small' href='{url_for('invoices.view_invoice', invoice_id=j['invoice_id'])}'>"
                f"{_t('View', 'Ver')} {escape(invoice_label)}</a>"
            )
            mobile_invoice_button_html = invoice_button_html
        elif clean_text_input(j["status"]) != "Invoiced":
            invoice_button_html = (
                f"<a class='btn success small' href='{url_for('jobs.convert_job_to_invoice', job_id=j['id'])}'>"
                f"{_t('Invoice This Visit', 'Facturar esta visita')}</a>"
            )
            mobile_invoice_button_html = invoice_button_html

        jobs_rows.append(
            f"""
            <tr>
                <td>#{j['id']}</td>
                <td class='wrap'>
                    <a href='{url_for("jobs.view_job", job_id=j["id"])}'>{escape(clean_text_display(j['title']))}</a>
                </td>
                <td>{escape(clean_text_display(j['scheduled_date']))}</td>
                <td>{escape(clean_text_display(j['scheduled_start_time']))}</td>
                <td>{escape(clean_text_display(j['scheduled_end_time']))}</td>
                <td>{escape(clean_text_display(j['status']))}</td>
                <td class='wrap'>
                    <div style='display:flex; gap:6px; flex-wrap:wrap;'>
                        <a class='btn warning small' href='{url_for("jobs.edit_job", job_id=j["id"])}'>{_t("Edit", "Editar")}</a>
                        {invoice_button_html or f"<span class='muted small'>{_t('—', '—')}</span>"}
                    </div>
                </td>
            </tr>
            """
        )

        invoice_meta_html = ""
        if j["invoice_id"]:
            invoice_label = clean_text_input(j["invoice_number"]) or f"{_t('Invoice', 'Factura')} #{j['invoice_id']}"
            invoice_meta_html = f"<div><span>{_t('Invoice', 'Factura')}</span><strong>{escape(invoice_label)}</strong></div>"

        jobs_mobile_cards.append(
            f"""
            <div class='mobile-list-card'>
                <div class='mobile-list-top'>
                    <div class='mobile-list-title'>
                        <a href='{url_for("jobs.view_job", job_id=j["id"])}'>#{j['id']} - {escape(clean_text_display(j['title']))}</a>
                    </div>
                    <div class='mobile-badge'>{escape(clean_text_display(j['status']))}</div>
                </div>

                <div class='mobile-list-grid'>
                    <div><span>{_t("Date", "Fecha")}</span><strong>{escape(clean_text_display(j['scheduled_date']))}</strong></div>
                    <div><span>{_t("Start", "Inicio")}</span><strong>{escape(clean_text_display(j['scheduled_start_time']))}</strong></div>
                    <div><span>{_t("End", "Fin")}</span><strong>{escape(clean_text_display(j['scheduled_end_time']))}</strong></div>
                    {invoice_meta_html}
                </div>

                <div class='mobile-list-actions'>
                    <a class='btn warning small' href='{url_for("jobs.edit_job", job_id=j["id"])}'>{_t("Edit", "Editar")}</a>
                    {mobile_invoice_button_html or ""}
                </div>
            </div>
            """
        )

    generated_jobs_table = "".join(jobs_rows)
    generated_jobs_mobile_html = "".join(jobs_mobile_cards)

    recurring_item_rows = []
    recurring_item_mobile_cards = []

    for item in recurring_items:
        delete_item_csrf = generate_csrf()

        unit_cost_display = f"${safe_float(item['unit_cost']):.2f}"
        sale_price_display = f"${safe_float(item['sale_price']):.2f}"
        total_cost_display = safe_float(item["quantity"]) * safe_float(item["unit_cost"])
        total_revenue_display = (
            safe_float(item["quantity"]) * safe_float(item["sale_price"])
            if item["billable"] else 0.0
        )

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
                <td class='center'>{_t('Yes', 'Sí') if item['billable'] else _t('No', 'No')}</td>
                <td class='money'>${total_revenue_display:.2f}</td>
                <td class='wrap'>
                    <form method='post'
                          action='{url_for("jobs.delete_recurring_schedule_item", schedule_id=schedule_id, item_id=item["id"])}'
                          style='margin:0;'
                          onsubmit="return confirm('{_t("Delete this recurring schedule item?", "¿Eliminar este artículo del programa recurrente?")}');">
                        <input type="hidden" name="csrf_token" value="{delete_item_csrf}">
                        <button class='btn danger small' type='submit'>{_t("Delete", "Eliminar")}</button>
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
                    <div class='mobile-badge'>{_t('Billable', 'Facturable') if item['billable'] else _t('Non-Billable', 'No facturable')}</div>
                </div>

                <div class='mobile-list-grid'>
                    <div><span>{_t("Qty", "Cant.")}</span><strong>{safe_float(item['quantity']):g}</strong></div>
                    <div><span>{_t("Unit", "Unidad")}</span><strong>{escape(clean_text_display(item['unit']))}</strong></div>
                    <div><span>{_t("Sale Price", "Precio de venta")}</span><strong>{sale_price_display}</strong></div>
                    <div><span>{_t("Unit Cost", "Costo unitario")}</span><strong>{unit_cost_display}</strong></div>
                    <div><span>{_t("Total Cost", "Costo total")}</span><strong>${total_cost_display:.2f}</strong></div>
                    <div><span>{_t("Total Revenue", "Ingreso total")}</span><strong>${total_revenue_display:.2f}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <form method='post'
                          action='{url_for("jobs.delete_recurring_schedule_item", schedule_id=schedule_id, item_id=item["id"])}'
                          style='margin:0;'
                          onsubmit="return confirm('{_t("Delete this recurring schedule item?", "¿Eliminar este artículo del programa recurrente?")}');">
                        <input type="hidden" name="csrf_token" value="{delete_item_csrf}">
                        <button class='btn danger small' type='submit'>{_t("Delete", "Eliminar")}</button>
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
    schedule_is_active = bool(schedule["active"])
    schedule_status_text = _t("Active", "Activo") if schedule_is_active else _t("Paused", "En pausa")
    schedule_status_class_name = "mowing" if schedule_is_active else "default"

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
                <h1 style='margin:0;'>{_t("Edit Recurring Mowing Schedule", "Editar programa recurrente de corte")} #{schedule['id']}</h1>
                <p class='muted' style='margin:6px 0 0 0;'>
                    {_t("This schedule keeps generating mowing jobs on its set interval until you pause it.", "Este programa sigue generando trabajos de corte según su intervalo hasta que lo pauses.")}
                </p>
            </div>
            <div style='display:flex; gap:8px; flex-wrap:wrap;'>
                <span class='service-chip mowing'>{_t("Mowing", "Corte de césped")}</span>
                <span class='service-chip default'>{escape(interval_label(schedule['interval_weeks']))}</span>
                <span class='service-chip {schedule_status_class_name}'>{schedule_status_text}</span>
            </div>
        </div>

        <div class='row-actions' style='margin-top:14px;'>
            <a class='btn secondary' href='{url_for("jobs.jobs")}'>{_t("Back to Jobs", "Volver a trabajos")}</a>

            <form method='post' action='{url_for("jobs.generate_recurring_schedule_jobs", schedule_id=schedule["id"])}' style='margin:0;'>
                <input type="hidden" name="csrf_token" value="{generate_jobs_csrf}">
                <button class='btn success' type='submit' {"disabled" if not schedule_is_active else ""}>
                    {_t("Generate Upcoming Jobs Now", "Generar próximos trabajos ahora")}
                </button>
            </form>

            <form method='post' action='{url_for("jobs.convert_recurring_schedule_to_invoice", schedule_id=schedule["id"])}' style='margin:0;'>
                <input type="hidden" name="csrf_token" value="{schedule_invoice_csrf}">
                <input type="hidden" name="invoice_mode" value="full_schedule">
                <button class='btn success' type='submit'>
                    {_t("Invoice Entire Schedule", "Facturar todo el programa")}
                </button>
            </form>

            <form method='post' action='{url_for("jobs.toggle_recurring_schedule", schedule_id=schedule["id"])}' style='margin:0;'>
                <input type="hidden" name="csrf_token" value="{toggle_csrf}">
                <button class='btn warning' type='submit'>{_t("Pause Schedule", "Pausar programa") if schedule_is_active else _t("Resume Schedule", "Reanudar programa")}</button>
            </form>
        </div>
    </div>

    <div class='card'>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{edit_csrf}">
            <div class='grid'>
                <div>
                    <label>{_t("Customer", "Cliente")}</label>
                    <select name='customer_id' required>
                        <option value=''>{_t("Select customer", "Selecciona cliente")}</option>
                        {customer_opts}
                    </select>
                </div>

                <div>
                    <label>{_t("Schedule Title", "Título del programa")}</label>
                    <input name='title' value="{escape(clean_text_input(schedule['title']) or _t('Recurring Mowing', 'Corte recurrente de césped'))}" required>
                </div>

                <div>
                    <label>{_t("Service Type", "Tipo de servicio")}</label>
                    <select name='service_type'>
                        {service_type_select_options(schedule['service_type'] or 'mowing')}
                    </select>
                </div>

                <div>
                    <label>{_t("Start Date", "Fecha de inicio")}</label>
                    <input type='date' name='start_date' value="{escape(date_to_iso(schedule['start_date']))}" required>
                </div>

                <div>
                    <label>{_t("Interval", "Intervalo")}</label>
                    <select name='interval_mode' id='edit_interval_mode' onchange='toggleEditCustomInterval()'>
                        <option value='weekly' {'selected' if interval_mode == 'weekly' else ''}>{_t("Weekly", "Semanal")}</option>
                        <option value='every_2' {'selected' if interval_mode == 'every_2' else ''}>{_t("Every 2 Weeks", "Cada 2 semanas")}</option>
                        <option value='custom' {'selected' if interval_mode == 'custom' else ''}>{_t("Custom Week Interval", "Intervalo personalizado en semanas")}</option>
                    </select>
                </div>

                <div id='edit_custom_interval_wrap' style='display:{custom_wrap_display};'>
                    <label>{_t("Custom Weeks", "Semanas personalizadas")}</label>
                    <input type='number' name='custom_interval_weeks' min='1' step='1' value='{safe_int(schedule["interval_weeks"], 1)}'>
                </div>

                <div>
                    <label>{_t("Next Run Date", "Próxima fecha de ejecución")}</label>
                    <input type='date' value="{escape(date_to_iso(computed_next_run or schedule['next_run_date']))}" disabled>
                    <div class='muted small' style='margin-top:4px;'>{_t("Auto-managed after generation.", "Se gestiona automáticamente después de generar.")}</div>
                </div>

                <div>
                    <label>{_t("End Date (Optional)", "Fecha de fin (opcional)")}</label>
                    <input type='date' name='end_date' value="{escape(date_to_iso(schedule['end_date']))}">
                    <div class='muted small' style='margin-top:4px;'>{_t("Leave blank to keep generating jobs until you pause the schedule.", "Déjalo vacío para seguir generando trabajos hasta que pauses el programa.")}</div>
                </div>

                <div>
                    <label>{_t("Start Time", "Hora de inicio")}</label>
                    <input type='time' name='scheduled_start_time' value="{escape(clean_text_input(schedule['scheduled_start_time']))}">
                </div>

                <div>
                    <label>{_t("End Time", "Hora de fin")}</label>
                    <input type='time' name='scheduled_end_time' value="{escape(clean_text_input(schedule['scheduled_end_time']))}">
                </div>

                <div>
                    <label>{_t("Assigned To", "Asignado a")}</label>
                    <input name='assigned_to' value="{escape(clean_text_input(schedule['assigned_to']))}">
                </div>

                <div>
                    <label>{_t("Default Job Status", "Estado predeterminado del trabajo")}</label>
                    <select name='status_default'>
                        <option {'selected' if clean_text_input(schedule['status_default']) == 'Scheduled' else ''}>{_t("Scheduled", "Programado")}</option>
                        <option {'selected' if clean_text_input(schedule['status_default']) == 'In Progress' else ''}>{_t("In Progress", "En progreso")}</option>
                        <option {'selected' if clean_text_input(schedule['status_default']) == 'Completed' else ''}>{_t("Completed", "Completado")}</option>
                        <option {'selected' if clean_text_input(schedule['status_default']) == 'Invoiced' else ''}>{_t("Invoiced", "Facturado")}</option>
                    </select>
                </div>

                <div>
                    <label>{_t("Address", "Dirección")}</label>
                    <input name='address' value="{escape(clean_text_input(schedule['address']))}">
                </div>
            </div>

            <br>
            <label>{_t("Notes", "Notas")}</label>
            <textarea name='notes'>{escape(clean_text_input(schedule['notes']))}</textarea>
            <br>
            <button class='btn'>{_t("Save Schedule Changes", "Guardar cambios del programa")}</button>
        </form>
    </div>

    <div class='card'>
        <h2>{_t("Recurring Schedule Items", "Artículos del programa recurrente")}</h2>
        <p class='muted'>{_t("These items will be copied into each newly generated recurring job. This is where you set the mowing price and costs.", "Estos artículos se copiarán en cada trabajo recurrente nuevo generado. Aquí es donde defines el precio y los costos del corte.")}</p>

        <form method='post' action='{url_for("jobs.add_recurring_schedule_item", schedule_id=schedule_id)}'>
            <input type="hidden" name="csrf_token" value="{add_recurring_item_csrf}">
            <div class='grid'>
                <div>
                    <label>{_t("Type", "Tipo")}</label>
                    <select name='item_type' id='recurring_item_type' onchange='toggleRecurringItemMode()'>
                        <option value='labor'>{_t("Labor", "Mano de obra")}</option>
                        <option value='fuel'>{_t("Fuel", "Combustible")}</option>
                        <option value='misc'>{_t("Misc", "Varios")}</option>
                        <option value='dump_fee'>{_t("Dump Fee", "Tarifa de vertedero")}</option>
                        <option value='equipment'>{_t("Equipment", "Equipo")}</option>
                        <option value='delivery'>{_t("Delivery", "Entrega")}</option>
                        <option value='mulch'>{_t("Mulch", "Mantillo")}</option>
                        <option value='stone'>{_t("Stone", "Piedra")}</option>
                        <option value='soil'>{_t("Soil", "Tierra")}</option>
                        <option value='fertilizer'>{_t("Fertilizer", "Fertilizante")}</option>
                        <option value='plants'>{_t("Plants", "Plantas")}</option>
                        <option value='trees'>{_t("Trees", "Árboles")}</option>
                        <option value='hardscape_material'>{_t("Hardscape Material", "Material de hardscape")}</option>
                    </select>
                </div>

                <div>
                    <label>{_t("Description", "Descripción")}</label>
                    <input name='description' value='{escape(_t("Mowing Service", "Servicio de corte"))}' required>
                </div>

                <div>
                    <label id='recurring_quantity_label'>{_t("Quantity", "Cantidad")}</label>
                    <input type='number' step='0.01' name='quantity' id='recurring_quantity' value='1' required>
                </div>

                <div>
                    <label>{_t("Unit", "Unidad")}</label>
                    <input name='unit' id='recurring_unit' value='{escape(_t("Hours", "Horas"))}'>
                </div>

                <div>
                    <label id='recurring_sale_price_label'>{_t("Sale Price", "Precio de venta")}</label>
                    <input type='number' step='0.01' name='sale_price' id='recurring_sale_price' value='0' required>
                </div>

                <div id="unit_cost_wrap">
                    <label id='recurring_cost_label'>{_t("Unit Cost", "Costo unitario")}</label>
                    <input type='number' step='0.01' name='unit_cost' id='recurring_unit_cost' value='0'>
                </div>

                <div>
                    <label>{_t("Billable?", "¿Facturable?")}</label>
                    <select name='billable'>
                        <option value='1'>{_t("Yes", "Sí")}</option>
                        <option value='0'>{_t("No", "No")}</option>
                    </select>
                </div>
            </div>

            <br>
            <button class='btn success' type='submit'>{_t("Add Recurring Item", "Agregar artículo recurrente")}</button>
        </form>
    </div>

    <div class='card'>
        <h2>{_t("Recurring Item_ List", "Lista de artículos recurrentes")}</h2>

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
                    <th>{_t("Type", "Tipo")}</th>
                    <th class='wrap'>{_t("Description", "Descripción")}</th>
                    <th class='money'>{_t("Qty", "Cant.")}</th>
                    <th>{_t("Unit", "Unidad")}</th>
                    <th class='money'>{_t("Sale Price", "Precio de venta")}</th>
                    <th class='money'>{_t("Unit Cost", "Costo unitario")}</th>
                    <th class='money'>{_t("Total Cost", "Costo total")}</th>
                    <th class='center'>{_t("Billable", "Facturable")}</th>
                    <th class='money'>{_t("Revenue", "Ingreso")}</th>
                    <th class='wrap'>{_t("Actions", "Acciones")}</th>
                </tr>
                {recurring_item_rows_html or f'<tr><td colspan="10" class="muted">{_t("No recurring items yet.", "Todavía no hay artículos recurrentes.")}</td></tr>'}
            </table>
        </div>

        <div class='mobile-only'>
            <div class='mobile-list'>
                {recurring_item_mobile_html or f"<div class='mobile-list-card'>{_t('No recurring items yet.', 'Todavía no hay artículos recurrentes.')}</div>"}
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>{_t("Generated Jobs Linked to This Schedule", "Trabajos generados vinculados a este programa")}</h2>
        <p class='muted' style='margin-top:0;'>
            {_t(
                "You can invoice the full recurring schedule from the top action bar, or invoice one visit at a time using the button on each generated job row.",
                "Puedes facturar todo el programa recurrente desde la barra superior de acciones, o facturar una visita a la vez usando el botón en cada fila de trabajo generado."
            )}
        </p>

        <div class='desktop-only'>
            <table class='static-table'>
                <colgroup>
                    <col style='width:8%;'>
                    <col style='width:24%;'>
                    <col style='width:14%;'>
                    <col style='width:12%;'>
                    <col style='width:12%;'>
                    <col style='width:12%;'>
                    <col style='width:18%;'>
                </colgroup>
                <tr>
                    <th>ID</th>
                    <th class='wrap'>{_t("Title", "Título")}</th>
                    <th>{_t("Date", "Fecha")}</th>
                    <th>{_t("Start", "Inicio")}</th>
                    <th>{_t("End", "Fin")}</th>
                    <th>{_t("Status", "Estado")}</th>
                    <th class='wrap'>{_t("Actions", "Acciones")}</th>
                </tr>
                {generated_jobs_table or f'<tr><td colspan="7" class="muted">{_t("No generated jobs yet.", "Todavía no hay trabajos generados.")}</td></tr>'}
            </table>
        </div>

        <div class='mobile-only'>
            <div class='mobile-list'>
                {generated_jobs_mobile_html or f"<div class='mobile-list-card'>{_t('No generated jobs yet.', 'Todavía no hay trabajos generados.')}</div>"}
            </div>
        </div>
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

            if (quantityLabel) quantityLabel.innerText = "{_t('Quantity', 'Cantidad')}";
            if (salePriceLabel) salePriceLabel.innerText = "{_t('Sale Price', 'Precio de venta')}";
            if (costLabel) costLabel.innerText = "{_t('Unit Cost', 'Costo unitario')}";

            if (quantityInput) {{
                quantityInput.readOnly = false;
                quantityInput.step = '0.01';
            }}

            if (unitCostWrap) unitCostWrap.style.display = 'block';

            if (type === 'labor') {{
                if (quantityLabel) quantityLabel.innerText = "{_t('Billable Hours', 'Horas facturables')}";
                if (salePriceLabel) salePriceLabel.innerText = "{_t('Hourly Rate', 'Tarifa por hora')}";
                if (costLabel) costLabel.innerText = "{_t('Hourly Cost', 'Costo por hora')}";
                if (unitInput) unitInput.value = "{_t('Hours', 'Horas')}";
                if (unitCostWrap) unitCostWrap.style.display = 'block';
            }}
            else if (type === 'mulch') {{
                if (quantityLabel) quantityLabel.innerText = "{_t('Yards', 'Yardas')}";
                if (unitInput) unitInput.value = "{_t('Yards', 'Yardas')}";
            }}
            else if (type === 'stone') {{
                if (quantityLabel) quantityLabel.innerText = "{_t('Tons', 'Toneladas')}";
                if (unitInput) unitInput.value = "{_t('Tons', 'Toneladas')}";
            }}
            else if (type === 'soil') {{
                if (quantityLabel) quantityLabel.innerText = "{_t('Yards', 'Yardas')}";
                if (unitInput) unitInput.value = "{_t('Yards', 'Yardas')}";
            }}
            else if (type === 'hardscape_material') {{
                if (quantityLabel) quantityLabel.innerText = "{_t('Tons', 'Toneladas')}";
                if (unitInput) unitInput.value = "{_t('Tons', 'Toneladas')}";
            }}
            else if (type === 'fuel') {{
                if (quantityLabel) quantityLabel.innerText = "{_t('Gallons', 'Galones')}";
                if (unitInput) unitInput.value = "{_t('Gallons', 'Galones')}";
            }}
            else if (type === 'delivery') {{
                if (quantityLabel) quantityLabel.innerText = "{_t('Miles', 'Millas')}";
                if (unitInput) unitInput.value = "{_t('Miles', 'Millas')}";
            }}
            else if (type === 'equipment') {{
                if (quantityLabel) quantityLabel.innerText = "{_t('Rentals', 'Alquileres')}";
                if (unitInput) unitInput.value = "{_t('Rentals', 'Alquileres')}";
            }}
            else if (type === 'dump_fee') {{
                if (quantityLabel) quantityLabel.innerText = "{_t('Fee', 'Tarifa')}";
                if (salePriceLabel) salePriceLabel.innerText = "{_t('Fee Amount', 'Monto de la tarifa')}";
                if (unitInput) unitInput.value = '';

                if (unitCostWrap) unitCostWrap.style.display = 'none';
                if (unitCostInput) unitCostInput.value = '0';

                if (quantityInput) {{
                    quantityInput.value = '1';
                    quantityInput.readOnly = true;
                }}
            }}
            else if (type === 'fertilizer') {{
                if (unitInput) unitInput.value = "{_t('Bags', 'Bolsas')}";
            }}
            else if (type === 'plants') {{
                if (unitInput) unitInput.value = 'EA';
            }}
            else if (type === 'trees') {{
                if (unitInput) unitInput.value = 'EA';
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
    return render_page(
        content,
        f"{_t('Recurring Schedule', 'Programa recurrente')} #{schedule_id}"
    )


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
        flash(_t("Recurring mowing schedule not found.", "Programa recurrente de corte no encontrado."))
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
        flash(_t("Description is required.", "La descripción es obligatoria."))
        return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

    if qty <= 0:
        qty = 1.0

    if item_type == "mulch" and not unit:
        unit = _t("Yards", "Yardas")
    elif item_type == "stone" and not unit:
        unit = _t("Tons", "Toneladas")
    elif item_type == "soil" and not unit:
        unit = _t("Yards", "Yardas")
    elif item_type == "hardscape_material" and not unit:
        unit = _t("Tons", "Toneladas")
    elif item_type == "fuel" and not unit:
        unit = _t("Gallons", "Galones")
    elif item_type == "delivery" and not unit:
        unit = _t("Miles", "Millas")
    elif item_type == "labor" and not unit:
        unit = _t("Hours", "Horas")
    elif item_type == "equipment" and not unit:
        unit = _t("Rentals", "Alquileres")
    elif item_type == "fertilizer" and not unit:
        unit = _t("Bags", "Bolsas")
    elif item_type in ["plants", "trees", "misc", "dump_fee"]:
        unit = ""

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
        flash(_t("Could not add recurring schedule item.", "No se pudo agregar el artículo del programa recurrente."))
        return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

    conn.commit()
    conn.close()

    flash(_t("Recurring schedule item added.", "Artículo del programa recurrente agregado."))
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
        flash(_t("Recurring mowing schedule not found.", "Programa recurrente de corte no encontrado."))
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
                invoice_label = clean_text_input(sample["invoice_number"]) or f"{_t('Invoice', 'Factura')} #{sample['invoice_id']}"
                flash(
                    _t(
                        f"Cannot delete this recurring mowing schedule because generated job #{sample['id']} is already tied to {invoice_label}. Remove or handle the invoice first.",
                        f"No se puede eliminar este programa recurrente de corte porque el trabajo generado #{sample['id']} ya está vinculado a {invoice_label}. Elimina o resuelve primero la factura."
                    )
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
        flash(
            _t(
                "Recurring mowing schedule and all non-invoiced generated jobs were deleted.",
                "Se eliminó el programa recurrente de corte y todos los trabajos generados no facturados."
            )
        )
    except Exception as e:
        conn.rollback()
        flash(_t(f"Could not delete recurring mowing schedule: {e}", f"No se pudo eliminar el programa recurrente de corte: {e}"))
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
        flash(_t("Recurring schedule item not found.", "Artículo del programa recurrente no encontrado."))
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

    flash(_t("Recurring schedule item deleted.", "Artículo del programa recurrente eliminado."))
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
        SELECT id, active, start_date, next_run_date
        FROM recurring_mowing_schedules
        WHERE id = %s AND company_id = %s
        """,
        (schedule_id, cid),
    ).fetchone()

    if not schedule:
        conn.close()
        flash(_t("Recurring mowing schedule not found.", "Programa recurrente de corte no encontrado."))
        return redirect(url_for("jobs.jobs"))

    new_active = not bool(schedule["active"])
    next_run_date = schedule["next_run_date"] or schedule["start_date"] or date.today().isoformat()

    conn.execute(
        """
        UPDATE recurring_mowing_schedules
        SET active = %s,
            next_run_date = COALESCE(next_run_date, %s),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s AND company_id = %s
        """,
        (new_active, next_run_date, schedule_id, cid),
    )

    try:
        if new_active:
            auto_generate_recurring_jobs(conn, cid)
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(_t(f"Could not update recurring mowing schedule: {e}", f"No se pudo actualizar el programa recurrente de corte: {e}"))
        return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

    conn.close()

    flash(
        _t("Recurring mowing schedule resumed.", "Programa recurrente de corte reanudado.")
        if new_active
        else _t("Recurring mowing schedule paused.", "Programa recurrente de corte pausado.")
    )
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
            SELECT rms.*, c.name AS customer_name
            FROM recurring_mowing_schedules rms
            JOIN customers c ON rms.customer_id = c.id
            WHERE rms.id = %s AND rms.company_id = %s
            """,
            (schedule_id, cid),
        ).fetchone()

        if not schedule:
            flash(_t("Recurring schedule not found.", "Programa recurrente no encontrado."))
            return redirect(url_for("jobs.jobs"))

        invoice_mode = (request.form.get("invoice_mode") or "").strip().lower()

        if invoice_mode not in {"full_schedule", "single_visit"}:
            invoice_mode = "full_schedule"

        if invoice_mode == "single_visit":
            flash(
                _t(
                    "Single-visit invoicing is still available. Open the specific generated job for that visit and use Convert to Invoice there.",
                    "La facturación por visita individual sigue disponible. Abre el trabajo generado específico de esa visita y usa Convertir en factura allí."
                )
            )
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        jobs = conn.execute(
            """
            SELECT *
            FROM jobs
            WHERE company_id = %s
              AND recurring_schedule_id = %s
            ORDER BY
                scheduled_date ASC NULLS LAST,
                scheduled_start_time ASC NULLS LAST,
                id ASC
            """,
            (cid, schedule_id),
        ).fetchall()

        if not jobs:
            flash(_t("No generated jobs were found for this recurring schedule yet.", "Todavía no se encontraron trabajos generados para este programa recurrente."))
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        job_ids = [j["id"] for j in jobs]

        invoices_cols = set()
        jobs_cols = set()
        invoice_items_cols = set()
        try:
            invoices_cols = set(table_columns(conn, "invoices"))
        except Exception:
            invoices_cols = set()

        try:
            jobs_cols = set(table_columns(conn, "jobs"))
        except Exception:
            jobs_cols = set()

        try:
            invoice_items_cols = set(table_columns(conn, "invoice_items"))
        except Exception:
            invoice_items_cols = set()

        already_invoiced_job_ids = set()

        if "job_id" in invoice_items_cols:
            rows = conn.execute(
                """
                SELECT DISTINCT ii.job_id
                FROM invoice_items ii
                JOIN invoices i ON i.id = ii.invoice_id
                WHERE i.company_id = %s
                  AND ii.job_id = ANY(%s)
                """,
                (cid, job_ids),
            ).fetchall()
            already_invoiced_job_ids.update(
                r["job_id"] for r in rows if r["job_id"] is not None
            )

        if "job_id" in invoices_cols:
            rows = conn.execute(
                """
                SELECT DISTINCT job_id
                FROM invoices
                WHERE company_id = %s
                  AND job_id = ANY(%s)
                """,
                (cid, job_ids),
            ).fetchall()
            already_invoiced_job_ids.update(
                r["job_id"] for r in rows if r["job_id"] is not None
            )

        if "invoice_id" in jobs_cols:
            rows = conn.execute(
                """
                SELECT id
                FROM jobs
                WHERE company_id = %s
                  AND id = ANY(%s)
                  AND invoice_id IS NOT NULL
                """,
                (cid, job_ids),
            ).fetchall()
            already_invoiced_job_ids.update(r["id"] for r in rows)

        if "converted_invoice_id" in jobs_cols:
            rows = conn.execute(
                """
                SELECT id
                FROM jobs
                WHERE company_id = %s
                  AND id = ANY(%s)
                  AND converted_invoice_id IS NOT NULL
                """,
                (cid, job_ids),
            ).fetchall()
            already_invoiced_job_ids.update(r["id"] for r in rows)

        eligible_jobs = [j for j in jobs if j["id"] not in already_invoiced_job_ids]
        skipped_jobs = [j for j in jobs if j["id"] in already_invoiced_job_ids]

        if not eligible_jobs:
            flash(_t("All jobs in this recurring schedule have already been invoiced.", "Todos los trabajos de este programa recurrente ya fueron facturados."))
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        schedule_title = (schedule["title"] or "").strip() if "title" in schedule.keys() else ""
        customer_id = schedule["customer_id"]
        service_type = (schedule["service_type"] or "").strip() if "service_type" in schedule.keys() else ""
        notes = (schedule["notes"] or "").strip() if "notes" in schedule.keys() else ""

        first_date = None
        last_date = None
        visit_count = 0

        for j in eligible_jobs:
            visit_count += 1
            job_date = j["scheduled_date"] if "scheduled_date" in j.keys() else None
            if job_date:
                if first_date is None or job_date < first_date:
                    first_date = job_date
                if last_date is None or job_date > last_date:
                    last_date = job_date

        invoice_title_parts = []
        if schedule_title:
            invoice_title_parts.append(schedule_title)
        elif service_type:
            invoice_title_parts.append(service_type.replace("_", " ").title())
        else:
            invoice_title_parts.append(_t("Recurring Schedule", "Programa recurrente"))

        invoice_title_parts.append(
            _t(f"{visit_count} Visit{'s' if visit_count != 1 else ''}", f"{visit_count} Visita{'s' if visit_count != 1 else ''}")
        )

        if first_date and last_date and first_date != last_date:
            invoice_title_parts.append(f"{first_date} {_t('to', 'a')} {last_date}")
        elif first_date:
            invoice_title_parts.append(str(first_date))

        invoice_title = " - ".join(invoice_title_parts)

        invoice_notes_parts = []
        if schedule_title:
            invoice_notes_parts.append(f"{_t('Recurring schedule:', 'Programa recurrente:')} {schedule_title}")
        else:
            invoice_notes_parts.append(f"{_t('Recurring schedule ID:', 'ID del programa recurrente:')} {schedule_id}")

        invoice_notes_parts.append(f"{_t('Visits included:', 'Visitas incluidas:')} {visit_count}")

        if skipped_jobs:
            invoice_notes_parts.append(
                f"{_t('Visits skipped because already invoiced:', 'Visitas omitidas por ya estar facturadas:')} {len(skipped_jobs)}"
            )

        if first_date and last_date and first_date != last_date:
            invoice_notes_parts.append(f"{_t('Service dates:', 'Fechas de servicio:')} {first_date} {_t('through', 'hasta')} {last_date}")
        elif first_date:
            invoice_notes_parts.append(f"{_t('Service date:', 'Fecha de servicio:')} {first_date}")

        if notes:
            invoice_notes_parts.append(f"{_t('Schedule notes:', 'Notas del programa:')} {notes}")

        invoice_notes = "\n".join(invoice_notes_parts)

        company_profile = conn.execute(
            """
            SELECT *
            FROM company_profile
            WHERE company_id = %s
            """,
            (cid,),
        ).fetchone()

        next_invoice_number = None
        if company_profile and "next_invoice_number" in company_profile.keys():
            next_invoice_number = company_profile["next_invoice_number"]

        if not next_invoice_number:
            last_invoice = conn.execute(
                """
                SELECT invoice_number
                FROM invoices
                WHERE company_id = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (cid,),
            ).fetchone()

            if last_invoice and last_invoice["invoice_number"]:
                try:
                    next_invoice_number = int(str(last_invoice["invoice_number"]).strip()) + 1
                except Exception:
                    next_invoice_number = 1001
            else:
                next_invoice_number = 1001

        insert_invoice_columns = [
            "company_id",
            "customer_id",
            "invoice_number",
            "title",
            "notes",
            "status",
            "total",
            "recurring_schedule_id",
            "service_type",
            "created_at",
        ]
        insert_invoice_values = [
            cid,
            customer_id,
            str(next_invoice_number),
            invoice_title,
            invoice_notes,
            _t("Unpaid", "No pagada"),
            0,
            schedule_id,
            service_type or None,
        ]
        insert_invoice_placeholders = ["%s"] * len(insert_invoice_values)
        created_at_sql = "NOW()"

        invoice_row = conn.execute(
            f"""
            INSERT INTO invoices (
                {", ".join(insert_invoice_columns)}
            )
            VALUES (
                {", ".join(insert_invoice_placeholders)},
                {created_at_sql}
            )
            RETURNING id
            """,
            tuple(insert_invoice_values),
        ).fetchone()

        invoice_id = invoice_row["id"]

        invoice_items_total = 0.0
        inserted_any_items = False

        for job in eligible_jobs:
            job_id = job["id"]

            if _table_exists(conn, "job_items"):
                job_items = conn.execute(
                    """
                    SELECT *
                    FROM job_items
                    WHERE job_id = %s
                    ORDER BY id ASC
                    """,
                    (job_id,),
                ).fetchall()
            else:
                job_items = []

            if job_items:
                for item in job_items:
                    description = ""
                    if "description" in item.keys() and item["description"]:
                        description = str(item["description"]).strip()
                    elif "name" in item.keys() and item["name"]:
                        description = str(item["name"]).strip()
                    else:
                        description = _t("Recurring service visit", "Visita de servicio recurrente")

                    quantity = 1
                    if "quantity" in item.keys() and item["quantity"] is not None:
                        quantity = _safe_float(item["quantity"], 1)
                    elif "qty" in item.keys() and item["qty"] is not None:
                        quantity = _safe_float(item["qty"], 1)

                    unit_price = 0.0
                    if "unit_price" in item.keys() and item["unit_price"] is not None:
                        unit_price = _safe_float(item["unit_price"], 0)
                    elif "price" in item.keys() and item["price"] is not None:
                        unit_price = _safe_float(item["price"], 0)
                    elif "rate" in item.keys() and item["rate"] is not None:
                        unit_price = _safe_float(item["rate"], 0)

                    line_total = quantity * unit_price

                    if "job_id" in invoice_items_cols:
                        conn.execute(
                            """
                            INSERT INTO invoice_items (
                                invoice_id,
                                job_id,
                                description,
                                quantity,
                                unit_price,
                                total
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (
                                invoice_id,
                                job_id,
                                description,
                                quantity,
                                unit_price,
                                line_total,
                            ),
                        )
                    else:
                        conn.execute(
                            """
                            INSERT INTO invoice_items (
                                invoice_id,
                                description,
                                quantity,
                                unit_price,
                                total
                            )
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                invoice_id,
                                description,
                                quantity,
                                unit_price,
                                line_total,
                            ),
                        )

                    invoice_items_total += line_total
                    inserted_any_items = True
            else:
                description_parts = []

                if schedule_title:
                    description_parts.append(schedule_title)
                elif service_type:
                    description_parts.append(service_type.replace("_", " ").title())
                else:
                    description_parts.append(_t("Recurring service", "Servicio recurrente"))

                if "scheduled_date" in job.keys() and job["scheduled_date"]:
                    description_parts.append(str(job["scheduled_date"]))

                fallback_description = " - ".join(description_parts)

                job_total = 0.0
                if "total" in job.keys() and job["total"] is not None:
                    job_total = _safe_float(job["total"], 0)
                elif "price" in job.keys() and job["price"] is not None:
                    job_total = _safe_float(job["price"], 0)

                if "job_id" in invoice_items_cols:
                    conn.execute(
                        """
                        INSERT INTO invoice_items (
                            invoice_id,
                            job_id,
                            description,
                            quantity,
                            unit_price,
                            total
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            invoice_id,
                            job_id,
                            fallback_description,
                            1,
                            job_total,
                            job_total,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO invoice_items (
                            invoice_id,
                            description,
                            quantity,
                            unit_price,
                            total
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            invoice_id,
                            fallback_description,
                            1,
                            job_total,
                            job_total,
                        ),
                    )

                invoice_items_total += job_total
                inserted_any_items = True

        if not inserted_any_items:
            conn.execute(
                "DELETE FROM invoices WHERE id = %s AND company_id = %s",
                (invoice_id, cid),
            )
            conn.commit()
            flash(
                _t(
                    "Could not build an invoice because no billable recurring visits were found.",
                    "No se pudo crear una factura porque no se encontraron visitas recurrentes facturables."
                )
            )
            return redirect(url_for("jobs.edit_recurring_schedule", schedule_id=schedule_id))

        conn.execute(
            """
            UPDATE invoices
            SET total = %s
            WHERE id = %s AND company_id = %s
            """,
            (invoice_items_total, invoice_id, cid),
        )

        if "invoice_id" in jobs_cols:
            conn.execute(
                """
                UPDATE jobs
                SET invoice_id = %s
                WHERE company_id = %s
                  AND id = ANY(%s)
                """,
                (invoice_id, cid, [j["id"] for j in eligible_jobs]),
            )

        if "converted_invoice_id" in jobs_cols:
            conn.execute(
                """
                UPDATE jobs
                SET converted_invoice_id = %s
                WHERE company_id = %s
                  AND id = ANY(%s)
                """,
                (invoice_id, cid, [j["id"] for j in eligible_jobs]),
            )

        if "status" in jobs_cols:
            conn.execute(
                """
                UPDATE jobs
                SET status = 'Invoiced'
                WHERE company_id = %s
                  AND id = ANY(%s)
                """,
                (cid, [j["id"] for j in eligible_jobs]),
            )

        if company_profile and "next_invoice_number" in company_profile.keys():
            conn.execute(
                """
                UPDATE company_profile
                SET next_invoice_number = %s
                WHERE company_id = %s
                """,
                (int(next_invoice_number) + 1, cid),
            )

        conn.commit()

        if skipped_jobs:
            flash(
                _t(
                    f"Recurring schedule invoiced successfully. Added {len(eligible_jobs)} visit{'s' if len(eligible_jobs) != 1 else ''} and skipped {len(skipped_jobs)} already invoiced visit{'s' if len(skipped_jobs) != 1 else ''}.",
                    f"Programa recurrente facturado correctamente. Se agregaron {len(eligible_jobs)} visita(s) y se omitieron {len(skipped_jobs)} visita(s) ya facturadas."
                )
            )
        else:
            flash(
                _t(
                    "Recurring schedule converted into a single invoice successfully.",
                    "El programa recurrente se convirtió correctamente en una sola factura."
                )
            )

        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    except Exception as e:
        conn.rollback()
        print("CONVERT RECURRING SCHEDULE TO INVOICE ERROR:", repr(e), flush=True)
        flash(
            _t(
                "Could not convert recurring schedule to an invoice.",
                "No se pudo convertir el programa recurrente en una factura."
            )
        )
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
        _t("Job ID", "ID del trabajo"),
        _t("Title", "Título"),
        _t("Service Type", "Tipo de servicio"),
        _t("Customer", "Cliente"),
        _t("Customer Email", "Correo del cliente"),
        _t("Scheduled Date", "Fecha programada"),
        _t("Start Time", "Hora de inicio"),
        _t("End Time", "Hora de fin"),
        _t("Assigned To", "Asignado a"),
        _t("Status", "Estado"),
        _t("Address", "Dirección"),
        _t("Revenue", "Ingresos"),
        _t("Costs", "Costos"),
        _t("Profit/Loss", "Ganancia/Pérdida"),
        _t("Recurring Schedule ID", "ID del programa recurrente"),
        _t("Generated From Schedule", "Generado desde programa"),
        _t("Notes", "Notas"),
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
            _t("Yes", "Sí") if r["generated_from_schedule"] else _t("No", "No"),
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

    existing_invoice = conn.execute(
        """
        SELECT id, invoice_number, status
        FROM invoices
        WHERE job_id = %s
          AND company_id = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (job_id, cid),
    ).fetchone()

    if request.method == "POST":
        item_type = clean_text_input(request.form.get("item_type", "")).lower()
        description = clean_text_input(request.form.get("description", ""))
        qty = safe_float(request.form.get("quantity"))
        unit = clean_text_input(request.form.get("unit", ""))
        sale_price = safe_float(request.form.get("sale_price"))
        unit_cost = safe_float(request.form.get("unit_cost"))
        billable = True if request.form.get("billable") == "1" else False

        if not description:
            conn.close()
            flash(_t("Description is required.", "La descripción es obligatoria."))
            return redirect(url_for("jobs.view_job", job_id=job_id))

        if item_type == "mulch" and not unit:
            unit = _t("Yards", "Yardas")
        elif item_type == "stone" and not unit:
            unit = _t("Tons", "Toneladas")
        elif item_type == "soil" and not unit:
            unit = _t("Yards", "Yardas")
        elif item_type == "hardscape_material" and not unit:
            unit = _t("Tons", "Toneladas")
        elif item_type == "fuel" and not unit:
            unit = _t("Gallons", "Galones")
        elif item_type == "delivery" and not unit:
            unit = _t("Miles", "Millas")
        elif item_type == "labor" and not unit:
            unit = _t("Hours", "Horas")
        elif item_type == "equipment" and not unit:
            unit = _t("Rentals", "Alquileres")
        elif item_type in ["plants", "trees", "misc", "dump_fee"]:
            unit = ""

        if item_type == "dump_fee":
            unit = ""
            if qty <= 0:
                qty = 1
            unit_cost = 0.0

        line_total = qty * sale_price if billable else 0.0
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
            flash(_t("Could not add job item.", "No se pudo agregar el artículo del trabajo."))
            return redirect(url_for("jobs.view_job", job_id=job_id))

        ensure_job_cost_ledger(conn, job_item_id)
        recalc_job(conn, job_id)
        conn.commit()
        conn.close()

        flash(_t("Job item added and bookkeeping updated.", "Artículo del trabajo agregado y contabilidad actualizada."))
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
        if clean_text_input(i["item_type"]).lower() != "dump_fee":
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
                <td class='center'>{_t('Yes', 'Sí') if i['billable'] else _t('No', 'No')}</td>
                <td class='money job-items-revenue'>${safe_float(i['line_total']):.2f}</td>
                <td class='wrap'>
                    <div class='static-actions'>
                        <a class='btn secondary small' href='{url_for("jobs.edit_job_item", job_id=job_id, item_id=i["id"])}#job-items-section'>{_t("Edit", "Editar")}</a>
                        <form method='post'
                              action='{url_for("jobs.delete_job_item", job_id=job_id, item_id=i["id"])}'
                              style='margin:0;'
                              onsubmit="saveJobsScrollPosition('job-items-section'); return confirm('{_t("Delete this job item?", "¿Eliminar este artículo del trabajo?")}');">
                            <input type="hidden" name="csrf_token" value="{delete_item_csrf}">
                            <button class='btn danger small' type='submit'>{_t("Delete", "Eliminar")}</button>
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
                    <div class='mobile-badge'>{_t('Billable', 'Facturable') if i['billable'] else _t('Non-Billable', 'No facturable')}</div>
                </div>

                <div class='mobile-list-grid'>
                    <div><span>{_t("Qty", "Cant.")}</span><strong>{safe_float(i['quantity']):g}</strong></div>
                    <div><span>{_t("Unit", "Unidad")}</span><strong>{escape(clean_text_display(i['unit']))}</strong></div>
                    <div><span>{_t("Sale Price", "Precio de venta")}</span><strong>${safe_float(i['sale_price']):.2f}</strong></div>
                    <div><span>{_t("Unit Cost", "Costo unitario")}</span><strong>{unit_cost_display}</strong></div>
                    <div><span>{_t("Total Cost", "Costo total")}</span><strong>${safe_float(i['cost_amount']):.2f}</strong></div>
                    <div><span>{_t("Revenue", "Ingresos")}</span><strong>${safe_float(i['line_total']):.2f}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <a class='btn secondary small' href='{url_for("jobs.edit_job_item", job_id=job_id, item_id=i["id"])}#job-items-section'>{_t("Edit", "Editar")}</a>
                    <form method='post'
                          action='{url_for("jobs.delete_job_item", job_id=job_id, item_id=i["id"])}'
                          style='margin:0;'
                          onsubmit="saveJobsScrollPosition('job-items-section'); return confirm('{_t("Delete this job item?", "¿Eliminar este artículo del trabajo?")}');">
                        <input type="hidden" name="csrf_token" value="{delete_item_csrf}">
                        <button class='btn danger small' type='submit'>{_t("Delete", "Eliminar")}</button>
                    </form>
                </div>
            </div>
            """
        )

    item_rows = "".join(item_row_list)
    item_mobile_cards = "".join(item_mobile_card_list)

    schedule_bits = []
    if clean_text_input(job["scheduled_date"]):
        schedule_bits.append(f"<strong>{_t('Date', 'Fecha')}:</strong> {escape(clean_text_display(job['scheduled_date']))}")
    if clean_text_input(job["scheduled_start_time"]):
        if clean_text_input(job["scheduled_end_time"]):
            schedule_bits.append(
                f"<strong>{_t('Time', 'Hora')}:</strong> {escape(clean_text_display(job['scheduled_start_time']))} - {escape(clean_text_display(job['scheduled_end_time']))}"
            )
        else:
            schedule_bits.append(f"<strong>{_t('Start', 'Inicio')}:</strong> {escape(clean_text_display(job['scheduled_start_time']))}")
    if clean_text_input(job["assigned_to"]):
        schedule_bits.append(f"<strong>{_t('Assigned To', 'Asignado a')}:</strong> {escape(clean_text_display(job['assigned_to']))}")

    schedule_html = "<br>".join(schedule_bits) if schedule_bits else f"<strong>{_t('Schedule', 'Horario')}:</strong> -"

    customer_email = clean_text_input(job["customer_email"])
    service_type_label = display_service_type(job["service_type"])
    service_type_class = service_type_badge_class(job["service_type"])

    recurring_link_block = ""
    if job["recurring_schedule_id"]:
        recurring_link_block = f"""
        <div class='job-summary-card'>
            <span>{_t("Recurring Schedule", "Programa recurrente")}</span>
            <strong>
                <a href='{url_for("jobs.edit_recurring_schedule", schedule_id=job["recurring_schedule_id"])}'>
                    {_t("Schedule", "Programa")} #{job["recurring_schedule_id"]}
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
            <button class="btn secondary" type="button" onclick="toggleUpdatesMenu(event)">{_t("Updates", "Actualizaciones")} ▼</button>

            <div id="updatesMenu" class="updates-menu">
                <form method="post" action="{url_for("jobs.send_update_email", job_id=job_id)}">
                    <input type="hidden" name="csrf_token" value="{email_csrf_1}">
                    <input type="hidden" name="update_type" value="on_the_way">
                    <button type="submit">{_t("Send On The Way Email", "Enviar correo de En camino")}</button>
                </form>

                <form method="post" action="{url_for("jobs.send_update_email", job_id=job_id)}">
                    <input type="hidden" name="csrf_token" value="{email_csrf_2}">
                    <input type="hidden" name="update_type" value="job_started">
                    <button type="submit">{_t("Send Job Started Email", "Enviar correo de Trabajo iniciado")}</button>
                </form>

                <form method="post" action="{url_for("jobs.send_update_email", job_id=job_id)}">
                    <input type="hidden" name="csrf_token" value="{email_csrf_3}">
                    <input type="hidden" name="update_type" value="job_completed">
                    <button type="submit">{_t("Send Job Finished Email", "Enviar correo de Trabajo terminado")}</button>
                </form>

                <div style="border-top:1px solid #e8ece7;"></div>

                <button type="button" onclick="toggleCustomUpdateCard()">{_t("Compose Custom Update", "Redactar actualización personalizada")}</button>
            </div>
        </div>

        <div id="customUpdateCard" class="card custom-update-card">
            <h3>{_t("Custom Job Update", "Actualización personalizada del trabajo")}</h3>

            <form method="post" action="{url_for("jobs.send_custom_email", job_id=job_id)}">
                <input type="hidden" name="csrf_token" value="{email_csrf_custom}">
                <div class="grid">
                    <div>
                        <label>{_t("To Email", "Correo destinatario")}</label>
                        <input
                            type="email"
                            name="to_email"
                            value="{escape(customer_email)}"
                            placeholder="{escape(_t("Enter customer email", "Ingresa el correo del cliente"))}"
                            required
                        >
                    </div>

                    <div>
                        <label>{_t("Subject", "Asunto")}</label>
                        <input
                            type="text"
                            name="subject"
                            value="{_t('Job Update', 'Actualización del trabajo')} - {escape(clean_text_display(job['title']))}"
                            required
                        >
                    </div>
                </div>

                <div style="margin-top:14px;">
                    <label>{_t("Message", "Mensaje")}</label>
                    <textarea name="message" required>{_t("Hello", "Hola")} {escape(clean_text_display(job['customer_name']))},

{_t("This is an update regarding your job", "Esta es una actualización sobre tu trabajo")} "{escape(clean_text_display(job['title']))}" ({escape(service_type_label)}).

{_t("Thank you", "Gracias")},
{escape(session.get("company_name") or "Your Company")}</textarea>
                </div>

                <div class="row-actions" style="margin-top:12px;">
                    <button class="btn success" type="submit">{_t("Send Email", "Enviar correo")}</button>
                    <button class="btn secondary" type="button" onclick="toggleCustomUpdateCard()">{_t("Cancel", "Cancelar")}</button>
                </div>
            </form>
        </div>
        """
    else:
        email_csrf_custom_empty = generate_csrf()
        email_buttons = """
        <div class='muted small'>{no_email_text}</div>
        <div id="customUpdateCard" class="card custom-update-card" style="display:block; margin-top:14px;">
            <h3>{custom_update_title}</h3>
            <div class="muted small" style="margin-bottom:12px;">{manual_email_text}</div>

            <form method="post" action="{send_custom_url}">
                <input type="hidden" name="csrf_token" value="{csrf_token_value}">
                <div class="grid">
                    <div>
                        <label>{to_email_label}</label>
                        <input
                            type="email"
                            name="to_email"
                            value=""
                            placeholder="{email_placeholder}"
                            required
                        >
                    </div>

                    <div>
                        <label>{subject_label}</label>
                        <input
                            type="text"
                            name="subject"
                            value="{job_update_label} - {job_title}"
                            required
                        >
                    </div>
                </div>

                <div style="margin-top:14px;">
                    <label>{message_label}</label>
                    <textarea name="message" required>{hello_label} {customer_name},

{update_text} "{job_title}" ({service_type_label}).

{thank_you_label},
{company_name}</textarea>
                </div>

                <div class="row-actions" style="margin-top:12px;">
                    <button class="btn success" type="submit">{send_email_label}</button>
                </div>
            </form>
        </div>
        """.format(
            no_email_text=_t("Add a customer email address to send job updates.", "Agrega un correo del cliente para enviar actualizaciones del trabajo."),
            custom_update_title=_t("Custom Job Update", "Actualización personalizada del trabajo"),
            manual_email_text=_t("No customer email is on file, but you can still enter one manually below.", "No hay un correo del cliente guardado, pero aún puedes escribir uno manualmente abajo."),
            send_custom_url=url_for("jobs.send_custom_email", job_id=job_id),
            csrf_token_value=email_csrf_custom_empty,
            to_email_label=_t("To Email", "Correo destinatario"),
            email_placeholder=_t("Enter recipient email", "Ingresa el correo del destinatario"),
            subject_label=_t("Subject", "Asunto"),
            job_update_label=_t("Job Update", "Actualización del trabajo"),
            job_title=escape(clean_text_display(job["title"])),
            message_label=_t("Message", "Mensaje"),
            hello_label=_t("Hello", "Hola"),
            customer_name=escape(clean_text_display(job["customer_name"])),
            update_text=_t("This is an update regarding your job", "Esta es una actualización sobre tu trabajo"),
            service_type_label=escape(service_type_label),
            thank_you_label=_t("Thank you", "Gracias"),
            company_name=escape(session.get("company_name") or "Your Company"),
            send_email_label=_t("Send Email", "Enviar correo"),
        )

    add_item_csrf = generate_csrf()

    invoice_action_html = ""
    if existing_invoice:
        invoice_label = clean_text_display(existing_invoice["invoice_number"]) or f"#{existing_invoice['id']}"
        invoice_action_html = f"""
            <a class='btn secondary' href='{url_for("invoices.view_invoice", invoice_id=existing_invoice["id"])}'>
                {_t("View Invoice", "Ver factura")} {escape(invoice_label)}
            </a>
        """
    else:
        invoice_button_text = _t("Convert to Invoice", "Convertir en factura")
        if job["recurring_schedule_id"]:
            invoice_button_text = _t("Invoice This Visit", "Facturar esta visita")

        invoice_action_html = f"""
            <a class='btn success' href='{url_for("jobs.convert_job_to_invoice", job_id=job_id)}'>
                {invoice_button_text}
            </a>
        """

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
                <h1>{_t("Job", "Trabajo")} #{job['id']} - {escape(clean_text_display(job['title']))}</h1>

                <div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
                    <span class='service-chip {service_type_class}'>{escape(service_type_label)}</span>
                    {'<span class="service-chip mowing">' + _t("Recurring", "Recurrente") + '</span>' if job["recurring_schedule_id"] else ''}
                </div>

                <div class='job-summary-grid'>
                    <div class='job-summary-card'>
                        <span>{_t("Customer", "Cliente")}</span>
                        <strong>{escape(clean_text_display(job['customer_name']))}</strong>
                    </div>
                    <div class='job-summary-card'>
                        <span>{_t("Email", "Correo")}</span>
                        <strong>{escape(clean_text_display(job['customer_email']))}</strong>
                    </div>
                    <div class='job-summary-card'>
                        <span>{_t("Status", "Estado")}</span>
                        <strong>{escape(clean_text_display(job['status']))}</strong>
                    </div>
                    <div class='job-summary-card'>
                        <span>{_t("Service Type", "Tipo de servicio")}</span>
                        <strong>{escape(service_type_label)}</strong>
                    </div>
                    <div class='job-summary-card'>
                        <span>{_t("Schedule", "Horario")}</span>
                        <strong>{schedule_html.replace("<br>", " | ")}</strong>
                    </div>
                    {recurring_link_block}
                </div>

                <div class='job-financials-grid'>
                    <div class='job-financial-card'>
                        <span>{_t("Revenue", "Ingresos")}</span>
                        <strong>${safe_float(job['revenue']):.2f}</strong>
                    </div>
                    <div class='job-financial-card'>
                        <span>{_t("Costs", "Costos")}</span>
                        <strong>${safe_float(job['cost_total']):.2f}</strong>
                    </div>
                    <div class='job-financial-card'>
                        <span>{_t("Profit/Loss", "Ganancia/Pérdida")}</span>
                        <strong>${safe_float(job['profit']):.2f}</strong>
                    </div>
                </div>

                <div class="row-actions" style="margin-top:14px;">
                    <a class='btn secondary' href='{url_for("jobs.jobs")}'>{_t("Done Editing", "Terminar edición")}</a>
                    <a class='btn warning' href='{url_for("jobs.edit_job", job_id=job_id)}'>{_t("Edit Job", "Editar trabajo")}</a>
                    {invoice_action_html}
                </div>

                <div class="row-actions" style="margin-top:12px;">
                    {email_buttons}
                </div>
            </div>

            <div class='card' id='add-job-item-section'>
                <h2>{_t("Add Job Item", "Agregar artículo del trabajo")}</h2>
                <p class='muted'>{_t("Any cost you enter here is automatically pushed into bookkeeping as an expense.", "Cualquier costo que ingreses aquí se envía automáticamente a contabilidad como gasto.")}</p>

                <form method='post' onsubmit="saveJobsScrollPosition('job-items-section');">
                    <input type="hidden" name="csrf_token" value="{add_item_csrf}">
                    <div class='grid'>

                        <div>
                            <label>{_t("Type", "Tipo")}</label>
                            <select name='item_type' id='item_type' onchange='toggleJobItemMode()'>
                                <option value='mulch'>{_t("Mulch", "Mantillo")}</option>
                                <option value='stone'>{_t("Stone", "Piedra")}</option>
                                <option value='dump_fee'>{_t("Dump Fee", "Tarifa de vertedero")}</option>
                                <option value='plants'>{_t("Plants", "Plantas")}</option>
                                <option value='trees'>{_t("Trees", "Árboles")}</option>
                                <option value='soil'>{_t("Soil", "Tierra")}</option>
                                <option value='fertilizer'>{_t("Fertilizer", "Fertilizante")}</option>
                                <option value='hardscape_material'>{_t("Hardscape Material", "Material de hardscape")}</option>
                                <option value='labor'>{_t("Labor", "Mano de obra")}</option>
                                <option value='equipment'>{_t("Equipment", "Equipo")}</option>
                                <option value='delivery'>{_t("Delivery", "Entrega")}</option>
                                <option value='fuel'>{_t("Fuel", "Combustible")}</option>
                                <option value='misc'>{_t("Misc", "Varios")}</option>
                            </select>
                        </div>

                        <div>
                            <label>{_t("Description", "Descripción")}</label>
                            <input name='description' required>
                        </div>

                        <div>
                            <label id='quantity_label'>{_t("Quantity", "Cantidad")}</label>
                            <input type='number' step='0.01' name='quantity' id='quantity' required>
                        </div>

                        <div>
                            <label>{_t("Unit", "Unidad")}</label>
                            <input name='unit' id='unit' placeholder='{escape(_t("Unit", "Unidad"))}'>
                        </div>

                        <div id='sale_price_wrap'>
                            <label id='sale_price_label'>{_t("Sale Price", "Precio de venta")}</label>
                            <input type='number' step='0.01' name='sale_price' id='sale_price' value='0' required>
                        </div>

                        <div id='unit_cost_wrap'>
                            <label id='cost_label'>{_t("Unit Cost", "Costo unitario")}</label>
                            <input type='number' step='0.01' name='unit_cost' id='unit_cost' value='0'>
                        </div>

                        <div>
                            <label>{_t("Billable?", "¿Facturable?")}</label>
                            <select name='billable'>
                                <option value='1'>{_t("Yes", "Sí")}</option>
                                <option value='0'>{_t("No", "No")}</option>
                            </select>
                        </div>

                    </div>

                    <br>
                    <button class='btn' type='submit'>{_t("Add Job Item", "Agregar artículo del trabajo")}</button>
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

                quantityLabel.innerText = '{_t("Quantity", "Cantidad")}';
                salePriceLabel.innerText = '{_t("Sale Price", "Precio de venta")}';
                costLabel.innerText = '{_t("Unit Cost", "Costo unitario")}';
                if (salePriceWrap) salePriceWrap.style.display = 'block';
                if (unitCostWrap) unitCostWrap.style.display = 'block';

                if (quantityInput) {{
                    quantityInput.readOnly = false;
                    quantityInput.step = '0.01';
                }}

                if (unitInput) unitInput.value = '';

                if (type === 'mulch') {{
                    quantityLabel.innerText = '{_t("Yards", "Yardas")}';
                    unitInput.value = '{_t("Yards", "Yardas")}';
                }} else if (type === 'stone') {{
                    quantityLabel.innerText = '{_t("Tons", "Toneladas")}';
                    unitInput.value = '{_t("Tons", "Toneladas")}';
                }} else if (type === 'soil') {{
                    quantityLabel.innerText = '{_t("Yards", "Yardas")}';
                    unitInput.value = '{_t("Yards", "Yardas")}';
                }} else if (type === 'hardscape_material') {{
                    quantityLabel.innerText = '{_t("Tons", "Toneladas")}';
                    unitInput.value = '{_t("Tons", "Toneladas")}';
                }} else if (type === 'fuel') {{
                    quantityLabel.innerText = '{_t("Gallons", "Galones")}';
                    unitInput.value = '{_t("Gallons", "Galones")}';
                }} else if (type === 'delivery') {{
                    quantityLabel.innerText = '{_t("Miles", "Millas")}';
                    unitInput.value = '{_t("Miles", "Millas")}';
                }} else if (type === 'labor') {{
                    quantityLabel.innerText = '{_t("Billable Hours", "Horas facturables")}';
                    salePriceLabel.innerText = '{_t("Hourly Rate", "Tarifa por hora")}';
                    costLabel.innerText = '{_t("Hourly Cost", "Costo por hora")}';
                    unitInput.value = '{_t("Hours", "Horas")}';
                    if (unitCostWrap) unitCostWrap.style.display = 'block';
                }} else if (type === 'equipment') {{
                    quantityLabel.innerText = '{_t("Rentals", "Alquileres")}';
                    unitInput.value = '{_t("Rentals", "Alquileres")}';
                }} else if (type === 'plants' || type === 'trees' || type === 'misc') {{
                    quantityLabel.innerText = '{_t("Quantity", "Cantidad")}';
                    unitInput.value = '';
                }} else if (type === 'dump_fee') {{
                    quantityLabel.innerText = '{_t("Fee", "Tarifa")}';
                    salePriceLabel.innerText = '{_t("Fee Amount", "Monto de la tarifa")}';
                    unitInput.value = '';
                    if (unitCostWrap) unitCostWrap.style.display = 'none';
                    if (unitCostInput) unitCostInput.value = '0';
                    if (quantityInput) {{
                        quantityInput.value = '1';
                        quantityInput.readOnly = true;
                    }}
                }} else if (type === 'fertilizer') {{
                    quantityLabel.innerText = '{_t("Quantity", "Cantidad")}';
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
                <h2>{_t("Job Items", "Artículos del trabajo")}</h2>

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
                            <th>{_t("Type", "Tipo")}</th>
                            <th class='wrap'>{_t("Description", "Descripción")}</th>
                            <th class='money'>{_t("Qty", "Cant.")}</th>
                            <th>{_t("Unit", "Unidad")}</th>
                            <th class='money'>{_t("Sale Price", "Precio de venta")}</th>
                            <th class='money'>{_t("Unit Cost", "Costo unitario")}</th>
                            <th class='money'>{_t("Total Cost", "Costo total")}</th>
                            <th class='center'>{_t("Billable", "Facturable")}</th>
                            <th class='money'>{_t("Revenue", "Ingresos")}</th>
                            <th class='wrap'>{_t("Actions", "Acciones")}</th>
                        </tr>
                        {item_rows or f'<tr><td colspan="10" class="muted">{_t("No job items yet.", "Todavía no hay artículos del trabajo.")}</td></tr>'}
                    </table>
                </div>

                <div class='mobile-only'>
                    <div class='mobile-list'>
                        {item_mobile_cards or f"<div class='mobile-list-card'>{_t('No job items yet.', 'Todavía no hay artículos del trabajo.')}</div>"}
                    </div>
                </div>
            </div>
        </div>
        """
    return render_page(content, f"{_t('Job', 'Trabajo')} #{job_id}")

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
        flash(_t("This customer does not have an email address.", "Este cliente no tiene una dirección de correo."))
        return redirect(url_for("jobs.view_job", job_id=job_id))

    update_type = clean_text_input(request.form.get("update_type", ""))
    if update_type not in {"on_the_way", "job_started", "job_completed"}:
        flash(_t("Invalid email update type.", "Tipo de actualización por correo no válido."))
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
            flash(_t("On the way email sent.", "Correo de en camino enviado."))
        elif update_type == "job_started":
            flash(_t("Job started email sent.", "Correo de trabajo iniciado enviado."))
        elif update_type == "job_completed":
            flash(_t("Job completed email sent.", "Correo de trabajo completado enviado."))
        else:
            flash(_t("Job update email sent.", "Correo de actualización del trabajo enviado."))
    else:
        flash(_t(f"Could not send email: {error_message}", f"No se pudo enviar el correo: {error_message}"))

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
        flash(_t("Recipient email is required.", "El correo del destinatario es obligatorio."))
        return redirect(url_for("jobs.view_job", job_id=job_id))

    if not subject:
        flash(_t("Email subject is required.", "El asunto del correo es obligatorio."))
        return redirect(url_for("jobs.view_job", job_id=job_id))

    if not message:
        flash(_t("Email message is required.", "El mensaje del correo es obligatorio."))
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
        flash(_t("Custom job update email sent.", "Correo personalizado de actualización del trabajo enviado."))
    except Exception as e:
        flash(_t(f"Could not send email: {e}", f"No se pudo enviar el correo: {e}"))

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
        flash(_t("Job not found.", "Trabajo no encontrado."))
        return redirect(url_for("jobs.jobs"))

    customers = conn.execute(
        """
        SELECT id, name
        FROM customers
        WHERE company_id = %s
        ORDER BY name
        """,
        (cid,),
    ).fetchall()

    if request.method == "POST":
        customer_id = request.form.get("customer_id", type=int)
        title = clean_text_input(request.form.get("title"))
        service_type = normalize_service_type(request.form.get("service_type"))
        scheduled_date = clean_text_input(request.form.get("scheduled_date"))
        scheduled_start_time = clean_text_input(request.form.get("scheduled_start_time"))
        scheduled_end_time = clean_text_input(request.form.get("scheduled_end_time"))
        assigned_to = clean_text_input(request.form.get("assigned_to"))
        status = clean_text_input(request.form.get("status")) or _t("Scheduled", "Programado")
        address = clean_text_input(request.form.get("address"))
        notes = clean_text_input(request.form.get("notes"))

        if not customer_id:
            conn.close()
            flash(_t("Please select a customer.", "Selecciona un cliente."))
            return redirect(url_for("jobs.edit_job", job_id=job_id))

        if not title:
            conn.close()
            flash(_t("Job title is required.", "El título del trabajo es obligatorio."))
            return redirect(url_for("jobs.edit_job", job_id=job_id))

        conflict = check_schedule_conflict(
            conn=conn,
            company_id=cid,
            scheduled_date=scheduled_date,
            start_time=scheduled_start_time,
            end_time=scheduled_end_time,
            assigned_to=assigned_to,
            exclude_job_id=job_id,
        )

        if conflict:
            conn.close()
            flash(
                _t(
                    f"Schedule conflict: '{conflict['title']}' is already scheduled for {assigned_to} from {conflict['start']} to {conflict['end']}.",
                    f"Conflicto de horario: '{conflict['title']}' ya está programado para {assigned_to} de {conflict['start']} a {conflict['end']}.",
                )
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
                service_type or None,
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

        recalc_job(conn, job_id)
        conn.commit()
        conn.close()

        flash(_t("Job updated.", "Trabajo actualizado."))
        return redirect(url_for("jobs.view_job", job_id=job_id))

    customer_opts = "".join(
        f"<option value='{c['id']}' {'selected' if c['id'] == job['customer_id'] else ''}>{escape(clean_text_display(c['name'], _t('Customer', 'Cliente') + ' #' + str(c['id'])))}</option>"
        for c in customers
    )

    edit_job_csrf = generate_csrf()

    recurring_note = ""
    if "recurring_schedule_id" in job.keys() and job["recurring_schedule_id"]:
        recurring_note = f"""
        <div class="card" style="margin-bottom:16px;">
            <strong>{_t("This job was generated from recurring schedule", "Este trabajo fue generado desde el programa recurrente")} #{job["recurring_schedule_id"]}.</strong><br>
            <a href="{url_for("jobs.edit_recurring_schedule", schedule_id=job["recurring_schedule_id"])}">{_t("Edit recurring schedule", "Editar programa recurrente")}</a>
        </div>
        """

    content = f"""
    {recurring_note}
    <div class='card'>
        <h1>{_t("Edit Job", "Editar trabajo")} #{job['id']}</h1>
        <form method='post'>
            <input type="hidden" name="csrf_token" value="{edit_job_csrf}">
            <div class='grid'>
                <div>
                    <label>{_t("Customer", "Cliente")}</label>
                    <select name='customer_id' required>
                        <option value=''>{_t("Select customer", "Selecciona cliente")}</option>
                        {customer_opts}
                    </select>
                </div>
                <div>
                    <label>{_t("Title", "Título")}</label>
                    <input name='title' value="{escape(clean_text_input(job['title']))}" required>
                </div>
                <div>
                    <label>{_t("Service Type", "Tipo de servicio")}</label>
                    <select name='service_type'>
                        {service_type_select_options(job['service_type'])}
                    </select>
                </div>
                <div>
                    <label>{_t("Scheduled Date", "Fecha programada")}</label>
                    <input type='date' name='scheduled_date' value="{escape(clean_text_input(job['scheduled_date']))}">
                </div>
                <div>
                    <label>{_t("Start Time", "Hora de inicio")}</label>
                    <input type='time' name='scheduled_start_time' value="{escape(clean_text_input(job['scheduled_start_time']))}">
                </div>
                <div>
                    <label>{_t("End Time", "Hora de fin")}</label>
                    <input type='time' name='scheduled_end_time' value="{escape(clean_text_input(job['scheduled_end_time']))}">
                </div>
                <div>
                    <label>{_t("Assigned To", "Asignado a")}</label>
                    <input name='assigned_to' value="{escape(clean_text_input(job['assigned_to']))}">
                </div>
                <div>
                    <label>{_t("Status", "Estado")}</label>
                    <select name='status'>
                        <option {'selected' if job['status'] == _t('Scheduled', 'Programado') or job['status'] == 'Scheduled' else ''}>{_t("Scheduled", "Programado")}</option>
                        <option {'selected' if job['status'] == _t('In Progress', 'En progreso') or job['status'] == 'In Progress' else ''}>{_t("In Progress", "En progreso")}</option>
                        <option {'selected' if job['status'] == _t('Completed', 'Completado') or job['status'] == 'Completed' else ''}>{_t("Completed", "Completado")}</option>
                        <option {'selected' if job['status'] == _t('Invoiced', 'Facturado') or job['status'] == 'Invoiced' else ''}>{_t("Invoiced", "Facturado")}</option>
                        <option {'selected' if job['status'] == _t('Finished', 'Terminado') or job['status'] == 'Finished' else ''}>{_t("Finished", "Terminado")}</option>
                    </select>
                </div>
                <div>
                    <label>{_t("Address", "Dirección")}</label>
                    <input name='address' value="{escape(clean_text_input(job['address']))}">
                </div>
            </div>
            <br>
            <label>{_t("Notes", "Notas")}</label>
            <textarea name='notes'>{escape(clean_text_input(job['notes']))}</textarea>
            <br>
            <button class='btn'>{_t("Save Changes", "Guardar cambios")}</button>
            <a class='btn secondary' href='{url_for("jobs.view_job", job_id=job_id)}'>{_t("Cancel", "Cancelar")}</a>
        </form>
    </div>
    """

    conn.close()
    return render_page(content, f"{_t('Edit Job', 'Editar trabajo')} #{job['id']}")

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
        flash(_t("Job item not found.", "Artículo del trabajo no encontrado."))
        return redirect(url_for("jobs.view_job", job_id=job_id))

    if request.method == "POST":
        item_type = clean_text_input(request.form.get("item_type", "")).lower()
        description = clean_text_input(request.form.get("description", ""))
        unit = clean_text_input(request.form.get("unit", ""))
        qty = safe_float(request.form.get("quantity"))
        sale_price = safe_float(request.form.get("sale_price"))
        unit_cost = safe_float(request.form.get("unit_cost"))
        billable = request.form.get("billable") == "1"

        if not description:
            conn.close()
            flash(_t("Description is required.", "La descripción es obligatoria."))
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

        if item_type == "dump_fee":
            unit = ""
            if qty <= 0:
                qty = 1
            unit_cost = 0.0

        line_total = qty * sale_price if billable else 0.0
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

        flash(_t("Job item updated.", "Artículo del trabajo actualizado."))
        return redirect(url_for("jobs.view_job", job_id=job_id))

    item_type_val = clean_text_input(item["item_type"]).lower()
    qty_val = safe_float(item["quantity"])
    sale_price_val = safe_float(item["sale_price"])
    unit_cost_val = (safe_float(item["cost_amount"]) / safe_float(item["quantity"])) if safe_float(item["quantity"]) else 0
    hide_cost = item_type_val in ["dump_fee", "labor"]
    edit_item_csrf = generate_csrf()

    content = f"""
    <div class='card'>
        <h1>{_t("Edit Job Item", "Editar artículo del trabajo")}</h1>
        <p>
            <strong>{_t("Job", "Trabajo")}:</strong> #{job['id']} - {escape(clean_text_display(job['title']))}<br>
            <strong>{_t("Customer", "Cliente")}:</strong> {escape(clean_text_display(job['customer_name']))}
        </p>

        <form method='post'>
            <input type="hidden" name="csrf_token" value="{edit_item_csrf}">
            <div class='grid'>
                <div>
                    <label>{_t("Type", "Tipo")}</label>
                    <select name='item_type' id='edit_item_type' onchange='toggleEditJobItemMode()'>
                        <option value='mulch' {'selected' if item_type_val == 'mulch' else ''}>{_t("Mulch", "Mantillo")}</option>
                        <option value='stone' {'selected' if item_type_val == 'stone' else ''}>{_t("Stone", "Piedra")}</option>
                        <option value='dump_fee' {'selected' if item_type_val == 'dump_fee' else ''}>{_t("Dump Fee", "Tarifa de vertedero")}</option>
                        <option value='plants' {'selected' if item_type_val == 'plants' else ''}>{_t("Plants", "Plantas")}</option>
                        <option value='trees' {'selected' if item_type_val == 'trees' else ''}>{_t("Trees", "Árboles")}</option>
                        <option value='soil' {'selected' if item_type_val == 'soil' else ''}>{_t("Soil", "Tierra")}</option>
                        <option value='fertilizer' {'selected' if item_type_val == 'fertilizer' else ''}>{_t("Fertilizer", "Fertilizante")}</option>
                        <option value='hardscape_material' {'selected' if item_type_val == 'hardscape_material' else ''}>{_t("Hardscape Material", "Material de hardscape")}</option>
                        <option value='labor' {'selected' if item_type_val == 'labor' else ''}>{_t("Labor", "Mano de obra")}</option>
                        <option value='equipment' {'selected' if item_type_val == 'equipment' else ''}>{_t("Equipment", "Equipo")}</option>
                        <option value='delivery' {'selected' if item_type_val == 'delivery' else ''}>{_t("Delivery", "Entrega")}</option>
                        <option value='fuel' {'selected' if item_type_val == 'fuel' else ''}>{_t("Fuel", "Combustible")}</option>
                        <option value='misc' {'selected' if item_type_val == 'misc' else ''}>{_t("Misc", "Varios")}</option>
                    </select>
                </div>

                <div>
                    <label>{_t("Description", "Descripción")}</label>
                    <input name='description' value="{escape(clean_text_input(item['description']))}" required>
                </div>

                <div>
                    <label id='edit_quantity_label'>{_t("Quantity", "Cantidad")}</label>
                    <input type='number' step='0.01' name='quantity' id='edit_quantity' value="{qty_val:.2f}" required>
                </div>

                <div>
                    <label>{_t("Unit", "Unidad")}</label>
                    <input name='unit' id='edit_unit' value="{escape(clean_text_input(item['unit']))}">
                </div>

                <div id='edit_sale_price_wrap'>
                    <label id='edit_sale_price_label'>{_t("Sale Price", "Precio de venta")}</label>
                    <input type='number' step='0.01' name='sale_price' id='edit_sale_price' value="{sale_price_val:.2f}">
                </div>

                <div id='edit_unit_cost_wrap' style="display:{'none' if hide_cost else 'block'};">
                    <label id='edit_cost_label'>{_t("Unit Cost", "Costo unitario")}</label>
                    <input type='number' step='0.01' name='unit_cost' id='edit_unit_cost' value="{unit_cost_val:.2f}">
                </div>

                <div>
                    <label>{_t("Billable?", "¿Facturable?")}</label>
                    <select name='billable'>
                        <option value='1' {'selected' if item['billable'] else ''}>{_t("Yes", "Sí")}</option>
                        <option value='0' {'selected' if not item['billable'] else ''}>{_t("No", "No")}</option>
                    </select>
                </div>
            </div>

            <br>
            <button class='btn'>{_t("Save Changes", "Guardar cambios")}</button>
            <a class='btn secondary' href='{url_for("jobs.view_job", job_id=job_id)}'>{_t("Cancel", "Cancelar")}</a>
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

        quantityLabel.innerText = '{_t("Quantity", "Cantidad")}';
        salePriceLabel.innerText = '{_t("Sale Price", "Precio de venta")}';
        costLabel.innerText = '{_t("Unit Cost", "Costo unitario")}';
        if (salePriceWrap) salePriceWrap.style.display = 'block';
        if (unitCostWrap) unitCostWrap.style.display = 'block';

        if (quantityInput) {{
            quantityInput.readOnly = false;
            quantityInput.step = '0.01';
        }}

        if (unitInput) unitInput.value = '';

        if (type === 'mulch') {{
            quantityLabel.innerText = '{_t("Yards", "Yardas")}';
            unitInput.value = 'Yards';
        }} else if (type === 'stone') {{
            quantityLabel.innerText = '{_t("Tons", "Toneladas")}';
            unitInput.value = 'Tons';
        }} else if (type === 'soil') {{
            quantityLabel.innerText = '{_t("Yards", "Yardas")}';
            unitInput.value = 'Yards';
        }} else if (type === 'hardscape_material') {{
            quantityLabel.innerText = '{_t("Tons", "Toneladas")}';
            unitInput.value = 'Tons';
        }} else if (type === 'fuel') {{
            quantityLabel.innerText = '{_t("Gallons", "Galones")}';
            unitInput.value = 'Gallons';
        }} else if (type === 'delivery') {{
            quantityLabel.innerText = '{_t("Miles", "Millas")}';
            unitInput.value = 'Miles';
        }} else if (type === 'labor') {{
            quantityLabel.innerText = '{_t("Billable Hours", "Horas facturables")}';
            salePriceLabel.innerText = '{_t("Hourly Rate", "Tarifa por hora")}';
            costLabel.innerText = '{_t("Hourly Cost", "Costo por hora")}';
            unitInput.value = 'Hours';
            if (unitCostWrap) unitCostWrap.style.display = 'block';
        }} else if (type === 'equipment') {{
            quantityLabel.innerText = '{_t("Rentals", "Alquileres")}';
            unitInput.value = 'Rentals';
        }} else if (type === 'plants' || type === 'trees' || type === 'misc') {{
            quantityLabel.innerText = '{_t("Quantity", "Cantidad")}';
            unitInput.value = '';
        }} else if (type === 'dump_fee') {{
            quantityLabel.innerText = '{_t("Fee", "Tarifa")}';
            salePriceLabel.innerText = '{_t("Fee Amount", "Importe de la tarifa")}';
            unitInput.value = '';
            if (unitCostWrap) unitCostWrap.style.display = 'none';
            if (unitCostInput) unitCostInput.value = '0';
            if (quantityInput) {{
                quantityInput.value = '1';
                quantityInput.readOnly = true;
            }}
        }} else if (type === 'fertilizer') {{
            quantityLabel.innerText = '{_t("Quantity", "Cantidad")}';
            unitInput.value = '';
        }}
    }}

    document.addEventListener('DOMContentLoaded', function() {{
        toggleEditJobItemMode();
    }});
    </script>
    """

    conn.close()
    return render_page(content, f"{_t('Edit Job Item', 'Editar artículo del trabajo')} #{item_id}")


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
    flash(_t("Job item deleted.", "Artículo del trabajo eliminado."))
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
            flash(_t("This job has already been converted to an invoice.", "Este trabajo ya fue convertido en factura."))
            return redirect(url_for("invoices.view_invoice", invoice_id=existing_invoice["id"]))

        items = conn.execute(
            """
            SELECT *
            FROM job_items
            WHERE job_id = %s
              AND COALESCE(billable, TRUE) = TRUE
            ORDER BY id
            """,
            (job_id,),
        ).fetchall()

        if not items:
            flash(_t("This job has no billable items to invoice.", "Este trabajo no tiene artículos facturables para facturar."))
            return redirect(url_for("jobs.view_job", job_id=job_id))

        invoice_date = date.today().isoformat()
        due_date = invoice_date
        notes = clean_text_input(job["notes"]) if "notes" in job.keys() and job["notes"] else ""
        invoice_number = f"INV-{int(datetime.now().timestamp())}"

        service_type = clean_text_input(job["service_type"]).lower() if "service_type" in job.keys() else ""
        job_title = clean_text_input(job["title"]) or _t("Service", "Servicio")
        job_address = clean_text_input(job["address"]) if "address" in job.keys() else ""
        scheduled_date = clean_text_input(job["scheduled_date"]) if "scheduled_date" in job.keys() else ""

        item_descriptions = []
        for i in items:
            desc = clean_text_input(i["description"]) if i["description"] else ""
            if desc:
                item_descriptions.append(desc)

        deduped_descriptions = []
        seen = set()
        for desc in item_descriptions:
            key = desc.lower()
            if key not in seen:
                seen.add(key)
                deduped_descriptions.append(desc)

        if service_type == "mowing":
            summary_parts = [_t("Mowing service completed", "Servicio de corte completado")]
            if job_address:
                summary_parts.append(_t(f"at {job_address}", f"en {job_address}"))
            if scheduled_date:
                summary_parts.append(_t(f"on {scheduled_date}", f"el {scheduled_date}"))

            summary_description = " ".join(summary_parts).strip()
            if not summary_description.endswith("."):
                summary_description += "."

            if deduped_descriptions:
                summary_description += f" {_t('Included', 'Incluye')} {', '.join(deduped_descriptions[:5])}."
            else:
                summary_description += f" {_t('Included service from job', 'Incluye servicio del trabajo')} '{job_title}'."

            display_mode = "summary_only"
        else:
            summary_parts = [job_title]
            if job_address:
                summary_parts.append(_t(f"at {job_address}", f"en {job_address}"))
            if scheduled_date:
                summary_parts.append(_t(f"on {scheduled_date}", f"el {scheduled_date}"))

            summary_description = " ".join(summary_parts).strip()
            if not summary_description.endswith("."):
                summary_description += "."

            if deduped_descriptions:
                summary_description += f" {_t('Included', 'Incluye')} {', '.join(deduped_descriptions[:5])}."

            display_mode = "summary_only"

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
                balance_due,
                display_mode,
                summary_description
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                _t("Unpaid", "No pagada"),
                notes,
                0,
                0,
                display_mode,
                summary_description,
            ),
        )

        row = cur.fetchone()
        if not row or "id" not in row:
            raise Exception(_t("Failed to create invoice record.", "No se pudo crear el registro de la factura."))

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
            (_t("Invoiced", "Facturado"), job_id, cid),
        )

        conn.commit()
        flash(_t("Job converted to invoice.", "Trabajo convertido en factura."))
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    except Exception as e:
        conn.rollback()
        flash(_t(f"Could not convert job to invoice: {e}", f"No se pudo convertir el trabajo en factura: {e}"))
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
    flash(_t("Job deleted.", "Trabajo eliminado."))
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
          AND (j.status = 'Finished' OR j.status = 'Terminado')
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
            recurring_note = f"<div class='small muted' style='margin-top:4px;'>{_t('Recurring', 'Recurrente')}: {_t('Schedule', 'Programa')} #{r['recurring_schedule_id']}</div>"

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
                        <a class='btn secondary small' href='{url_for("jobs.view_job", job_id=r["id"])}'>{_t("View", "Ver")}</a>
                        <a class='btn warning small' href='{url_for("jobs.reopen_job", job_id=r["id"])}'>{_t("Reopen", "Reabrir")}</a>
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
                    {'<span class="service-chip mowing">' + escape(_t("Recurring", "Recurrente")) + '</span>' if r["recurring_schedule_id"] else ''}
                </div>

                <div class='mobile-list-grid'>
                    <div><span>{_t("Customer", "Cliente")}</span><strong>{escape(clean_text_display(r['customer_name']))}</strong></div>
                    <div><span>{_t("Date", "Fecha")}</span><strong>{escape(clean_text_display(r['scheduled_date']))}</strong></div>
                    <div><span>{_t("Start", "Inicio")}</span><strong>{escape(clean_text_display(r['scheduled_start_time']))}</strong></div>
                    <div><span>{_t("End", "Fin")}</span><strong>{escape(clean_text_display(r['scheduled_end_time']))}</strong></div>
                    <div><span>{_t("Assigned To", "Asignado a")}</span><strong>{escape(clean_text_display(r['assigned_to']))}</strong></div>
                    <div><span>{_t("Revenue", "Ingresos")}</span><strong>${safe_float(r['revenue']):.2f}</strong></div>
                    <div><span>{_t("Costs", "Costos")}</span><strong>${safe_float(r['cost_total']):.2f}</strong></div>
                    <div><span>{_t("Profit/Loss", "Ganancia/Pérdida")}</span><strong>${safe_float(r['profit']):.2f}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <a class='btn secondary small' href='{url_for("jobs.view_job", job_id=r["id"])}'>{_t("View", "Ver")}</a>
                    <a class='btn warning small' href='{url_for("jobs.reopen_job", job_id=r["id"])}'>{_t("Reopen", "Reabrir")}</a>
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
                <h1 style='margin:0;'>{_t("Finished Jobs", "Trabajos terminados")}</h1>
                <p class='muted' style='margin:6px 0 0 0;'>{_t("Completed and fully paid jobs.", "Trabajos completados y totalmente pagados.")}</p>
            </div>
            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("jobs.jobs")}'>{_t("Back to Active Jobs", "Volver a trabajos activos")}</a>
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
                    <th class='wrap'>{_t("Title", "Título")}</th>
                    <th>{_t("Service", "Servicio")}</th>
                    <th class='wrap'>{_t("Customer", "Cliente")}</th>
                    <th>{_t("Date", "Fecha")}</th>
                    <th>{_t("Start", "Inicio")}</th>
                    <th>{_t("End", "Fin")}</th>
                    <th class='wrap'>{_t("Assigned To", "Asignado a")}</th>
                    <th>{_t("Status", "Estado")}</th>
                    <th class='money'>{_t("Revenue", "Ingresos")}</th>
                    <th class='money'>{_t("Costs", "Costos")}</th>
                    <th class='money'>{_t("Profit/Loss", "Ganancia/Pérdida")}</th>
                    <th class='wrap'>{_t("Actions", "Acciones")}</th>
                </tr>
                {job_rows or f'<tr><td colspan="13" class="muted">{_t("No finished jobs yet.", "Todavía no hay trabajos terminados.")}</td></tr>'}
            </table>
        </div>

        <div class='mobile-only'>
            <div class='mobile-list'>
                {mobile_cards_html or f"<div class='mobile-list-card'>{_t('No finished jobs yet.', 'Todavía no hay trabajos terminados.')}</div>"}
            </div>
        </div>
    </div>
    """
    return render_page(content, _t("Finished Jobs", "Trabajos terminados"))


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
        flash(_t("Job not found.", "Trabajo no encontrado."))
        return redirect(url_for("jobs.finished_jobs"))

    conn.execute(
        """
        UPDATE jobs
        SET status = %s
        WHERE id = %s AND company_id = %s
        """,
        (_t("Invoiced", "Facturado"), job_id, cid),
    )

    conn.commit()
    conn.close()

    flash(_t("Job reopened.", "Trabajo reabierto."))
    return redirect(url_for("jobs.view_job", job_id=job_id))