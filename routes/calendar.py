# TerraLedger/routes/calendar.py

from flask import Blueprint, request, session, render_template_string, url_for, redirect, flash
from datetime import date, datetime, timedelta
from html import escape
import calendar as pycalendar

from db import get_db_connection
from decorators import login_required, require_permission, subscription_required

calendar_bp = Blueprint("calendar", __name__)


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


def _dict_row(row):
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    try:
        return dict(row)
    except Exception:
        try:
            return {k: row[k] for k in row.keys()}
        except Exception:
            return {}


def _safe_text(value, default=""):
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _normalize_service_type(value):
    key = _safe_text(value).lower().replace("-", "_").replace(" ", "_")
    return key if key in SERVICE_TYPE_LABELS else ""


def _display_service_type(value):
    key = _normalize_service_type(value)
    if not key:
        return ""
    return SERVICE_TYPE_LABELS.get(key, "")


def _service_class(value):
    key = _normalize_service_type(value)
    if key == "mowing":
        return "service-mowing"
    if key in {"mulch", "installation", "hardscape"}:
        return "service-material"
    if key in {"cleanup", "snow_removal"}:
        return "service-seasonal"
    if key:
        return "service-default"
    return ""


def ensure_job_schedule_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        try:
            cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_date DATE")
            cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_start_time TIME")
            cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_end_time TIME")
            cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS assigned_to TEXT")
            cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS service_type TEXT")
            conn.commit()
            return
        except Exception:
            conn.rollback()

        try:
            cur.execute("PRAGMA table_info(jobs)")
            cols = [row[1] for row in cur.fetchall()]

            if "scheduled_date" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN scheduled_date TEXT")
            if "scheduled_start_time" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN scheduled_start_time TEXT")
            if "scheduled_end_time" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN scheduled_end_time TEXT")
            if "assigned_to" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN assigned_to TEXT")
            if "service_type" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN service_type TEXT")

            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()


def _parse_month_year():
    today = date.today()

    year_raw = _safe_text(request.args.get("year"))
    month_raw = _safe_text(request.args.get("month"))

    try:
        year = int(year_raw) if year_raw else today.year
    except Exception:
        year = today.year

    try:
        month = int(month_raw) if month_raw else today.month
    except Exception:
        month = today.month

    if month < 1:
        month = 1
    if month > 12:
        month = 12

    return year, month


def _parse_view_mode():
    view = _safe_text(request.args.get("view"), "month").lower()
    return "week" if view == "week" else "month"


def _parse_focus_date():
    raw = _safe_text(request.args.get("date"))
    if raw:
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except Exception:
            pass
    return date.today()


def _parse_calendar_filters():
    status_filter = _safe_text(request.args.get("status")).lower()
    service_filter = _normalize_service_type(request.args.get("service_type"))
    crew_filter = _safe_text(request.args.get("crew")).lower()

    if status_filter in {"all", ""}:
        status_filter = ""
    return status_filter, service_filter, crew_filter


def _prev_month(year, month):
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _next_month(year, month):
    if month == 12:
        return year + 1, 1
    return year, month + 1


def _sunday_start(day):
    days_since_sunday = (day.weekday() + 1) % 7
    return day - timedelta(days=days_since_sunday)


def _fetch_jobs_between(conn, company_id, start_date, end_date):
    sql_pg = """
        SELECT
            id,
            title,
            status,
            scheduled_date,
            scheduled_start_time,
            scheduled_end_time,
            assigned_to,
            service_type
        FROM jobs
        WHERE company_id = %s
          AND scheduled_date IS NOT NULL
          AND scheduled_date >= %s
          AND scheduled_date <= %s
        ORDER BY
            scheduled_date ASC,
            scheduled_start_time ASC NULLS LAST,
            id ASC
    """

    sql_sqlite = """
        SELECT
            id,
            title,
            status,
            scheduled_date,
            scheduled_start_time,
            scheduled_end_time,
            assigned_to,
            service_type
        FROM jobs
        WHERE company_id = ?
          AND scheduled_date IS NOT NULL
          AND scheduled_date >= ?
          AND scheduled_date <= ?
        ORDER BY
            scheduled_date ASC,
            scheduled_start_time ASC,
            id ASC
    """

    cur = conn.cursor()

    try:
        cur.execute(sql_pg, (company_id, start_date, end_date))
        return cur.fetchall()
    except Exception:
        cur.execute(sql_sqlite, (company_id, start_date, end_date))
        return cur.fetchall()


def _fetch_jobs_for_day(conn, company_id, day_iso):
    sql_pg = """
        SELECT
            id,
            title,
            status,
            scheduled_date,
            scheduled_start_time,
            scheduled_end_time,
            assigned_to,
            service_type
        FROM jobs
        WHERE company_id = %s
          AND scheduled_date = %s
        ORDER BY
            scheduled_start_time ASC NULLS LAST,
            id ASC
    """

    sql_sqlite = """
        SELECT
            id,
            title,
            status,
            scheduled_date,
            scheduled_start_time,
            scheduled_end_time,
            assigned_to,
            service_type
        FROM jobs
        WHERE company_id = ?
          AND scheduled_date = ?
        ORDER BY
            scheduled_start_time ASC,
            id ASC
    """

    cur = conn.cursor()

    try:
        cur.execute(sql_pg, (company_id, day_iso))
        return cur.fetchall()
    except Exception:
        cur.execute(sql_sqlite, (company_id, day_iso))
        return cur.fetchall()


def _fetch_crews(conn, company_id):
    sql_pg = """
        SELECT DISTINCT assigned_to
        FROM jobs
        WHERE company_id = %s
          AND assigned_to IS NOT NULL
          AND TRIM(assigned_to) <> ''
        ORDER BY assigned_to ASC
    """

    sql_sqlite = """
        SELECT DISTINCT assigned_to
        FROM jobs
        WHERE company_id = ?
          AND assigned_to IS NOT NULL
          AND TRIM(assigned_to) <> ''
        ORDER BY assigned_to ASC
    """

    cur = conn.cursor()
    try:
        try:
            cur.execute(sql_pg, (company_id,))
            rows = cur.fetchall()
        except Exception:
            cur.execute(sql_sqlite, (company_id,))
            rows = cur.fetchall()

        crews = []
        for row in rows:
            try:
                value = row["assigned_to"]
            except Exception:
                value = row[0]
            value = _safe_text(value)
            if value:
                crews.append(value)
        return crews
    finally:
        cur.close()


def _normalize_date_value(value):
    if value is None:
        return None

    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    return text[:10]


def _normalize_time_value(value):
    if value is None:
        return ""

    if hasattr(value, "strftime"):
        try:
            return value.strftime("%H:%M")
        except Exception:
            pass

    text = str(value).strip()
    if not text:
        return ""

    return text[:5]


def _format_time_12hr(value):
    text = _normalize_time_value(value)
    if not text:
        return ""

    try:
        parsed = datetime.strptime(text, "%H:%M")
        return parsed.strftime("%I:%M %p").lstrip("0")
    except Exception:
        return text


def _build_time_range(start_value, end_value):
    start_text = _format_time_12hr(start_value)
    end_text = _format_time_12hr(end_value)

    if start_text and end_text:
        return f"{start_text} - {end_text}"
    if start_text:
        return start_text
    return "All Day"


def _status_class(status):
    value = _safe_text(status).lower()

    if value == "scheduled":
        return "status-scheduled"
    if value == "in progress":
        return "status-in-progress"
    if value in {"completed", "finished"}:
        return "status-completed"
    if value == "invoiced":
        return "status-invoiced"

    return "status-default"


def _time_to_minutes(value):
    text = _normalize_time_value(value)
    if not text:
        return None
    try:
        parts = text.split(":")
        hour = int(parts[0])
        minute = int(parts[1])
        return hour * 60 + minute
    except Exception:
        return None


def _minutes_to_label(minutes):
    hour = minutes // 60
    minute = minutes % 60
    dt = datetime.strptime(f"{hour:02d}:{minute:02d}", "%H:%M")
    return dt.strftime("%I:%M %p").lstrip("0")


def _job_payload(row):
    row = _dict_row(row)
    job_id = row.get("id")
    status = row.get("status") or ""
    service_type = _normalize_service_type(row.get("service_type"))
    service_label = _display_service_type(service_type)

    start_time = _normalize_time_value(row.get("scheduled_start_time"))
    end_time = _normalize_time_value(row.get("scheduled_end_time"))

    start_minutes = _time_to_minutes(start_time)
    end_minutes = _time_to_minutes(end_time) if end_time else None

    if start_minutes is None:
        start_minutes = 360

    if end_minutes is None or end_minutes <= start_minutes:
        end_minutes = start_minutes + 60

    return {
        "id": job_id,
        "title": row.get("title") or f"Job #{job_id}",
        "status": status,
        "status_class": _status_class(status),
        "service_type": service_type,
        "service_label": service_label,
        "service_class": _service_class(service_type),
        "is_mowing": service_type == "mowing",
        "scheduled_date": _normalize_date_value(row.get("scheduled_date")),
        "scheduled_start_time": start_time,
        "scheduled_end_time": end_time,
        "start_minutes": start_minutes,
        "end_minutes": end_minutes,
        "time_label": _build_time_range(
            row.get("scheduled_start_time"),
            row.get("scheduled_end_time"),
        ),
        "assigned_to": row.get("assigned_to") or "",
        "label": row.get("title") or f"Job #{job_id}",
        "search_text": " ".join([
            _safe_text(row.get("title")),
            _safe_text(row.get("status")),
            _safe_text(row.get("assigned_to")),
            service_label,
        ]).lower(),
        "url": url_for("jobs.view_job", job_id=job_id),
        "lane_index": 0,
        "lane_count": 1,
    }


def _job_matches_filters(job, status_filter="", service_filter="", crew_filter=""):
    if status_filter:
        if _safe_text(job.get("status")).lower() != status_filter:
            return False

    if service_filter:
        if _normalize_service_type(job.get("service_type")) != service_filter:
            return False

    if crew_filter:
        if crew_filter not in _safe_text(job.get("assigned_to")).lower():
            return False

    return True


def _build_week_columns(week_start, raw_jobs, status_filter="", service_filter="", crew_filter=""):
    jobs_by_day = {}
    for raw in raw_jobs:
        job = _job_payload(raw)
        if not _job_matches_filters(job, status_filter, service_filter, crew_filter):
            continue
        iso_day = job["scheduled_date"]
        if not iso_day:
            continue
        jobs_by_day.setdefault(iso_day, []).append(job)

    week_days = []
    for offset in range(7):
        day = week_start + timedelta(days=offset)
        iso = day.isoformat()
        laid_out_jobs = _layout_day_jobs(jobs_by_day.get(iso, []))

        week_days.append({
            "date": day,
            "iso": iso,
            "label": day.strftime("%A"),
            "short_label": day.strftime("%a"),
            "day_num": day.day,
            "month_label": day.strftime("%b"),
            "is_today": day == date.today(),
            "jobs": laid_out_jobs,
        })
    return week_days


def _build_week_time_rows(start_hour=6, end_hour=20):
    rows = []
    for hour in range(start_hour, end_hour + 1):
        label = _minutes_to_label(hour * 60)
        rows.append({
            "hour": hour,
            "minutes": hour * 60,
            "label": label,
        })
    return rows


def _layout_day_jobs(day_jobs):
    if not day_jobs:
        return []

    jobs = sorted(day_jobs, key=lambda j: (j["start_minutes"], j["end_minutes"], j["id"]))

    active = []
    groups = []
    current_group = []

    for job in jobs:
        active = [a for a in active if a["end_minutes"] > job["start_minutes"]]

        if not active and current_group:
            groups.append(current_group)
            current_group = []

        used_lanes = {a["lane_index"] for a in active}
        lane_index = 0
        while lane_index in used_lanes:
            lane_index += 1

        job["lane_index"] = lane_index
        active.append(job)
        current_group.append(job)

    if current_group:
        groups.append(current_group)

    for group in groups:
        lane_count = max(j["lane_index"] for j in group) + 1
        for job in group:
            job["lane_count"] = lane_count

    return jobs


@calendar_bp.route("/calendar", methods=["GET"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def calendar_page():
    ensure_job_schedule_columns()

    company_id = session.get("company_id")
    if not company_id:
        flash("Missing company session.")
        return redirect(url_for("dashboard.dashboard"))

    today = date.today()
    today_iso = today.isoformat()
    view_mode = _parse_view_mode()
    focus_date = _parse_focus_date()
    status_filter, service_filter, crew_filter = _parse_calendar_filters()

    year, month = _parse_month_year()
    first_day = date(year, month, 1)
    last_day_num = pycalendar.monthrange(year, month)[1]
    last_day = date(year, month, last_day_num)
    prev_year, prev_month = _prev_month(year, month)
    next_year, next_month = _next_month(year, month)

    week_start = _sunday_start(focus_date)
    week_end = week_start + timedelta(days=6)
    prev_week = week_start - timedelta(days=7)
    next_week = week_start + timedelta(days=7)

    conn = get_db_connection()
    try:
        month_jobs_raw = _fetch_jobs_between(
            conn,
            company_id,
            first_day.isoformat(),
            last_day.isoformat(),
        )
        today_jobs_raw = _fetch_jobs_for_day(conn, company_id, today_iso)
        week_jobs_raw = _fetch_jobs_between(
            conn,
            company_id,
            week_start.isoformat(),
            week_end.isoformat(),
        )
        crews = _fetch_crews(conn, company_id)
    finally:
        conn.close()

    today_jobs = [
        _job_payload(row)
        for row in today_jobs_raw
        if _job_matches_filters(_job_payload(row), status_filter, service_filter, crew_filter)
    ]

    jobs_by_day = {}
    for raw in month_jobs_raw:
        payload = _job_payload(raw)
        if not _job_matches_filters(payload, status_filter, service_filter, crew_filter):
            continue
        job_date = payload.get("scheduled_date")
        if not job_date:
            continue
        jobs_by_day.setdefault(job_date, []).append(payload)

    cal = pycalendar.Calendar(firstweekday=6)
    month_weeks = cal.monthdatescalendar(year, month)

    weeks = []
    for week in month_weeks:
        week_cells = []
        for day in week:
            iso_day = day.isoformat()
            week_cells.append({
                "date": day,
                "iso": iso_day,
                "day_num": day.day,
                "in_month": day.month == month,
                "is_today": day == today,
                "jobs": jobs_by_day.get(iso_day, []),
            })
        weeks.append(week_cells)

    month_mobile_days = []
    for week in weeks:
        for cell in week:
            if cell["in_month"]:
                month_mobile_days.append(cell)

    week_days = _build_week_columns(
        week_start,
        week_jobs_raw,
        status_filter=status_filter,
        service_filter=service_filter,
        crew_filter=crew_filter,
    )
    week_time_rows = _build_week_time_rows(6, 20)

    month_name = first_day.strftime("%B %Y")
    today_label = today.strftime("%A, %B %d, %Y")
    week_label = f"{week_start.strftime('%B %d, %Y')} - {week_end.strftime('%B %d, %Y')}"

    html = """
    <!doctype html>
    <html>
    <head>
        <title>Calendar - TerraLedger</title>
        <style>
            :root {
                --bg: #0f172a;
                --panel: #111827;
                --panel-2: #1f2937;
                --border: #334155;
                --text: #f8fafc;
                --muted: #94a3b8;
                --today-ring: #38bdf8;

                --scheduled-bg: rgba(59, 130, 246, 0.14);
                --scheduled-border: rgba(59, 130, 246, 0.45);
                --scheduled-text: #bfdbfe;

                --progress-bg: rgba(249, 115, 22, 0.14);
                --progress-border: rgba(249, 115, 22, 0.45);
                --progress-text: #fdba74;

                --completed-bg: rgba(34, 197, 94, 0.15);
                --completed-border: rgba(34, 197, 94, 0.38);
                --completed-text: #bbf7d0;

                --invoiced-bg: rgba(168, 85, 247, 0.14);
                --invoiced-border: rgba(168, 85, 247, 0.45);
                --invoiced-text: #ddd6fe;

                --default-bg: rgba(148, 163, 184, 0.12);
                --default-border: rgba(148, 163, 184, 0.35);
                --default-text: #cbd5e1;

                --service-mowing-bg: #ecfdf3;
                --service-mowing-border: #22c55e;
                --service-mowing-text: #166534;

                --service-material-bg: #fff7ed;
                --service-material-border: #f97316;
                --service-material-text: #9a3412;

                --service-seasonal-bg: #eff6ff;
                --service-seasonal-border: #3b82f6;
                --service-seasonal-text: #1d4ed8;

                --service-default-bg: #f8fafc;
                --service-default-border: #94a3b8;
                --service-default-text: #334155;
            }

            * { box-sizing: border-box; }

            body {
                margin: 0;
                background: #0b1220;
                color: var(--text);
                font-family: Arial, Helvetica, sans-serif;
            }

            .wrap {
                max-width: 1500px;
                margin: 0 auto;
                padding: 24px;
            }

            .topbar {
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 16px;
                margin-bottom: 18px;
                flex-wrap: wrap;
            }

            .title-block h1 {
                margin: 0;
                font-size: 28px;
            }

            .title-block p {
                margin: 6px 0 0;
                color: var(--muted);
                font-size: 14px;
            }

            .nav-actions, .view-actions, .filter-actions {
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
            }

            .controls-row {
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 12px;
                margin-bottom: 18px;
                flex-wrap: wrap;
            }

            .btn {
                display: inline-block;
                text-decoration: none;
                color: var(--text);
                background: var(--panel-2);
                border: 1px solid var(--border);
                border-radius: 10px;
                padding: 10px 14px;
                font-size: 14px;
                cursor: pointer;
            }

            .btn:hover {
                border-color: #22c55e;
                background: #182234;
            }

            .btn.active {
                border-color: #38bdf8;
                background: rgba(56, 189, 248, 0.16);
            }

            .filter-shell {
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 18px;
                padding: 18px;
                margin-bottom: 18px;
                box-shadow: 0 12px 30px rgba(0,0,0,0.2);
            }

            .filter-grid {
                display: grid;
                grid-template-columns: repeat(4, minmax(0, 1fr));
                gap: 12px;
            }

            .filter-grid label {
                display: block;
                margin-bottom: 6px;
                font-size: 13px;
                font-weight: 700;
                color: var(--muted);
            }

            .filter-grid select,
            .filter-grid input {
                width: 100%;
                padding: 10px 12px;
                border-radius: 10px;
                border: 1px solid var(--border);
                background: #0f172a;
                color: var(--text);
                font-size: 14px;
            }

            .quick-tools {
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
                margin-top: 14px;
            }

            .chip-btn {
                border: 1px solid var(--border);
                background: #0f172a;
                color: var(--text);
                border-radius: 999px;
                padding: 8px 12px;
                font-size: 13px;
                font-weight: 700;
                cursor: pointer;
            }

            .chip-btn:hover {
                border-color: #22c55e;
            }

            .today-shell {
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 18px;
                padding: 18px;
                margin-bottom: 18px;
                box-shadow: 0 12px 30px rgba(0,0,0,0.2);
            }

            .today-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 12px;
                flex-wrap: wrap;
                margin-bottom: 14px;
            }

            .today-header h2 {
                margin: 0;
                font-size: 22px;
            }

            .today-date {
                color: var(--muted);
                font-size: 14px;
            }

            .today-list,
            .mobile-day-list,
            .mobile-week-list {
                display: grid;
                gap: 10px;
            }

            .today-link,
            .job-link,
            .week-job-link,
            .mobile-job-link {
                display: block;
                text-decoration: none;
                color: inherit;
            }

            .today-card {
                display: grid;
                grid-template-columns: 160px 1fr auto;
                gap: 14px;
                align-items: center;
                border-radius: 12px;
                padding: 12px 14px;
                transition: transform 0.12s ease;
                border: 1px solid var(--default-border);
                background: var(--default-bg);
            }

            .today-link:hover .today-card,
            .job-link:hover .job-card,
            .week-job-link:hover .week-job-card,
            .mobile-job-link:hover .mobile-job-card {
                transform: translateY(-1px);
            }

            .today-time {
                font-size: 13px;
                font-weight: 700;
            }

            .today-title {
                font-size: 15px;
                font-weight: 700;
                margin-bottom: 3px;
            }

            .today-meta {
                font-size: 13px;
            }

            .status-pill {
                font-size: 11px;
                font-weight: 700;
                padding: 5px 9px;
                border-radius: 999px;
                border: 1px solid transparent;
                white-space: nowrap;
            }

            .service-pill {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                font-size: 11px;
                font-weight: 700;
                padding: 4px 8px;
                border-radius: 999px;
                border: 1px solid transparent;
                white-space: nowrap;
                margin-right: 6px;
            }

            .service-mowing {
                background: var(--service-mowing-bg);
                color: var(--service-mowing-text);
                border-color: var(--service-mowing-border);
            }

            .service-material {
                background: var(--service-material-bg);
                color: var(--service-material-text);
                border-color: var(--service-material-border);
            }

            .service-seasonal {
                background: var(--service-seasonal-bg);
                color: var(--service-seasonal-text);
                border-color: var(--service-seasonal-border);
            }

            .service-default {
                background: var(--service-default-bg);
                color: var(--service-default-text);
                border-color: var(--service-default-border);
            }

            .today-empty {
                color: var(--muted);
                font-size: 14px;
            }

            .calendar-shell {
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 18px;
                overflow: hidden;
                box-shadow: 0 12px 30px rgba(0,0,0,0.25);
            }

            .month-header, .week-header {
                display: flex;
                justify-content: center;
                align-items: center;
                padding: 16px;
                font-size: 20px;
                font-weight: 700;
                border-bottom: 1px solid var(--border);
                background: #0f172a;
            }

            .month-tools {
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 12px;
                padding: 14px 16px;
                border-bottom: 1px solid var(--border);
                background: #0d1526;
                flex-wrap: wrap;
            }

            .month-tools-left {
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
                align-items: center;
            }

            .month-tools-right {
                color: var(--muted);
                font-size: 13px;
            }

            .dow-row,
            .week-row {
                display: grid;
                grid-template-columns: repeat(7, 1fr);
            }

            .dow {
                padding: 12px;
                text-align: center;
                font-size: 13px;
                color: var(--muted);
                border-bottom: 1px solid var(--border);
                background: #0b1324;
                font-weight: 700;
            }

            .day-cell {
                min-height: 180px;
                border-right: 1px solid var(--border);
                border-bottom: 1px solid var(--border);
                padding: 10px;
                background: #111827;
            }

            .week-row .day-cell:last-child,
            .dow-row .dow:last-child {
                border-right: none;
            }

            .day-cell.outside {
                background: #0d1526;
                color: #64748b;
            }

            .day-top {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 10px;
            }

            .day-number {
                font-size: 14px;
                font-weight: 700;
                color: var(--text);
            }

            .outside .day-number {
                color: #64748b;
            }

            .today-badge {
                font-size: 11px;
                color: #001018;
                background: var(--today-ring);
                border-radius: 999px;
                padding: 2px 8px;
                font-weight: 700;
            }

            .day-cell.today {
                box-shadow: inset 0 0 0 2px var(--today-ring);
            }

            .job-card {
                border-radius: 10px;
                padding: 8px 9px;
                margin-bottom: 8px;
                transition: transform 0.12s ease;
                border: 1px solid var(--default-border);
                background: var(--default-bg);
                position: relative;
                overflow: hidden;
            }

            .job-card.service-mowing,
            .week-job-card.service-mowing,
            .today-card.service-mowing,
            .mobile-job-card.service-mowing {
                border-color: var(--service-mowing-border) !important;
                box-shadow: inset 3px 0 0 var(--service-mowing-border);
            }

            .job-time {
                font-size: 12px;
                margin-bottom: 4px;
                font-weight: 700;
            }

            .job-label {
                font-size: 13px;
                font-weight: 700;
                color: var(--text);
                margin-bottom: 3px;
                line-height: 1.3;
            }

            .job-meta {
                font-size: 12px;
            }

            .job-tools {
                display: flex;
                gap: 6px;
                flex-wrap: wrap;
                margin-top: 6px;
            }

            .mini-link {
                font-size: 11px;
                color: var(--text);
                text-decoration: none;
                padding: 4px 7px;
                border: 1px solid var(--border);
                border-radius: 999px;
                background: rgba(255,255,255,0.04);
            }

            .mini-link:hover {
                border-color: #22c55e;
            }

            .empty-note {
                color: var(--muted);
                font-size: 12px;
                margin-top: 8px;
            }

            .week-shell {
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 18px;
                overflow: hidden;
                box-shadow: 0 12px 30px rgba(0,0,0,0.25);
            }

            .week-grid {
                overflow-x: auto;
            }

            .week-grid-inner {
                min-width: 1120px;
            }

            .week-grid-header {
                display: grid;
                grid-template-columns: 90px repeat(7, 1fr);
                background: #0b1324;
                border-bottom: 1px solid var(--border);
            }

            .week-time-head,
            .week-day-head {
                padding: 12px 10px;
                border-right: 1px solid var(--border);
            }

            .week-time-head {
                color: var(--muted);
                font-weight: 700;
                font-size: 13px;
            }

            .week-day-head:last-child {
                border-right: none;
            }

            .week-day-head {
                text-align: center;
            }

            .week-day-name {
                font-size: 13px;
                color: var(--muted);
                font-weight: 700;
            }

            .week-day-number {
                font-size: 18px;
                font-weight: 700;
                margin-top: 4px;
            }

            .week-day-head.today {
                background: rgba(56, 189, 248, 0.10);
                box-shadow: inset 0 -2px 0 var(--today-ring);
            }

            .week-grid-body {
                display: grid;
                grid-template-columns: 90px repeat(7, 1fr);
            }

            .week-time-col {
                background: #0f172a;
                border-right: 1px solid var(--border);
            }

            .week-time-slot {
                height: 72px;
                border-bottom: 1px solid var(--border);
                padding: 6px 8px;
                color: var(--muted);
                font-size: 12px;
                font-weight: 700;
            }

            .week-day-col {
                position: relative;
                border-right: 1px solid var(--border);
                min-height: 1080px;
                background:
                    repeating-linear-gradient(
                        to bottom,
                        transparent,
                        transparent 71px,
                        rgba(148, 163, 184, 0.14) 71px,
                        rgba(148, 163, 184, 0.14) 72px
                    );
            }

            .week-day-col:last-child {
                border-right: none;
            }

            .week-day-col.today {
                background:
                    linear-gradient(rgba(56, 189, 248, 0.06), rgba(56, 189, 248, 0.06)),
                    repeating-linear-gradient(
                        to bottom,
                        transparent,
                        transparent 71px,
                        rgba(148, 163, 184, 0.14) 71px,
                        rgba(148, 163, 184, 0.14) 72px
                    );
            }

            .week-job-link {
                position: absolute;
                left: 8px;
                right: 8px;
            }

            .week-job-card {
                border-radius: 10px;
                padding: 8px 9px;
                border: 1px solid var(--default-border);
                background: var(--default-bg);
                overflow: hidden;
                min-height: 36px;
                transition: transform 0.12s ease;
            }

            .week-job-time {
                font-size: 11px;
                font-weight: 700;
                margin-bottom: 3px;
            }

            .week-job-title {
                font-size: 12px;
                font-weight: 700;
                line-height: 1.25;
                margin-bottom: 2px;
            }

            .week-job-meta {
                font-size: 11px;
                line-height: 1.2;
            }

            .week-job-tools {
                margin-top: 6px;
                display: flex;
                gap: 5px;
                flex-wrap: wrap;
            }

            .week-mini-link {
                font-size: 10px;
                color: var(--text);
                text-decoration: none;
                padding: 3px 6px;
                border: 1px solid var(--border);
                border-radius: 999px;
                background: rgba(255,255,255,0.04);
            }

            .week-mini-link:hover {
                border-color: #22c55e;
            }

            .mobile-only {
                display: none;
            }

            .desktop-only {
                display: block;
            }

            .mobile-month-shell,
            .mobile-week-shell {
                display: grid;
                gap: 12px;
            }

            .mobile-day-card {
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 16px;
                padding: 14px;
                box-shadow: 0 12px 30px rgba(0,0,0,0.18);
            }

            .mobile-day-head {
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 10px;
                flex-wrap: wrap;
                margin-bottom: 10px;
            }

            .mobile-day-title {
                font-size: 16px;
                font-weight: 700;
            }

            .mobile-day-sub {
                color: var(--muted);
                font-size: 13px;
            }

            .mobile-job-card {
                border-radius: 12px;
                padding: 10px 12px;
                border: 1px solid var(--default-border);
                background: var(--default-bg);
            }

            .mobile-job-time {
                font-size: 12px;
                font-weight: 700;
                margin-bottom: 4px;
            }

            .mobile-job-title {
                font-size: 14px;
                font-weight: 700;
                margin-bottom: 3px;
                line-height: 1.3;
            }

            .mobile-job-meta {
                font-size: 12px;
            }

            .mobile-job-tools {
                display: flex;
                gap: 6px;
                flex-wrap: wrap;
                margin-top: 8px;
            }

            .legend {
                margin-top: 14px;
                color: var(--muted);
                font-size: 13px;
                line-height: 1.5;
            }

            .status-scheduled {
                background: var(--scheduled-bg);
                border-color: var(--scheduled-border);
            }

            .status-scheduled .job-time,
            .status-scheduled .today-time,
            .status-scheduled .job-meta,
            .status-scheduled .today-meta,
            .status-scheduled .status-pill,
            .status-scheduled .week-job-time,
            .status-scheduled .week-job-meta,
            .status-scheduled .mobile-job-time,
            .status-scheduled .mobile-job-meta {
                color: var(--scheduled-text);
            }

            .status-scheduled .status-pill {
                border-color: var(--scheduled-border);
                background: rgba(59, 130, 246, 0.18);
            }

            .status-in-progress {
                background: var(--progress-bg);
                border-color: var(--progress-border);
            }

            .status-in-progress .job-time,
            .status-in-progress .today-time,
            .status-in-progress .job-meta,
            .status-in-progress .today-meta,
            .status-in-progress .status-pill,
            .status-in-progress .week-job-time,
            .status-in-progress .week-job-meta,
            .status-in-progress .mobile-job-time,
            .status-in-progress .mobile-job-meta {
                color: var(--progress-text);
            }

            .status-in-progress .status-pill {
                border-color: var(--progress-border);
                background: rgba(249, 115, 22, 0.18);
            }

            .status-completed {
                background: var(--completed-bg);
                border-color: var(--completed-border);
            }

            .status-completed .job-time,
            .status-completed .today-time,
            .status-completed .job-meta,
            .status-completed .today-meta,
            .status-completed .status-pill,
            .status-completed .week-job-time,
            .status-completed .week-job-meta,
            .status-completed .mobile-job-time,
            .status-completed .mobile-job-meta {
                color: var(--completed-text);
            }

            .status-completed .status-pill {
                border-color: var(--completed-border);
                background: rgba(34, 197, 94, 0.18);
            }

            .status-invoiced {
                background: var(--invoiced-bg);
                border-color: var(--invoiced-border);
            }

            .status-invoiced .job-time,
            .status-invoiced .today-time,
            .status-invoiced .job-meta,
            .status-invoiced .today-meta,
            .status-invoiced .status-pill,
            .status-invoiced .week-job-time,
            .status-invoiced .week-job-meta,
            .status-invoiced .mobile-job-time,
            .status-invoiced .mobile-job-meta {
                color: var(--invoiced-text);
            }

            .status-invoiced .status-pill {
                border-color: var(--invoiced-border);
                background: rgba(168, 85, 247, 0.18);
            }

            .status-default {
                background: var(--default-bg);
                border-color: var(--default-border);
            }

            .status-default .job-time,
            .status-default .today-time,
            .status-default .job-meta,
            .status-default .today-meta,
            .status-default .status-pill,
            .status-default .week-job-time,
            .status-default .week-job-meta,
            .status-default .mobile-job-time,
            .status-default .mobile-job-meta {
                color: var(--default-text);
            }

            .status-default .status-pill {
                border-color: var(--default-border);
                background: rgba(148, 163, 184, 0.16);
            }

            @media (max-width: 1100px) {
                .filter-grid {
                    grid-template-columns: repeat(2, minmax(0, 1fr));
                }
            }

            @media (max-width: 980px) {
                .today-card {
                    grid-template-columns: 1fr;
                    gap: 8px;
                }
            }

            @media (max-width: 760px) {
                .desktop-only {
                    display: none !important;
                }

                .mobile-only {
                    display: block !important;
                }

                .wrap {
                    padding: 16px;
                }

                .title-block h1 {
                    font-size: 24px;
                }

                .month-header,
                .week-header {
                    font-size: 18px;
                }

                .filter-grid {
                    grid-template-columns: 1fr;
                }
            }
        </style>
    </head>
    <body>
        <div class="wrap">
            <div class="topbar">
                <div class="title-block">
                    <h1>Calendar</h1>
                    <p>View scheduled jobs by month or week.</p>
                </div>

                <div class="view-actions" style="flex-direction: column; align-items: flex-end;">
                    <div style="display:flex; gap:10px; flex-wrap:wrap;">
                        <a class="btn {% if view_mode == 'month' %}active{% endif %}"
                           href="{{ url_for('calendar.calendar_page', view='month', year=year, month=month, date=focus_date.isoformat(), status=status_filter, service_type=service_filter, crew=crew_filter) }}">
                            Month View
                        </a>

                        <a class="btn {% if view_mode == 'week' %}active{% endif %}"
                           href="{{ url_for('calendar.calendar_page', view='week', date=focus_date.isoformat(), status=status_filter, service_type=service_filter, crew=crew_filter) }}">
                            Week View
                        </a>
                    </div>

                    <a class="btn secondary" style="margin-top:8px;"
                       href="{{ url_for('dashboard.dashboard') }}">
                        ← Back to Dashboard
                    </a>
                </div>
            </div>

            <div class="filter-shell">
                <form method="get">
                    <input type="hidden" name="view" value="{{ view_mode }}">
                    {% if view_mode == 'month' %}
                        <input type="hidden" name="year" value="{{ year }}">
                        <input type="hidden" name="month" value="{{ month }}">
                    {% endif %}
                    <input type="hidden" name="date" value="{{ focus_date.isoformat() }}">

                    <div class="filter-grid">
                        <div>
                            <label>Status</label>
                            <select name="status">
                                <option value="" {% if not status_filter %}selected{% endif %}>All Statuses</option>
                                <option value="scheduled" {% if status_filter == 'scheduled' %}selected{% endif %}>Scheduled</option>
                                <option value="in progress" {% if status_filter == 'in progress' %}selected{% endif %}>In Progress</option>
                                <option value="completed" {% if status_filter == 'completed' %}selected{% endif %}>Completed</option>
                                <option value="finished" {% if status_filter == 'finished' %}selected{% endif %}>Finished</option>
                                <option value="invoiced" {% if status_filter == 'invoiced' %}selected{% endif %}>Invoiced</option>
                            </select>
                        </div>

                        <div>
                            <label>Service Type</label>
                            <select name="service_type" id="serviceTypeFilter">
                                <option value="" {% if not service_filter %}selected{% endif %}>All Services</option>
                                <option value="mowing" {% if service_filter == 'mowing' %}selected{% endif %}>Mowing</option>
                                <option value="mulch" {% if service_filter == 'mulch' %}selected{% endif %}>Mulch</option>
                                <option value="cleanup" {% if service_filter == 'cleanup' %}selected{% endif %}>Cleanup</option>
                                <option value="installation" {% if service_filter == 'installation' %}selected{% endif %}>Installation</option>
                                <option value="hardscape" {% if service_filter == 'hardscape' %}selected{% endif %}>Hardscape</option>
                                <option value="snow_removal" {% if service_filter == 'snow_removal' %}selected{% endif %}>Snow Removal</option>
                                <option value="fertilizing" {% if service_filter == 'fertilizing' %}selected{% endif %}>Fertilizing</option>
                                <option value="other" {% if service_filter == 'other' %}selected{% endif %}>Other</option>
                            </select>
                        </div>

                        <div>
                            <label>Assigned To</label>
                            <select name="crew">
                                <option value="" {% if not crew_filter %}selected{% endif %}>All Crews / Employees</option>
                                {% for crew in crews %}
                                    <option value="{{ crew }}" {% if crew_filter == crew.lower() %}selected{% endif %}>{{ crew }}</option>
                                {% endfor %}
                            </select>
                        </div>

                        <div>
                            <label>Quick Jump</label>
                            <input type="date" name="jump_date" id="jumpDate" value="{{ focus_date.isoformat() }}">
                        </div>
                    </div>

                    <div class="quick-tools">
                        <button class="btn" type="submit">Apply Filters</button>
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view=view_mode, date=focus_date.isoformat(), year=year, month=month) }}">Clear Filters</a>
                        <button class="chip-btn" type="button" onclick="setMowingOnly()">Mowing Only</button>
                        <button class="chip-btn" type="button" onclick="goToToday()">Today</button>
                        <button class="chip-btn" type="button" onclick="jumpToDate()">Jump to Date</button>
                    </div>
                </form>
            </div>

            <div class="controls-row">
                {% if view_mode == 'month' %}
                    <div class="nav-actions">
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view='month', year=prev_year, month=prev_month, date=focus_date.isoformat(), status=status_filter, service_type=service_filter, crew=crew_filter) }}">← Previous Month</a>
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view='month', year=today.year, month=today.month, date=today.isoformat(), status=status_filter, service_type=service_filter, crew=crew_filter) }}">This Month</a>
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view='month', year=next_year, month=next_month, date=focus_date.isoformat(), status=status_filter, service_type=service_filter, crew=crew_filter) }}">Next Month →</a>
                    </div>
                {% else %}
                    <div class="nav-actions">
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view='week', date=prev_week.isoformat(), status=status_filter, service_type=service_filter, crew=crew_filter) }}">← Previous Week</a>
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view='week', date=today.isoformat(), status=status_filter, service_type=service_filter, crew=crew_filter) }}">This Week</a>
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view='week', date=next_week.isoformat(), status=status_filter, service_type=service_filter, crew=crew_filter) }}">Next Week →</a>
                    </div>
                {% endif %}
            </div>

            <div class="today-shell">
                <div class="today-header">
                    <h2>Today's Schedule</h2>
                    <div class="today-date">{{ today_label }}</div>
                </div>

                {% if today_jobs %}
                    <div class="today-list">
                        {% for job in today_jobs %}
                            <a class="today-link" href="{{ job.url }}">
                                <div class="today-card {{ job.status_class }} {{ job.service_class }}">
                                    <div class="today-time">{{ job.time_label }}</div>
                                    <div>
                                        <div class="today-title">{{ job.label }}</div>
                                        <div style="margin-bottom:6px;">
                                            {% if job.service_label %}
                                                <span class="service-pill {{ job.service_class }}">{{ job.service_label }}</span>
                                            {% endif %}
                                            <span class="status-pill">{{ job.status or "No Status" }}</span>
                                        </div>
                                        {% if job.assigned_to %}
                                            <div class="today-meta">Assigned: {{ job.assigned_to }}</div>
                                        {% endif %}
                                    </div>
                                    <div>
                                        <span class="mini-link">Open</span>
                                    </div>
                                </div>
                            </a>
                        {% endfor %}
                    </div>
                {% else %}
                    <div class="today-empty">No jobs scheduled for today.</div>
                {% endif %}
            </div>

            {% if view_mode == 'month' %}
                <div class="desktop-only">
                    <div class="calendar-shell">
                        <div class="month-header">{{ month_name }}</div>

                        <div class="month-tools">
                            <div class="month-tools-left">
                                <span class="service-pill service-mowing">Mowing</span>
                                <span class="service-pill service-material">Material / Install</span>
                                <span class="service-pill service-seasonal">Seasonal</span>
                                <span class="service-pill service-default">Other</span>
                            </div>
                            <div class="month-tools-right">
                                Click any job to open it.
                            </div>
                        </div>

                        <div class="dow-row">
                            <div class="dow">Sunday</div>
                            <div class="dow">Monday</div>
                            <div class="dow">Tuesday</div>
                            <div class="dow">Wednesday</div>
                            <div class="dow">Thursday</div>
                            <div class="dow">Friday</div>
                            <div class="dow">Saturday</div>
                        </div>

                        {% for week in weeks %}
                            <div class="week-row">
                                {% for cell in week %}
                                    <div class="day-cell {% if not cell.in_month %}outside{% endif %} {% if cell.is_today %}today{% endif %}">
                                        <div class="day-top">
                                            <div class="day-number">{{ cell.day_num }}</div>
                                            {% if cell.is_today %}
                                                <div class="today-badge">Today</div>
                                            {% endif %}
                                        </div>

                                        {% if cell.jobs %}
                                            {% for job in cell.jobs %}
                                                <a class="job-link" href="{{ job.url }}" data-job-search="{{ job.search_text }}">
                                                    <div class="job-card {{ job.status_class }} {{ job.service_class }}">
                                                        <div style="display:flex; justify-content:space-between; gap:8px; align-items:flex-start; margin-bottom:4px; flex-wrap:wrap;">
                                                            <div class="job-time">{{ job.time_label }}</div>
                                                            {% if job.service_label %}
                                                                <span class="service-pill {{ job.service_class }}">{{ job.service_label }}</span>
                                                            {% endif %}
                                                        </div>
                                                        <div class="job-label">{{ job.label }}</div>
                                                        {% if job.assigned_to %}
                                                            <div class="job-meta">Assigned: {{ job.assigned_to }}</div>
                                                        {% endif %}
                                                        <div class="job-meta">{{ job.status or "No Status" }}</div>

                                                        <div class="job-tools">
                                                            <span class="mini-link">View</span>
                                                            <span class="mini-link">Edit</span>
                                                        </div>
                                                    </div>
                                                </a>
                                            {% endfor %}
                                        {% else %}
                                            <div class="empty-note">No scheduled jobs</div>
                                        {% endif %}
                                    </div>
                                {% endfor %}
                            </div>
                        {% endfor %}
                    </div>
                </div>

                <div class="mobile-only">
                    <div class="mobile-month-shell">
                        <div class="month-header" style="border-radius:18px;">{{ month_name }}</div>

                        {% for cell in month_mobile_days %}
                            <div class="mobile-day-card {% if cell.is_today %}today{% endif %}" {% if cell.is_today %}style="box-shadow: inset 0 0 0 2px var(--today-ring), 0 12px 30px rgba(0,0,0,0.18);" {% endif %}>
                                <div class="mobile-day-head">
                                    <div>
                                        <div class="mobile-day-title">{{ cell.date.strftime("%A, %B") }} {{ cell.day_num }}</div>
                                        <div class="mobile-day-sub">
                                            {% if cell.is_today %}Today{% else %}{{ cell.date.strftime("%Y-%m-%d") }}{% endif %}
                                        </div>
                                    </div>
                                </div>

                                {% if cell.jobs %}
                                    <div class="mobile-day-list">
                                        {% for job in cell.jobs %}
                                            <a class="mobile-job-link" href="{{ job.url }}">
                                                <div class="mobile-job-card {{ job.status_class }} {{ job.service_class }}">
                                                    <div style="display:flex; justify-content:space-between; gap:8px; flex-wrap:wrap; margin-bottom:4px;">
                                                        <div class="mobile-job-time">{{ job.time_label }}</div>
                                                        {% if job.service_label %}
                                                            <span class="service-pill {{ job.service_class }}">{{ job.service_label }}</span>
                                                        {% endif %}
                                                    </div>
                                                    <div class="mobile-job-title">{{ job.label }}</div>
                                                    {% if job.assigned_to %}
                                                        <div class="mobile-job-meta">Assigned: {{ job.assigned_to }}</div>
                                                    {% endif %}
                                                    <div class="mobile-job-meta">{{ job.status or "No Status" }}</div>

                                                    <div class="mobile-job-tools">
                                                        <span class="mini-link">Open</span>
                                                    </div>
                                                </div>
                                            </a>
                                        {% endfor %}
                                    </div>
                                {% else %}
                                    <div class="empty-note">No scheduled jobs</div>
                                {% endif %}
                            </div>
                        {% endfor %}
                    </div>
                </div>
            {% else %}
                <div class="desktop-only">
                    <div class="week-shell">
                        <div class="week-header">{{ week_label }}</div>

                        <div class="week-grid">
                            <div class="week-grid-inner">
                                <div class="week-grid-header">
                                    <div class="week-time-head">Time</div>
                                    {% for day in week_days %}
                                        <div class="week-day-head {% if day.is_today %}today{% endif %}">
                                            <div class="week-day-name">{{ day.label }}</div>
                                            <div class="week-day-number">{{ day.month_label }} {{ day.day_num }}</div>
                                        </div>
                                    {% endfor %}
                                </div>

                                <div class="week-grid-body">
                                    <div class="week-time-col">
                                        {% for row in week_time_rows %}
                                            <div class="week-time-slot">{{ row.label }}</div>
                                        {% endfor %}
                                    </div>

                                    {% for day in week_days %}
                                        <div class="week-day-col {% if day.is_today %}today{% endif %}">
                                            {% for job in day.jobs %}
                                                {% set display_start = job.start_minutes if job.start_minutes >= 360 else 360 %}
                                                {% set display_end = job.end_minutes if job.end_minutes > display_start else (display_start + 60) %}
                                                {% set top_px = ((display_start - 360) * 1.2) %}
                                                {% set height_px = ((display_end - display_start) * 1.2) %}
                                                {% if height_px < 42 %}
                                                    {% set height_px = 42 %}
                                                {% endif %}

                                                {% set lane_width = 100 / job.lane_count %}
                                                {% set left_pct = lane_width * job.lane_index %}
                                                {% set width_pct = lane_width %}

                                                <a class="week-job-link"
                                                   href="{{ job.url }}"
                                                   style="
                                                       top: {{ top_px }}px;
                                                       height: {{ height_px }}px;
                                                       left: calc({{ left_pct }}% + 4px);
                                                       width: calc({{ width_pct }}% - 8px);
                                                       right: auto;
                                                   ">
                                                    <div class="week-job-card {{ job.status_class }} {{ job.service_class }}" style="height: {{ height_px }}px;">
                                                        <div style="display:flex; justify-content:space-between; gap:5px; flex-wrap:wrap; margin-bottom:3px;">
                                                            <div class="week-job-time">{{ job.time_label }}</div>
                                                            {% if job.service_label %}
                                                                <span class="service-pill {{ job.service_class }}">{{ job.service_label }}</span>
                                                            {% endif %}
                                                        </div>
                                                        <div class="week-job-title">{{ job.label }}</div>
                                                        {% if job.assigned_to %}
                                                            <div class="week-job-meta">{{ job.assigned_to }}</div>
                                                        {% endif %}
                                                        <div class="week-job-meta">{{ job.status or "No Status" }}</div>

                                                        {% if height_px >= 68 %}
                                                            <div class="week-job-tools">
                                                                <span class="week-mini-link">Open</span>
                                                            </div>
                                                        {% endif %}
                                                    </div>
                                                </a>
                                            {% endfor %}
                                        </div>
                                    {% endfor %}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <div class="mobile-only">
                    <div class="mobile-week-shell">
                        <div class="week-header" style="border-radius:18px;">{{ week_label }}</div>

                        {% for day in week_days %}
                            <div class="mobile-day-card {% if day.is_today %}today{% endif %}" {% if day.is_today %}style="box-shadow: inset 0 0 0 2px var(--today-ring), 0 12px 30px rgba(0,0,0,0.18);" {% endif %}>
                                <div class="mobile-day-head">
                                    <div>
                                        <div class="mobile-day-title">{{ day.label }}, {{ day.month_label }} {{ day.day_num }}</div>
                                        <div class="mobile-day-sub">{% if day.is_today %}Today{% else %}{{ day.iso }}{% endif %}</div>
                                    </div>
                                </div>

                                {% if day.jobs %}
                                    <div class="mobile-day-list">
                                        {% for job in day.jobs %}
                                            <a class="mobile-job-link" href="{{ job.url }}">
                                                <div class="mobile-job-card {{ job.status_class }} {{ job.service_class }}">
                                                    <div style="display:flex; justify-content:space-between; gap:8px; flex-wrap:wrap; margin-bottom:4px;">
                                                        <div class="mobile-job-time">{{ job.time_label }}</div>
                                                        {% if job.service_label %}
                                                            <span class="service-pill {{ job.service_class }}">{{ job.service_label }}</span>
                                                        {% endif %}
                                                    </div>
                                                    <div class="mobile-job-title">{{ job.label }}</div>
                                                    {% if job.assigned_to %}
                                                        <div class="mobile-job-meta">Assigned: {{ job.assigned_to }}</div>
                                                    {% endif %}
                                                    <div class="mobile-job-meta">{{ job.status or "No Status" }}</div>

                                                    <div class="mobile-job-tools">
                                                        <span class="mini-link">Open</span>
                                                    </div>
                                                </div>
                                            </a>
                                        {% endfor %}
                                    </div>
                                {% else %}
                                    <div class="empty-note">No scheduled jobs</div>
                                {% endif %}
                            </div>
                        {% endfor %}
                    </div>
                </div>
            {% endif %}

            <div class="legend">
                Blue = Scheduled, Orange = In Progress, Green = Completed/Finished, Purple = Invoiced.<br>
                Mowing jobs are marked with a green service badge and green edge so they stand out immediately.
            </div>
        </div>

        <script>
            function currentBaseParams() {
                const params = new URLSearchParams(window.location.search);
                return params;
            }

            function setMowingOnly() {
                const params = currentBaseParams();
                params.set("service_type", "mowing");
                window.location.search = params.toString();
            }

            function goToToday() {
                const params = currentBaseParams();
                const today = "{{ today.isoformat() }}";
                params.set("date", today);
                params.set("year", "{{ today.year }}");
                params.set("month", "{{ today.month }}");
                window.location.search = params.toString();
            }

            function jumpToDate() {
                const input = document.getElementById("jumpDate");
                if (!input || !input.value) return;
                const params = currentBaseParams();
                const parts = input.value.split("-");
                params.set("date", input.value);
                if (parts.length === 3) {
                    params.set("year", String(parseInt(parts[0], 10)));
                    params.set("month", String(parseInt(parts[1], 10)));
                }
                window.location.search = params.toString();
            }
        </script>
    </body>
    </html>
    """

    return render_template_string(
        html,
        view_mode=view_mode,
        year=year,
        month=month,
        focus_date=focus_date,
        today=today,
        today_jobs=today_jobs,
        today_label=today_label,
        month_name=month_name,
        weeks=weeks,
        month_mobile_days=month_mobile_days,
        prev_year=prev_year,
        prev_month=prev_month,
        next_year=next_year,
        next_month=next_month,
        week_days=week_days,
        week_time_rows=week_time_rows,
        week_label=week_label,
        prev_week=prev_week,
        next_week=next_week,
        status_filter=status_filter,
        service_filter=service_filter,
        crew_filter=crew_filter,
        crews=crews,
    )