# TerraLedger/routes/calendar.py

from flask import Blueprint, request, session, render_template_string, url_for
from datetime import date, datetime, timedelta
import calendar as pycalendar

from db import get_db_connection
from decorators import login_required

calendar_bp = Blueprint("calendar", __name__)


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


def ensure_job_schedule_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_date DATE")
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_start_time TIME")
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_end_time TIME")
        cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS assigned_to TEXT")
        conn.commit()
        conn.close()
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

        conn.commit()
    finally:
        conn.close()


def _parse_month_year():
    today = date.today()

    year_raw = (request.args.get("year") or "").strip()
    month_raw = (request.args.get("month") or "").strip()

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
    view = (request.args.get("view") or "month").strip().lower()
    return "week" if view == "week" else "month"


def _parse_focus_date():
    raw = (request.args.get("date") or "").strip()
    if raw:
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except Exception:
            pass
    return date.today()


def _prev_month(year, month):
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _next_month(year, month):
    if month == 12:
        return year + 1, 1
    return year, month + 1


def _sunday_start(day):
    # Python Monday=0 ... Sunday=6
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
            assigned_to
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
            assigned_to
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
            assigned_to
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
            assigned_to
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
    value = (status or "").strip().lower()

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

    return {
        "id": job_id,
        "title": row.get("title") or f"Job #{job_id}",
        "status": status,
        "status_class": _status_class(status),
        "scheduled_date": _normalize_date_value(row.get("scheduled_date")),
        "scheduled_start_time": _normalize_time_value(row.get("scheduled_start_time")),
        "scheduled_end_time": _normalize_time_value(row.get("scheduled_end_time")),
        "time_label": _build_time_range(
            row.get("scheduled_start_time"),
            row.get("scheduled_end_time"),
        ),
        "assigned_to": row.get("assigned_to") or "",
        "label": row.get("title") or f"Job #{job_id}",
        "url": url_for("jobs.view_job", job_id=job_id),
    }


def _build_week_columns(week_start, raw_jobs):
    jobs_by_day = {}
    for raw in raw_jobs:
        job = _job_payload(raw)
        iso_day = job["scheduled_date"]
        if not iso_day:
            continue
        jobs_by_day.setdefault(iso_day, []).append(job)

    week_days = []
    for offset in range(7):
        day = week_start + timedelta(days=offset)
        iso = day.isoformat()
        week_days.append({
            "date": day,
            "iso": iso,
            "label": day.strftime("%A"),
            "short_label": day.strftime("%a"),
            "day_num": day.day,
            "month_label": day.strftime("%b"),
            "is_today": day == date.today(),
            "jobs": jobs_by_day.get(iso, []),
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


@calendar_bp.route("/calendar", methods=["GET"])
@login_required
def calendar_page():
    ensure_job_schedule_columns()

    company_id = session.get("company_id")
    if not company_id:
        return "Missing company session.", 400

    today = date.today()
    today_iso = today.isoformat()
    view_mode = _parse_view_mode()
    focus_date = _parse_focus_date()

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
    finally:
        conn.close()

    today_jobs = [_job_payload(row) for row in today_jobs_raw]

    jobs_by_day = {}
    for raw in month_jobs_raw:
        row = _dict_row(raw)
        job_date = _normalize_date_value(row.get("scheduled_date"))
        if not job_date:
            continue
        jobs_by_day.setdefault(job_date, []).append(_job_payload(row))

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

    week_days = _build_week_columns(week_start, week_jobs_raw)
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

                --blue-soft: rgba(56, 189, 248, 0.12);
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

            .nav-actions, .view-actions {
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
            }

            .btn:hover {
                border-color: #22c55e;
                background: #182234;
            }

            .btn.active {
                border-color: #38bdf8;
                background: rgba(56, 189, 248, 0.16);
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

            .today-list {
                display: grid;
                gap: 10px;
            }

            .today-link,
            .job-link,
            .week-job-link {
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
            .week-job-link:hover .week-job-card {
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

            .legend {
                margin-top: 14px;
                color: var(--muted);
                font-size: 13px;
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
            .status-scheduled .week-job-meta {
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
            .status-in-progress .week-job-meta {
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
            .status-completed .week-job-meta {
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
            .status-invoiced .week-job-meta {
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
            .status-default .week-job-meta {
                color: var(--default-text);
            }

            .status-default .status-pill {
                border-color: var(--default-border);
                background: rgba(148, 163, 184, 0.16);
            }

            @media (max-width: 980px) {
                .dow-row,
                .week-row {
                    grid-template-columns: repeat(1, 1fr);
                }

                .dow-row {
                    display: none;
                }

                .day-cell {
                    min-height: auto;
                }

                .day-top {
                    margin-bottom: 8px;
                }

                .today-card {
                    grid-template-columns: 1fr;
                    gap: 8px;
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

                <div class="view-actions">
                    <a class="btn {% if view_mode == 'month' %}active{% endif %}" href="{{ url_for('calendar.calendar_page', view='month', year=year, month=month, date=focus_date.isoformat()) }}">Month View</a>
                    <a class="btn {% if view_mode == 'week' %}active{% endif %}" href="{{ url_for('calendar.calendar_page', view='week', date=focus_date.isoformat()) }}">Week View</a>
                </div>
            </div>

            <div class="controls-row">
                {% if view_mode == 'month' %}
                    <div class="nav-actions">
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view='month', year=prev_year, month=prev_month, date=focus_date.isoformat()) }}">← Previous Month</a>
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view='month', year=today.year, month=today.month, date=today.isoformat()) }}">This Month</a>
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view='month', year=next_year, month=next_month, date=focus_date.isoformat()) }}">Next Month →</a>
                    </div>
                {% else %}
                    <div class="nav-actions">
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view='week', date=prev_week.isoformat()) }}">← Previous Week</a>
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view='week', date=today.isoformat()) }}">This Week</a>
                        <a class="btn" href="{{ url_for('calendar.calendar_page', view='week', date=next_week.isoformat()) }}">Next Week →</a>
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
                                <div class="today-card {{ job.status_class }}">
                                    <div class="today-time">{{ job.time_label }}</div>
                                    <div>
                                        <div class="today-title">{{ job.label }}</div>
                                        {% if job.assigned_to %}
                                            <div class="today-meta">Assigned: {{ job.assigned_to }}</div>
                                        {% endif %}
                                    </div>
                                    <div class="status-pill">{{ job.status or "No Status" }}</div>
                                </div>
                            </a>
                        {% endfor %}
                    </div>
                {% else %}
                    <div class="today-empty">No jobs scheduled for today.</div>
                {% endif %}
            </div>

            {% if view_mode == 'month' %}
                <div class="calendar-shell">
                    <div class="month-header">{{ month_name }}</div>

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
                                            <a class="job-link" href="{{ job.url }}">
                                                <div class="job-card {{ job.status_class }}">
                                                    <div class="job-time">{{ job.time_label }}</div>
                                                    <div class="job-label">{{ job.label }}</div>
                                                    {% if job.assigned_to %}
                                                        <div class="job-meta">Assigned: {{ job.assigned_to }}</div>
                                                    {% endif %}
                                                    <div class="job-meta">{{ job.status or "No Status" }}</div>
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
            {% else %}
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
                                            {% set start_minutes = (job.scheduled_start_time[:2]|int * 60 + job.scheduled_start_time[3:5]|int) if job.scheduled_start_time else 360 %}
                                            {% set end_minutes = (job.scheduled_end_time[:2]|int * 60 + job.scheduled_end_time[3:5]|int) if job.scheduled_end_time else (start_minutes + 60) %}
                                            {% set display_start = start_minutes if start_minutes >= 360 else 360 %}
                                            {% set display_end = end_minutes if end_minutes > display_start else (display_start + 60) %}
                                            {% set top_px = ((display_start - 360) * 1.2) %}
                                            {% set height_px = ((display_end - display_start) * 1.2) %}
                                            {% if height_px < 42 %}
                                                {% set height_px = 42 %}
                                            {% endif %}

                                            <a class="week-job-link" href="{{ job.url }}" style="top: {{ top_px }}px; height: {{ height_px }}px;">
                                                <div class="week-job-card {{ job.status_class }}" style="height: {{ height_px }}px;">
                                                    <div class="week-job-time">{{ job.time_label }}</div>
                                                    <div class="week-job-title">{{ job.label }}</div>
                                                    {% if job.assigned_to %}
                                                        <div class="week-job-meta">{{ job.assigned_to }}</div>
                                                    {% endif %}
                                                    <div class="week-job-meta">{{ job.status or "No Status" }}</div>
                                                </div>
                                            </a>
                                        {% endfor %}
                                    </div>
                                {% endfor %}
                            </div>
                        </div>
                    </div>
                </div>
            {% endif %}

            <div class="legend">
                Blue = Scheduled, Orange = In Progress, Green = Completed/Finished, Purple = Invoiced.
            </div>
        </div>
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
        prev_year=prev_year,
        prev_month=prev_month,
        next_year=next_year,
        next_month=next_month,
        week_days=week_days,
        week_time_rows=week_time_rows,
        week_label=week_label,
        prev_week=prev_week,
        next_week=next_week,
    )