from datetime import date, timedelta
from helpers import safe_int, parse_iso_date, date_to_iso
from routes.jobs import create_job_from_recurring_schedule

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