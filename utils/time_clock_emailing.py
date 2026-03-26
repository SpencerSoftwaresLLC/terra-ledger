from utils.emailing import send_company_email
from utils.time_clock import get_previous_pay_period, weekday_label
from db import get_db_connection, get_company_profile_row, get_company_users


def build_time_clock_summary_email(company_name, start_day, pay_period_start, pay_period_end, rows):
    day_label = weekday_label(start_day)
    end_label = weekday_label((start_day - 1) % 7)

    subject = f"{company_name} - Employee Hours Summary ({pay_period_start} to {pay_period_end})"

    if rows:
        html_rows = "".join(
            f"""
            <tr>
                <td style="padding:8px; border-bottom:1px solid #e5e7eb;">{row['employee_name']}</td>
                <td style="padding:8px; border-bottom:1px solid #e5e7eb; text-align:right;">{float(row['regular_hours'] or 0):.2f}</td>
                <td style="padding:8px; border-bottom:1px solid #e5e7eb; text-align:right;">{float(row['overtime_hours'] or 0):.2f}</td>
                <td style="padding:8px; border-bottom:1px solid #e5e7eb; text-align:right; font-weight:700;">{float(row['total_hours'] or 0):.2f}</td>
            </tr>
            """
            for row in rows
        )

        text_lines = []
        for row in rows:
            text_lines.append(
                f"{row['employee_name']}: Regular {float(row['regular_hours'] or 0):.2f}, "
                f"Overtime {float(row['overtime_hours'] or 0):.2f}, "
                f"Total {float(row['total_hours'] or 0):.2f}"
            )
        text_summary = "\n".join(text_lines)
    else:
        html_rows = """
        <tr>
            <td colspan="4" style="padding:10px;">No employee hours were recorded for this pay period.</td>
        </tr>
        """
        text_summary = "No employee hours were recorded for this pay period."

    html_body = f"""
    <div style="font-family: Arial, sans-serif; color:#1f2933; line-height:1.5;">
        <h2 style="margin-bottom:8px;">Employee Hours Summary</h2>
        <p style="margin-top:0;">
            <strong>Company:</strong> {company_name}<br>
            <strong>Pay Period:</strong> {pay_period_start} to {pay_period_end}<br>
            <strong>Cycle:</strong> {day_label} through {end_label}
        </p>

        <table style="width:100%; border-collapse:collapse; margin-top:16px;">
            <thead>
                <tr>
                    <th style="text-align:left; padding:8px; border-bottom:2px solid #d1d5db;">Employee</th>
                    <th style="text-align:right; padding:8px; border-bottom:2px solid #d1d5db;">Regular</th>
                    <th style="text-align:right; padding:8px; border-bottom:2px solid #d1d5db;">Overtime</th>
                    <th style="text-align:right; padding:8px; border-bottom:2px solid #d1d5db;">Total</th>
                </tr>
            </thead>
            <tbody>
                {html_rows}
            </tbody>
        </table>
    </div>
    """

    text_body = (
        f"Employee Hours Summary\n\n"
        f"Company: {company_name}\n"
        f"Pay Period: {pay_period_start} to {pay_period_end}\n"
        f"Cycle: {day_label} through {end_label}\n\n"
        f"{text_summary}"
    )

    return subject, html_body, text_body


def get_pay_period_employee_hours(company_id, pay_period_start, pay_period_end):
    conn = get_db_connection()

    rows = conn.execute(
        """
        SELECT
            e.id AS employee_id,
            COALESCE(NULLIF(TRIM(CONCAT(COALESCE(e.first_name, ''), ' ', COALESCE(e.last_name, ''))), ''), e.full_name, CONCAT('Employee #', e.id)) AS employee_name,
            COALESCE(SUM(t.total_hours), 0) AS total_hours
        FROM employees e
        LEFT JOIN employee_time_entries t
            ON t.employee_id = e.id
           AND t.company_id = e.company_id
           AND DATE(t.clock_in) >= %s
           AND DATE(t.clock_in) <= %s
        WHERE e.company_id = %s
          AND e.is_active = 1
        GROUP BY e.id, e.first_name, e.last_name, e.full_name
        ORDER BY employee_name
        """,
        (pay_period_start.isoformat(), pay_period_end.isoformat(), company_id),
    ).fetchall()

    conn.close()

    results = []
    for row in rows:
        total_hours = float(row["total_hours"] or 0)
        regular_hours = min(total_hours, 40.0)
        overtime_hours = max(total_hours - 40.0, 0.0)

        results.append({
            "employee_id": row["employee_id"],
            "employee_name": row["employee_name"],
            "regular_hours": round(regular_hours, 2),
            "overtime_hours": round(overtime_hours, 2),
            "total_hours": round(total_hours, 2),
        })

    return results


def send_pay_period_summary_emails_for_company(company_id):
    profile = get_company_profile_row(company_id)
    users = get_company_users(company_id)

    if not users:
        return {"sent": 0, "skipped": 1, "reason": "No active users with email."}

    start_day = 2
    if profile and profile.get("time_clock_pay_period_start_day") is not None:
        try:
            start_day = int(profile["time_clock_pay_period_start_day"])
        except Exception:
            start_day = 2

    pay_period_start, pay_period_end = get_previous_pay_period(start_day)

    company_name = "TerraLedger"
    if profile and profile.get("display_name"):
        company_name = profile["display_name"]

    rows = get_pay_period_employee_hours(company_id, pay_period_start, pay_period_end)
    subject, html_body, text_body = build_time_clock_summary_email(
        company_name=company_name,
        start_day=start_day,
        pay_period_start=pay_period_start.isoformat(),
        pay_period_end=pay_period_end.isoformat(),
        rows=rows,
    )

    sent_count = 0

    for user in users:
        user_email = (user.get("email") or "").strip()
        if not user_email:
            continue

        send_company_email(
            company_id=company_id,
            user_id=user["id"],
            to_email=user_email,
            subject=subject,
            html=html_body,
            body=text_body,
        )
        sent_count += 1

    return {"sent": sent_count, "skipped": 0, "reason": None}