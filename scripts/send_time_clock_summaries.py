from datetime import date, timedelta

from db import get_db_connection, get_company_profile_row
from utils.time_clock import get_company_time_clock_start_day
from utils.time_clock_emailing import send_pay_period_summary_emails_for_company


def is_pay_period_end_day(start_day):
    today = date.today()
    end_day = (int(start_day) - 1) % 7
    return today.weekday() == end_day


def run_time_clock_summary_job():
    conn = get_db_connection()

    try:
        companies = conn.execute(
            """
            SELECT id
            FROM companies
            ORDER BY id
            """
        ).fetchall()
    finally:
        conn.close()

    for company in companies:
        company_id = company["id"]
        profile = get_company_profile_row(company_id)
        start_day = get_company_time_clock_start_day(profile)

        if is_pay_period_end_day(start_day):
            try:
                result = send_pay_period_summary_emails_for_company(company_id)
                print(f"Company {company_id}: sent={result['sent']} reason={result['reason']}", flush=True)
            except Exception as e:
                print(f"Company {company_id}: failed - {e}", flush=True)


if __name__ == "__main__":
    run_time_clock_summary_job()