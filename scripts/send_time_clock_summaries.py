from datetime import date
import sys
import traceback

from db import get_db_connection, get_company_profile_row
from utils.time_clock import get_company_time_clock_start_day
from utils.time_clock_emailing import send_pay_period_summary_emails_for_company


def _safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _row_value(row, key, index=0, default=None):
    if row is None:
        return default

    try:
        if hasattr(row, "keys") and key in row.keys():
            return row[key]
    except Exception:
        pass

    try:
        return row[key]
    except Exception:
        pass

    try:
        return row[index]
    except Exception:
        pass

    return default


def is_pay_period_end_day(start_day):
    today = date.today()
    start_day_int = _safe_int(start_day, 0) % 7
    end_day = (start_day_int - 1) % 7
    return today.weekday() == end_day


def get_all_company_ids():
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT id
            FROM companies
            ORDER BY id
            """
        ).fetchall()

        company_ids = []
        for row in rows:
            company_id = _row_value(row, "id", 0)
            if company_id is not None:
                company_ids.append(company_id)

        return company_ids
    finally:
        conn.close()


def run_time_clock_summary_job():
    try:
        company_ids = get_all_company_ids()
    except Exception as e:
        print(f"Failed to load companies: {e}", flush=True)
        traceback.print_exc()
        return 1

    print(f"Found {len(company_ids)} companies.", flush=True)

    failures = 0

    for company_id in company_ids:
        try:
            profile = get_company_profile_row(company_id)
            start_day = get_company_time_clock_start_day(profile)

            if not is_pay_period_end_day(start_day):
                print(
                    f"Company {company_id}: skipped (not pay period end day, start_day={start_day})",
                    flush=True,
                )
                continue

            result = send_pay_period_summary_emails_for_company(company_id)

            sent = 0
            reason = ""
            if isinstance(result, dict):
                sent = result.get("sent", 0)
                reason = result.get("reason", "")
            else:
                reason = str(result)

            print(
                f"Company {company_id}: sent={sent} reason={reason}",
                flush=True,
            )

        except Exception as e:
            failures += 1
            print(f"Company {company_id}: failed - {e}", flush=True)
            traceback.print_exc()

    if failures:
        print(f"Job finished with {failures} company failure(s).", flush=True)
        return 1

    print("Job finished successfully.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(run_time_clock_summary_job())