import os
from flask import Blueprint, request

from db import get_db_connection
from utils.recurring import auto_generate_recurring_jobs
from utils.time_clock_emailing import run_time_clock_summary_emails

cron_bp = Blueprint("cron", __name__)


@cron_bp.route("/cron/run-all", methods=["POST"])
def run_all_cron():
    auth = request.headers.get("Authorization")
    if auth != os.environ.get("CRON_SECRET"):
        return "Unauthorized", 403

    conn = get_db_connection()

    try:
        companies = conn.execute(
            """
            SELECT id
            FROM companies
            """
        ).fetchall()

        for c in companies:
            try:
                auto_generate_recurring_jobs(conn, c["id"])
            except Exception as e:
                print(f"Recurring job error for company {c['id']}: {e}")

        try:
            run_time_clock_summary_emails()
        except Exception as e:
            print("Time clock summary email error:", e)

        conn.commit()
        return "Cron ran successfully", 200

    except Exception as e:
        conn.rollback()
        return f"Cron failed: {e}", 500

    finally:
        conn.close()