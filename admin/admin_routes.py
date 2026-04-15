from flask import Blueprint, redirect, url_for, flash, session
from decorators import login_required, subscription_required, require_permission
from db import get_db_connection
from permissions import get_role_defaults

admin_bp = Blueprint("admin", __name__)

@admin_bp.route("/fix-billable-column")
@login_required
@require_permission("can_manage_jobs")
def fix_billable_column():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # recurring_mowing_schedule_items.billable
        cur.execute("""
            ALTER TABLE recurring_mowing_schedule_items
            ALTER COLUMN billable DROP DEFAULT
        """)
        cur.execute("""
            ALTER TABLE recurring_mowing_schedule_items
            ALTER COLUMN billable TYPE BOOLEAN
            USING CASE
                WHEN COALESCE(billable::text, '') IN ('1', 'true', 't', 'yes', 'on') THEN TRUE
                ELSE FALSE
            END
        """)
        cur.execute("""
            ALTER TABLE recurring_mowing_schedule_items
            ALTER COLUMN billable SET DEFAULT TRUE
        """)

        # job_items.billable
        cur.execute("""
            ALTER TABLE job_items
            ALTER COLUMN billable DROP DEFAULT
        """)
        cur.execute("""
            ALTER TABLE job_items
            ALTER COLUMN billable TYPE BOOLEAN
            USING CASE
                WHEN COALESCE(billable::text, '') IN ('1', 'true', 't', 'yes', 'on') THEN TRUE
                ELSE FALSE
            END
        """)
        cur.execute("""
            ALTER TABLE job_items
            ALTER COLUMN billable SET DEFAULT TRUE
        """)

        conn.commit()
        return "Billable columns fixed successfully ✅"

    except Exception as e:
        conn.rollback()
        return f"Error: {e}"

    finally:
        conn.close()

@admin_bp.route("/fix_invoice_statuses")
@login_required
@subscription_required
@require_permission("can_manage_invoices")
def fix_invoice_statuses():
    conn = get_db_connection()
    cid = session["company_id"]

    try:
        rows = conn.execute(
            """
            SELECT j.id, j.title, j.recurring_schedule_id
            FROM jobs j
            WHERE j.company_id = %s
              AND COALESCE(j.generated_from_schedule, FALSE) = TRUE
              AND COALESCE(j.status, '') = 'Invoiced'
              AND NOT EXISTS (
                  SELECT 1
                  FROM invoices i
                  WHERE i.company_id = j.company_id
                    AND i.notes ILIKE %s
              )
            ORDER BY j.id ASC
            """,
            (cid, "Recurring schedule invoice for Schedule #" + "%"),
        ).fetchall()

        fixed_count = 0

        for row in rows:
            schedule_id = row["recurring_schedule_id"]
            if not schedule_id:
                continue

            invoice_match = conn.execute(
                """
                SELECT id
                FROM invoices
                WHERE company_id = %s
                  AND notes ILIKE %s
                LIMIT 1
                """,
                (cid, f"Recurring schedule invoice for Schedule #{schedule_id}%"),
            ).fetchone()

            if not invoice_match:
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'Completed'
                    WHERE id = %s
                      AND company_id = %s
                      AND COALESCE(status, '') = 'Invoiced'
                    """,
                    (row["id"], cid),
                )
                fixed_count += 1

        conn.commit()
        flash(f"Invoice status repair complete. Fixed {fixed_count} recurring job(s).")
    except Exception as e:
        conn.rollback()
        flash(f"Could not repair invoice statuses: {e}")
    finally:
        conn.close()

    return redirect(url_for("invoices.invoices"))

@admin_bp.route("/admin/fix_company_language")
@login_required
def fix_company_language():
    conn = get_db_connection()
    try:
        conn.execute(
            """
            ALTER TABLE company_profile
            ADD COLUMN IF NOT EXISTS language_preference TEXT DEFAULT 'en'
            """
        )
        conn.execute(
            """
            UPDATE company_profile
            SET language_preference = 'en'
            WHERE language_preference IS NULL OR TRIM(language_preference) = ''
            """
        )
        conn.commit()
        return "company_profile.language_preference fixed successfully."
    except Exception as e:
        conn.rollback()
        return f"Error: {e}"
    finally:
        conn.close()

