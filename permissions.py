from db import get_db_connection


def get_role_defaults(role):
    role = (role or "").strip().lower()

    # =========================
    # OWNER (FULL ACCESS)
    # =========================
    if role == "owner":
        return {
            "can_manage_users": 1,
            "can_view_payroll": 1,
            "can_manage_payroll": 1,
            "can_view_bookkeeping": 1,
            "can_manage_bookkeeping": 1,
            "can_manage_jobs": 1,
            "can_manage_customers": 1,
            "can_manage_invoices": 1,
            "can_manage_settings": 1,

            # NEW
            "can_manage_messages": 1,
            "can_manage_payments": 1,
            "can_view_calendar": 1,
        }

    # =========================
    # MANAGER (LIMITED ADMIN)
    # =========================
    if role == "manager":
        return {
            "can_manage_users": 0,  # 🔒 safer default
            "can_view_payroll": 0,
            "can_manage_payroll": 0,
            "can_view_bookkeeping": 0,
            "can_manage_bookkeeping": 0,
            "can_manage_jobs": 1,
            "can_manage_customers": 1,
            "can_manage_invoices": 1,
            "can_manage_settings": 0,

            # NEW
            "can_manage_messages": 1,
            "can_manage_payments": 0,
            "can_view_calendar": 1,
        }

    # =========================
    # DEFAULT USER (EMPLOYEE)
    # =========================
    return {
        "can_manage_users": 0,
        "can_view_payroll": 0,
        "can_manage_payroll": 0,
        "can_view_bookkeeping": 0,
        "can_manage_bookkeeping": 0,
        "can_manage_jobs": 0,
        "can_manage_customers": 0,
        "can_manage_invoices": 0,
        "can_manage_settings": 0,

        # NEW
        "can_manage_messages": 0,
        "can_manage_payments": 0,
        "can_view_calendar": 1,  # employees can still see schedule
    }