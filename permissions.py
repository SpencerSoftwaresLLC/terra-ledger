from db import get_db_connection

def get_role_defaults(role):
    role = (role or "").strip().lower()

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
        }

    if role == "manager":
        return {
            "can_manage_users": 1,
            "can_view_payroll": 0,
            "can_manage_payroll": 0,
            "can_view_bookkeeping": 0,
            "can_manage_bookkeeping": 0,
            "can_manage_jobs": 1,
            "can_manage_customers": 1,
            "can_manage_invoices": 1,
            "can_manage_settings": 0,
        }

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
    }