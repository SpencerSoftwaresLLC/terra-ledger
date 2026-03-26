import os

from calculations import (
    calculate_federal_withholding_2026,
    get_company_tax_rates,
    calculate_state_withholding,
    calculate_local_withholding,
)


def safe_float(value, default=0.0):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return default


def get_employee_w4_value(employee, key, default=None):
    try:
        if hasattr(employee, "keys") and key in employee.keys():
            value = employee[key]
            return default if value is None else value
    except Exception:
        pass
    return default


def calculate_with_fallback_rules(
    conn,
    company_id,
    gross_pay,
    employee=None,
    employee_id=None,
    pay_date=None,
    pay_frequency=None,
    filing_status=None,
):
    company_tax = get_company_tax_rates(company_id, conn)
    gross_pay = safe_float(gross_pay, 0.0)

    employee_pay_frequency = get_employee_w4_value(employee, "pay_frequency", None)
    employee_filing_status = get_employee_w4_value(employee, "w4_filing_status", None) or get_employee_w4_value(
        employee, "federal_filing_status", None
    )

    pay_frequency = (pay_frequency or employee_pay_frequency or "Biweekly")
    filing_status = (filing_status or employee_filing_status or "Single")

    step2_checked = bool(get_employee_w4_value(employee, "w4_step2_checked", 0))
    step3_amount = safe_float(get_employee_w4_value(employee, "w4_step3_amount", 0), 0.0)
    step4a_other_income = safe_float(get_employee_w4_value(employee, "w4_step4a_other_income", 0), 0.0)
    step4b_deductions = safe_float(get_employee_w4_value(employee, "w4_step4b_deductions", 0), 0.0)
    step4c_extra_withholding = safe_float(get_employee_w4_value(employee, "w4_step4c_extra_withholding", 0), 0.0)

    federal_withholding = round(
        calculate_federal_withholding_2026(
            gross_pay=gross_pay,
            filing_status=filing_status,
            pay_frequency=pay_frequency,
            step2_checked=step2_checked,
            step3_amount=step3_amount,
            step4a_other_income=step4a_other_income,
            step4b_deductions=step4b_deductions,
            step4c_extra_withholding=step4c_extra_withholding,
        ),
        2,
    )

    state_withholding = calculate_state_withholding(
        gross_pay,
        company_tax.get("state", ""),
        filing_status=filing_status,
    )

    local_tax = calculate_local_withholding(
        gross_pay,
        company_tax.get("state", ""),
        company_tax.get("county", ""),
    )

    social_security = round(gross_pay * 0.062, 2)
    medicare = round(gross_pay * 0.0145, 2)

    net_pay = round(
        gross_pay
        - federal_withholding
        - state_withholding
        - local_tax
        - social_security
        - medicare,
        2,
    )

    return {
        "provider": "fallback",
        "federal_withholding": federal_withholding,
        "state_withholding": state_withholding,
        "local_tax": local_tax,
        "social_security": social_security,
        "medicare": medicare,
        "net_pay": net_pay,
        "state_name": company_tax.get("state", ""),
        "local_name": company_tax.get("local_name", ""),
    }


def calculate_with_vertex(
    conn,
    company_id,
    gross_pay,
    employee=None,
    employee_id=None,
    pay_date=None,
    pay_frequency=None,
    filing_status=None,
):
    raise NotImplementedError("Vertex integration not implemented yet.")


def calculate_with_symmetry(
    conn,
    company_id,
    gross_pay,
    employee=None,
    employee_id=None,
    pay_date=None,
    pay_frequency=None,
    filing_status=None,
):
    raise NotImplementedError("Symmetry integration not implemented yet.")


def calculate_payroll_taxes_for_employee(
    employee=None,
    gross_pay=0,
    company_id=None,
    conn=None,
    employee_id=None,
    pay_date=None,
    pay_frequency=None,
    filing_status=None,
):
    provider = (os.environ.get("PAYROLL_TAX_PROVIDER") or "fallback").strip().lower()

    if conn is None or company_id is None:
        raise ValueError("conn and company_id are required")

    try:
        if provider == "vertex":
            return calculate_with_vertex(
                conn=conn,
                company_id=company_id,
                gross_pay=gross_pay,
                employee=employee,
                employee_id=employee_id,
                pay_date=pay_date,
                pay_frequency=pay_frequency,
                filing_status=filing_status,
            )

        if provider == "symmetry":
            return calculate_with_symmetry(
                conn=conn,
                company_id=company_id,
                gross_pay=gross_pay,
                employee=employee,
                employee_id=employee_id,
                pay_date=pay_date,
                pay_frequency=pay_frequency,
                filing_status=filing_status,
            )
    except NotImplementedError:
        pass

    return calculate_with_fallback_rules(
        conn=conn,
        company_id=company_id,
        gross_pay=gross_pay,
        employee=employee,
        employee_id=employee_id,
        pay_date=pay_date,
        pay_frequency=pay_frequency,
        filing_status=filing_status,
    )