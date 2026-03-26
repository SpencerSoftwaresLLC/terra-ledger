from datetime import datetime


def _money(value):
    try:
        return round(float(value or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def _clean(value):
    text = str(value or "").strip()
    if text.lower() in {"none", "null", "n/a"}:
        return ""
    return text


def _row_get(row, key, default=None):
    if row is None:
        return default
    if hasattr(row, "keys"):
        return row[key] if key in row.keys() and row[key] is not None else default
    return row.get(key, default)


def get_company_w2_readiness(company_profile):
    checks = [
        ("Legal Business Name", _row_get(company_profile, "legal_name", "")),
        ("EIN", _row_get(company_profile, "ein", "")),
        ("Address Line 1", _row_get(company_profile, "address_line_1", "")),
        ("City", _row_get(company_profile, "city", "")),
        ("State", _row_get(company_profile, "state", "")),
        ("ZIP Code", _row_get(company_profile, "zip_code", "")),
        ("W-2 Contact Name", _row_get(company_profile, "w2_contact_name", "")),
        ("W-2 Contact Phone", _row_get(company_profile, "w2_contact_phone", "")),
        ("W-2 Contact Email", _row_get(company_profile, "w2_contact_email", "")),
    ]

    missing = [label for label, value in checks if not _clean(value)]

    return {
        "ready": len(missing) == 0,
        "missing": missing,
    }


def get_employee_w2_readiness(employee_row):
    full_name = (
        _clean(_row_get(employee_row, "full_name"))
        or " ".join(
            part for part in [
                _clean(_row_get(employee_row, "first_name")),
                _clean(_row_get(employee_row, "middle_name")),
                _clean(_row_get(employee_row, "last_name")),
                _clean(_row_get(employee_row, "suffix")),
            ]
            if part
        ).strip()
    )

    address_line_1 = (
        _clean(_row_get(employee_row, "w2_address_line_1"))
        or _clean(_row_get(employee_row, "address_line_1"))
    )
    city = _clean(_row_get(employee_row, "w2_city")) or _clean(_row_get(employee_row, "city"))
    state = _clean(_row_get(employee_row, "w2_state")) or _clean(_row_get(employee_row, "state"))
    zip_code = _clean(_row_get(employee_row, "w2_zip")) or _clean(_row_get(employee_row, "zip"))

    checks = [
        ("Employee Full Name", full_name),
        ("SSN", _clean(_row_get(employee_row, "ssn", ""))),
        ("W-2 Address Line 1", address_line_1),
        ("W-2 City", city),
        ("W-2 State", state),
        ("W-2 ZIP", zip_code),
    ]

    missing = [label for label, value in checks if not _clean(value)]

    return {
        "ready": len(missing) == 0,
        "missing": missing,
        "resolved_name": full_name,
        "resolved_address_line_1": address_line_1,
        "resolved_address_line_2": _clean(_row_get(employee_row, "w2_address_line_2")) or _clean(_row_get(employee_row, "address_line_2")),
        "resolved_city": city,
        "resolved_state": state,
        "resolved_zip": zip_code,
    }


def get_employee_w2_source_data(conn, company_id, employee_id, tax_year):
    tax_year = str(tax_year)

    employee = conn.execute(
        """
        SELECT *
        FROM employees
        WHERE id = %s AND company_id = %s
        """,
        (employee_id, company_id),
    ).fetchone()

    if not employee:
        return None

    payroll_summary = conn.execute(
        """
        SELECT
            COALESCE(SUM(gross_pay), 0) AS gross_pay,
            COALESCE(SUM(federal_withholding), 0) AS federal_withholding,
            COALESCE(SUM(social_security), 0) AS social_security_tax,
            COALESCE(SUM(medicare), 0) AS medicare_tax,
            COALESCE(SUM(state_withholding), 0) AS state_withholding,
            COALESCE(SUM(local_tax), 0) AS local_tax,
            COALESCE(SUM(other_deductions), 0) AS other_deductions
        FROM payroll_entries
        WHERE company_id = %s
          AND employee_id = %s
          AND EXTRACT(YEAR FROM pay_date) = %s
        """,
        (company_id, employee_id, tax_year),
    ).fetchone()

    return {
        "employee": employee,
        "payroll_summary": payroll_summary,
        "tax_year": int(tax_year),
    }


def calculate_w2_boxes(source_data, employee_row=None, company_row=None):
    if not source_data:
        return None

    employee = employee_row or source_data.get("employee")
    payroll_summary = source_data.get("payroll_summary") or {}

    gross_pay = _money(_row_get(payroll_summary, "gross_pay", 0))
    federal_withholding = _money(_row_get(payroll_summary, "federal_withholding", 0))
    social_security_tax = _money(_row_get(payroll_summary, "social_security_tax", 0))
    medicare_tax = _money(_row_get(payroll_summary, "medicare_tax", 0))
    state_withholding = _money(_row_get(payroll_summary, "state_withholding", 0))
    local_tax = _money(_row_get(payroll_summary, "local_tax", 0))

    # Foundation assumptions for TerraLedger v1:
    # - Box 1 wages uses gross payroll wages
    # - Box 3 SS wages uses gross payroll wages
    # - Box 5 Medicare wages uses gross payroll wages
    # - State/local wages default to gross payroll wages
    # Later versions can subtract pre-tax benefits if those are added to payroll logic.
    box_1_wages = gross_pay
    box_2_federal = federal_withholding
    box_3_ss_wages = gross_pay
    box_4_ss_tax = social_security_tax
    box_5_medicare_wages = gross_pay
    box_6_medicare_tax = medicare_tax
    box_16_state_wages = gross_pay
    box_17_state_tax = state_withholding
    box_18_local_wages = gross_pay if local_tax > 0 else 0.0
    box_19_local_tax = local_tax
    box_20_locality_name = _clean(_row_get(employee, "county_of_residence")) or _clean(_row_get(employee, "county_of_principal_employment"))

    return {
        "box_1_wages": box_1_wages,
        "box_2_federal_withholding": box_2_federal,
        "box_3_social_security_wages": box_3_ss_wages,
        "box_4_social_security_tax": box_4_ss_tax,
        "box_5_medicare_wages_and_tips": box_5_medicare_wages,
        "box_6_medicare_tax_withheld": box_6_medicare_tax,
        "box_16_state_wages_tips_etc": box_16_state_wages,
        "box_17_state_income_tax": box_17_state_tax,
        "box_18_local_wages_tips_etc": box_18_local_wages,
        "box_19_local_income_tax": box_19_local_tax,
        "box_20_locality_name": box_20_locality_name,
    }


def build_w2_summary_data(conn, company_id, employee_id, tax_year):
    source_data = get_employee_w2_source_data(conn, company_id, employee_id, tax_year)
    if not source_data:
        return None

    employee = source_data["employee"]
    payroll_summary = source_data["payroll_summary"]
    boxes = calculate_w2_boxes(source_data, employee_row=employee)

    employee_readiness = get_employee_w2_readiness(employee)

    employee_name = (
        employee_readiness["resolved_name"]
        or f"Employee #{_row_get(employee, 'id', '')}"
    )

    summary = {
        "employee_id": _row_get(employee, "id"),
        "employee_name": employee_name,
        "ssn": _clean(_row_get(employee, "ssn")),
        "tax_year": source_data["tax_year"],
        "address_line_1": employee_readiness["resolved_address_line_1"],
        "address_line_2": employee_readiness["resolved_address_line_2"],
        "city": employee_readiness["resolved_city"],
        "state": employee_readiness["resolved_state"],
        "zip": employee_readiness["resolved_zip"],
        "gross_pay": _money(_row_get(payroll_summary, "gross_pay", 0)),
        "federal_withholding": _money(_row_get(payroll_summary, "federal_withholding", 0)),
        "social_security_tax": _money(_row_get(payroll_summary, "social_security_tax", 0)),
        "medicare_tax": _money(_row_get(payroll_summary, "medicare_tax", 0)),
        "state_withholding": _money(_row_get(payroll_summary, "state_withholding", 0)),
        "local_tax": _money(_row_get(payroll_summary, "local_tax", 0)),
        "boxes": boxes,
        "readiness": employee_readiness,
    }

    return summary


def get_company_w2_year_summary(conn, company_id, tax_year):
    tax_year = str(tax_year)

    row = conn.execute(
        """
        SELECT
            COALESCE(SUM(gross_pay), 0) AS total_wages,
            COALESCE(SUM(federal_withholding), 0) AS total_federal_withholding,
            COALESCE(SUM(social_security), 0) AS total_social_security_tax,
            COALESCE(SUM(medicare), 0) AS total_medicare_tax,
            COALESCE(SUM(state_withholding), 0) AS total_state_withholding,
            COALESCE(SUM(local_tax), 0) AS total_local_tax
        FROM payroll_entries
        WHERE company_id = %s
          AND EXTRACT(YEAR FROM pay_date) = %s
        """,
        (company_id, tax_year),
    ).fetchone()

    return {
        "tax_year": int(tax_year),
        "total_wages": _money(_row_get(row, "total_wages", 0)),
        "total_federal_withholding": _money(_row_get(row, "total_federal_withholding", 0)),
        "total_social_security_tax": _money(_row_get(row, "total_social_security_tax", 0)),
        "total_medicare_tax": _money(_row_get(row, "total_medicare_tax", 0)),
        "total_state_withholding": _money(_row_get(row, "total_state_withholding", 0)),
        "total_local_tax": _money(_row_get(row, "total_local_tax", 0)),
    }


def list_employee_w2_summaries(conn, company_id, tax_year):
    tax_year = str(tax_year)

    employees = conn.execute(
        """
        SELECT *
        FROM employees
        WHERE company_id = %s
        ORDER BY
            COALESCE(NULLIF(last_name, ''), NULLIF(full_name, ''), NULLIF(first_name, ''), 'ZZZ'),
            COALESCE(NULLIF(first_name, ''), ''),
            id
        """,
        (company_id,),
    ).fetchall()

    payroll_rows = conn.execute(
        """
        SELECT
            employee_id,
            COALESCE(SUM(gross_pay), 0) AS gross_pay,
            COALESCE(SUM(federal_withholding), 0) AS federal_withholding,
            COALESCE(SUM(social_security), 0) AS social_security_tax,
            COALESCE(SUM(medicare), 0) AS medicare_tax,
            COALESCE(SUM(state_withholding), 0) AS state_withholding,
            COALESCE(SUM(local_tax), 0) AS local_tax,
            COALESCE(SUM(other_deductions), 0) AS other_deductions
        FROM payroll_entries
        WHERE company_id = %s
          AND EXTRACT(YEAR FROM pay_date) = %s
        GROUP BY employee_id
        """,
        (company_id, tax_year),
    ).fetchall()

    payroll_map = {_row_get(row, "employee_id"): row for row in payroll_rows}

    summaries = []
    for employee in employees:
        employee_id = _row_get(employee, "id")
        summary_row = payroll_map.get(employee_id)

        source_data = {
            "employee": employee,
            "payroll_summary": summary_row or {},
            "tax_year": int(tax_year),
        }

        employee_readiness = get_employee_w2_readiness(employee)
        boxes = calculate_w2_boxes(source_data, employee_row=employee)

        employee_name = (
            employee_readiness["resolved_name"]
            or f"Employee #{employee_id}"
        )

        summaries.append({
            "employee_id": employee_id,
            "employee_name": employee_name,
            "tax_year": int(tax_year),
            "has_payroll_data": summary_row is not None,
            "gross_pay": _money(_row_get(summary_row, "gross_pay", 0)),
            "federal_withholding": _money(_row_get(summary_row, "federal_withholding", 0)),
            "social_security_tax": _money(_row_get(summary_row, "social_security_tax", 0)),
            "medicare_tax": _money(_row_get(summary_row, "medicare_tax", 0)),
            "state_withholding": _money(_row_get(summary_row, "state_withholding", 0)),
            "local_tax": _money(_row_get(summary_row, "local_tax", 0)),
            "readiness": employee_readiness,
            "boxes": boxes,
        })

    return summaries