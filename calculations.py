def recalc_quote(conn, quote_id):
    subtotal = conn.execute(
        "SELECT COALESCE(SUM(line_total), 0) FROM quote_items WHERE quote_id = ?",
        (quote_id,),
    ).fetchone()[0]

    total = float(subtotal or 0)

    conn.execute(
        "UPDATE quotes SET subtotal = ?, total = ? WHERE id = ?",
        (total, total, quote_id),
    )


def recalc_invoice(conn, invoice_id):
    subtotal = conn.execute(
        "SELECT COALESCE(SUM(line_total), 0) FROM invoice_items WHERE invoice_id = ?",
        (invoice_id,),
    ).fetchone()[0]

    row = conn.execute(
        "SELECT amount_paid FROM invoices WHERE id = ?",
        (invoice_id,),
    ).fetchone()

    amount_paid = float(row["amount_paid"] or 0) if row else 0
    total = float(subtotal or 0)
    balance_due = total - amount_paid
    status = "Paid" if balance_due <= 0 and total > 0 else "Unpaid"

    conn.execute(
        "UPDATE invoices SET subtotal = ?, total = ?, balance_due = ?, status = ? WHERE id = ?",
        (total, total, balance_due, status, invoice_id),
    )


def recalc_job(conn, job_id):
    row = conn.execute(
        """
        SELECT
            COALESCE(SUM(line_total), 0) AS revenue,
            COALESCE(SUM(cost_amount), 0) AS cost_total
        FROM job_items
        WHERE job_id = ?
        """,
        (job_id,),
    ).fetchone()

    revenue = float(row["revenue"] or 0)
    cost_total = float(row["cost_total"] or 0)
    profit = revenue - cost_total

    conn.execute(
        """
        UPDATE jobs
        SET revenue = ?, cost_total = ?, profit = ?
        WHERE id = ?
        """,
        (revenue, cost_total, profit, job_id),
    )


def get_pay_periods_per_year(pay_frequency: str) -> int:
    value = (pay_frequency or "Biweekly").strip().lower()

    mapping = {
        "weekly": 52,
        "biweekly": 26,
        "semi-monthly": 24,
        "semimonthly": 24,
        "monthly": 12,
        "quarterly": 4,
        "yearly": 1,
    }

    return mapping.get(value, 26)


def get_2026_tax_config(filing_status: str):
    filing_status = (filing_status or "Single").strip()

    if filing_status == "Married Filing Jointly":
        standard_deduction = 32200.0
        brackets = [
            (0, 0.10),
            (24800, 0.12),
            (100800, 0.22),
            (211400, 0.24),
            (403550, 0.32),
            (512450, 0.35),
            (768700, 0.37),
        ]
    elif filing_status == "Head of Household":
        standard_deduction = 24150.0
        brackets = [
            (0, 0.10),
            (17700, 0.12),
            (67450, 0.22),
            (108700, 0.24),
            (208850, 0.32),
            (536200, 0.35),
            (563350, 0.37),
        ]
    else:
        standard_deduction = 16100.0
        brackets = [
            (0, 0.10),
            (12400, 0.12),
            (50400, 0.22),
            (105700, 0.24),
            (201775, 0.32),
            (256225, 0.35),
            (640600, 0.37),
        ]

    return standard_deduction, brackets


def compute_annual_tax_from_brackets(taxable_income: float, brackets) -> float:
    taxable_income = max(0.0, float(taxable_income or 0))
    tax = 0.0

    for i, (lower, rate) in enumerate(brackets):
        upper = brackets[i + 1][0] if i + 1 < len(brackets) else None

        if taxable_income <= lower:
            break

        if upper is None:
            taxed_amount = taxable_income - lower
        else:
            taxed_amount = min(taxable_income, upper) - lower

        if taxed_amount > 0:
            tax += taxed_amount * rate

    return max(0.0, tax)


def calculate_federal_withholding_2026(
    gross_pay: float,
    filing_status: str,
    pay_frequency: str,
    step2_checked: bool = False,
    step3_amount: float = 0.0,
    step4a_other_income: float = 0.0,
    step4b_deductions: float = 0.0,
    step4c_extra_withholding: float = 0.0,
) -> float:
    periods = get_pay_periods_per_year(pay_frequency)
    standard_deduction, brackets = get_2026_tax_config(filing_status)

    gross_pay = max(0.0, float(gross_pay or 0))
    step3_amount = float(step3_amount or 0)
    step4a_other_income = float(step4a_other_income or 0)
    step4b_deductions = float(step4b_deductions or 0)
    step4c_extra_withholding = float(step4c_extra_withholding or 0)

    annual_wages = gross_pay * periods
    annual_taxable = annual_wages + step4a_other_income

    effective_deduction = standard_deduction + step4b_deductions

    if step2_checked:
        effective_deduction = effective_deduction / 2.0

    annual_taxable -= effective_deduction
    annual_tax = compute_annual_tax_from_brackets(annual_taxable, brackets)

    # W-4 Step 3 reduces annual withholding dollar-for-dollar
    annual_tax -= step3_amount
    annual_tax = max(0.0, annual_tax)

    period_withholding = annual_tax / periods
    period_withholding += step4c_extra_withholding

    return max(0.0, round(period_withholding, 2))


def calculate_federal_tax_annual(annual_income, filing_status="single"):
    annual_income = max(float(annual_income or 0), 0)
    filing_status = (filing_status or "single").strip().lower()

    if filing_status in ("married", "married filing jointly"):
        standard_deduction = 29200
        brackets = [
            (23200, 0.10),
            (94300, 0.12),
            (201050, 0.22),
            (383900, 0.24),
            (487450, 0.32),
            (731200, 0.35),
            (float("inf"), 0.37),
        ]
    else:
        standard_deduction = 14600
        brackets = [
            (11600, 0.10),
            (47150, 0.12),
            (100525, 0.22),
            (191950, 0.24),
            (243725, 0.32),
            (609350, 0.35),
            (float("inf"), 0.37),
        ]

    taxable_income = max(annual_income - standard_deduction, 0)

    tax = 0
    previous_limit = 0

    for limit, rate in brackets:
        if taxable_income > limit:
            taxed_amount = limit - previous_limit
        else:
            taxed_amount = taxable_income - previous_limit

        if taxed_amount > 0:
            tax += taxed_amount * rate

        if taxable_income <= limit:
            break

        previous_limit = limit

    return round(tax, 2)


def get_company_tax_rates(company_id, conn):
    profile = conn.execute(
        """
        SELECT
            state,
            county,
            city
        FROM company_profile
        WHERE company_id = ?
        """,
        (company_id,),
    ).fetchone()

    state = ""
    county = ""
    city = ""

    if profile:
        state = (profile["state"] or "").strip().upper()
        county = (profile["county"] or "").strip()
        city = (profile["city"] or "").strip()

    result = {
        "state": state,
        "county": county,
        "city": city,
        "state_rate": 0.0,
        "local_rate": 0.0,
        "local_name": "",
    }

    if state == "IN":
        result["state_rate"] = 0.0295

        indiana_county_rates = {
            "Tippecanoe": 0.0170,
            "Marion": 0.0202,
            "Hamilton": 0.0110,
            "Allen": 0.0148,
            "Lake": 0.0150,
        }

        county_key = county.replace(" County", "").strip().title()
        if county_key in indiana_county_rates:
            result["local_rate"] = indiana_county_rates[county_key]
            result["local_name"] = f"{county_key} County Income Tax"

    return result


def calculate_state_withholding(gross_pay, state_rate=0.0):
    gross_pay = max(float(gross_pay or 0), 0.0)
    return round(gross_pay * float(state_rate or 0), 2)


def calculate_local_withholding(gross_pay, local_rate=0.0):
    gross_pay = max(float(gross_pay or 0), 0.0)
    return round(gross_pay * float(local_rate or 0), 2)


def calculate_payroll_taxes(
    gross_pay,
    pay_schedule="Biweekly",
    filing_status="Single",
    state_tax_rate=0.0,
    local_tax_rate=0.0,
    include_social_security=True,
    include_medicare=True,
    step2_checked=False,
    step3_amount=0.0,
    step4a_other_income=0.0,
    step4b_deductions=0.0,
    step4c_extra_withholding=0.0,
):
    gross_pay = max(float(gross_pay or 0), 0)

    federal_tax = calculate_federal_withholding_2026(
        gross_pay=gross_pay,
        filing_status=filing_status,
        pay_frequency=pay_schedule,
        step2_checked=step2_checked,
        step3_amount=step3_amount,
        step4a_other_income=step4a_other_income,
        step4b_deductions=step4b_deductions,
        step4c_extra_withholding=step4c_extra_withholding,
    )

    state_tax = calculate_state_withholding(gross_pay, state_tax_rate)
    local_tax = calculate_local_withholding(gross_pay, local_tax_rate)

    social_security = gross_pay * 0.062 if include_social_security else 0
    medicare = gross_pay * 0.0145 if include_medicare else 0

    net_pay = gross_pay - federal_tax - state_tax - local_tax - social_security - medicare
    pay_periods = get_pay_periods_per_year(pay_schedule)
    annual_income = gross_pay * pay_periods

    return {
        "annual_income": round(annual_income, 2),
        "federal_tax": round(federal_tax, 2),
        "state_tax": round(state_tax, 2),
        "local_tax": round(local_tax, 2),
        "social_security": round(social_security, 2),
        "medicare": round(medicare, 2),
        "net_pay": round(net_pay, 2),
    }


def calculate_payroll_taxes_for_employee(employee, gross_pay, company_id, conn):
    gross_pay = max(float(gross_pay or 0), 0.0)

    pay_frequency = (
        employee["pay_frequency"]
        if "pay_frequency" in employee.keys() and employee["pay_frequency"]
        else "Biweekly"
    )

    filing_status = (
        employee["w4_filing_status"]
        if "w4_filing_status" in employee.keys() and employee["w4_filing_status"]
        else "Single"
    )

    step2_checked = False
    if "w4_step2_checked" in employee.keys():
        step2_checked = bool(employee["w4_step2_checked"])

    step3_amount = 0.0
    if "w4_step3_amount" in employee.keys():
        step3_amount = float(employee["w4_step3_amount"] or 0)

    step4a_other_income = 0.0
    if "w4_step4a_other_income" in employee.keys():
        step4a_other_income = float(employee["w4_step4a_other_income"] or 0)

    step4b_deductions = 0.0
    if "w4_step4b_deductions" in employee.keys():
        step4b_deductions = float(employee["w4_step4b_deductions"] or 0)

    step4c_extra_withholding = 0.0
    if "w4_step4c_extra_withholding" in employee.keys():
        step4c_extra_withholding = float(employee["w4_step4c_extra_withholding"] or 0)

    tax_rates = get_company_tax_rates(company_id, conn)
    state_tax_rate = float(tax_rates.get("state_rate", 0) or 0)
    local_tax_rate = float(tax_rates.get("local_rate", 0) or 0)

    return calculate_payroll_taxes(
        gross_pay=gross_pay,
        pay_schedule=pay_frequency,
        filing_status=filing_status,
        state_tax_rate=state_tax_rate,
        local_tax_rate=local_tax_rate,
        include_social_security=True,
        include_medicare=True,
        step2_checked=step2_checked,
        step3_amount=step3_amount,
        step4a_other_income=step4a_other_income,
        step4b_deductions=step4b_deductions,
        step4c_extra_withholding=step4c_extra_withholding,
    )