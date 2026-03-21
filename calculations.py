def _safe_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def recalc_quote(conn, quote_id):
    row = conn.execute(
        """
        SELECT COALESCE(SUM(line_total), 0) AS subtotal
        FROM quote_items
        WHERE quote_id = ?
        """,
        (quote_id,),
    ).fetchone()

    subtotal = _safe_float(row["subtotal"] if row and "subtotal" in row else 0)
    total = subtotal

    conn.execute(
        """
        UPDATE quotes
        SET subtotal = ?, total = ?
        WHERE id = ?
        """,
        (total, total, quote_id),
    )


def recalc_invoice(conn, invoice_id):
    subtotal_row = conn.execute(
        """
        SELECT COALESCE(SUM(line_total), 0) AS subtotal
        FROM invoice_items
        WHERE invoice_id = ?
        """,
        (invoice_id,),
    ).fetchone()

    subtotal = _safe_float(subtotal_row["subtotal"] if subtotal_row and "subtotal" in subtotal_row else 0)

    invoice_row = conn.execute(
        """
        SELECT amount_paid
        FROM invoices
        WHERE id = ?
        """,
        (invoice_id,),
    ).fetchone()

    amount_paid = _safe_float(invoice_row["amount_paid"] if invoice_row and "amount_paid" in invoice_row else 0)

    total = subtotal
    balance_due = total - amount_paid

    if total <= 0:
        status = "Draft"
    elif amount_paid <= 0:
        status = "Unpaid"
    elif balance_due > 0:
        status = "Partial"
    else:
        status = "Paid"

    conn.execute(
        """
        UPDATE invoices
        SET subtotal = ?, total = ?, balance_due = ?, status = ?
        WHERE id = ?
        """,
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

    revenue = _safe_float(row["revenue"] if row and "revenue" in row else 0)
    cost_total = _safe_float(row["cost_total"] if row and "cost_total" in row else 0)
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
    gross_pay,
    filing_status,
    pay_frequency,
    step2_checked=False,
    step3_amount=0.0,
    step4a_other_income=0.0,
    step4b_deductions=0.0,
    step4c_extra_withholding=0.0,
):
    periods = get_pay_periods_per_year(pay_frequency)
    standard_deduction, brackets = get_2026_tax_config(filing_status)

    gross_pay = _safe_float(gross_pay)

    annual_wages = gross_pay * periods
    annual_taxable = annual_wages + step4a_other_income

    effective_deduction = standard_deduction + step4b_deductions

    if step2_checked:
        effective_deduction = effective_deduction / 2.0

    annual_taxable -= effective_deduction
    annual_tax = compute_annual_tax_from_brackets(annual_taxable, brackets)

    annual_tax -= step3_amount
    annual_tax = max(0.0, annual_tax)

    period_withholding = annual_tax / periods
    period_withholding += step4c_extra_withholding

    return max(0.0, round(period_withholding, 2))


def get_company_tax_rates(company_id, conn):
    profile = conn.execute(
        """
        SELECT state, county, city
        FROM company_profile
        WHERE company_id = ?
        """,
        (company_id,),
    ).fetchone()

    state = (profile["state"] or "").strip().upper() if profile else ""
    county = (profile["county"] or "").strip() if profile else ""
    city = (profile["city"] or "").strip() if profile else ""

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