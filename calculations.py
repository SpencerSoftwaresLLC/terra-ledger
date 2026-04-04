def _safe_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------
# Basic US tax tables (starter set - expandable)
# ---------------------------------------------------------

US_STATE_TAX_TABLES = {
    "IN": {
        "type": "flat",
        "rate": 0.0295,
    },
    "TX": {
        "type": "none",
        "rate": 0.0,
    },
    "FL": {
        "type": "none",
        "rate": 0.0,
    },
    "CA": {
        "type": "progressive",
        "brackets": [
            (0, 0.01),
            (10412, 0.02),
            (24684, 0.04),
            (38959, 0.06),
            (54081, 0.08),
            (68350, 0.093),
        ],
    },
    "OH": {
        "type": "flat",
        "rate": 0.0,
    },
}

US_LOCAL_TAX_TABLES = {
    "IN": {
        "tippecanoe": 0.0170,
        "marion": 0.0202,
        "hamilton": 0.0110,
        "allen": 0.0148,
        "lake": 0.0150,
    },
    "OH": {
        "columbus": 0.0250,
        "cleveland": 0.0250,
    },
}


def recalc_quote(conn, quote_id):
    row = conn.execute(
        """
        SELECT COALESCE(SUM(line_total), 0) AS subtotal
        FROM quote_items
        WHERE quote_id = %s
        """,
        (quote_id,),
    ).fetchone()

    subtotal = _safe_float(row["subtotal"] if row and "subtotal" in row else 0)
    total = subtotal

    conn.execute(
        """
        UPDATE quotes
        SET subtotal = %s, total = %s
        WHERE id = %s
        """,
        (total, total, quote_id),
    )


def recalc_invoice(conn, invoice_id):
    subtotal_row = conn.execute(
        """
        SELECT COALESCE(SUM(line_total), 0) AS subtotal
        FROM invoice_items
        WHERE invoice_id = %s
        """,
        (invoice_id,),
    ).fetchone()

    subtotal = _safe_float(
        subtotal_row["subtotal"] if subtotal_row and "subtotal" in subtotal_row else 0
    )

    invoice_row = conn.execute(
        """
        SELECT amount_paid
        FROM invoices
        WHERE id = %s
        """,
        (invoice_id,),
    ).fetchone()

    amount_paid = _safe_float(
        invoice_row["amount_paid"] if invoice_row and "amount_paid" in invoice_row else 0
    )

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
        SET subtotal = %s, total = %s, balance_due = %s, status = %s
        WHERE id = %s
        """,
        (total, total, balance_due, status, invoice_id),
    )


def recalc_job(conn, job_id):
    row = conn.execute(
        """
        SELECT
            COALESCE(
                SUM(
                    CASE
                        WHEN COALESCE(billable, 1) = 1
                            THEN COALESCE(
                                line_total,
                                COALESCE(quantity, 0) * COALESCE(unit_price, COALESCE(sale_price, 0)),
                                0
                            )
                        ELSE 0
                    END
                ),
                0
            ) AS revenue,
            COALESCE(
                SUM(
                    COALESCE(
                        cost_amount,
                        COALESCE(quantity, 0) * COALESCE(unit_cost, 0),
                        0
                    )
                ),
                0
            ) AS cost_total
        FROM job_items
        WHERE job_id = %s
        """,
        (job_id,),
    ).fetchone()

    revenue = _safe_float(row["revenue"] if row else 0)
    cost_total = _safe_float(row["cost_total"] if row else 0)
    profit = revenue - cost_total

    conn.execute(
        """
        UPDATE jobs
        SET revenue = %s, cost_total = %s, profit = %s
        WHERE id = %s
        """,
        (revenue, cost_total, profit, job_id),
    )

def recalc_all_recurring_jobs(conn, company_id):
    rows = conn.execute(
        """
        SELECT id
        FROM jobs
        WHERE company_id = %s
          AND recurring_schedule_id IS NOT NULL
        ORDER BY id ASC
        """,
        (company_id,),
    ).fetchall()

    for row in rows:
        recalc_job(conn, row["id"])

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
    taxable_income = max(0.0, _safe_float(taxable_income))
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

    gross_pay = max(0.0, _safe_float(gross_pay))
    step3_amount = _safe_float(step3_amount)
    step4a_other_income = _safe_float(step4a_other_income)
    step4b_deductions = _safe_float(step4b_deductions)
    step4c_extra_withholding = _safe_float(step4c_extra_withholding)

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


def calculate_federal_tax_annual(annual_income, filing_status="single"):
    annual_income = max(_safe_float(annual_income), 0)
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
        SELECT state, county, city
        FROM company_profile
        WHERE company_id = %s
        """,
        (company_id,),
    ).fetchone()

    state = (profile["state"] or "").strip().upper() if profile else ""
    county = (profile["county"] or "").strip().lower() if profile else ""
    city = (profile["city"] or "").strip().lower() if profile else ""

    county = county.replace(" county", "").replace("county", "").strip()
    city = city.strip()

    state_rate = 0.0
    local_rate = 0.0
    local_name = ""

    state_table = US_STATE_TAX_TABLES.get(state)
    if state_table:
        if state_table["type"] == "flat":
            state_rate = _safe_float(state_table.get("rate", 0.0))
        elif state_table["type"] == "none":
            state_rate = 0.0

    local_table = US_LOCAL_TAX_TABLES.get(state, {})

    if county and county in local_table:
        local_rate = _safe_float(local_table.get(county, 0.0))
        local_name = f"{county.title()} County Tax"
    elif city and city in local_table:
        local_rate = _safe_float(local_table.get(city, 0.0))
        local_name = f"{city.title()} Local Tax"

    return {
        "state": state,
        "county": county,
        "city": city,
        "state_rate": state_rate,
        "local_rate": local_rate,
        "local_name": local_name,
    }


def _calculate_progressive_state_tax(gross_pay, brackets):
    gross_pay = max(_safe_float(gross_pay), 0.0)
    tax = 0.0
    previous_limit = 0

    for limit, rate in brackets:
        if gross_pay > limit:
            taxable = limit - previous_limit
        else:
            taxable = gross_pay - previous_limit

        if taxable > 0:
            tax += taxable * rate

        if gross_pay <= limit:
            break

        previous_limit = limit

    return round(tax, 2)


def calculate_state_withholding(gross_pay, state_or_rate, filing_status="Single"):
    gross_pay = max(_safe_float(gross_pay), 0.0)

    # backward compatibility: if caller passes a numeric rate, use it directly
    if isinstance(state_or_rate, (int, float)):
        return round(gross_pay * _safe_float(state_or_rate), 2)

    if isinstance(state_or_rate, str):
        raw = state_or_rate.strip()

        # backward compatibility: numeric string rate
        try:
            numeric_rate = float(raw)
            return round(gross_pay * numeric_rate, 2)
        except ValueError:
            pass

        state_code = raw.upper()
    else:
        state_code = ""

    if not state_code:
        return 0.0

    table = US_STATE_TAX_TABLES.get(state_code)
    if not table:
        return 0.0

    table_type = table.get("type", "none")

    if table_type == "none":
        return 0.0

    if table_type == "flat":
        return round(gross_pay * _safe_float(table.get("rate", 0.0)), 2)

    if table_type == "progressive":
        return _calculate_progressive_state_tax(gross_pay, table.get("brackets", []))

    return 0.0


def calculate_local_withholding(gross_pay, state_or_rate, county_or_city=None):
    gross_pay = max(_safe_float(gross_pay), 0.0)

    # backward compatibility: if only a numeric rate is passed, use it directly
    if county_or_city is None:
        if isinstance(state_or_rate, (int, float)):
            return round(gross_pay * _safe_float(state_or_rate), 2)

        if isinstance(state_or_rate, str):
            raw = state_or_rate.strip()
            try:
                numeric_rate = float(raw)
                return round(gross_pay * numeric_rate, 2)
            except ValueError:
                return 0.0

        return 0.0

    state = (state_or_rate or "").strip().upper()
    locality = (county_or_city or "").strip().lower().replace(" county", "").strip()

    state_table = US_LOCAL_TAX_TABLES.get(state, {})
    rate = _safe_float(state_table.get(locality, 0.0))

    return round(gross_pay * rate, 2)


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
    gross_pay = max(_safe_float(gross_pay), 0)

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

    state_tax = calculate_state_withholding(gross_pay, state_tax_rate, filing_status=filing_status)
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
    gross_pay = max(_safe_float(gross_pay), 0.0)

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
        step3_amount = _safe_float(employee["w4_step3_amount"])

    step4a_other_income = 0.0
    if "w4_step4a_other_income" in employee.keys():
        step4a_other_income = _safe_float(employee["w4_step4a_other_income"])

    step4b_deductions = 0.0
    if "w4_step4b_deductions" in employee.keys():
        step4b_deductions = _safe_float(employee["w4_step4b_deductions"])

    step4c_extra_withholding = 0.0
    if "w4_step4c_extra_withholding" in employee.keys():
        step4c_extra_withholding = _safe_float(employee["w4_step4c_extra_withholding"])

    tax_rates = get_company_tax_rates(company_id, conn)

    state_tax = calculate_state_withholding(
        gross_pay,
        tax_rates.get("state", ""),
        filing_status=filing_status,
    )

    local_tax = calculate_local_withholding(
        gross_pay,
        tax_rates.get("state", ""),
        tax_rates.get("county") or tax_rates.get("city") or "",
    )

    federal_tax = calculate_federal_withholding_2026(
        gross_pay=gross_pay,
        filing_status=filing_status,
        pay_frequency=pay_frequency,
        step2_checked=step2_checked,
        step3_amount=step3_amount,
        step4a_other_income=step4a_other_income,
        step4b_deductions=step4b_deductions,
        step4c_extra_withholding=step4c_extra_withholding,
    )

    social_security = round(gross_pay * 0.062, 2)
    medicare = round(gross_pay * 0.0145, 2)

    net_pay = round(
        gross_pay
        - federal_tax
        - state_tax
        - local_tax
        - social_security
        - medicare,
        2,
    )

    return {
        "annual_income": round(gross_pay * get_pay_periods_per_year(pay_frequency), 2),
        "federal_tax": round(federal_tax, 2),
        "federal_withholding": round(federal_tax, 2),
        "state_tax": round(state_tax, 2),
        "state_withholding": round(state_tax, 2),
        "local_tax": round(local_tax, 2),
        "social_security": round(social_security, 2),
        "medicare": round(medicare, 2),
        "net_pay": round(net_pay, 2),
        "state_name": tax_rates.get("state", ""),
        "local_name": tax_rates.get("local_name", ""),
    }