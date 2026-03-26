from datetime import date

from db import get_db_connection


SOCIAL_SECURITY_RATE = 0.062
MEDICARE_RATE = 0.0145


# Update these as needed.
# Use Title Case county names to match employees.py selections.
INDIANA_COUNTY_TAX_RATES = {
    "Adams": 0.01624,
    "Allen": 0.0148,
    "Bartholomew": 0.0150,
    "Benton": 0.0,
    "Blackford": 0.0175,
    "Boone": 0.011,
    "Brown": 0.01,
    "Carroll": 0.0245,
    "Cass": 0.0295,
    "Clark": 0.02,
    "Clay": 0.0175,
    "Clinton": 0.0225,
    "Crawford": 0.01,
    "Daviess": 0.0175,
    "Dearborn": 0.012,
    "Decatur": 0.02,
    "DeKalb": 0.0169,
    "Delaware": 0.0205,
    "Dubois": 0.0172,
    "Elkhart": 0.015,
    "Fayette": 0.012,
    "Floyd": 0.0175,
    "Fountain": 0.01,
    "Franklin": 0.01,
    "Fulton": 0.018,
    "Gibson": 0.01,
    "Grant": 0.0245,
    "Greene": 0.02,
    "Hamilton": 0.011,
    "Hancock": 0.017,
    "Harrison": 0.01,
    "Hendricks": 0.012,
    "Henry": 0.017,
    "Howard": 0.0195,
    "Huntington": 0.0125,
    "Jackson": 0.015,
    "Jasper": 0.022,
    "Jay": 0.0225,
    "Jefferson": 0.012,
    "Jennings": 0.01,
    "Johnson": 0.012,
    "Knox": 0.015,
    "Kosciusko": 0.0105,
    "LaGrange": 0.0165,
    "Lake": 0.015,
    "LaPorte": 0.0095,
    "Lawrence": 0.0175,
    "Madison": 0.0175,
    "Marion": 0.0202,
    "Marshall": 0.0181,
    "Martin": 0.015,
    "Miami": 0.0245,
    "Monroe": 0.02035,
    "Montgomery": 0.0232,
    "Morgan": 0.0175,
    "Newton": 0.01,
    "Noble": 0.0165,
    "Ohio": 0.01,
    "Orange": 0.012,
    "Owen": 0.01,
    "Parke": 0.02,
    "Perry": 0.01,
    "Pike": 0.0125,
    "Porter": 0.005,
    "Posey": 0.01,
    "Pulaski": 0.0145,
    "Putnam": 0.0175,
    "Randolph": 0.0175,
    "Ripley": 0.01,
    "Rush": 0.017,
    "St. Joseph": 0.01,
    "Scott": 0.012,
    "Shelby": 0.0125,
    "Spencer": 0.01,
    "Starke": 0.0175,
    "Steuben": 0.0195,
    "Sullivan": 0.016,
    "Switzerland": 0.01,
    "Tippecanoe": 0.015,
    "Tipton": 0.0175,
    "Union": 0.012,
    "Vanderburgh": 0.012,
    "Vermillion": 0.01,
    "Vigo": 0.0175,
    "Wabash": 0.0225,
    "Warren": 0.01,
    "Warrick": 0.01,
    "Washington": 0.012,
    "Wayne": 0.0125,
    "Wells": 0.015,
    "White": 0.0147,
    "Whitley": 0.0148,
}


def _safe_float(value, default=0.0):
    try:
        return float(value or 0)
    except Exception:
        return default


def _safe_int(value, default=0):
    try:
        return int(value or 0)
    except Exception:
        return default


def _clean_text(value):
    text = str(value or "").strip()
    if text.lower() in {"none", "null", "n/a"}:
        return ""
    return text


def _normalize_county_name(value):
    county = _clean_text(value)
    if not county:
        return ""

    county = county.replace(" County", "").strip()

    lowered = county.lower()
    special = {
        "st joseph": "St. Joseph",
        "saint joseph": "St. Joseph",
        "de kalb": "DeKalb",
        "dekalb": "DeKalb",
        "laporte": "LaPorte",
        "la porte": "LaPorte",
    }
    if lowered in special:
        return special[lowered]

    return " ".join(part.capitalize() for part in county.split())


def _get_company_profile(company_id):
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT * FROM company_profile WHERE company_id = %s",
            (company_id,),
        ).fetchone()
        return row
    finally:
        conn.close()


def _get_company_location_fallback(company_id):
    profile = _get_company_profile(company_id)
    if not profile:
        return {
            "state": "IN",
            "county": "",
        }

    keys = profile.keys()

    state = ""
    county = ""

    if "state" in keys and profile["state"]:
        state = _clean_text(profile["state"])
    elif "company_state" in keys and profile["company_state"]:
        state = _clean_text(profile["company_state"])

    if "county" in keys and profile["county"]:
        county = _clean_text(profile["county"])
    elif "company_county" in keys and profile["company_county"]:
        county = _clean_text(profile["company_county"])

    return {
        "state": state or "IN",
        "county": _normalize_county_name(county),
    }


def _annualize_pay(gross_pay, pay_frequency):
    gross_pay = _safe_float(gross_pay, 0)
    pay_frequency = _clean_text(pay_frequency) or "Biweekly"

    frequency_map = {
        "Weekly": 52,
        "Biweekly": 26,
        "Semimonthly": 24,
        "Semi-Monthly": 24,
        "Monthly": 12,
        "Quarterly": 4,
        "Yearly": 1,
    }

    periods = frequency_map.get(pay_frequency, 26)
    annualized = gross_pay * periods

    return annualized, periods


def _federal_withholding_annual(employee, annualized_wages):
    filing_status = _clean_text(
        employee.get("w4_filing_status")
        or employee.get("federal_filing_status")
        or "Single"
    )

    step2_checked = 1 if employee.get("w4_step2_checked") else 0
    step3_amount = _safe_float(employee.get("w4_step3_amount"), 0)
    step4a_other_income = _safe_float(employee.get("w4_step4a_other_income"), 0)
    step4b_deductions = _safe_float(employee.get("w4_step4b_deductions"), 0)
    step4c_extra_withholding = _safe_float(employee.get("w4_step4c_extra_withholding"), 0)

    taxable_annual = annualized_wages + step4a_other_income - step4b_deductions
    if taxable_annual < 0:
        taxable_annual = 0

    # Very simplified annualized federal withholding model.
    # Keeps your app working cleanly without breaking existing preview/save.
    if filing_status == "Married Filing Jointly":
        standard_allowance = 29200
        brackets = [
            (0, 0.10),
            (23200, 0.12),
            (94300, 0.22),
            (201050, 0.24),
            (383900, 0.32),
            (487450, 0.35),
            (731200, 0.37),
        ]
    elif filing_status == "Head of Household":
        standard_allowance = 21900
        brackets = [
            (0, 0.10),
            (16550, 0.12),
            (63100, 0.22),
            (100500, 0.24),
            (191950, 0.32),
            (243700, 0.35),
            (609350, 0.37),
        ]
    else:
        standard_allowance = 14600
        brackets = [
            (0, 0.10),
            (11600, 0.12),
            (47150, 0.22),
            (100525, 0.24),
            (191950, 0.32),
            (243725, 0.35),
            (609350, 0.37),
        ]

    if step2_checked:
        standard_allowance = standard_allowance / 2

    taxable_after_allowance = max(taxable_annual - standard_allowance, 0)

    annual_tax = 0.0
    for i, (floor, rate) in enumerate(brackets):
        ceiling = brackets[i + 1][0] if i + 1 < len(brackets) else None
        if taxable_after_allowance <= floor:
            break
        if ceiling is None:
            annual_tax += (taxable_after_allowance - floor) * rate
        else:
            annual_tax += (min(taxable_after_allowance, ceiling) - floor) * rate

    annual_tax = max(annual_tax - step3_amount, 0)
    annual_tax += step4c_extra_withholding * 26  # safe default scaling for extra withholding

    return annual_tax


def _get_indiana_state_tax(gross_pay):
    return _safe_float(gross_pay, 0) * 0.0295


def _pick_local_tax_county(employee, company_id):
    is_indiana_resident = bool(employee.get("is_indiana_resident"))
    residence_county = _normalize_county_name(employee.get("county_of_residence"))
    work_county = _normalize_county_name(employee.get("county_of_principal_employment"))

    employee_state = _clean_text(employee.get("state") or "IN").upper()
    company_fallback = _get_company_location_fallback(company_id)

    if is_indiana_resident:
        if residence_county:
            return {
                "county_used": residence_county,
                "county_source": "residence",
                "local_name": f"{residence_county} County",
            }
        if work_county:
            return {
                "county_used": work_county,
                "county_source": "principal_employment_fallback",
                "local_name": f"{work_county} County",
            }
    else:
        if work_county:
            return {
                "county_used": work_county,
                "county_source": "principal_employment",
                "local_name": f"{work_county} County",
            }

    if employee_state == "IN" and residence_county:
        return {
            "county_used": residence_county,
            "county_source": "residence_state_fallback",
            "local_name": f"{residence_county} County",
        }

    if company_fallback["county"]:
        return {
            "county_used": company_fallback["county"],
            "county_source": "company_fallback",
            "local_name": f"{company_fallback['county']} County",
        }

    return {
        "county_used": "",
        "county_source": "none",
        "local_name": "-",
    }


def calculate_payroll_taxes_for_employee(employee, gross_pay, company_id, conn=None):
    gross_pay = round(_safe_float(gross_pay, 0), 2)
    pay_frequency = _clean_text(employee.get("pay_frequency")) or "Biweekly"

    annualized_wages, periods = _annualize_pay(gross_pay, pay_frequency)

    annual_federal = _federal_withholding_annual(employee, annualized_wages)
    federal_withholding = round(annual_federal / periods, 2)

    state_withholding = round(_get_indiana_state_tax(gross_pay), 2)
    social_security = round(gross_pay * SOCIAL_SECURITY_RATE, 2)
    medicare = round(gross_pay * MEDICARE_RATE, 2)

    # --- LOCAL TAX FIX START ---
    county_info = _pick_local_tax_county(employee, company_id)

    county_used = _normalize_county_name(county_info.get("county_used"))
    county_source = county_info.get("county_source")
    local_name = county_info.get("local_name")

    # 🔥 EXTRA SAFETY: try multiple key formats
    local_tax_rate = 0.0

    if county_used:
        # Exact match
        if county_used in INDIANA_COUNTY_TAX_RATES:
            local_tax_rate = INDIANA_COUNTY_TAX_RATES[county_used]

        # Try stripping "County"
        elif county_used.replace(" County", "") in INDIANA_COUNTY_TAX_RATES:
            local_tax_rate = INDIANA_COUNTY_TAX_RATES[county_used.replace(" County", "")]

        # Try title-case normalization
        else:
            normalized = " ".join(word.capitalize() for word in county_used.split())
            local_tax_rate = INDIANA_COUNTY_TAX_RATES.get(normalized, 0.0)

    local_tax = round(gross_pay * local_tax_rate, 2)

    # --- DEBUG (THIS IS HUGE FOR YOU) ---
    print("---- LOCAL TAX DEBUG ----", flush=True)
    print("Employee ID:", employee.get("id"), flush=True)
    print("Is IN Resident:", employee.get("is_indiana_resident"), flush=True)
    print("County Used:", county_used, flush=True)
    print("County Source:", county_source, flush=True)
    print("Local Tax Rate:", local_tax_rate, flush=True)
    print("Local Tax Amount:", local_tax, flush=True)
    print("-------------------------", flush=True)
    # -----------------------------------

    return {
        "provider": "internal",
        "state_name": "Indiana",
        "local_name": local_name,
        "county_used": county_used or "",
        "county_source": county_source,
        "local_tax_rate": local_tax_rate,
        "federal_withholding": federal_withholding,
        "federal_tax": federal_withholding,
        "state_withholding": state_withholding,
        "state_tax": state_withholding,
        "social_security": social_security,
        "medicare": medicare,
        "local_tax": local_tax,
        "local_withholding": local_tax,
    }