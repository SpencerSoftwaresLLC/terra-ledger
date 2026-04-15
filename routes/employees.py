from flask import Blueprint, request, redirect, url_for, session, flash, render_template_string
from flask_wtf.csrf import generate_csrf
from html import escape
from datetime import datetime, date, timedelta

from db import (
    get_db_connection,
    ensure_employee_status_column,
    ensure_employee_name_columns,
    ensure_employee_payroll_columns,
    ensure_employee_tax_columns,
    get_employee_columns,
    ensure_employee_time_entries_table,
    ensure_company_profile_table,
    ensure_company_time_clock_columns,
)
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page
from utils.time_clock_emailing import send_pay_period_summary_emails_for_company
from utils.time_clock import get_previous_pay_period


employees_bp = Blueprint("employees", __name__)


INDIANA_COUNTIES = [
    "Adams", "Allen", "Bartholomew", "Benton", "Blackford", "Boone", "Brown",
    "Carroll", "Cass", "Clark", "Clay", "Clinton", "Crawford", "Daviess",
    "Dearborn", "Decatur", "DeKalb", "Delaware", "Dubois", "Elkhart", "Fayette",
    "Floyd", "Fountain", "Franklin", "Fulton", "Gibson", "Grant", "Greene",
    "Hamilton", "Hancock", "Harrison", "Hendricks", "Henry", "Howard",
    "Huntington", "Jackson", "Jasper", "Jay", "Jefferson", "Jennings",
    "Johnson", "Knox", "Kosciusko", "LaGrange", "Lake", "LaPorte", "Lawrence",
    "Madison", "Marion", "Marshall", "Martin", "Miami", "Monroe", "Montgomery",
    "Morgan", "Newton", "Noble", "Ohio", "Orange", "Owen", "Parke", "Perry",
    "Pike", "Porter", "Posey", "Pulaski", "Putnam", "Randolph", "Ripley",
    "Rush", "St. Joseph", "Scott", "Shelby", "Spencer", "Starke", "Steuben",
    "Sullivan", "Switzerland", "Tippecanoe", "Tipton", "Union", "Vanderburgh",
    "Vermillion", "Vigo", "Wabash", "Warren", "Warrick", "Washington", "Wayne",
    "Wells", "White", "Whitley",
]


def _get_lang():
    lang = str(session.get("language") or session.get("language_preference") or "en").strip().lower()
    return "es" if lang == "es" else "en"


def _t(lang, en, es):
    return es if lang == "es" else en


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


def _normalize_ssn(value):
    raw = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(raw) == 9:
        return f"{raw[:3]}-{raw[3:5]}-{raw[5:]}"
    return str(value or "").strip()


def _employee_is_active_is_boolean():
    conn = get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_name = 'employees'
              AND column_name = 'is_active'
            """
        ).fetchone()

        if not row:
            return False

        return str(row["data_type"]).lower() == "boolean"
    finally:
        conn.close()


def _active_where_sql(column_name="is_active"):
    if _employee_is_active_is_boolean():
        return f"COALESCE({column_name}, TRUE) = TRUE"
    return f"COALESCE({column_name}, 1) = 1"


def _active_update_value(is_active: bool):
    if _employee_is_active_is_boolean():
        return True if is_active else False
    return 1 if is_active else 0


def ensure_employee_profile_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS first_name TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS middle_name TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS last_name TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS suffix TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS full_name TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS phone TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS email TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS position TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS hire_date DATE")

    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS pay_type TEXT DEFAULT 'Hourly'")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS hourly_rate NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS overtime_rate NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS salary_amount NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS default_hours NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS payroll_notes TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE")

    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS federal_filing_status TEXT DEFAULT 'Single'")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS pay_frequency TEXT DEFAULT 'Biweekly'")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w4_step2_checked BOOLEAN DEFAULT FALSE")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w4_step3_amount NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w4_step4a_other_income NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w4_step4b_deductions NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w4_step4c_extra_withholding NUMERIC(12,2) DEFAULT 0")

    conn.commit()
    conn.close()


def ensure_employee_local_tax_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS address_line_1 TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS address_line_2 TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS city TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS state TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS zip TEXT")

    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS county_of_residence TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS county_of_principal_employment TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS county_tax_effective_year INTEGER")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS is_indiana_resident BOOLEAN DEFAULT TRUE")

    conn.commit()
    conn.close()


def ensure_employee_w2_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS ssn TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w2_address_line_1 TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w2_address_line_2 TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w2_city TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w2_state TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w2_zip TEXT")

    conn.commit()
    conn.close()


def _employee_display_name(employee):
    cols = employee.keys()

    first_name = employee["first_name"] if "first_name" in cols and employee["first_name"] else ""
    middle_name = employee["middle_name"] if "middle_name" in cols and employee["middle_name"] else ""
    last_name = employee["last_name"] if "last_name" in cols and employee["last_name"] else ""
    suffix = employee["suffix"] if "suffix" in cols and employee["suffix"] else ""
    full_name = employee["full_name"] if "full_name" in cols and employee["full_name"] else ""
    single_name = employee["name"] if "name" in cols and employee["name"] else ""
    employee_name_field = employee["employee_name"] if "employee_name" in cols and employee["employee_name"] else ""
    display_name = employee["display_name"] if "display_name" in cols and employee["display_name"] else ""

    assembled = " ".join(part for part in [first_name, middle_name, last_name, suffix] if part).strip()
    if assembled:
        return assembled
    if full_name:
        return full_name
    if single_name:
        return single_name
    if employee_name_field:
        return employee_name_field
    if display_name:
        return display_name
    return f"Employee #{employee['id']}"


def _county_options_html(selected_value="", lang="en"):
    selected_value = str(selected_value or "").strip()
    options = [f"<option value=''>{_t(lang, 'Select county', 'Seleccionar condado')}</option>"]
    for county in INDIANA_COUNTIES:
        sel = "selected" if county == selected_value else ""
        options.append(f"<option value='{escape(county)}' {sel}>{escape(county)}</option>")
    return "".join(options)


def _employee_form_html(
    employee=None,
    form_action="",
    submit_label="Save Employee",
    page_title="Employee",
    lang="en",
    user_options=None,
    selected_user_id=None,
):
    employee = employee or {}
    user_options = user_options or []

    def val(key, default=""):
        if hasattr(employee, "keys"):
            value = employee[key] if key in employee.keys() and employee[key] is not None else default
        else:
            value = employee.get(key, default)
        return escape(str(value))

    def selected(key, expected, default=""):
        if hasattr(employee, "keys"):
            current = employee[key] if key in employee.keys() and employee[key] is not None else default
        else:
            current = employee.get(key, default)
        return "selected" if str(current) == str(expected) else ""

    def checked(key, default=False):
        if hasattr(employee, "keys"):
            if key in employee.keys() and employee[key] is not None:
                return "checked" if bool(employee[key]) else ""
            return "checked" if default else ""
        return "checked" if bool(employee.get(key, default)) else ""

    county_of_residence = ""
    county_of_principal_employment = ""
    if hasattr(employee, "keys"):
        county_of_residence = employee["county_of_residence"] if "county_of_residence" in employee.keys() and employee["county_of_residence"] else ""
        county_of_principal_employment = employee["county_of_principal_employment"] if "county_of_principal_employment" in employee.keys() and employee["county_of_principal_employment"] else ""
    else:
        county_of_residence = employee.get("county_of_residence", "")
        county_of_principal_employment = employee.get("county_of_principal_employment", "")

    current_year = date.today().year
    county_tax_year_default = current_year
    if hasattr(employee, "keys"):
        if "county_tax_effective_year" in employee.keys() and employee["county_tax_effective_year"] is not None:
            county_tax_year_default = employee["county_tax_effective_year"]
    else:
        county_tax_year_default = employee.get("county_tax_effective_year", current_year)

    if selected_user_id is None:
        if hasattr(employee, "keys"):
            if "user_id" in employee.keys() and employee["user_id"] is not None:
                selected_user_id = employee["user_id"]
        else:
            selected_user_id = employee.get("user_id")

    selected_user_id = "" if selected_user_id in (None, "", 0, "0") else str(selected_user_id)

    user_options_html = [
        f"<option value=''>{_t(lang, 'None', 'Ninguno')}</option>"
    ]
    for user in user_options:
        uid = escape(str(user["id"]))
        uname = escape(str(user["name"] or ""))
        uemail = escape(str(user["email"] or ""))
        label = f"{uname} ({uemail})" if uemail else uname
        is_selected = "selected" if str(user["id"]) == selected_user_id else ""
        user_options_html.append(
            f"<option value='{uid}' {is_selected}>{label}</option>"
        )

    csrf_token_value = generate_csrf()

    content = f"""
    <style>
        .employee-form-page {{
            display:grid;
            gap:18px;
        }}

        .employee-form-head {{
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:12px;
            flex-wrap:wrap;
        }}

        @media (max-width: 640px) {{
            .employee-form-head {{
                align-items:flex-start;
            }}
        }}
    </style>

    <div class='employee-form-page'>
        <div class='card'>
            <div class='employee-form-head'>
                <div>
                    <h1 style='margin-bottom:6px;'>{escape(page_title)}</h1>
                    <p class='muted' style='margin:0;'>{_t(lang, 'Manage employee information, payroll setup, federal withholding, Indiana local tax setup, and W-2 identity details.', 'Administra la información del empleado, la configuración de nómina, la retención federal, la configuración del impuesto local de Indiana y los datos de identidad W-2.')}</p>
                </div>
                <div class='row-actions'>
                    <a class='btn warning' href='{url_for("payroll.employee_payroll")}'>{_t(lang, 'Payroll', 'Nómina')}</a>
                    <a class='btn secondary' href='{url_for("employees.employees")}'>{_t(lang, 'Back to Employees', 'Volver a Empleados')}</a>
                </div>
            </div>
        </div>

        <form method='post' action='{form_action}'>
            <input type="hidden" name="csrf_token" value="{csrf_token_value}">
            <div class='card'>
                <h2>{_t(lang, 'Employee Information', 'Información del Empleado')}</h2>
                <div class='grid'>
                    <div>
                        <label>{_t(lang, 'First Name', 'Nombre')}</label>
                        <input name='first_name' value='{val("first_name")}' required>
                    </div>
                    <div>
                        <label>{_t(lang, 'Middle Name', 'Segundo Nombre')}</label>
                        <input name='middle_name' value='{val("middle_name")}'>
                    </div>
                    <div>
                        <label>{_t(lang, 'Last Name', 'Apellido')}</label>
                        <input name='last_name' value='{val("last_name")}' required>
                    </div>
                    <div>
                        <label>{_t(lang, 'Suffix', 'Sufijo')}</label>
                        <input name='suffix' value='{val("suffix")}' placeholder='Jr, Sr, II'>
                    </div>
                    <div>
                        <label>{_t(lang, 'Phone', 'Teléfono')}</label>
                        <input name='phone' value='{val("phone")}'>
                    </div>
                    <div>
                        <label>{_t(lang, 'Email', 'Correo')}</label>
                        <input name='email' type='email' value='{val("email")}'>
                    </div>
                    <div>
                        <label>{_t(lang, 'Position', 'Puesto')}</label>
                        <input name='position' value='{val("position")}'>
                    </div>
                    <div>
                        <label>{_t(lang, 'Hire Date', 'Fecha de Contratación')}</label>
                        <input name='hire_date' type='date' value='{val("hire_date")}'>
                    </div>
                    <div style='grid-column:1 / -1;'>
                        <label>{_t(lang, 'Linked User Login', 'Usuario Vinculado')}</label>
                        <select name='user_id'>
                            {"".join(user_options_html)}
                        </select>
                        <div class='muted small' style='margin-top:6px;'>
                            {_t(lang, 'Optional. Link this employee to a TerraLedger user login so they can use employee-specific features like self clock in / clock out.', 'Opcional. Vincula este empleado con un usuario de TerraLedger para que pueda usar funciones específicas del empleado como registrar su propia entrada / salida.')}
                        </div>
                    </div>
                </div>
            </div>

            <div class='card'>
                <h2>{_t(lang, 'Employee Address', 'Dirección del Empleado')}</h2>
                <div class='grid'>
                    <div style='grid-column:1 / -1;'>
                        <label>{_t(lang, 'Address Line 1', 'Dirección Línea 1')}</label>
                        <input name='address_line_1' value='{val("address_line_1")}'>
                    </div>
                    <div style='grid-column:1 / -1;'>
                        <label>{_t(lang, 'Address Line 2', 'Dirección Línea 2')}</label>
                        <input name='address_line_2' value='{val("address_line_2")}'>
                    </div>
                    <div>
                        <label>{_t(lang, 'City', 'Ciudad')}</label>
                        <input name='city' value='{val("city")}'>
                    </div>
                    <div>
                        <label>{_t(lang, 'State', 'Estado')}</label>
                        <input name='state' value='{val("state", "IN")}' maxlength='2'>
                    </div>
                    <div>
                        <label>{_t(lang, 'ZIP', 'Código Postal')}</label>
                        <input name='zip' value='{val("zip")}'>
                    </div>
                </div>
            </div>

            <div class='card'>
                <h2>{_t(lang, 'W-2 Identity & Mailing Info', 'Información de Identidad y Envío W-2')}</h2>
                <div class='grid'>
                    <div>
                        <label>SSN</label>
                        <input name='ssn' value='{val("ssn")}' placeholder='123-45-6789'>
                    </div>

                    <div style='grid-column:1 / -1;'>
                        <label>{_t(lang, 'W-2 Address Line 1', 'Dirección W-2 Línea 1')}</label>
                        <input name='w2_address_line_1' value='{val("w2_address_line_1")}' placeholder='{_t(lang, "Leave blank to use employee address later if desired", "Déjalo en blanco para usar la dirección del empleado después si lo deseas")}'>
                    </div>

                    <div style='grid-column:1 / -1;'>
                        <label>{_t(lang, 'W-2 Address Line 2', 'Dirección W-2 Línea 2')}</label>
                        <input name='w2_address_line_2' value='{val("w2_address_line_2")}'>
                    </div>

                    <div>
                        <label>{_t(lang, 'W-2 City', 'Ciudad W-2')}</label>
                        <input name='w2_city' value='{val("w2_city")}'>
                    </div>

                    <div>
                        <label>{_t(lang, 'W-2 State', 'Estado W-2')}</label>
                        <input name='w2_state' value='{val("w2_state")}' maxlength='2'>
                    </div>

                    <div>
                        <label>{_t(lang, 'W-2 ZIP', 'Código Postal W-2')}</label>
                        <input name='w2_zip' value='{val("w2_zip")}'>
                    </div>
                </div>

                <div class='muted' style='margin-top:12px;'>
                    {_t(lang, 'These fields will be used for year-end W-2 preparation and employee statement printing.', 'Estos campos se usarán para la preparación de fin de año del W-2 y para imprimir estados del empleado.')}
                </div>
            </div>

            <div class='card'>
                <h2>{_t(lang, 'Payroll Setup', 'Configuración de Nómina')}</h2>
                <div class='grid'>
                    <div>
                        <label>{_t(lang, 'Pay Type', 'Tipo de Pago')}</label>
                        <select name='pay_type'>
                            <option value='Hourly' {selected("pay_type", "Hourly", "Hourly")}>{_t(lang, 'Hourly', 'Por Hora')}</option>
                            <option value='Salary' {selected("pay_type", "Salary")}>{_t(lang, 'Salary', 'Salario')}</option>
                        </select>
                    </div>
                    <div>
                        <label>{_t(lang, 'Hourly Rate', 'Tarifa por Hora')}</label>
                        <input name='hourly_rate' type='number' step='0.01' value='{val("hourly_rate", "0")}'>
                    </div>
                    <div>
                        <label>{_t(lang, 'Overtime Rate', 'Tarifa de Horas Extra')}</label>
                        <input name='overtime_rate' type='number' step='0.01' value='{val("overtime_rate", "0")}'>
                    </div>
                    <div>
                        <label>{_t(lang, 'Salary Amount', 'Monto del Salario')}</label>
                        <input name='salary_amount' type='number' step='0.01' value='{val("salary_amount", "0")}'>
                    </div>
                    <div>
                        <label>{_t(lang, 'Default Hours', 'Horas Predeterminadas')}</label>
                        <input name='default_hours' type='number' step='0.01' value='{val("default_hours", "0")}'>
                    </div>
                    <div>
                        <label>{_t(lang, 'Pay Frequency', 'Frecuencia de Pago')}</label>
                        <select name='pay_frequency'>
                            <option value='Weekly' {selected("pay_frequency", "Weekly")}>{_t(lang, 'Weekly', 'Semanal')}</option>
                            <option value='Biweekly' {selected("pay_frequency", "Biweekly", "Biweekly")}>{_t(lang, 'Biweekly', 'Quincenal')}</option>
                            <option value='Semimonthly' {selected("pay_frequency", "Semimonthly")}>{_t(lang, 'Semimonthly', 'Dos Veces al Mes')}</option>
                            <option value='Monthly' {selected("pay_frequency", "Monthly")}>{_t(lang, 'Monthly', 'Mensual')}</option>
                        </select>
                    </div>
                </div>

                <div style='margin-top:16px;'>
                    <label>{_t(lang, 'Payroll Notes', 'Notas de Nómina')}</label>
                    <textarea name='payroll_notes'>{val("payroll_notes")}</textarea>
                </div>
            </div>

            <div class='card'>
                <h2>{_t(lang, 'Federal Tax / W-4', 'Impuesto Federal / W-4')}</h2>
                <div class='grid'>
                    <div>
                        <label>{_t(lang, 'Federal Filing Status', 'Estado de Declaración Federal')}</label>
                        <select name='federal_filing_status'>
                            <option value='Single' {selected("federal_filing_status", "Single", "Single")}>{_t(lang, 'Single', 'Soltero')}</option>
                            <option value='Married Filing Jointly' {selected("federal_filing_status", "Married Filing Jointly")}>{_t(lang, 'Married Filing Jointly', 'Casado Declarando en Conjunto')}</option>
                            <option value='Married Filing Separately' {selected("federal_filing_status", "Married Filing Separately")}>{_t(lang, 'Married Filing Separately', 'Casado Declarando por Separado')}</option>
                            <option value='Head of Household' {selected("federal_filing_status", "Head of Household")}>{_t(lang, 'Head of Household', 'Cabeza de Familia')}</option>
                        </select>
                    </div>

                    <div class='checkbox-field'>
                        <label class='checkbox-label'>
                            <input type='checkbox' name='w4_step2_checked' {checked("w4_step2_checked")}>
                            {_t(lang, 'Step 2 Box Checked', 'Casilla del Paso 2 Marcada')}
                        </label>
                        <div class='muted small'>{_t(lang, 'Check if employee has multiple jobs or spouse works.', 'Márcalo si el empleado tiene varios trabajos o si su cónyuge trabaja.')}</div>
                    </div>

                    <div>
                        <label>{_t(lang, 'Step 3 Credits', 'Créditos del Paso 3')}</label>
                        <input name='w4_step3_amount' type='number' step='0.01' value='{val("w4_step3_amount", "0")}'>
                    </div>

                    <div>
                        <label>{_t(lang, 'Step 4(a) Other Income', 'Paso 4(a) Otros Ingresos')}</label>
                        <input name='w4_step4a_other_income' type='number' step='0.01' value='{val("w4_step4a_other_income", "0")}'>
                    </div>

                    <div>
                        <label>{_t(lang, 'Step 4(b) Deductions', 'Paso 4(b) Deducciones')}</label>
                        <input name='w4_step4b_deductions' type='number' step='0.01' value='{val("w4_step4b_deductions", "0")}'>
                    </div>

                    <div>
                        <label>{_t(lang, 'Step 4(c) Extra Withholding', 'Paso 4(c) Retención Extra')}</label>
                        <input name='w4_step4c_extra_withholding' type='number' step='0.01' value='{val("w4_step4c_extra_withholding", "0")}'>
                    </div>
                </div>
            </div>

            <div class='card'>
                <h2>{_t(lang, 'Indiana Local Tax Setup', 'Configuración de Impuesto Local de Indiana')}</h2>
                <div class='grid'>
                    <div class='checkbox-field'>
                        <label class='checkbox-label'>
                            <input type='checkbox' name='is_indiana_resident' {checked("is_indiana_resident", True)}>
                            {_t(lang, 'Indiana Resident on January 1', 'Residente de Indiana el 1 de Enero')}
                        </label>
                        <div class='muted small'>{_t(lang, 'Residents usually use county of residence. Non-residents usually use county of principal employment.', 'Los residentes normalmente usan el condado de residencia. Los no residentes normalmente usan el condado de empleo principal.')}</div>
                    </div>

                    <div>
                        <label>{_t(lang, 'County Tax Effective Year', 'Año Efectivo del Impuesto del Condado')}</label>
                        <input name='county_tax_effective_year' type='number' step='1' value='{escape(str(county_tax_year_default))}'>
                    </div>

                    <div>
                        <label>{_t(lang, 'County of Residence', 'Condado de Residencia')}</label>
                        <select name='county_of_residence'>
                            {_county_options_html(county_of_residence, lang)}
                        </select>
                    </div>

                    <div>
                        <label>{_t(lang, 'County of Principal Employment', 'Condado de Empleo Principal')}</label>
                        <select name='county_of_principal_employment'>
                            {_county_options_html(county_of_principal_employment, lang)}
                        </select>
                    </div>
                </div>

                <div class='muted' style='margin-top:12px;'>
                    {_t(lang, 'Store the January 1 county values here so payroll can calculate Indiana local withholding correctly.', 'Guarda aquí los valores del condado del 1 de enero para que la nómina pueda calcular correctamente la retención local de Indiana.')}
                </div>
            </div>

            <div class='card'>
                <div class='row-actions'>
                    <button class='btn success' type='submit'>{escape(submit_label)}</button>
                    <a class='btn secondary' href='{url_for("employees.employees")}'>{_t(lang, 'Cancel', 'Cancelar')}</a>
                </div>
            </div>
        </form>
    </div>
    """
    return content


def _format_hours(hours_value):
    try:
        return f"{float(hours_value or 0):.2f}"
    except Exception:
        return "0.00"


def _weekday_label(day_number, lang="en"):
    labels_en = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }
    labels_es = {
        0: "Lunes",
        1: "Martes",
        2: "Miércoles",
        3: "Jueves",
        4: "Viernes",
        5: "Sábado",
        6: "Domingo",
    }
    labels = labels_es if lang == "es" else labels_en
    return labels.get(int(day_number), "Wednesday" if lang != "es" else "Miércoles")


def _get_company_time_clock_start_day(company_id):
    ensure_company_profile_table()
    ensure_company_time_clock_columns()

    conn = get_db_connection()
    row = conn.execute(
        """
        SELECT time_clock_pay_period_start_day
        FROM company_profile
        WHERE company_id = %s
        """,
        (company_id,),
    ).fetchone()
    conn.close()

    if row and row["time_clock_pay_period_start_day"] is not None:
        try:
            value = int(row["time_clock_pay_period_start_day"])
            if 0 <= value <= 6:
                return value
        except Exception:
            pass

    return 2


def _get_current_pay_period(start_day):
    today = date.today()
    start_day = int(start_day)

    days_since_start = (today.weekday() - start_day) % 7
    start_date = today - timedelta(days=days_since_start)
    end_date = start_date + timedelta(days=6)

    return start_date, end_date


@employees_bp.route("/employees")
@login_required
@subscription_required
@require_permission("can_manage_employees")
def employees():
    lang = _get_lang()

    ensure_employee_profile_columns()
    ensure_employee_status_column()
    ensure_employee_name_columns()
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()
    ensure_employee_local_tax_columns()
    ensure_employee_w2_columns()

    conn = get_db_connection()
    cid = session["company_id"]
    show = (request.args.get("show", "active") or "active").strip().lower()

    cols = get_employee_columns()

    if "full_name" in cols:
        name_col = "full_name"
    elif "name" in cols:
        name_col = "name"
    elif "employee_name" in cols:
        name_col = "employee_name"
    else:
        name_col = "id"

    active_sql = _active_where_sql("is_active")

    if show == "all":
        rows = conn.execute(
            f"""
            SELECT *
            FROM employees
            WHERE company_id = %s
            ORDER BY {name_col} ASC, id DESC
            """,
            (cid,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT *
            FROM employees
            WHERE company_id = %s
              AND {active_sql}
            ORDER BY {name_col} ASC, id DESC
            """,
            (cid,),
        ).fetchall()

    conn.close()

    employee_rows_list = []
    mobile_cards = []

    for r in rows:
        csrf_token_value = generate_csrf()

        display_name = (
            escape(str(r[name_col])) if name_col != "id" and r[name_col] else f"Employee #{r['id']}"
        )
        position_value = escape(str(r["position"])) if "position" in r.keys() and r["position"] else "-"
        status_text = _t(lang, "Active", "Activo") if bool(r["is_active"]) else _t(lang, "Inactive", "Inactivo")
        status_badge_class = "active" if bool(r["is_active"]) else "inactive"

        if bool(r["is_active"]):
            status_form = f"""
            <form method='post' action='{url_for('employees.deactivate_employee', employee_id=r['id'])}' class='inline-form'>
                <input type='hidden' name='csrf_token' value='{csrf_token_value}'>
                <button class='btn warning small' type='submit'>{_t(lang, 'Set Inactive', 'Marcar Inactivo')}</button>
            </form>
            """
        else:
            status_form = f"""
            <form method='post' action='{url_for('employees.activate_employee', employee_id=r['id'])}' class='inline-form'>
                <input type='hidden' name='csrf_token' value='{csrf_token_value}'>
                <button class='btn success small' type='submit'>{_t(lang, 'Set Active', 'Marcar Activo')}</button>
            </form>
            """

        delete_form = f"""
        <form method='post'
              action='{url_for("employees.delete_employee", employee_id=r["id"])}'
              class='inline-form'
              onsubmit="return confirm('{_t(lang, "Delete this employee? This cannot be undone.", "¿Eliminar este empleado? Esto no se puede deshacer.")}');">
            <input type="hidden" name="csrf_token" value="{csrf_token_value}">
            <button class='btn danger small' type='submit'>{_t(lang, 'Delete', 'Eliminar')}</button>
        </form>
        """

        employee_rows_list.append(
            f"""
            <tr>
                <td>{display_name}</td>
                <td>{position_value}</td>
                <td><span class='status-pill {status_badge_class}'>{status_text}</span></td>
                <td>
                    <div class='row-actions'>
                        <a class='btn secondary small' href='{url_for("employees.view_employee", employee_id=r["id"])}'>{_t(lang, 'View', 'Ver')}</a>
                        {status_form}
                        {delete_form}
                    </div>
                </td>
            </tr>
            """
        )

        mobile_cards.append(
            f"""
            <div class='mobile-list-card employee-simple-card'>
                <div class='mobile-list-top'>
                    <div>
                        <div class='mobile-list-title'>{display_name}</div>
                        <div class='mobile-list-subtitle'>{position_value}</div>
                    </div>
                    <div class='mobile-badge {status_badge_class}'>{status_text}</div>
                </div>

                <div class='mobile-list-actions'>
                    <a class='btn secondary small' href='{url_for("employees.view_employee", employee_id=r["id"])}'>{_t(lang, 'View', 'Ver')}</a>
                    {status_form}
                    {delete_form}
                </div>
            </div>
            """
        )

    employee_rows = "".join(employee_rows_list)
    mobile_cards_html = "".join(mobile_cards)

    active_btn_class = "btn" if show != "all" else "btn secondary"
    all_btn_class = "btn" if show == "all" else "btn secondary"

    content = f"""
    <style>
        .employees-page {{
            display:grid;
            gap:18px;
        }}

        .employees-head {{
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:12px;
            flex-wrap:wrap;
        }}

        .table-wrap {{
            width:100%;
            overflow-x:auto;
        }}

        .mobile-only {{
            display:none;
        }}

        .desktop-only {{
            display:block;
        }}

        .mobile-list {{
            display:grid;
            gap:12px;
        }}

        .mobile-list-card {{
            border:1px solid rgba(15, 23, 42, 0.08);
            border-radius:14px;
            padding:14px;
            background:#fff;
            box-shadow:0 1px 2px rgba(15, 23, 42, 0.04);
        }}

        .employee-simple-card {{
            display:grid;
            gap:12px;
        }}

        .mobile-list-top {{
            display:flex;
            justify-content:space-between;
            align-items:flex-start;
            gap:10px;
        }}

        .mobile-list-title {{
            font-weight:700;
            color:#0f172a;
            line-height:1.25;
            word-break:break-word;
            font-size:1rem;
        }}

        .mobile-list-subtitle {{
            margin-top:4px;
            font-size:.9rem;
            color:#64748b;
            line-height:1.25;
            word-break:break-word;
        }}

        .mobile-badge {{
            font-size:.85rem;
            font-weight:700;
            padding:6px 10px;
            border-radius:999px;
            white-space:nowrap;
            border:1px solid rgba(15, 23, 42, 0.08);
            background:#f1f5f9;
            color:#334155;
        }}

        .mobile-badge.active {{
            background:#ecfdf3;
            color:#166534;
            border-color:rgba(22, 101, 52, 0.14);
        }}

        .mobile-badge.inactive {{
            background:#fef2f2;
            color:#991b1b;
            border-color:rgba(153, 27, 27, 0.14);
        }}

        .status-pill {{
            display:inline-flex;
            align-items:center;
            justify-content:center;
            padding:6px 10px;
            border-radius:999px;
            font-size:.85rem;
            font-weight:700;
            border:1px solid rgba(15, 23, 42, 0.08);
            background:#f1f5f9;
            color:#334155;
            white-space:nowrap;
        }}

        .status-pill.active {{
            background:#ecfdf3;
            color:#166534;
            border-color:rgba(22, 101, 52, 0.14);
        }}

        .status-pill.inactive {{
            background:#fef2f2;
            color:#991b1b;
            border-color:rgba(153, 27, 27, 0.14);
        }}

        .mobile-list-actions {{
            display:flex;
            gap:8px;
            flex-wrap:wrap;
        }}

        @media (max-width: 640px) {{
            .desktop-only {{
                display:none !important;
            }}

            .mobile-only {{
                display:block !important;
            }}

            .employees-head .row-actions {{
                width:100%;
            }}

            .employees-head .row-actions .btn {{
                flex:1 1 auto;
                text-align:center;
            }}

            .mobile-list-actions .btn,
            .mobile-list-actions form {{
                flex:1 1 auto;
            }}

            .mobile-list-actions .btn,
            .mobile-list-actions button {{
                width:100%;
                text-align:center;
            }}
        }}
    </style>

    <div class='employees-page'>
        <div class='card'>
            <div class='employees-head'>
                <div>
                    <h1 style='margin-bottom:6px;'>{_t(lang, 'Employees', 'Empleados')}</h1>
                    <p class='muted' style='margin:0;'>{_t(lang, 'Manage active and inactive employees.', 'Administra empleados activos e inactivos.')}</p>
                </div>
                <div class='row-actions'>
                    <a href='{url_for("employees.employees", show="active")}' class='{active_btn_class}'>{_t(lang, 'Active Employees', 'Empleados Activos')}</a>
                    <a href='{url_for("employees.employees", show="all")}' class='{all_btn_class}'>{_t(lang, 'All Employees', 'Todos los Empleados')}</a>
                    <a href='{url_for("payroll.employee_payroll")}' class='btn warning'>{_t(lang, 'Payroll', 'Nómina')}</a>
                    <a href='{url_for("employees.new_employee")}' class='btn success'>+ {_t(lang, 'New Employee', 'Nuevo Empleado')}</a>
                </div>
            </div>
        </div>

        <div class='card'>
            <h2>{_t(lang, 'All Employees', 'Todos los Empleados') if show == 'all' else _t(lang, 'Active Employees', 'Empleados Activos')}</h2>

            <div class='table-wrap desktop-only'>
                <table>
                    <tr>
                        <th>{_t(lang, 'Name', 'Nombre')}</th>
                        <th>{_t(lang, 'Position', 'Puesto')}</th>
                        <th>{_t(lang, 'Status', 'Estado')}</th>
                        <th>{_t(lang, 'Actions', 'Acciones')}</th>
                    </tr>
                    {employee_rows or f"<tr><td colspan='4' class='muted'>{_t(lang, 'No employees found.', 'No se encontraron empleados.')}</td></tr>"}
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {mobile_cards_html or f"<div class='mobile-list-card muted'>{_t(lang, 'No employees found.', 'No se encontraron empleados.')}</div>"}
                </div>
            </div>
        </div>
    </div>
    """
    return render_page(content, _t(lang, "Employees", "Empleados"))


@employees_bp.route("/employees/new", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_employees")
def new_employee():
    lang = _get_lang()

    ensure_employee_profile_columns()
    ensure_employee_name_columns()
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()
    ensure_employee_status_column()
    ensure_employee_local_tax_columns()
    ensure_employee_w2_columns()
    ensure_employee_user_link_column()

    conn = get_db_connection()
    cid = session["company_id"]

    cols = get_employee_columns()

    user_options = conn.execute(
        """
        SELECT id, name, email
        FROM users
        WHERE company_id = %s
        ORDER BY name ASC, email ASC
        """,
        (cid,),
    ).fetchall()

    if request.method == "POST":
        first_name = _clean_text(request.form.get("first_name"))
        middle_name = _clean_text(request.form.get("middle_name"))
        last_name = _clean_text(request.form.get("last_name"))
        suffix = _clean_text(request.form.get("suffix"))
        phone = _clean_text(request.form.get("phone"))
        email = _clean_text(request.form.get("email"))
        position = _clean_text(request.form.get("position"))
        hire_date = _clean_text(request.form.get("hire_date"))
        pay_type = _clean_text(request.form.get("pay_type")) or "Hourly"
        linked_user_id = _safe_int(request.form.get("user_id"))

        address_line_1 = _clean_text(request.form.get("address_line_1"))
        address_line_2 = _clean_text(request.form.get("address_line_2"))
        city = _clean_text(request.form.get("city"))
        state = (_clean_text(request.form.get("state")) or "IN").upper()
        zip_code = _clean_text(request.form.get("zip"))

        ssn = _normalize_ssn(request.form.get("ssn"))
        w2_address_line_1 = _clean_text(request.form.get("w2_address_line_1"))
        w2_address_line_2 = _clean_text(request.form.get("w2_address_line_2"))
        w2_city = _clean_text(request.form.get("w2_city"))
        w2_state = _clean_text(request.form.get("w2_state")).upper()
        w2_zip = _clean_text(request.form.get("w2_zip"))

        federal_filing_status = _clean_text(request.form.get("federal_filing_status")) or "Single"
        pay_frequency = _clean_text(request.form.get("pay_frequency")) or "Biweekly"

        hourly_rate = _safe_float(request.form.get("hourly_rate"), 0)
        overtime_rate = _safe_float(request.form.get("overtime_rate"), 0)
        salary_amount = _safe_float(request.form.get("salary_amount"), 0)
        default_hours = _safe_float(request.form.get("default_hours"), 0)
        payroll_notes = _clean_text(request.form.get("payroll_notes"))

        county_of_residence = _clean_text(request.form.get("county_of_residence"))
        county_of_principal_employment = _clean_text(request.form.get("county_of_principal_employment"))
        county_tax_effective_year = _safe_int(
            request.form.get("county_tax_effective_year"),
            date.today().year
        )

        if not first_name or not last_name:
            flash(_t(lang, "First and last name are required.", "El nombre y el apellido son obligatorios."))
            conn.close()
            try:
                return render_page(
                    _employee_form_html(
                        form_action=url_for("employees.new_employee"),
                        submit_label=_t(lang, "Save Employee", "Guardar Empleado"),
                        page_title=_t(lang, "Add Employee", "Agregar Empleado"),
                        lang=lang,
                        user_options=user_options,
                        selected_user_id=linked_user_id,
                    ),
                    _t(lang, "Add Employee", "Agregar Empleado"),
                )
            except TypeError:
                return render_page(
                    _employee_form_html(
                        form_action=url_for("employees.new_employee"),
                        submit_label=_t(lang, "Save Employee", "Guardar Empleado"),
                        page_title=_t(lang, "Add Employee", "Agregar Empleado"),
                        lang=lang,
                    ),
                    _t(lang, "Add Employee", "Agregar Empleado"),
                )

        if linked_user_id:
            linked_user = conn.execute(
                """
                SELECT id
                FROM users
                WHERE id = %s
                  AND company_id = %s
                """,
                (linked_user_id, cid),
            ).fetchone()

            if not linked_user:
                conn.close()
                flash(_t(lang, "Selected user was not found.", "El usuario seleccionado no fue encontrado."))
                try:
                    return render_page(
                        _employee_form_html(
                            form_action=url_for("employees.new_employee"),
                            submit_label=_t(lang, "Save Employee", "Guardar Empleado"),
                            page_title=_t(lang, "Add Employee", "Agregar Empleado"),
                            lang=lang,
                            user_options=user_options,
                            selected_user_id=linked_user_id,
                        ),
                        _t(lang, "Add Employee", "Agregar Empleado"),
                    )
                except TypeError:
                    return render_page(
                        _employee_form_html(
                            form_action=url_for("employees.new_employee"),
                            submit_label=_t(lang, "Save Employee", "Guardar Empleado"),
                            page_title=_t(lang, "Add Employee", "Agregar Empleado"),
                            lang=lang,
                        ),
                        _t(lang, "Add Employee", "Agregar Empleado"),
                    )

            if "user_id" in cols:
                existing_link = conn.execute(
                    """
                    SELECT id, full_name, first_name, last_name
                    FROM employees
                    WHERE company_id = %s
                      AND user_id = %s
                    LIMIT 1
                    """,
                    (cid, linked_user_id),
                ).fetchone()

                if existing_link:
                    existing_name = (
                        (existing_link["full_name"] or "").strip()
                        or f"{(existing_link['first_name'] or '').strip()} {(existing_link['last_name'] or '').strip()}".strip()
                        or f"Employee #{existing_link['id']}"
                    )
                    conn.close()
                    flash(_t(
                        lang,
                        f"That user is already linked to {existing_name}.",
                        f"Ese usuario ya está vinculado a {existing_name}.",
                    ))
                    try:
                        return render_page(
                            _employee_form_html(
                                form_action=url_for("employees.new_employee"),
                                submit_label=_t(lang, "Save Employee", "Guardar Empleado"),
                                page_title=_t(lang, "Add Employee", "Agregar Empleado"),
                                lang=lang,
                                user_options=user_options,
                                selected_user_id=linked_user_id,
                            ),
                            _t(lang, "Add Employee", "Agregar Empleado"),
                        )
                    except TypeError:
                        return render_page(
                            _employee_form_html(
                                form_action=url_for("employees.new_employee"),
                                submit_label=_t(lang, "Save Employee", "Guardar Empleado"),
                                page_title=_t(lang, "Add Employee", "Agregar Empleado"),
                                lang=lang,
                            ),
                            _t(lang, "Add Employee", "Agregar Empleado"),
                        )

        full_name = " ".join(part for part in [first_name, middle_name, last_name, suffix] if part).strip()

        is_active = _active_update_value(True)
        w4_step2_checked = 1 if request.form.get("w4_step2_checked") else 0
        is_indiana_resident = bool(request.form.get("is_indiana_resident"))

        w4_step3_amount = _safe_float(request.form.get("w4_step3_amount"), 0)
        w4_step4a_other_income = _safe_float(request.form.get("w4_step4a_other_income"), 0)
        w4_step4b_deductions = _safe_float(request.form.get("w4_step4b_deductions"), 0)
        w4_step4c_extra_withholding = _safe_float(request.form.get("w4_step4c_extra_withholding"), 0)

        if pay_type.lower() == "salary":
            hourly_rate = 0
            overtime_rate = 0
        else:
            salary_amount = 0

        data = {
            "company_id": cid,
            "user_id": linked_user_id,
            "first_name": first_name,
            "middle_name": middle_name,
            "last_name": last_name,
            "suffix": suffix,
            "full_name": full_name,
            "phone": phone,
            "email": email,
            "position": position,
            "hire_date": hire_date,
            "address_line_1": address_line_1,
            "address_line_2": address_line_2,
            "city": city,
            "state": state,
            "zip": zip_code,
            "ssn": ssn,
            "w2_address_line_1": w2_address_line_1,
            "w2_address_line_2": w2_address_line_2,
            "w2_city": w2_city,
            "w2_state": w2_state,
            "w2_zip": w2_zip,
            "pay_type": pay_type,
            "hourly_rate": hourly_rate,
            "overtime_rate": overtime_rate,
            "salary_amount": salary_amount,
            "default_hours": default_hours,
            "payroll_notes": payroll_notes,
            "is_active": is_active,
            "federal_filing_status": federal_filing_status,
            "pay_frequency": pay_frequency,
            "w4_step2_checked": w4_step2_checked,
            "w4_step3_amount": w4_step3_amount,
            "w4_step4a_other_income": w4_step4a_other_income,
            "w4_step4b_deductions": w4_step4b_deductions,
            "w4_step4c_extra_withholding": w4_step4c_extra_withholding,
            "is_indiana_resident": is_indiana_resident,
            "county_of_residence": county_of_residence,
            "county_of_principal_employment": county_of_principal_employment,
            "county_tax_effective_year": county_tax_effective_year,
        }

        ordered_columns = [
            "company_id",
            "user_id",
            "first_name",
            "middle_name",
            "last_name",
            "suffix",
            "full_name",
            "phone",
            "email",
            "position",
            "hire_date",
            "address_line_1",
            "address_line_2",
            "city",
            "state",
            "zip",
            "ssn",
            "w2_address_line_1",
            "w2_address_line_2",
            "w2_city",
            "w2_state",
            "w2_zip",
            "pay_type",
            "hourly_rate",
            "overtime_rate",
            "salary_amount",
            "default_hours",
            "payroll_notes",
            "is_active",
            "federal_filing_status",
            "pay_frequency",
            "w4_step2_checked",
            "w4_step3_amount",
            "w4_step4a_other_income",
            "w4_step4b_deductions",
            "w4_step4c_extra_withholding",
            "is_indiana_resident",
            "county_of_residence",
            "county_of_principal_employment",
            "county_tax_effective_year",
        ]

        insert_cols = [col for col in ordered_columns if col in cols]
        insert_vals = [data[col] for col in insert_cols]

        placeholders = ", ".join(["%s"] * len(insert_cols))
        col_sql = ", ".join(insert_cols)

        conn.execute(
            f"INSERT INTO employees ({col_sql}) VALUES ({placeholders})",
            tuple(insert_vals),
        )
        conn.commit()
        conn.close()

        flash(_t(lang, "Employee added successfully.", "Empleado agregado correctamente."))
        return redirect(url_for("employees.employees"))

    conn.close()

    try:
        return render_page(
            _employee_form_html(
                form_action=url_for("employees.new_employee"),
                submit_label=_t(lang, "Save Employee", "Guardar Empleado"),
                page_title=_t(lang, "Add Employee", "Agregar Empleado"),
                lang=lang,
                user_options=user_options,
                selected_user_id=None,
            ),
            _t(lang, "Add Employee", "Agregar Empleado"),
        )
    except TypeError:
        return render_page(
            _employee_form_html(
                form_action=url_for("employees.new_employee"),
                submit_label=_t(lang, "Save Employee", "Guardar Empleado"),
                page_title=_t(lang, "Add Employee", "Agregar Empleado"),
                lang=lang,
            ),
            _t(lang, "Add Employee", "Agregar Empleado"),
        )


@employees_bp.route("/employees/<int:employee_id>")
@login_required
@subscription_required
@require_permission("can_view_employees")
def view_employee(employee_id):
    lang = _get_lang()

    ensure_employee_profile_columns()
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()
    ensure_employee_local_tax_columns()
    ensure_employee_w2_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    employee = conn.execute(
        """
        SELECT *
        FROM employees
        WHERE id = %s AND company_id = %s
        """,
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        flash(_t(lang, "Employee not found.", "Empleado no encontrado."))
        return redirect(url_for("employees.employees"))

    try:
        payroll_rows = conn.execute(
            """
            SELECT
                id,
                pay_date,
                pay_period_start,
                pay_period_end,
                pay_type,
                hours_regular,
                hours_overtime,
                gross_pay,
                net_pay
            FROM payroll_entries
            WHERE employee_id = %s AND company_id = %s
            ORDER BY
                CASE WHEN pay_date IS NULL OR pay_date = '' THEN 1 ELSE 0 END,
                pay_date DESC,
                id DESC
            """,
            (employee_id, cid),
        ).fetchall()
    except Exception:
        payroll_rows = []

    conn.close()

    cols = employee.keys()
    employee_name = _employee_display_name(employee)

    def get_text(column_name, default="-"):
        if column_name in cols and employee[column_name] not in (None, ""):
            return str(employee[column_name])
        return default

    def get_float(column_name, default=0.0):
        if column_name in cols and employee[column_name] is not None:
            try:
                return float(employee[column_name])
            except (TypeError, ValueError):
                return default
        return default

    def get_bool(column_name, default=False):
        if column_name in cols:
            return bool(employee[column_name])
        return default

    phone = get_text("phone")
    email = get_text("email")
    position = get_text("position")
    pay_type = get_text("pay_type")
    hire_date = get_text("hire_date")
    payroll_notes = get_text("payroll_notes")
    default_hours = (
        employee["default_hours"]
        if "default_hours" in cols and employee["default_hours"] is not None
        else "-"
    )

    hourly_rate = get_float("hourly_rate")
    overtime_rate = get_float("overtime_rate")
    salary_amount = get_float("salary_amount")
    status_text = _t(lang, "Active", "Activo") if get_bool("is_active", True) else _t(lang, "Inactive", "Inactivo")

    middle_name = get_text("middle_name")
    suffix = get_text("suffix")
    ssn = get_text("ssn")

    address_line_1 = get_text("address_line_1")
    address_line_2 = get_text("address_line_2")
    city = get_text("city")
    state = get_text("state")
    zip_code = get_text("zip")

    w2_address_line_1 = get_text("w2_address_line_1")
    w2_address_line_2 = get_text("w2_address_line_2")
    w2_city = get_text("w2_city")
    w2_state = get_text("w2_state")
    w2_zip = get_text("w2_zip")

    federal_filing_status = get_text("federal_filing_status", "Single")
    pay_frequency = get_text("pay_frequency")
    w4_step2_checked = _t(lang, "Yes", "Sí") if get_bool("w4_step2_checked") else _t(lang, "No", "No")
    w4_step3_amount = get_float("w4_step3_amount")
    w4_step4a_other_income = get_float("w4_step4a_other_income")
    w4_step4b_deductions = get_float("w4_step4b_deductions")
    w4_step4c_extra_withholding = get_float("w4_step4c_extra_withholding")

    is_indiana_resident = _t(lang, "Yes", "Sí") if get_bool("is_indiana_resident") else _t(lang, "No", "No")
    county_of_residence = get_text("county_of_residence")
    county_of_principal_employment = get_text("county_of_principal_employment")
    county_tax_effective_year = get_text("county_tax_effective_year")

    if pay_type == "Salary":
        pay_display = f"${salary_amount:,.2f} / {_t(lang, 'year', 'año')}"
    elif pay_type == "Hourly":
        pay_display = f"${hourly_rate:,.2f} / {_t(lang, 'hour', 'hora')}"
    else:
        pay_display = "-"

    payroll_history_rows = "".join(
        f"""
        <tr>
            <td>{escape(str(r['pay_date'] or '-'))}</td>
            <td>{escape(str(r['pay_period_start'] or '-'))} to {escape(str(r['pay_period_end'] or '-'))}</td>
            <td>{escape(str(r['pay_type'] or '-'))}</td>
            <td>{float(r['hours_regular'] or 0):.2f}</td>
            <td>{float(r['hours_overtime'] or 0):.2f}</td>
            <td>${float(r['gross_pay'] or 0):.2f}</td>
            <td>${float(r['net_pay'] or 0):.2f}</td>
        </tr>
        """
        for r in payroll_rows
    )

    payroll_history_mobile = "".join(
        f"""
        <div class="mobile-pay-card">
            <div class="mobile-pay-top">
                <div class="mobile-pay-date">{escape(str(r['pay_date'] or '-'))}</div>
                <div class="mobile-pay-net">${float(r['net_pay'] or 0):.2f}</div>
            </div>
            <div class="mobile-pay-grid">
                <div><span>{_t(lang, 'Pay Period', 'Periodo de Pago')}</span><strong>{escape(str(r['pay_period_start'] or '-'))} to {escape(str(r['pay_period_end'] or '-'))}</strong></div>
                <div><span>{_t(lang, 'Pay Type', 'Tipo de Pago')}</span><strong>{escape(str(r['pay_type'] or '-'))}</strong></div>
                <div><span>{_t(lang, 'Regular Hours', 'Horas Regulares')}</span><strong>{float(r['hours_regular'] or 0):.2f}</strong></div>
                <div><span>{_t(lang, 'OT Hours', 'Horas Extra')}</span><strong>{float(r['hours_overtime'] or 0):.2f}</strong></div>
                <div><span>{_t(lang, 'Gross Pay', 'Pago Bruto')}</span><strong>${float(r['gross_pay'] or 0):.2f}</strong></div>
                <div><span>{_t(lang, 'Net Pay', 'Pago Neto')}</span><strong>${float(r['net_pay'] or 0):.2f}</strong></div>
            </div>
        </div>
        """
        for r in payroll_rows
    )

    content = f"""
    <style>
        .employee-view-page {{
            display: grid;
            gap: 18px;
        }}

        .employee-hero {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 16px;
            flex-wrap: wrap;
        }}

        .employee-hero-left {{
            min-width: 260px;
            flex: 1 1 320px;
        }}

        .employee-hero h1 {{
            margin: 0 0 8px;
            color: #2f4f1f;
        }}

        .employee-subtitle {{
            margin: 0;
            color: #6b7280;
        }}

        .employee-badges {{
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 12px;
        }}

        .employee-badge {{
            display: inline-flex;
            align-items: center;
            padding: 8px 14px;
            border-radius: 999px;
            font-weight: 700;
            font-size: .92rem;
            background: #f3f4f6;
            color: #374151;
            border: 1px solid rgba(15, 23, 42, 0.08);
        }}

        .employee-badge.active {{
            background: #ecfdf3;
            color: #166534;
            border-color: rgba(22, 101, 52, 0.15);
        }}

        .employee-badge.inactive {{
            background: #fef2f2;
            color: #991b1b;
            border-color: rgba(153, 27, 27, 0.12);
        }}

        .row-actions {{
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            align-items: center;
        }}

        .section-title {{
            margin: 0 0 14px;
            color: #2f4f1f;
        }}

        .section-head {{
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:12px;
            flex-wrap:wrap;
            margin-bottom:14px;
        }}

        .employee-meta-grid {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 12px;
        }}

        .employee-meta-card {{
            background: #fff;
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 14px;
            padding: 14px;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }}

        .employee-meta-card span {{
            display: block;
            font-size: .8rem;
            color: #64748b;
            margin-bottom: 4px;
        }}

        .employee-meta-card strong {{
            display: block;
            color: #0f172a;
            line-height: 1.35;
            word-break: break-word;
        }}

        .payroll-notes-box {{
            margin-top: 16px;
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 14px;
            padding: 14px;
            background: #fafafa;
            color: #0f172a;
            line-height: 1.5;
            word-break: break-word;
        }}

        .table-wrap {{
            width: 100%;
            overflow-x: auto;
        }}

        .desktop-only {{
            display: block;
        }}

        .mobile-only {{
            display: none;
        }}

        .mobile-pay-list {{
            display: grid;
            gap: 12px;
        }}

        .mobile-pay-card {{
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 14px;
            padding: 14px;
            background: #fff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }}

        .mobile-pay-top {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 10px;
            margin-bottom: 10px;
        }}

        .mobile-pay-date {{
            font-weight: 700;
            color: #0f172a;
            line-height: 1.25;
        }}

        .mobile-pay-net {{
            font-size: .85rem;
            font-weight: 700;
            color: #334155;
            background: #f1f5f9;
            padding: 6px 10px;
            border-radius: 999px;
            white-space: nowrap;
        }}

        .mobile-pay-grid {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 10px 12px;
        }}

        .mobile-pay-grid span {{
            display: block;
            font-size: .78rem;
            color: #64748b;
            margin-bottom: 3px;
        }}

        .mobile-pay-grid strong {{
            display: block;
            color: #0f172a;
            font-size: .95rem;
            line-height: 1.25;
            word-break: break-word;
        }}

        @media (max-width: 900px) {{
            .employee-meta-grid {{
                grid-template-columns: 1fr 1fr;
            }}
        }}

        @media (max-width: 640px) {{
            .employee-hero {{
                flex-direction: column;
                align-items: stretch;
            }}

            .row-actions {{
                width: 100%;
            }}

            .row-actions .btn {{
                flex: 1 1 auto;
                text-align: center;
            }}

            .employee-meta-grid {{
                grid-template-columns: 1fr;
            }}

            .desktop-only {{
                display: none !important;
            }}

            .mobile-only {{
                display: block !important;
            }}

            .mobile-pay-grid {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>

    <div class="employee-view-page">
        <div class="card">
            <div class="employee-hero">
                <div class="employee-hero-left">
                    <h1>{escape(employee_name)}</h1>
                    <p class="employee-subtitle">{_t(lang, 'Full employee profile, payroll setup, tax setup, and payroll history.', 'Perfil completo del empleado, configuración de nómina, configuración de impuestos e historial de nómina.')}</p>
                    <div class="employee-badges">
                        <div class="employee-badge {'active' if status_text == _t(lang, 'Active', 'Activo') else 'inactive'}">{escape(status_text)}</div>
                        <div class="employee-badge">{escape(str(position))}</div>
                        <div class="employee-badge">{escape(str(pay_type))}</div>
                    </div>
                </div>
                <div class="row-actions">
                    <a class="btn" href="{url_for('employees.edit_employee', employee_id=employee_id)}">{_t(lang, 'Edit Employee', 'Editar Empleado')}</a>
                    <a class="btn warning" href="{url_for('payroll.employee_payroll')}">{_t(lang, 'Payroll', 'Nómina')}</a>
                    <a class="btn secondary" href="{url_for('employees.employees')}">{_t(lang, 'Back to Employees', 'Volver a Empleados')}</a>
                </div>
            </div>
        </div>

        <div class="card">
            <h2 class="section-title">{_t(lang, 'Employee Information', 'Información del Empleado')}</h2>
            <div class="employee-meta-grid">
                <div class="employee-meta-card"><span>{_t(lang, 'Name', 'Nombre')}</span><strong>{escape(employee_name)}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Status', 'Estado')}</span><strong>{escape(status_text)}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Position', 'Puesto')}</span><strong>{escape(str(position))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Phone', 'Teléfono')}</span><strong>{escape(str(phone))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Email', 'Correo')}</span><strong>{escape(str(email))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Hire Date', 'Fecha de Contratación')}</span><strong>{escape(str(hire_date))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Middle Name', 'Segundo Nombre')}</span><strong>{escape(str(middle_name))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Suffix', 'Sufijo')}</span><strong>{escape(str(suffix))}</strong></div>
            </div>
        </div>

        <div class="card">
            <h2 class="section-title">{_t(lang, 'Employee Address', 'Dirección del Empleado')}</h2>
            <div class="employee-meta-grid">
                <div class="employee-meta-card"><span>{_t(lang, 'Address Line 1', 'Dirección Línea 1')}</span><strong>{escape(str(address_line_1))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Address Line 2', 'Dirección Línea 2')}</span><strong>{escape(str(address_line_2))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'City', 'Ciudad')}</span><strong>{escape(str(city))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'State', 'Estado')}</span><strong>{escape(str(state))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'ZIP', 'Código Postal')}</span><strong>{escape(str(zip_code))}</strong></div>
            </div>
        </div>

        <div class="card">
            <h2 class="section-title">{_t(lang, 'W-2 Identity & Mailing Info', 'Información de Identidad y Envío W-2')}</h2>
            <div class="employee-meta-grid">
                <div class="employee-meta-card"><span>SSN</span><strong>{escape(str(ssn))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'W-2 Address Line 1', 'Dirección W-2 Línea 1')}</span><strong>{escape(str(w2_address_line_1))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'W-2 Address Line 2', 'Dirección W-2 Línea 2')}</span><strong>{escape(str(w2_address_line_2))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'W-2 City', 'Ciudad W-2')}</span><strong>{escape(str(w2_city))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'W-2 State', 'Estado W-2')}</span><strong>{escape(str(w2_state))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'W-2 ZIP', 'Código Postal W-2')}</span><strong>{escape(str(w2_zip))}</strong></div>
            </div>
        </div>

        <div class="card">
            <h2 class="section-title">{_t(lang, 'Payroll Setup', 'Configuración de Nómina')}</h2>
            <div class="employee-meta-grid">
                <div class="employee-meta-card"><span>{_t(lang, 'Pay Type', 'Tipo de Pago')}</span><strong>{escape(str(pay_type))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Rate / Salary', 'Tarifa / Salario')}</span><strong>{pay_display}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Overtime Rate', 'Tarifa de Horas Extra')}</span><strong>${overtime_rate:,.2f}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Default Hours', 'Horas Predeterminadas')}</span><strong>{escape(str(default_hours))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Pay Frequency', 'Frecuencia de Pago')}</span><strong>{escape(str(pay_frequency))}</strong></div>
            </div>

            <div class="payroll-notes-box">
                <span style="display:block; font-size:.8rem; color:#64748b; margin-bottom:6px;">{_t(lang, 'Payroll Notes', 'Notas de Nómina')}</span>
                <strong style="font-weight:600;">{escape(str(payroll_notes))}</strong>
            </div>
        </div>

        <div class="card">
            <h2 class="section-title">{_t(lang, 'Federal Tax / W-4', 'Impuesto Federal / W-4')}</h2>
            <div class="employee-meta-grid">
                <div class="employee-meta-card"><span>{_t(lang, 'Federal Filing Status', 'Estado de Declaración Federal')}</span><strong>{escape(str(federal_filing_status))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Step 2 Box Checked', 'Casilla del Paso 2 Marcada')}</span><strong>{escape(w4_step2_checked)}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Step 3 Credits', 'Créditos del Paso 3')}</span><strong>${w4_step3_amount:,.2f}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Step 4(a) Other Income', 'Paso 4(a) Otros Ingresos')}</span><strong>${w4_step4a_other_income:,.2f}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Step 4(b) Deductions', 'Paso 4(b) Deducciones')}</span><strong>${w4_step4b_deductions:,.2f}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'Step 4(c) Extra Withholding', 'Paso 4(c) Retención Extra')}</span><strong>${w4_step4c_extra_withholding:,.2f}</strong></div>
            </div>
        </div>

        <div class="card">
            <h2 class="section-title">{_t(lang, 'Indiana Local Tax Setup', 'Configuración de Impuesto Local de Indiana')}</h2>
            <div class="employee-meta-grid">
                <div class="employee-meta-card"><span>{_t(lang, 'Indiana Resident on Jan 1', 'Residente de Indiana el 1 de Enero')}</span><strong>{escape(is_indiana_resident)}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'County Tax Effective Year', 'Año Efectivo del Impuesto del Condado')}</span><strong>{escape(str(county_tax_effective_year))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'County of Residence', 'Condado de Residencia')}</span><strong>{escape(str(county_of_residence))}</strong></div>
                <div class="employee-meta-card"><span>{_t(lang, 'County of Principal Employment', 'Condado de Empleo Principal')}</span><strong>{escape(str(county_of_principal_employment))}</strong></div>
            </div>
        </div>

        <div class="card">
            <div class="section-head">
                <h2 class="section-title" style="margin-bottom:0;">{_t(lang, 'Payroll History', 'Historial de Nómina')}</h2>
                <a class="btn small" href="{url_for('payroll.employee_payroll')}">{_t(lang, 'Open Payroll', 'Abrir Nómina')}</a>
            </div>

            <div class="table-wrap desktop-only">
                <table>
                    <tr>
                        <th>{_t(lang, 'Pay Date', 'Fecha de Pago')}</th>
                        <th>{_t(lang, 'Pay Period', 'Periodo de Pago')}</th>
                        <th>{_t(lang, 'Pay Type', 'Tipo de Pago')}</th>
                        <th>{_t(lang, 'Reg Hours', 'Horas Reg')}</th>
                        <th>{_t(lang, 'OT Hours', 'Horas Extra')}</th>
                        <th>{_t(lang, 'Gross Pay', 'Pago Bruto')}</th>
                        <th>{_t(lang, 'Net Pay', 'Pago Neto')}</th>
                    </tr>
                    {payroll_history_rows or f"<tr><td colspan='7' class='muted'>{_t(lang, 'No payroll history found.', 'No se encontró historial de nómina.')}</td></tr>"}
                </table>
            </div>

            <div class="mobile-only">
                <div class="mobile-pay-list">
                    {payroll_history_mobile or f"<div class='mobile-pay-card muted'>{_t(lang, 'No payroll history found.', 'No se encontró historial de nómina.')}</div>"}
                </div>
            </div>
        </div>
    </div>
    """

    return render_page(content, employee_name)


@employees_bp.route("/employees/<int:employee_id>/edit", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_employees")
def edit_employee(employee_id):
    lang = _get_lang()

    ensure_employee_profile_columns()
    ensure_employee_name_columns()
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()
    ensure_employee_local_tax_columns()
    ensure_employee_w2_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    employee = conn.execute(
        "SELECT * FROM employees WHERE id = %s AND company_id = %s",
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        flash(_t(lang, "Employee not found.", "Empleado no encontrado."))
        return redirect(url_for("employees.employees"))

    if request.method == "POST":
        first_name = _clean_text(request.form.get("first_name"))
        middle_name = _clean_text(request.form.get("middle_name"))
        last_name = _clean_text(request.form.get("last_name"))
        suffix = _clean_text(request.form.get("suffix"))
        phone = _clean_text(request.form.get("phone"))
        email = _clean_text(request.form.get("email"))
        position = _clean_text(request.form.get("position"))
        hire_date = _clean_text(request.form.get("hire_date"))

        if not first_name or not last_name:
            conn.close()
            flash(_t(lang, "First and last name are required.", "El nombre y el apellido son obligatorios."))
            return redirect(url_for("employees.edit_employee", employee_id=employee_id))

        address_line_1 = _clean_text(request.form.get("address_line_1"))
        address_line_2 = _clean_text(request.form.get("address_line_2"))
        city = _clean_text(request.form.get("city"))
        state = (_clean_text(request.form.get("state")) or "IN").upper()
        zip_code = _clean_text(request.form.get("zip"))

        ssn = _normalize_ssn(request.form.get("ssn"))
        w2_address_line_1 = _clean_text(request.form.get("w2_address_line_1"))
        w2_address_line_2 = _clean_text(request.form.get("w2_address_line_2"))
        w2_city = _clean_text(request.form.get("w2_city"))
        w2_state = (_clean_text(request.form.get("w2_state")) or "").upper()
        w2_zip = _clean_text(request.form.get("w2_zip"))

        pay_type = _clean_text(request.form.get("pay_type")) or "Hourly"
        hourly_rate = _safe_float(request.form.get("hourly_rate"), 0)
        overtime_rate = _safe_float(request.form.get("overtime_rate"), 0)
        salary_amount = _safe_float(request.form.get("salary_amount"), 0)
        default_hours = _safe_float(request.form.get("default_hours"), 0)
        payroll_notes = _clean_text(request.form.get("payroll_notes"))

        if pay_type.lower() == "salary":
            hourly_rate = 0
            overtime_rate = 0
        else:
            salary_amount = 0

        federal_filing_status = _clean_text(request.form.get("federal_filing_status")) or "Single"
        pay_frequency = _clean_text(request.form.get("pay_frequency")) or "Weekly"

        w4_step2_checked = 1 if request.form.get("w4_step2_checked") else 0
        is_indiana_resident = True if request.form.get("is_indiana_resident") else False

        w4_step3_amount = _safe_float(request.form.get("w4_step3_amount"), 0)
        w4_step4a_other_income = _safe_float(request.form.get("w4_step4a_other_income"), 0)
        w4_step4b_deductions = _safe_float(request.form.get("w4_step4b_deductions"), 0)
        w4_step4c_extra_withholding = _safe_float(request.form.get("w4_step4c_extra_withholding"), 0)

        county_of_residence = _clean_text(request.form.get("county_of_residence"))
        county_of_principal_employment = _clean_text(request.form.get("county_of_principal_employment"))
        county_tax_effective_year = _safe_int(
            request.form.get("county_tax_effective_year"),
            date.today().year
        )

        full_name = " ".join(
            part for part in [first_name, middle_name, last_name, suffix] if part
        ).strip()

        conn.execute(
            """
            UPDATE employees
            SET first_name = %s,
                middle_name = %s,
                last_name = %s,
                suffix = %s,
                full_name = %s,
                phone = %s,
                email = %s,
                position = %s,
                hire_date = %s,
                address_line_1 = %s,
                address_line_2 = %s,
                city = %s,
                state = %s,
                zip = %s,
                ssn = %s,
                w2_address_line_1 = %s,
                w2_address_line_2 = %s,
                w2_city = %s,
                w2_state = %s,
                w2_zip = %s,
                pay_type = %s,
                hourly_rate = %s,
                overtime_rate = %s,
                salary_amount = %s,
                default_hours = %s,
                payroll_notes = %s,
                federal_filing_status = %s,
                pay_frequency = %s,
                w4_step2_checked = %s,
                w4_step3_amount = %s,
                w4_step4a_other_income = %s,
                w4_step4b_deductions = %s,
                w4_step4c_extra_withholding = %s,
                is_indiana_resident = %s,
                county_of_residence = %s,
                county_of_principal_employment = %s,
                county_tax_effective_year = %s
            WHERE id = %s AND company_id = %s
            """,
            (
                first_name,
                middle_name,
                last_name,
                suffix,
                full_name,
                phone,
                email,
                position,
                hire_date,
                address_line_1,
                address_line_2,
                city,
                state,
                zip_code,
                ssn,
                w2_address_line_1,
                w2_address_line_2,
                w2_city,
                w2_state,
                w2_zip,
                pay_type,
                hourly_rate,
                overtime_rate,
                salary_amount,
                default_hours,
                payroll_notes,
                federal_filing_status,
                pay_frequency,
                w4_step2_checked,
                w4_step3_amount,
                w4_step4a_other_income,
                w4_step4b_deductions,
                w4_step4c_extra_withholding,
                is_indiana_resident,
                county_of_residence,
                county_of_principal_employment,
                county_tax_effective_year,
                employee_id,
                cid,
            ),
        )

        conn.commit()
        conn.close()

        flash(_t(lang, "Employee updated successfully.", "Empleado actualizado correctamente."))
        return redirect(url_for("employees.view_employee", employee_id=employee_id))

    content = _employee_form_html(
        employee=employee,
        form_action=url_for("employees.edit_employee", employee_id=employee_id),
        submit_label=_t(lang, "Save Changes", "Guardar Cambios"),
        page_title=f"{_t(lang, 'Edit', 'Editar')} {_employee_display_name(employee)}",
        lang=lang,
    )
    conn.close()
    return render_page(content, _t(lang, "Edit Employee", "Editar Empleado"))


@employees_bp.route("/employees/<int:employee_id>/activate", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_employees")
def activate_employee(employee_id):
    lang = _get_lang()

    ensure_employee_profile_columns()
    ensure_employee_status_column()

    conn = get_db_connection()
    cid = session["company_id"]

    conn.execute(
        """
        UPDATE employees
        SET is_active = %s
        WHERE id = %s AND company_id = %s
        """,
        (_active_update_value(True), employee_id, cid),
    )
    conn.commit()
    conn.close()

    flash(_t(lang, "Employee marked active.", "Empleado marcado como activo."))
    return redirect(url_for("employees.employees", show="all"))


@employees_bp.route("/employees/<int:employee_id>/deactivate", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_employees")
def deactivate_employee(employee_id):
    lang = _get_lang()

    ensure_employee_profile_columns()
    ensure_employee_status_column()

    conn = get_db_connection()
    cid = session["company_id"]

    conn.execute(
        """
        UPDATE employees
        SET is_active = %s
        WHERE id = %s AND company_id = %s
        """,
        (_active_update_value(False), employee_id, cid),
    )
    conn.commit()
    conn.close()

    flash(_t(lang, "Employee marked inactive.", "Empleado marcado como inactivo."))
    return redirect(url_for("employees.employees"))


@employees_bp.route("/employees/<int:employee_id>/delete", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_employees")
def delete_employee(employee_id):
    lang = _get_lang()

    ensure_employee_profile_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    employee = conn.execute(
        """
        SELECT id
        FROM employees
        WHERE id = %s AND company_id = %s
        """,
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        flash(_t(lang, "Employee not found.", "Empleado no encontrado."))
        return redirect(url_for("employees.employees"))

    try:
        payroll_ledger_ids = conn.execute(
            """
            SELECT ledger_entry_id
            FROM payroll_entries
            WHERE employee_id = %s AND company_id = %s AND ledger_entry_id IS NOT NULL
            """,
            (employee_id, cid),
        ).fetchall()

        for row in payroll_ledger_ids:
            conn.execute(
                "DELETE FROM ledger_entries WHERE id = %s AND company_id = %s",
                (row["ledger_entry_id"], cid),
            )

        conn.execute(
            """
            DELETE FROM payroll_entries
            WHERE employee_id = %s AND company_id = %s
            """,
            (employee_id, cid),
        )
    except Exception:
        pass

    try:
        conn.execute(
            """
            DELETE FROM employee_time_entries
            WHERE employee_id = %s AND company_id = %s
            """,
            (employee_id, cid),
        )
    except Exception:
        pass

    conn.execute(
        """
        DELETE FROM employees
        WHERE id = %s AND company_id = %s
        """,
        (employee_id, cid),
    )

    conn.commit()
    conn.close()

    flash(_t(lang, "Employee deleted.", "Empleado eliminado."))
    return redirect(url_for("employees.employees"))


@employees_bp.route("/employees/time-clock", methods=["GET"])
@login_required
@subscription_required
def time_clock():
    lang = _get_lang()

    ensure_employee_profile_columns()
    ensure_employee_time_entries_table()
    ensure_company_profile_table()
    ensure_company_time_clock_columns()

    def _session_truthy(value):
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _session_int(value):
        try:
            return int(str(value).strip())
        except Exception:
            return None

    def _session_permission_enabled(name):
        permissions = (
            session.get("permissions")
            or session.get("user_permissions")
            or session.get("permission_map")
            or {}
        )

        if isinstance(permissions, dict):
            return _session_truthy(permissions.get(name))

        if isinstance(permissions, (list, tuple, set)):
            return name in permissions

        if isinstance(permissions, str):
            raw = permissions.strip()
            if not raw:
                return False
            lowered = raw.lower()
            if lowered in {"all", "*", "admin"}:
                return True
            return name in {part.strip() for part in raw.split(",") if part.strip()}

        return False

    def _can_manage_time_clock():
        role = str(
            session.get("role")
            or session.get("user_role")
            or session.get("account_role")
            or ""
        ).strip().lower()

        if role in {"owner", "admin", "manager"}:
            return True

        if _session_truthy(session.get("is_admin")) or _session_truthy(session.get("is_owner")):
            return True

        return _session_permission_enabled("can_manage_employees")

    def _current_employee_id():
        for key in ("employee_id", "linked_employee_id", "staff_employee_id"):
            value = _session_int(session.get(key))
            if value:
                return value
        return None

    def _can_use_time_clock():
        return _can_manage_time_clock() or bool(_current_employee_id())

    def _visible_employee_filter(all_rows):
        if _can_manage_time_clock():
            return list(all_rows)

        current_emp_id = _current_employee_id()
        if not current_emp_id:
            return []

        return [row for row in all_rows if int(row["employee_id"]) == int(current_emp_id)]

    def _action_employee_id_from_row(row):
        return int(row["employee_id"])

    if not _can_use_time_clock():
        flash(_t(lang, "You do not have access to the time clock.", "No tienes acceso al reloj de tiempo."))
        return redirect(url_for("dashboard.dashboard"))

    conn = get_db_connection()
    cid = session["company_id"]

    pay_period_start_day = _get_company_time_clock_start_day(cid)
    pay_period_start, pay_period_end = _get_current_pay_period(pay_period_start_day)
    previous_pay_period_start, previous_pay_period_end = get_previous_pay_period(pay_period_start_day)
    pay_period_end_day = (pay_period_start_day - 1) % 7

    selected_period = (request.args.get("period") or "current").strip().lower()
    if selected_period not in {"current", "previous"}:
        selected_period = "current"

    if selected_period == "previous":
        entries_start = previous_pay_period_start
        entries_end = previous_pay_period_end
        entries_heading = _t(lang, "Previous Pay Period Time Entries", "Entradas de Tiempo del Periodo de Pago Anterior")
        entries_description = _t(lang, "Review previous pay period punches and send the last hours summary email manually.", "Revisa los registros del periodo de pago anterior y envía manualmente el correo con el resumen de horas.")
        empty_entries_message = _t(lang, "No time entries for the previous pay period.", "No hay entradas de tiempo para el periodo de pago anterior.")
    else:
        entries_start = pay_period_start
        entries_end = pay_period_end
        entries_heading = _t(lang, "Current Pay Period Time Entries", "Entradas de Tiempo del Periodo de Pago Actual")
        entries_description = _t(lang, "Review current pay period punches and send the last hours summary email manually.", "Revisa los registros del periodo de pago actual y envía manualmente el correo con el resumen de horas.")
        empty_entries_message = _t(lang, "No time entries for the current pay period.", "No hay entradas de tiempo para el periodo de pago actual.")

    active_sql = _active_where_sql("is_active")
    status_active_sql = _active_where_sql("e.is_active")

    employees = conn.execute(
        f"""
        SELECT
            id,
            first_name,
            last_name,
            full_name,
            email,
            is_active
        FROM employees
        WHERE company_id = %s
          AND {active_sql}
        ORDER BY
            COALESCE(NULLIF(last_name, ''), NULLIF(full_name, ''), NULLIF(first_name, ''), 'ZZZ'),
            COALESCE(NULLIF(first_name, ''), ''),
            id
        """,
        (cid,),
    ).fetchall()

    status_rows = conn.execute(
        f"""
        SELECT
            e.id AS employee_id,
            e.first_name,
            e.last_name,
            e.full_name,
            t.id AS time_entry_id,
            t.clock_in,
            t.clock_out,
            t.total_hours
        FROM employees e
        LEFT JOIN employee_time_entries t
            ON t.employee_id = e.id
           AND t.company_id = e.company_id
           AND t.clock_out IS NULL
        WHERE e.company_id = %s
          AND {status_active_sql}
        ORDER BY
            COALESCE(NULLIF(e.last_name, ''), NULLIF(e.full_name, ''), NULLIF(e.first_name, ''), 'ZZZ'),
            COALESCE(NULLIF(e.first_name, ''), ''),
            e.id
        """,
        (cid,),
    ).fetchall()

    today_rows = conn.execute(
        """
        SELECT
            employee_id,
            COALESCE(SUM(total_hours), 0) AS today_hours
        FROM employee_time_entries
        WHERE company_id = %s
          AND DATE(clock_in) = %s
        GROUP BY employee_id
        """,
        (cid, date.today().isoformat()),
    ).fetchall()

    pay_period_rows = conn.execute(
        """
        SELECT
            employee_id,
            COALESCE(SUM(total_hours), 0) AS pay_period_hours
        FROM employee_time_entries
        WHERE company_id = %s
          AND DATE(clock_in) >= %s
          AND DATE(clock_in) <= %s
        GROUP BY employee_id
        """,
        (cid, pay_period_start.isoformat(), pay_period_end.isoformat()),
    ).fetchall()

    recent_entries = conn.execute(
        """
        SELECT
            t.id,
            t.employee_id,
            t.clock_in,
            t.clock_out,
            t.total_hours,
            t.notes,
            e.first_name,
            e.last_name,
            e.full_name
        FROM employee_time_entries t
        JOIN employees e ON t.employee_id = e.id
        WHERE t.company_id = %s
          AND DATE(t.clock_in) >= %s
          AND DATE(t.clock_in) <= %s
        ORDER BY t.clock_in DESC, t.id DESC
        LIMIT 100
        """,
        (cid, entries_start.isoformat(), entries_end.isoformat()),
    ).fetchall()

    conn.close()

    today_map = {row["employee_id"]: float(row["today_hours"] or 0) for row in today_rows}
    pay_period_map = {row["employee_id"]: float(row["pay_period_hours"] or 0) for row in pay_period_rows}

    employees_with_status = []
    currently_clocked_in = 0

    for row in status_rows:
        employee_name = (
            f"{(row['first_name'] or '').strip()} {(row['last_name'] or '').strip()}".strip()
            or (row["full_name"] or "").strip()
            or f"Employee #{row['employee_id']}"
        )

        is_clocked_in = bool(row["time_entry_id"])
        if is_clocked_in:
            currently_clocked_in += 1

        employees_with_status.append({
            "employee_id": row["employee_id"],
            "employee_name": employee_name,
            "is_clocked_in": is_clocked_in,
            "clock_in": row["clock_in"],
            "today_hours": today_map.get(row["employee_id"], 0),
            "pay_period_hours": pay_period_map.get(row["employee_id"], 0),
        })

    employees_with_status = _visible_employee_filter(employees_with_status)

    visible_employee_ids = {int(row["employee_id"]) for row in employees_with_status}

    recent_entries = [
        row for row in recent_entries
        if int(row["employee_id"]) in visible_employee_ids
    ]

    clocked_in_ids = {
        row["employee_id"]
        for row in employees_with_status
        if row["is_clocked_in"]
    }

    time_clock_html = """
    <style>
        .time-clock-page {
            display:grid;
            gap:18px;
        }

        .time-clock-stat-grid {
            display:grid;
            grid-template-columns:repeat(auto-fit, minmax(220px, 1fr));
            gap:16px;
        }

        .time-clock-form-grid {
            display:grid;
            grid-template-columns:minmax(220px, 1fr) auto;
            gap:12px;
            align-items:end;
        }

        .time-clock-period-toggle {
            display:flex;
            gap:8px;
            flex-wrap:wrap;
            align-items:center;
        }

        .time-clock-period-link {
            display:inline-flex;
            align-items:center;
            justify-content:center;
            padding:8px 12px;
            border-radius:10px;
            border:1px solid #d1d5db;
            text-decoration:none;
            color:#334155;
            background:#fff;
            font-weight:600;
        }

        .time-clock-period-link.active {
            background:#eef6ff;
            border-color:#93c5fd;
            color:#1d4ed8;
        }

        .mobile-only {
            display:none;
        }

        .desktop-only {
            display:block;
        }

        .mobile-list {
            display:grid;
            gap:12px;
        }

        .mobile-list-card {
            border:1px solid rgba(15, 23, 42, 0.08);
            border-radius:14px;
            padding:14px;
            background:#fff;
            box-shadow:0 1px 2px rgba(15, 23, 42, 0.04);
        }

        .mobile-list-top {
            display:flex;
            justify-content:space-between;
            align-items:flex-start;
            gap:10px;
            margin-bottom:10px;
        }

        .mobile-list-title {
            font-weight:700;
            color:#0f172a;
            line-height:1.25;
            word-break:break-word;
        }

        .mobile-badge {
            font-size:.85rem;
            font-weight:700;
            color:#334155;
            background:#f1f5f9;
            padding:6px 10px;
            border-radius:999px;
            white-space:nowrap;
        }

        .mobile-list-grid {
            display:grid;
            grid-template-columns:1fr 1fr;
            gap:10px 12px;
            margin-bottom:12px;
        }

        .mobile-list-grid span {
            display:block;
            font-size:.78rem;
            color:#64748b;
            margin-bottom:3px;
        }

        .mobile-list-grid strong {
            display:block;
            color:#0f172a;
            font-size:.95rem;
            line-height:1.25;
            word-break:break-word;
        }

        .time-clock-action-cell {
            min-width:145px;
            text-align:right;
        }

        .time-clock-inline-form {
            margin:0;
            display:flex;
            justify-content:flex-end;
        }

        .time-clock-inline-form .btn {
            min-width:110px;
        }

        .time-clock-mobile-action {
            margin-top:12px;
            display:flex;
            justify-content:flex-start;
        }

        @media (max-width: 640px) {
            .time-clock-form-grid {
                grid-template-columns:1fr;
            }

            .desktop-only {
                display:none !important;
            }

            .mobile-only {
                display:block !important;
            }

            .mobile-list-grid {
                grid-template-columns:1fr;
            }
        }
    </style>

    <div class='time-clock-page'>
        <div class='card'>
            <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
                <div>
                    <h1 style='margin-bottom:6px;'>{{ t("Clock In / Out", "Entrada / Salida") }}</h1>
                    <p class='muted' style='margin:0;'>{{ t("Track employee hours using your company's chosen pay period.", "Registra las horas de los empleados usando el periodo de pago elegido por tu empresa.") }}</p>
                </div>
                <div class='row-actions'>
                    <a class='btn secondary' href='{{ url_for("dashboard.dashboard") }}'>{{ t("Back to Dashboard", "Volver al Panel") }}</a>
                </div>
            </div>
        </div>

        {% if can_manage_time_clock %}
        <div class='card'>
            <h2>{{ t("Pay Period Settings", "Configuración del Periodo de Pago") }}</h2>
            <form method='post' action='{{ url_for("employees.update_time_clock_settings") }}'>
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                <div class='time-clock-form-grid'>
                    <div>
                        <label>{{ t("Pay Period Start Day", "Día de Inicio del Periodo de Pago") }}</label>
                        <select name='time_clock_pay_period_start_day' required>
                            {% for day_number, day_name in weekday_options %}
                                <option value='{{ day_number }}' {% if day_number == pay_period_start_day %}selected{% endif %}>{{ day_name }}</option>
                            {% endfor %}
                        </select>
                        <div class='muted' style='margin-top:6px;'>
                            {{ t("Current pay period runs", "El periodo de pago actual va de") }} {{ pay_period_start_label }} {{ t("through", "hasta") }} {{ pay_period_end_label }}.
                        </div>
                    </div>
                    <div>
                        <button class='btn' type='submit'>{{ t("Save Pay Period", "Guardar Periodo de Pago") }}</button>
                    </div>
                </div>
            </form>
        </div>
        {% endif %}

        <div class='card'>
            <div class='time-clock-stat-grid'>
                <div style='border:1px solid #e5e7eb; border-radius:12px; padding:16px; background:#f8fafc;'>
                    <div class='muted' style='margin-bottom:6px;'>{{ t("Current Pay Period", "Periodo de Pago Actual") }}</div>
                    <div style='font-size:1.1rem; font-weight:700;'>{{ pay_period_start }} {{ t("to", "a") }} {{ pay_period_end }}</div>
                </div>

                <div style='border:1px solid #e5e7eb; border-radius:12px; padding:16px; background:#f8fafc;'>
                    <div class='muted' style='margin-bottom:6px;'>{{ t("Employees", "Empleados") }}</div>
                    <div style='font-size:1.4rem; font-weight:700;'>{{ employees_with_status|length }}</div>
                </div>

                <div style='border:1px solid #e5e7eb; border-radius:12px; padding:16px; background:#f8fafc;'>
                    <div class='muted' style='margin-bottom:6px;'>{{ t("Currently Clocked In", "Actualmente Registrados") }}</div>
                    <div style='font-size:1.4rem; font-weight:700;'>{{ currently_clocked_in_visible }}</div>
                </div>
            </div>
        </div>

        <div class='card'>
            <h2>{{ t("Current Employee Status", "Estado Actual de los Empleados") }}</h2>
            {% if employees_with_status %}
                <div class='desktop-only' style='overflow-x:auto;'>
                    <table class='table'>
                        <thead>
                            <tr>
                                <th>{{ t("Employee", "Empleado") }}</th>
                                <th>{{ t("Status", "Estado") }}</th>
                                <th>{{ t("Clocked In At", "Entrada Registrada a las") }}</th>
                                <th>{{ t("Today", "Hoy") }}</th>
                                <th>{{ t("This Pay Period", "Este Periodo de Pago") }}</th>
                                <th>{{ t("Action", "Acción") }}</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for row in employees_with_status %}
                                <tr>
                                    <td>{{ row.employee_name }}</td>
                                    <td>
                                        {% if row.is_clocked_in %}
                                            <span style='color:#166534; font-weight:700;'>{{ t("Clocked In", "Registrado") }}</span>
                                        {% else %}
                                            <span style='color:#666;'>{{ t("Clocked Out", "Fuera de Registro") }}</span>
                                        {% endif %}
                                    </td>
                                    <td>{{ row.clock_in or "-" }}</td>
                                    <td>{{ format_hours(row.today_hours) }} hrs</td>
                                    <td>{{ format_hours(row.pay_period_hours) }} hrs</td>
                                    <td class='time-clock-action-cell'>
                                        {% if row.is_clocked_in %}
                                            <form method='post' action='{{ url_for("employees.time_clock_clock_out") }}' class='time-clock-inline-form'>
                                                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                                                <input type="hidden" name="employee_id" value="{{ row.employee_id }}">
                                                <button class='btn warning' type='submit'>{{ t("Clock Out", "Registrar Salida") }}</button>
                                            </form>
                                        {% else %}
                                            <form method='post' action='{{ url_for("employees.time_clock_clock_in") }}' class='time-clock-inline-form'>
                                                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                                                <input type="hidden" name="employee_id" value="{{ row.employee_id }}">
                                                <button class='btn success' type='submit'>{{ t("Clock In", "Registrar Entrada") }}</button>
                                            </form>
                                        {% endif %}
                                    </td>
                                </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>

                <div class='mobile-only'>
                    <div class='mobile-list'>
                        {% for row in employees_with_status %}
                            <div class='mobile-list-card'>
                                <div class='mobile-list-top'>
                                    <div class='mobile-list-title'>{{ row.employee_name }}</div>
                                    <div class='mobile-badge'>
                                        {% if row.is_clocked_in %}{{ t("Clocked In", "Registrado") }}{% else %}{{ t("Clocked Out", "Fuera de Registro") }}{% endif %}
                                    </div>
                                </div>

                                <div class='mobile-list-grid'>
                                    <div><span>{{ t("Clocked In At", "Entrada Registrada a las") }}</span><strong>{{ row.clock_in or "-" }}</strong></div>
                                    <div><span>{{ t("Today", "Hoy") }}</span><strong>{{ format_hours(row.today_hours) }} hrs</strong></div>
                                    <div><span>{{ t("This Pay Period", "Este Periodo de Pago") }}</span><strong>{{ format_hours(row.pay_period_hours) }} hrs</strong></div>
                                </div>

                                <div class='time-clock-mobile-action'>
                                    {% if row.is_clocked_in %}
                                        <form method='post' action='{{ url_for("employees.time_clock_clock_out") }}' style='margin:0;'>
                                            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                                            <input type="hidden" name="employee_id" value="{{ row.employee_id }}">
                                            <button class='btn warning' type='submit'>{{ t("Clock Out", "Registrar Salida") }}</button>
                                        </form>
                                    {% else %}
                                        <form method='post' action='{{ url_for("employees.time_clock_clock_in") }}' style='margin:0;'>
                                            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                                            <input type="hidden" name="employee_id" value="{{ row.employee_id }}">
                                            <button class='btn success' type='submit'>{{ t("Clock In", "Registrar Entrada") }}</button>
                                        </form>
                                    {% endif %}
                                </div>
                            </div>
                        {% endfor %}
                    </div>
                </div>
            {% else %}
                <p class='muted'>{{ t("No employees found.", "No se encontraron empleados.") }}</p>
            {% endif %}
        </div>

        <div class='card'>
            <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:14px;'>
                <div>
                    <h2 style='margin-bottom:4px;'>{{ entries_heading }}</h2>
                    <div class='muted'>{{ entries_description }}</div>
                </div>

                <div class='row-actions' style='display:flex; gap:10px; flex-wrap:wrap; align-items:center;'>
                    <div class='time-clock-period-toggle'>
                        <a class='time-clock-period-link {% if selected_period == "current" %}active{% endif %}'
                           href='{{ url_for("employees.time_clock", period="current") }}'>
                            {{ t("Current Pay Period", "Periodo de Pago Actual") }}
                        </a>
                        <a class='time-clock-period-link {% if selected_period == "previous" %}active{% endif %}'
                           href='{{ url_for("employees.time_clock", period="previous") }}'>
                            {{ t("Previous Pay Period", "Periodo de Pago Anterior") }}
                        </a>
                    </div>

                    {% if can_manage_time_clock %}
                    <form method='post' action='{{ url_for("employees.send_time_clock_summary_now") }}' style='margin:0;'>
                        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                        <button class='btn warning' type='submit'>{{ t("Send Last Pay Period Summary", "Enviar Resumen del Último Periodo de Pago") }}</button>
                    </form>
                    {% endif %}
                </div>
            </div>

            {% if recent_entries %}
                <div class='desktop-only' style='overflow-x:auto;'>
                    <table class='table'>
                        <thead>
                            <tr>
                                <th>{{ t("Employee", "Empleado") }}</th>
                                <th>{{ t("Clock In", "Entrada") }}</th>
                                <th>{{ t("Clock Out", "Salida") }}</th>
                                <th>{{ t("Total Hours", "Horas Totales") }}</th>
                                <th>{{ t("Notes", "Notas") }}</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for row in recent_entries %}
                            {% set entry_name = ((row["first_name"] or "") ~ " " ~ (row["last_name"] or "")).strip() or (row["full_name"] or "") or ("Employee #" ~ row["employee_id"]) %}
                                <tr>
                                    <td>{{ entry_name }}</td>
                                    <td>{{ row["clock_in"] }}</td>
                                    <td>{{ row["clock_out"] or "-" }}</td>
                                    <td>{{ format_hours(row["total_hours"]) }}</td>
                                    <td>{{ row["notes"] or "" }}</td>
                                </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>

                <div class='mobile-only'>
                    <div class='mobile-list'>
                        {% for row in recent_entries %}
                        {% set entry_name = ((row["first_name"] or "") ~ " " ~ (row["last_name"] or "")).strip() or (row["full_name"] or "") or ("Employee #" ~ row["employee_id"]) %}
                            <div class='mobile-list-card'>
                                <div class='mobile-list-top'>
                                    <div class='mobile-list-title'>{{ entry_name }}</div>
                                    <div class='mobile-badge'>{{ format_hours(row["total_hours"]) }} hrs</div>
                                </div>

                                <div class='mobile-list-grid'>
                                    <div><span>{{ t("Clock In", "Entrada") }}</span><strong>{{ row["clock_in"] }}</strong></div>
                                    <div><span>{{ t("Clock Out", "Salida") }}</span><strong>{{ row["clock_out"] or "-" }}</strong></div>
                                    <div><span>{{ t("Notes", "Notas") }}</span><strong>{{ row["notes"] or "" }}</strong></div>
                                </div>
                            </div>
                        {% endfor %}
                    </div>
                </div>
            {% else %}
                <p class='muted'>{{ empty_entries_message }}</p>
            {% endif %}
        </div>
    </div>
    """

    currently_clocked_in_visible = sum(1 for row in employees_with_status if row["is_clocked_in"])

    return render_page(
        render_template_string(
            time_clock_html,
            employees=employees,
            employees_with_status=employees_with_status,
            recent_entries=recent_entries,
            currently_clocked_in=currently_clocked_in,
            currently_clocked_in_visible=currently_clocked_in_visible,
            pay_period_start=pay_period_start.isoformat(),
            pay_period_end=pay_period_end.isoformat(),
            previous_pay_period_start=previous_pay_period_start.isoformat(),
            previous_pay_period_end=previous_pay_period_end.isoformat(),
            pay_period_start_label=_weekday_label(pay_period_start_day, lang),
            pay_period_end_label=_weekday_label(pay_period_end_day, lang),
            pay_period_start_day=pay_period_start_day,
            weekday_options=[
                (0, _weekday_label(0, lang)),
                (1, _weekday_label(1, lang)),
                (2, _weekday_label(2, lang)),
                (3, _weekday_label(3, lang)),
                (4, _weekday_label(4, lang)),
                (5, _weekday_label(5, lang)),
                (6, _weekday_label(6, lang)),
            ],
            selected_period=selected_period,
            entries_heading=entries_heading,
            entries_description=entries_description,
            empty_entries_message=empty_entries_message,
            clocked_in_ids=clocked_in_ids,
            format_hours=_format_hours,
            can_manage_time_clock=_can_manage_time_clock(),
            t=lambda en, es: _t(lang, en, es),
        ),
        _t(lang, "Clock In / Out", "Entrada / Salida"),
    )


@employees_bp.route("/employees/time-clock/settings", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_employees")
def update_time_clock_settings():
    lang = _get_lang()

    ensure_company_profile_table()
    ensure_company_time_clock_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    raw_value = (request.form.get("time_clock_pay_period_start_day") or "").strip()

    try:
        start_day = int(raw_value)
    except Exception:
        conn.close()
        flash(_t(lang, "Invalid pay period start day.", "Día de inicio del periodo de pago no válido."))
        return redirect(url_for("employees.time_clock"))

    if start_day < 0 or start_day > 6:
        conn.close()
        flash(_t(lang, "Invalid pay period start day.", "Día de inicio del periodo de pago no válido."))
        return redirect(url_for("employees.time_clock"))

    existing_profile = conn.execute(
        """
        SELECT id
        FROM company_profile
        WHERE company_id = %s
        """,
        (cid,),
    ).fetchone()

    if existing_profile:
        conn.execute(
            """
            UPDATE company_profile
            SET time_clock_pay_period_start_day = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE company_id = %s
            """,
            (start_day, cid),
        )
    else:
        conn.execute(
            """
            INSERT INTO company_profile (company_id, time_clock_pay_period_start_day)
            VALUES (%s, %s)
            """,
            (cid, start_day),
        )

    conn.commit()
    conn.close()

    flash(f"{_t(lang, 'Pay period updated. It now starts on', 'Periodo de pago actualizado. Ahora inicia el')} {_weekday_label(start_day, lang)}.")
    return redirect(url_for("employees.time_clock"))


@employees_bp.route("/employees/time-clock/clock-in", methods=["POST"])
@login_required
@subscription_required
def time_clock_clock_in():
    lang = _get_lang()

    ensure_employee_profile_columns()
    ensure_employee_time_entries_table()

    def _session_truthy(value):
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _session_int(value):
        try:
            return int(str(value).strip())
        except Exception:
            return None

    def _session_permission_enabled(name):
        permissions = (
            session.get("permissions")
            or session.get("user_permissions")
            or session.get("permission_map")
            or {}
        )

        if isinstance(permissions, dict):
            return _session_truthy(permissions.get(name))

        if isinstance(permissions, (list, tuple, set)):
            return name in permissions

        if isinstance(permissions, str):
            raw = permissions.strip()
            if not raw:
                return False
            lowered = raw.lower()
            if lowered in {"all", "*", "admin"}:
                return True
            return name in {part.strip() for part in raw.split(",") if part.strip()}

        return False

    def _can_manage_time_clock():
        role = str(
            session.get("role")
            or session.get("user_role")
            or session.get("account_role")
            or ""
        ).strip().lower()

        if role in {"owner", "admin", "manager"}:
            return True

        if _session_truthy(session.get("is_admin")) or _session_truthy(session.get("is_owner")):
            return True

        return _session_permission_enabled("can_manage_employees")

    def _current_employee_id():
        for key in ("employee_id", "linked_employee_id", "staff_employee_id"):
            value = _session_int(session.get(key))
            if value:
                return value
        return None

    def _can_use_time_clock():
        return _can_manage_time_clock() or bool(_current_employee_id())

    if not _can_use_time_clock():
        flash(_t(lang, "You do not have access to the time clock.", "No tienes acceso al reloj de tiempo."))
        return redirect(url_for("dashboard.dashboard"))

    conn = get_db_connection()
    cid = session["company_id"]

    posted_employee_id = _session_int(request.form.get("employee_id"))
    session_employee_id = _current_employee_id()

    if _can_manage_time_clock():
        employee_id = posted_employee_id
    else:
        employee_id = session_employee_id

    if not employee_id:
        conn.close()
        flash(_t(lang, "Please select an employee.", "Por favor selecciona un empleado."))
        return redirect(url_for("employees.time_clock"))

    active_sql = _active_where_sql("is_active")

    employee = conn.execute(
        f"""
        SELECT id, first_name, last_name, full_name
        FROM employees
        WHERE id = %s
          AND company_id = %s
          AND {active_sql}
        """,
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        flash(_t(lang, "Employee not found.", "Empleado no encontrado."))
        return redirect(url_for("employees.time_clock"))

    existing_open = conn.execute(
        """
        SELECT id
        FROM employee_time_entries
        WHERE company_id = %s
          AND employee_id = %s
          AND clock_out IS NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        (cid, employee_id),
    ).fetchone()

    if existing_open:
        conn.close()
        flash(_t(lang, "That employee is already clocked in.", "Ese empleado ya tiene entrada registrada."))
        return redirect(url_for("employees.time_clock"))

    conn.execute(
        """
        INSERT INTO employee_time_entries (
            company_id,
            employee_id,
            clock_in,
            clock_out,
            total_hours,
            notes
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (cid, employee_id, datetime.now(), None, 0, None),
    )

    conn.commit()
    conn.close()

    flash(_t(lang, "Employee clocked in successfully.", "Entrada del empleado registrada correctamente."))
    return redirect(url_for("employees.time_clock"))


@employees_bp.route("/employees/time-clock/clock-out", methods=["POST"])
@login_required
@subscription_required
def time_clock_clock_out():
    lang = _get_lang()

    ensure_employee_profile_columns()
    ensure_employee_time_entries_table()

    def _session_truthy(value):
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _session_int(value):
        try:
            return int(str(value).strip())
        except Exception:
            return None

    def _session_permission_enabled(name):
        permissions = (
            session.get("permissions")
            or session.get("user_permissions")
            or session.get("permission_map")
            or {}
        )

        if isinstance(permissions, dict):
            return _session_truthy(permissions.get(name))

        if isinstance(permissions, (list, tuple, set)):
            return name in permissions

        if isinstance(permissions, str):
            raw = permissions.strip()
            if not raw:
                return False
            lowered = raw.lower()
            if lowered in {"all", "*", "admin"}:
                return True
            return name in {part.strip() for part in raw.split(",") if part.strip()}

        return False

    def _can_manage_time_clock():
        role = str(
            session.get("role")
            or session.get("user_role")
            or session.get("account_role")
            or ""
        ).strip().lower()

        if role in {"owner", "admin", "manager"}:
            return True

        if _session_truthy(session.get("is_admin")) or _session_truthy(session.get("is_owner")):
            return True

        return _session_permission_enabled("can_manage_employees")

    def _current_employee_id():
        for key in ("employee_id", "linked_employee_id", "staff_employee_id"):
            value = _session_int(session.get(key))
            if value:
                return value
        return None

    def _can_use_time_clock():
        return _can_manage_time_clock() or bool(_current_employee_id())

    if not _can_use_time_clock():
        flash(_t(lang, "You do not have access to the time clock.", "No tienes acceso al reloj de tiempo."))
        return redirect(url_for("dashboard.dashboard"))

    conn = get_db_connection()
    cid = session["company_id"]

    posted_employee_id = _session_int(request.form.get("employee_id"))
    session_employee_id = _current_employee_id()

    if _can_manage_time_clock():
        employee_id = posted_employee_id
    else:
        employee_id = session_employee_id

    if not employee_id:
        conn.close()
        flash(_t(lang, "Please select an employee.", "Por favor selecciona un empleado."))
        return redirect(url_for("employees.time_clock"))

    open_entry = conn.execute(
        """
        SELECT id, clock_in
        FROM employee_time_entries
        WHERE company_id = %s
          AND employee_id = %s
          AND clock_out IS NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        (cid, employee_id),
    ).fetchone()

    if not open_entry:
        conn.close()
        flash(_t(lang, "That employee is not currently clocked in.", "Ese empleado no tiene una entrada activa."))
        return redirect(url_for("employees.time_clock"))

    clock_in_time = open_entry["clock_in"]
    clock_out_time = datetime.now()

    total_seconds = (clock_out_time - clock_in_time).total_seconds()
    total_hours = round(max(total_seconds / 3600.0, 0), 2)

    conn.execute(
        """
        UPDATE employee_time_entries
        SET clock_out = %s,
            total_hours = %s
        WHERE id = %s
        """,
        (clock_out_time, total_hours, open_entry["id"]),
    )

    conn.commit()
    conn.close()

    flash(f"{_t(lang, 'Employee clocked out successfully. Total hours:', 'Salida del empleado registrada correctamente. Horas totales:')} {total_hours:.2f}")
    return redirect(url_for("employees.time_clock"))


@employees_bp.route("/employees/time-clock/send-summary", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_employees")
def send_time_clock_summary_now():
    lang = _get_lang()
    cid = session["company_id"]

    try:
        result = send_pay_period_summary_emails_for_company(cid)
        flash(f"{_t(lang, 'Hours summary email sent. Emails delivered:', 'Correo de resumen de horas enviado. Correos entregados:')} {result['sent']}")
    except Exception as e:
        flash(f"{_t(lang, 'Could not send hours summary email:', 'No se pudo enviar el correo de resumen de horas:')} {e}")

    return redirect(url_for("employees.time_clock"))