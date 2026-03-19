import os
import sqlite3
from datetime import date
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_DIR, "yardledger_rebuild.db")

print("USING DB:", DB_NAME)


def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def table_exists(conn, table_name):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def table_columns(conn, table_name):
    if not table_exists(conn, table_name):
        return []
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [r["name"] if "name" in r.keys() else r[1] for r in rows]


def has_col(conn, table_name, col_name):
    return col_name in table_columns(conn, table_name)


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.executescript("""
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS companies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT,
        phone TEXT,
        address TEXT,
        default_quote_notes TEXT,
        default_invoice_notes TEXT,
        payment_terms TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        role TEXT DEFAULT 'owner',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        company TEXT,
        email TEXT,
        phone TEXT,
        billing_address TEXT,
        service_address TEXT,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS quotes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,
        customer_id INTEGER NOT NULL,
        quote_number TEXT,
        quote_date TEXT,
        expiration_date TEXT,
        status TEXT DEFAULT 'Draft',
        notes TEXT,
        subtotal REAL DEFAULT 0,
        tax REAL DEFAULT 0,
        total REAL DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
        FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS quote_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        quote_id INTEGER NOT NULL,
        description TEXT NOT NULL,
        quantity REAL DEFAULT 0,
        unit TEXT,
        unit_price REAL DEFAULT 0,
        line_total REAL DEFAULT 0,
        FOREIGN KEY (quote_id) REFERENCES quotes(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,
        customer_id INTEGER NOT NULL,
        quote_id INTEGER,
        title TEXT NOT NULL,
        scheduled_date TEXT,
        status TEXT DEFAULT 'Scheduled',
        address TEXT,
        notes TEXT,
        revenue REAL DEFAULT 0,
        cost_total REAL DEFAULT 0,
        profit REAL DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
        FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
        FOREIGN KEY (quote_id) REFERENCES quotes(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS job_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        item_type TEXT DEFAULT 'material',
        description TEXT NOT NULL,
        quantity REAL DEFAULT 0,
        unit TEXT,
        unit_price REAL DEFAULT 0,
        sale_price REAL DEFAULT 0,
        cost_amount REAL DEFAULT 0,
        line_total REAL DEFAULT 0,
        billable INTEGER DEFAULT 1,
        ledger_entry_id INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,
        customer_id INTEGER NOT NULL,
        job_id INTEGER,
        quote_id INTEGER,
        invoice_number TEXT,
        invoice_date TEXT,
        due_date TEXT,
        status TEXT DEFAULT 'Unpaid',
        notes TEXT,
        subtotal REAL DEFAULT 0,
        tax REAL DEFAULT 0,
        total REAL DEFAULT 0,
        amount_paid REAL DEFAULT 0,
        balance_due REAL DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
        FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE SET NULL,
        FOREIGN KEY (quote_id) REFERENCES quotes(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS invoice_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id INTEGER NOT NULL,
        description TEXT NOT NULL,
        quantity REAL DEFAULT 0,
        unit TEXT,
        unit_price REAL DEFAULT 0,
        line_total REAL DEFAULT 0,
        FOREIGN KEY (invoice_id) REFERENCES invoices(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS ledger_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,
        entry_date TEXT NOT NULL,
        entry_type TEXT NOT NULL,
        category TEXT NOT NULL,
        description TEXT NOT NULL,
        amount REAL NOT NULL,
        source_type TEXT,
        source_id INTEGER,
        customer_id INTEGER,
        invoice_id INTEGER,
        job_id INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS employees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,
        employee_number TEXT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        phone TEXT,
        email TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zip_code TEXT,
        hire_date TEXT,
        job_title TEXT,
        pay_type TEXT DEFAULT 'Hourly',
        pay_rate REAL DEFAULT 0,
        overtime_rate REAL DEFAULT 0,
        federal_tax_rate REAL DEFAULT 0,
        state_tax_rate REAL DEFAULT 3.15,
        filing_status TEXT DEFAULT 'single',
        pay_schedule TEXT DEFAULT 'weekly',
        status TEXT DEFAULT 'Active',
        emergency_contact_name TEXT,
        emergency_contact_phone TEXT,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS company_tax_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL UNIQUE,
        federal_withholding_rate REAL NOT NULL DEFAULT 0,
        state_withholding_rate REAL NOT NULL DEFAULT 0,
        social_security_rate REAL NOT NULL DEFAULT 6.2,
        medicare_rate REAL NOT NULL DEFAULT 1.45,
        local_tax_rate REAL NOT NULL DEFAULT 0,
        unemployment_rate REAL NOT NULL DEFAULT 0,
        workers_comp_rate REAL NOT NULL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS payroll_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,
        employee_id INTEGER NOT NULL,
        pay_date TEXT,
        pay_period_start TEXT,
        pay_period_end TEXT,
        pay_type TEXT,
        hours_regular REAL NOT NULL DEFAULT 0,
        hours_overtime REAL NOT NULL DEFAULT 0,
        rate_regular REAL NOT NULL DEFAULT 0,
        rate_overtime REAL NOT NULL DEFAULT 0,
        gross_pay REAL NOT NULL DEFAULT 0,
        federal_withholding REAL NOT NULL DEFAULT 0,
        state_withholding REAL NOT NULL DEFAULT 0,
        social_security REAL NOT NULL DEFAULT 0,
        medicare REAL NOT NULL DEFAULT 0,
        local_tax REAL NOT NULL DEFAULT 0,
        other_deductions REAL NOT NULL DEFAULT 0,
        net_pay REAL NOT NULL DEFAULT 0,
        notes TEXT,
        ledger_entry_id INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE
    );
    """)

    migrations = [
        "ALTER TABLE employees ADD COLUMN federal_tax_rate REAL DEFAULT 0",
        "ALTER TABLE employees ADD COLUMN state_tax_rate REAL DEFAULT 3.15",
        "ALTER TABLE employees ADD COLUMN employee_number TEXT",
        "ALTER TABLE employees ADD COLUMN filing_status TEXT DEFAULT 'single'",
        "ALTER TABLE employees ADD COLUMN pay_schedule TEXT DEFAULT 'weekly'",
        "ALTER TABLE employees ADD COLUMN city TEXT",
        "ALTER TABLE employees ADD COLUMN state TEXT",
        "ALTER TABLE employees ADD COLUMN zip_code TEXT",
        "ALTER TABLE employees ADD COLUMN hire_date TEXT",
        "ALTER TABLE employees ADD COLUMN job_title TEXT",
        "ALTER TABLE employees ADD COLUMN pay_type TEXT DEFAULT 'Hourly'",
        "ALTER TABLE employees ADD COLUMN pay_rate REAL DEFAULT 0",
        "ALTER TABLE employees ADD COLUMN overtime_rate REAL DEFAULT 0",
        "ALTER TABLE employees ADD COLUMN status TEXT DEFAULT 'Active'",
        "ALTER TABLE employees ADD COLUMN emergency_contact_name TEXT",
        "ALTER TABLE employees ADD COLUMN emergency_contact_phone TEXT",
        "ALTER TABLE payroll_entries ADD COLUMN ledger_entry_id INTEGER",
        "ALTER TABLE payroll_entries ADD COLUMN social_security REAL DEFAULT 0",
        "ALTER TABLE payroll_entries ADD COLUMN medicare REAL DEFAULT 0",
        "ALTER TABLE payroll_entries ADD COLUMN local_tax REAL NOT NULL DEFAULT 0",
        "ALTER TABLE companies ADD COLUMN default_quote_notes TEXT",
        "ALTER TABLE companies ADD COLUMN default_invoice_notes TEXT",
        "ALTER TABLE companies ADD COLUMN payment_terms TEXT",
    ]

    for sql in migrations:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass

    conn.commit()
    conn.close()

    ensure_company_profile_columns()
    ensure_employee_status_column()
    ensure_employee_name_columns()
    ensure_employee_payroll_columns()
    ensure_payroll_columns()
    ensure_payroll_table_structure()
    ensure_company_tax_settings_table()
    ensure_employee_tax_columns()
    ensure_bookkeeping_history_table()
    ensure_invoice_payments_table()
    ensure_user_permission_columns()
    ensure_customer_name_columns()
    ensure_billing_tables()
    ensure_company_profile_location_columns()
    ensure_company_profile_email_columns()


def ensure_company_profile_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(companies)")
    columns = [row[1] for row in cur.fetchall()]

    needed_columns = {
        "phone": "TEXT",
        "email": "TEXT",
        "website": "TEXT",
        "address_line_1": "TEXT",
        "address_line_2": "TEXT",
        "city": "TEXT",
        "state": "TEXT",
        "zip_code": "TEXT",
        "tax_id": "TEXT",
    }

    for col, col_type in needed_columns.items():
        if col not in columns:
            cur.execute(f"ALTER TABLE companies ADD COLUMN {col} {col_type}")

    conn.commit()
    conn.close()


def ensure_company_profile_email_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_profile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL UNIQUE,
            display_name TEXT,
            legal_name TEXT,
            logo_url TEXT,
            phone TEXT,
            email TEXT,
            website TEXT,
            address_line_1 TEXT,
            address_line_2 TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            invoice_header_name TEXT,
            quote_header_name TEXT,
            invoice_footer_note TEXT,
            quote_footer_note TEXT,
            email_from_name TEXT,
            reply_to_email TEXT,
            platform_sender_enabled INTEGER NOT NULL DEFAULT 1,
            reply_to_mode TEXT DEFAULT 'company',
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute("PRAGMA table_info(company_profile)")
    cols = [row[1] for row in cur.fetchall()]

    needed = {
        "phone": "TEXT",
        "email": "TEXT",
        "website": "TEXT",
        "address_line_1": "TEXT",
        "address_line_2": "TEXT",
        "city": "TEXT",
        "state": "TEXT",
        "zip_code": "TEXT",
        "invoice_header_name": "TEXT",
        "quote_header_name": "TEXT",
        "invoice_footer_note": "TEXT",
        "quote_footer_note": "TEXT",
        "email_from_name": "TEXT",
        "reply_to_email": "TEXT",
        "platform_sender_enabled": "INTEGER NOT NULL DEFAULT 1",
        "reply_to_mode": "TEXT DEFAULT 'company'",
        "updated_at": "TEXT DEFAULT CURRENT_TIMESTAMP",
    }

    for col, col_type in needed.items():
        if col not in cols:
            cur.execute(f"ALTER TABLE company_profile ADD COLUMN {col} {col_type}")

    conn.commit()
    conn.close()


def ensure_employee_status_column():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(employees)")
    columns = [row[1] for row in cur.fetchall()]

    if "is_active" not in columns:
        cur.execute("ALTER TABLE employees ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")

    conn.commit()
    conn.close()


def ensure_employee_name_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(employees)")
    cols = [row[1] for row in cur.fetchall()]

    if "first_name" not in cols:
        cur.execute("ALTER TABLE employees ADD COLUMN first_name TEXT")
    if "last_name" not in cols:
        cur.execute("ALTER TABLE employees ADD COLUMN last_name TEXT")
    if "full_name" not in cols:
        cur.execute("ALTER TABLE employees ADD COLUMN full_name TEXT")

    conn.commit()
    conn.close()


def ensure_employee_payroll_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(employees)")
    cols = [row[1] for row in cur.fetchall()]

    needed_columns = {
        "pay_type": "TEXT DEFAULT 'Hourly'",
        "pay_frequency": "TEXT DEFAULT 'Biweekly'",
        "hourly_rate": "REAL NOT NULL DEFAULT 0",
        "overtime_rate": "REAL NOT NULL DEFAULT 0",
        "salary_amount": "REAL NOT NULL DEFAULT 0",
        "federal_filing_status": "TEXT DEFAULT 'Single'",
        "w4_filing_status": "TEXT DEFAULT 'Single'",
        "w4_step2_checked": "INTEGER NOT NULL DEFAULT 0",
        "w4_step3_amount": "REAL NOT NULL DEFAULT 0",
        "w4_step4a_other_income": "REAL NOT NULL DEFAULT 0",
        "w4_step4b_deductions": "REAL NOT NULL DEFAULT 0",
        "w4_step4c_extra_withholding": "REAL NOT NULL DEFAULT 0",
        "is_active": "INTEGER NOT NULL DEFAULT 1",
    }

    for col_name, col_def in needed_columns.items():
        if col_name not in cols:
            cur.execute(f"ALTER TABLE employees ADD COLUMN {col_name} {col_def}")

    conn.commit()
    conn.close()


def ensure_company_profile_location_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(company_profile)")
    cols = [row[1] for row in cur.fetchall()]

    if "city" not in cols:
        cur.execute("ALTER TABLE company_profile ADD COLUMN city TEXT")

    if "state" not in cols:
        cur.execute("ALTER TABLE company_profile ADD COLUMN state TEXT")

    if "county" not in cols:
        cur.execute("ALTER TABLE company_profile ADD COLUMN county TEXT")

    conn.commit()
    conn.close()


def ensure_employee_tax_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(employees)")
    cols = [row[1] for row in cur.fetchall()]

    needed_columns = {
        "federal_filing_status": "TEXT DEFAULT 'Single'",
        "pay_frequency": "TEXT DEFAULT 'Biweekly'",
        "w4_step2_checked": "INTEGER NOT NULL DEFAULT 0",
        "w4_step3_amount": "REAL NOT NULL DEFAULT 0",
        "w4_step4a_other_income": "REAL NOT NULL DEFAULT 0",
        "w4_step4b_deductions": "REAL NOT NULL DEFAULT 0",
        "w4_step4c_extra_withholding": "REAL NOT NULL DEFAULT 0",
    }

    for col, col_type in needed_columns.items():
        if col not in cols:
            cur.execute(f"ALTER TABLE employees ADD COLUMN {col} {col_type}")

    conn.commit()
    conn.close()


def ensure_payroll_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(payroll_entries)")
    cols = [row[1] for row in cur.fetchall()]

    if "pay_type" not in cols:
        cur.execute("ALTER TABLE payroll_entries ADD COLUMN pay_type TEXT")

    conn.commit()
    conn.close()


def ensure_payroll_table_structure():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(payroll_entries)")
    cols = [row[1] for row in cur.fetchall()]

    needed_columns = {
        "company_id": "INTEGER NOT NULL DEFAULT 0",
        "employee_id": "INTEGER NOT NULL DEFAULT 0",
        "pay_date": "TEXT",
        "pay_period_start": "TEXT",
        "pay_period_end": "TEXT",
        "pay_type": "TEXT",
        "hours_regular": "REAL NOT NULL DEFAULT 0",
        "hours_overtime": "REAL NOT NULL DEFAULT 0",
        "rate_regular": "REAL NOT NULL DEFAULT 0",
        "rate_overtime": "REAL NOT NULL DEFAULT 0",
        "gross_pay": "REAL NOT NULL DEFAULT 0",
        "federal_withholding": "REAL NOT NULL DEFAULT 0",
        "state_withholding": "REAL NOT NULL DEFAULT 0",
        "social_security": "REAL NOT NULL DEFAULT 0",
        "medicare": "REAL NOT NULL DEFAULT 0",
        "local_tax": "REAL NOT NULL DEFAULT 0",
        "other_deductions": "REAL NOT NULL DEFAULT 0",
        "net_pay": "REAL NOT NULL DEFAULT 0",
        "notes": "TEXT",
        "ledger_entry_id": "INTEGER",
        "created_at": "TEXT DEFAULT CURRENT_TIMESTAMP",
    }

    for col, col_type in needed_columns.items():
        if col not in cols:
            cur.execute(f"ALTER TABLE payroll_entries ADD COLUMN {col} {col_type}")

    conn.commit()
    conn.close()


def ensure_billing_tables():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL UNIQUE,
            stripe_customer_id TEXT,
            stripe_subscription_id TEXT,
            stripe_price_id TEXT,
            plan_name TEXT,
            billing_interval TEXT,
            amount_cents INTEGER,
            status TEXT,
            auto_renew INTEGER NOT NULL DEFAULT 1,
            cancel_at_period_end INTEGER NOT NULL DEFAULT 0,
            current_period_start TEXT,
            current_period_end TEXT,
            payment_method_type TEXT,
            payment_method_last4 TEXT,
            payment_method_label TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS billing_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            stripe_invoice_id TEXT,
            stripe_event_id TEXT,
            event_type TEXT,
            amount_cents INTEGER,
            currency TEXT,
            status TEXT,
            hosted_invoice_url TEXT,
            invoice_pdf TEXT,
            event_date TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


def get_company_subscription(company_id):
    conn = get_db_connection()
    row = conn.execute(
        "SELECT * FROM subscriptions WHERE company_id = ?",
        (company_id,)
    ).fetchone()
    conn.close()
    return row


def upsert_company_subscription(
    company_id,
    stripe_customer_id=None,
    stripe_subscription_id=None,
    stripe_price_id=None,
    plan_name=None,
    billing_interval=None,
    amount_cents=None,
    status=None,
    auto_renew=1,
    cancel_at_period_end=0,
    current_period_start=None,
    current_period_end=None,
    payment_method_type=None,
    payment_method_last4=None,
    payment_method_label=None,
):
    conn = get_db_connection()
    existing = conn.execute(
        "SELECT id FROM subscriptions WHERE company_id = ?",
        (company_id,)
    ).fetchone()

    if existing:
        conn.execute("""
            UPDATE subscriptions
            SET stripe_customer_id = COALESCE(?, stripe_customer_id),
                stripe_subscription_id = COALESCE(?, stripe_subscription_id),
                stripe_price_id = COALESCE(?, stripe_price_id),
                plan_name = COALESCE(?, plan_name),
                billing_interval = COALESCE(?, billing_interval),
                amount_cents = COALESCE(?, amount_cents),
                status = COALESCE(?, status),
                auto_renew = ?,
                cancel_at_period_end = ?,
                current_period_start = COALESCE(?, current_period_start),
                current_period_end = COALESCE(?, current_period_end),
                payment_method_type = COALESCE(?, payment_method_type),
                payment_method_last4 = COALESCE(?, payment_method_last4),
                payment_method_label = COALESCE(?, payment_method_label),
                updated_at = CURRENT_TIMESTAMP
            WHERE company_id = ?
        """, (
            stripe_customer_id,
            stripe_subscription_id,
            stripe_price_id,
            plan_name,
            billing_interval,
            amount_cents,
            status,
            auto_renew,
            cancel_at_period_end,
            current_period_start,
            current_period_end,
            payment_method_type,
            payment_method_last4,
            payment_method_label,
            company_id,
        ))
    else:
        conn.execute("""
            INSERT INTO subscriptions (
                company_id,
                stripe_customer_id,
                stripe_subscription_id,
                stripe_price_id,
                plan_name,
                billing_interval,
                amount_cents,
                status,
                auto_renew,
                cancel_at_period_end,
                current_period_start,
                current_period_end,
                payment_method_type,
                payment_method_last4,
                payment_method_label
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            company_id,
            stripe_customer_id,
            stripe_subscription_id,
            stripe_price_id,
            plan_name,
            billing_interval,
            amount_cents,
            status,
            auto_renew,
            cancel_at_period_end,
            current_period_start,
            current_period_end,
            payment_method_type,
            payment_method_last4,
            payment_method_label,
        ))

    conn.commit()
    conn.close()


def insert_billing_event(
    company_id,
    stripe_invoice_id=None,
    stripe_event_id=None,
    event_type=None,
    amount_cents=None,
    currency=None,
    status=None,
    hosted_invoice_url=None,
    invoice_pdf=None,
    event_date=None,
    notes=None,
):
    conn = get_db_connection()
    conn.execute("""
        INSERT INTO billing_events (
            company_id,
            stripe_invoice_id,
            stripe_event_id,
            event_type,
            amount_cents,
            currency,
            status,
            hosted_invoice_url,
            invoice_pdf,
            event_date,
            notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        company_id,
        stripe_invoice_id,
        stripe_event_id,
        event_type,
        amount_cents,
        currency,
        status,
        hosted_invoice_url,
        invoice_pdf,
        event_date,
        notes,
    ))
    conn.commit()
    conn.close()


def ensure_company_profile_tax_location_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(company_profile)")
    cols = [row[1] for row in cur.fetchall()]

    if "city" not in cols:
        cur.execute("ALTER TABLE company_profile ADD COLUMN city TEXT")

    if "state" not in cols:
        cur.execute("ALTER TABLE company_profile ADD COLUMN state TEXT")

    if "county" not in cols:
        cur.execute("ALTER TABLE company_profile ADD COLUMN county TEXT")

    conn.commit()
    conn.close()


def get_billing_history(company_id, limit=20):
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT *
        FROM billing_events
        WHERE company_id = ?
        ORDER BY COALESCE(event_date, created_at) DESC, id DESC
        LIMIT ?
    """, (company_id, limit)).fetchall()
    conn.close()
    return rows


def ensure_company_tax_settings_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_tax_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL UNIQUE,
            federal_withholding_rate REAL NOT NULL DEFAULT 0,
            state_withholding_rate REAL NOT NULL DEFAULT 0,
            social_security_rate REAL NOT NULL DEFAULT 6.2,
            medicare_rate REAL NOT NULL DEFAULT 1.45,
            local_tax_rate REAL NOT NULL DEFAULT 0,
            unemployment_rate REAL NOT NULL DEFAULT 0,
            workers_comp_rate REAL NOT NULL DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


def ensure_user_permission_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cols = [r["name"] for r in cur.execute("PRAGMA table_info(users)").fetchall()]

    if "role" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'Employee'")
    if "is_active" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1")
    if "can_manage_users" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN can_manage_users INTEGER DEFAULT 0")
    if "can_view_payroll" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN can_view_payroll INTEGER DEFAULT 0")
    if "can_manage_payroll" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN can_manage_payroll INTEGER DEFAULT 0")
    if "can_view_bookkeeping" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN can_view_bookkeeping INTEGER DEFAULT 0")
    if "can_manage_bookkeeping" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN can_manage_bookkeeping INTEGER DEFAULT 0")
    if "can_manage_jobs" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN can_manage_jobs INTEGER DEFAULT 0")
    if "can_manage_customers" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN can_manage_customers INTEGER DEFAULT 0")
    if "can_manage_invoices" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN can_manage_invoices INTEGER DEFAULT 0")
    if "can_manage_settings" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN can_manage_settings INTEGER DEFAULT 0")
    if "can_manage_employees" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN can_manage_employees INTEGER DEFAULT 0")
    if "can_view_employees" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN can_view_employees INTEGER DEFAULT 0")
    if "can_manage_quotes" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN can_manage_quotes INTEGER DEFAULT 0")

    conn.commit()
    conn.close()


def create_owner_user(company_id, name, email, password_hash):
    ensure_user_permission_columns()

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO users (
            company_id,
            name,
            email,
            password_hash,
            role,
            is_active,
            can_manage_users,
            can_view_payroll,
            can_manage_payroll,
            can_view_bookkeeping,
            can_manage_bookkeeping,
            can_manage_jobs,
            can_manage_customers,
            can_manage_invoices,
            can_manage_settings,
            can_manage_employees,
            can_view_employees,
            can_manage_quotes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            company_id,
            name,
            email,
            password_hash,
            "Owner",
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
        ),
    )

    user_id = cur.lastrowid
    conn.commit()
    conn.close()
    return user_id


def get_employee_columns():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(employees)")
    cols = [row[1] for row in cur.fetchall()]
    conn.close()
    return cols


def ensure_invoice_payments_table():
    conn = get_db_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS invoice_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            invoice_id INTEGER NOT NULL,
            payment_date TEXT,
            amount REAL NOT NULL DEFAULT 0,
            payment_method TEXT,
            reference TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def ensure_bookkeeping_history_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bookkeeping_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            entry_date TEXT NOT NULL,
            category TEXT NOT NULL,
            entry_type TEXT NOT NULL,
            description TEXT NOT NULL,
            amount REAL NOT NULL DEFAULT 0,
            money_in REAL NOT NULL DEFAULT 0,
            money_out REAL NOT NULL DEFAULT 0,
            reference_type TEXT,
            reference_id INTEGER,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    existing_cols = {
        row["name"] for row in cur.execute("PRAGMA table_info(bookkeeping_history)").fetchall()
    }

    if "company_id" not in existing_cols:
        cur.execute("ALTER TABLE bookkeeping_history ADD COLUMN company_id INTEGER NOT NULL DEFAULT 0")
    if "entry_date" not in existing_cols:
        cur.execute("ALTER TABLE bookkeeping_history ADD COLUMN entry_date TEXT")
    if "category" not in existing_cols:
        cur.execute("ALTER TABLE bookkeeping_history ADD COLUMN category TEXT")
    if "entry_type" not in existing_cols:
        cur.execute("ALTER TABLE bookkeeping_history ADD COLUMN entry_type TEXT")
    if "description" not in existing_cols:
        cur.execute("ALTER TABLE bookkeeping_history ADD COLUMN description TEXT")
    if "amount" not in existing_cols:
        cur.execute("ALTER TABLE bookkeeping_history ADD COLUMN amount REAL NOT NULL DEFAULT 0")
    if "money_in" not in existing_cols:
        cur.execute("ALTER TABLE bookkeeping_history ADD COLUMN money_in REAL NOT NULL DEFAULT 0")
    if "money_out" not in existing_cols:
        cur.execute("ALTER TABLE bookkeeping_history ADD COLUMN money_out REAL NOT NULL DEFAULT 0")
    if "reference_type" not in existing_cols:
        cur.execute("ALTER TABLE bookkeeping_history ADD COLUMN reference_type TEXT")
    if "reference_id" not in existing_cols:
        cur.execute("ALTER TABLE bookkeeping_history ADD COLUMN reference_id INTEGER")
    if "notes" not in existing_cols:
        cur.execute("ALTER TABLE bookkeeping_history ADD COLUMN notes TEXT")
    if "created_at" not in existing_cols:
        cur.execute("ALTER TABLE bookkeeping_history ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP")

    conn.commit()
    conn.close()


def add_bookkeeping_history_entry(
    company_id,
    entry_date,
    category,
    entry_type,
    description,
    amount=0,
    money_in=0,
    money_out=0,
    reference_type=None,
    reference_id=None,
    notes=None,
):
    ensure_bookkeeping_history_table()

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO bookkeeping_history (
            company_id,
            entry_date,
            category,
            entry_type,
            description,
            amount,
            money_in,
            money_out,
            reference_type,
            reference_id,
            notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        company_id,
        entry_date,
        category,
        entry_type,
        description,
        amount,
        money_in,
        money_out,
        reference_type,
        reference_id,
        notes,
    ))

    conn.commit()
    conn.close()


def add_bookkeeping_entry(*args, **kwargs):
    return add_bookkeeping_history_entry(*args, **kwargs)


def backfill_payroll_bookkeeping_history():
    ensure_bookkeeping_history_table()

    conn = get_db_connection()
    cur = conn.cursor()

    rows = conn.execute("""
        SELECT
            p.id,
            p.company_id,
            p.pay_date,
            p.gross_pay,
            p.notes,
            e.first_name,
            e.last_name,
            e.full_name
        FROM payroll_entries p
        LEFT JOIN employees e ON p.employee_id = e.id
    """).fetchall()

    for row in rows:
        existing = conn.execute("""
            SELECT id
            FROM bookkeeping_history
            WHERE company_id = ?
              AND reference_type = 'payroll'
              AND reference_id = ?
        """, (row["company_id"], row["id"])).fetchone()

        if existing:
            continue

        employee_name = ""
        if "first_name" in row.keys() and row["first_name"]:
            employee_name = row["first_name"]
            if row["last_name"]:
                employee_name += " " + row["last_name"]
        elif "full_name" in row.keys() and row["full_name"]:
            employee_name = row["full_name"]
        else:
            employee_name = f"Employee #{row['id']}"

        gross_pay = float(row["gross_pay"] or 0)

        cur.execute("""
            INSERT INTO bookkeeping_history (
                company_id,
                entry_date,
                category,
                entry_type,
                description,
                amount,
                money_in,
                money_out,
                reference_type,
                reference_id,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["company_id"],
            row["pay_date"],
            "Payroll",
            "Expense",
            f"Payroll - {employee_name}",
            gross_pay,
            0,
            gross_pay,
            "payroll",
            row["id"],
            row["notes"],
        ))

    conn.commit()
    conn.close()


def ensure_payroll_ledger_entry(conn, payroll_id):
    payroll = conn.execute("""
        SELECT
            p.*,
            e.first_name,
            e.last_name,
            e.full_name
        FROM payroll_entries p
        LEFT JOIN employees e ON p.employee_id = e.id
        WHERE p.id = ?
    """, (payroll_id,)).fetchone()

    if not payroll:
        return

    employee_name = ""
    cols = payroll.keys()

    if "first_name" in cols and payroll["first_name"]:
        employee_name = payroll["first_name"]

    if "last_name" in cols and payroll["last_name"]:
        employee_name = (employee_name + " " + payroll["last_name"]).strip()

    if not employee_name and "full_name" in cols and payroll["full_name"]:
        employee_name = payroll["full_name"]

    if not employee_name:
        employee_name = f"Employee #{payroll['employee_id']}"

    pay_date = payroll["pay_date"] if "pay_date" in cols and payroll["pay_date"] else None
    gross_pay = float(payroll["gross_pay"] or 0)

    description = f"Payroll - {employee_name}"
    if pay_date:
        description += f" - {pay_date}"

    ledger_cols = conn.execute("PRAGMA table_info(ledger_entries)").fetchall()
    ledger_col_names = [r["name"] if "name" in r.keys() else r[1] for r in ledger_cols]

    has_source_type = "source_type" in ledger_col_names
    has_source_id = "source_id" in ledger_col_names
    has_reference_type = "reference_type" in ledger_col_names
    has_reference_id = "reference_id" in ledger_col_names
    has_entry_date = "entry_date" in ledger_col_names
    has_date = "date" in ledger_col_names
    has_created_at = "created_at" in ledger_col_names
    has_description = "description" in ledger_col_names
    has_memo = "memo" in ledger_col_names
    has_notes = "notes" in ledger_col_names
    has_category = "category" in ledger_col_names

    existing = None

    if has_source_type and has_source_id:
        existing = conn.execute("""
            SELECT id
            FROM ledger_entries
            WHERE source_type = 'payroll' AND source_id = ?
        """, (payroll_id,)).fetchone()
    elif has_reference_type and has_reference_id:
        existing = conn.execute("""
            SELECT id
            FROM ledger_entries
            WHERE reference_type = 'payroll' AND reference_id = ?
        """, (payroll_id,)).fetchone()

    set_parts = ["company_id = ?", "entry_type = ?", "amount = ?"]
    values = [payroll["company_id"], "Expense", gross_pay]

    if has_category:
        set_parts.append("category = ?")
        values.append("Payroll")

    if has_description:
        set_parts.append("description = ?")
        values.append(description)
    elif has_memo:
        set_parts.append("memo = ?")
        values.append(description)
    elif has_notes:
        set_parts.append("notes = ?")
        values.append(description)

    if has_entry_date:
        set_parts.append("entry_date = ?")
        values.append(pay_date)
    elif has_date:
        set_parts.append("date = ?")
        values.append(pay_date)
    elif has_created_at:
        set_parts.append("created_at = ?")
        values.append(pay_date)

    if has_source_type:
        set_parts.append("source_type = ?")
        values.append("payroll")
    if has_source_id:
        set_parts.append("source_id = ?")
        values.append(payroll_id)
    if has_reference_type:
        set_parts.append("reference_type = ?")
        values.append("payroll")
    if has_reference_id:
        set_parts.append("reference_id = ?")
        values.append(payroll_id)

    if existing:
        values.append(existing["id"])
        conn.execute(f"""
            UPDATE ledger_entries
            SET {", ".join(set_parts)}
            WHERE id = ?
        """, values)
    else:
        insert_cols = []
        insert_vals = []
        placeholders = []

        def add_col(col, val):
            insert_cols.append(col)
            insert_vals.append(val)
            placeholders.append("?")

        add_col("company_id", payroll["company_id"])
        add_col("entry_type", "Expense")
        add_col("amount", gross_pay)

        if has_category:
            add_col("category", "Payroll")

        if has_description:
            add_col("description", description)
        elif has_memo:
            add_col("memo", description)
        elif has_notes:
            add_col("notes", description)

        if has_entry_date:
            add_col("entry_date", pay_date)
        elif has_date:
            add_col("date", pay_date)
        elif has_created_at:
            add_col("created_at", pay_date)

        if has_source_type:
            add_col("source_type", "payroll")
        if has_source_id:
            add_col("source_id", payroll_id)
        if has_reference_type:
            add_col("reference_type", "payroll")
        if has_reference_id:
            add_col("reference_id", payroll_id)

        conn.execute(f"""
            INSERT INTO ledger_entries ({", ".join(insert_cols)})
            VALUES ({", ".join(placeholders)})
        """, insert_vals)


def ensure_job_cost_ledger(conn, job_item_id):
    item = conn.execute(
        """
        SELECT
            ji.*,
            j.company_id,
            j.customer_id
        FROM job_items ji
        JOIN jobs j ON ji.job_id = j.id
        WHERE ji.id = ?
        """,
        (job_item_id,),
    ).fetchone()

    if not item:
        return

    def infer_item_type(raw_item_type, description):
        item_type = (raw_item_type or "").strip().lower()
        desc = (description or "").strip().lower()

        if item_type and item_type != "material":
            return item_type

        if "labor" in desc or "labour" in desc or "hour" in desc or "hours" in desc or "hr" in desc or "hrs" in desc:
            return "labor"
        if "fuel" in desc:
            return "fuel"
        if "equipment" in desc:
            return "equipment"
        if "delivery" in desc:
            return "delivery"
        if "misc" in desc:
            return "misc"

        return item_type or "material"

    item_type = infer_item_type(item["item_type"], item["description"])

    category_map = {
        "material": "Material",
        "labor": "Labor",
        "fuel": "Fuel",
        "equipment": "Equipment",
        "delivery": "Delivery",
        "misc": "Misc",
    }

    category = category_map.get(item_type, "Material")
    description = (item["description"] or f"Job {category} item").strip()

    quantity = float(item["quantity"] or 0)
    cost_amount = float(item["cost_amount"] or 0)
    line_total = float(item["line_total"] or 0)
    unit_price = float(item["unit_price"] or 0)
    sale_price = float(item["sale_price"] or 0)

    amount = abs(cost_amount)

    if amount <= 0 and item_type == "labor":
        if line_total > 0:
            amount = abs(line_total)
        elif quantity > 0 and sale_price > 0:
            amount = abs(quantity * sale_price)
        elif quantity > 0 and unit_price > 0:
            amount = abs(quantity * unit_price)

    if amount <= 0 and quantity > 0 and unit_price > 0:
        amount = abs(quantity * unit_price)

    entry_date = date.today().isoformat()

    existing_ledger_id = item["ledger_entry_id"] if "ledger_entry_id" in item.keys() else None
    existing_ledger = None

    if existing_ledger_id:
        existing_ledger = conn.execute(
            """
            SELECT id
            FROM ledger_entries
            WHERE id = ?
            """,
            (existing_ledger_id,),
        ).fetchone()

    if not existing_ledger:
        existing_ledger = conn.execute(
            """
            SELECT id
            FROM ledger_entries
            WHERE source_type = 'job_item' AND source_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (job_item_id,),
        ).fetchone()

    if item["item_type"] != item_type:
        conn.execute(
            """
            UPDATE job_items
            SET item_type = ?
            WHERE id = ?
            """,
            (item_type, job_item_id),
        )

    if existing_ledger:
        ledger_id = existing_ledger["id"]

        conn.execute(
            """
            UPDATE ledger_entries
            SET
                company_id = ?,
                entry_date = ?,
                entry_type = 'Expense',
                category = ?,
                description = ?,
                amount = ?,
                source_type = 'job_item',
                source_id = ?,
                customer_id = ?,
                invoice_id = NULL,
                job_id = ?
            WHERE id = ?
            """,
            (
                item["company_id"],
                entry_date,
                category,
                description,
                amount,
                job_item_id,
                item["customer_id"],
                item["job_id"],
                ledger_id,
            ),
        )

        if existing_ledger_id != ledger_id:
            conn.execute(
                """
                UPDATE job_items
                SET ledger_entry_id = ?
                WHERE id = ?
                """,
                (ledger_id, job_item_id),
            )
    else:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO ledger_entries (
                company_id,
                entry_date,
                entry_type,
                category,
                description,
                amount,
                source_type,
                source_id,
                customer_id,
                invoice_id,
                job_id
            )
            VALUES (?, ?, 'Expense', ?, ?, ?, 'job_item', ?, ?, NULL, ?)
            """,
            (
                item["company_id"],
                entry_date,
                category,
                description,
                amount,
                job_item_id,
                item["customer_id"],
                item["job_id"],
            ),
        )
        new_ledger_id = cur.lastrowid

        conn.execute(
            """
            UPDATE job_items
            SET ledger_entry_id = ?
            WHERE id = ?
            """,
            (new_ledger_id, job_item_id),
        )


def repair_all_job_item_ledgers(conn, company_id):
    rows = conn.execute(
        """
        SELECT ji.id
        FROM job_items ji
        JOIN jobs j ON ji.job_id = j.id
        WHERE j.company_id = ?
        ORDER BY ji.id
        """,
        (company_id,),
    ).fetchall()

    repaired = 0
    for row in rows:
        ensure_job_cost_ledger(conn, row["id"])
        repaired += 1

    return repaired


def create_income_ledger_for_payment(conn, invoice_id, payment_amount):
    inv = conn.execute("SELECT * FROM invoices WHERE id=?", (invoice_id,)).fetchone()
    if not inv or payment_amount <= 0:
        return

    desc = f"Invoice #{inv['invoice_number'] or inv['id']} payment"
    conn.execute(
        """
        INSERT INTO ledger_entries (
            company_id, entry_date, entry_type, category, description, amount,
            source_type, source_id, customer_id, invoice_id, job_id
        )
        VALUES (?, ?, 'Income', 'Invoice Payment', ?, ?, 'invoice_payment', ?, ?, ?, ?)
        """,
        (
            inv["company_id"],
            date.today().isoformat(),
            desc,
            payment_amount,
            inv["id"],
            inv["customer_id"],
            inv["id"],
            inv["job_id"],
        ),
    )


def update_invoice_balance(invoice_id):
    conn = get_db_connection()

    invoice = conn.execute(
        "SELECT id, total FROM invoices WHERE id = ?",
        (invoice_id,),
    ).fetchone()

    if not invoice:
        conn.close()
        return

    paid_row = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) AS paid_total FROM invoice_payments WHERE invoice_id = ?",
        (invoice_id,),
    ).fetchone()

    paid_total = float(paid_row["paid_total"] or 0)
    total = float(invoice["total"] or 0)
    balance_due = max(total - paid_total, 0)

    if paid_total <= 0:
        status = "Unpaid"
    elif balance_due > 0:
        status = "Partial"
    else:
        status = "Paid"

    conn.execute(
        """
        UPDATE invoices
        SET balance_due = ?, status = ?
        WHERE id = ?
        """,
        (balance_due, status, invoice_id),
    )
    conn.commit()
    conn.close()


def create_payroll_ledger_entry(conn, payroll_entry_id):
    row = conn.execute(
        """
        SELECT pe.*, e.first_name, e.last_name
        FROM payroll_entries pe
        JOIN employees e ON pe.employee_id = e.id
        WHERE pe.id=?
        """,
        (payroll_entry_id,),
    ).fetchone()

    if not row:
        return

    employee_name = f"{row['first_name']} {row['last_name']}"
    description = f"Payroll for {employee_name}"

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO ledger_entries (
            company_id, entry_date, entry_type, category, description, amount,
            source_type, source_id
        )
        VALUES (?, ?, 'Expense', 'Payroll', ?, ?, 'payroll', ?)
        """,
        (
            row["company_id"],
            row["pay_date"] or date.today().isoformat(),
            description,
            float(row["gross_pay"] or 0),
            row["id"],
        ),
    )
    ledger_id = cur.lastrowid

    conn.execute(
        "UPDATE payroll_entries SET ledger_entry_id=? WHERE id=?",
        (ledger_id, payroll_entry_id),
    )


def ensure_customer_name_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cols = [r["name"] for r in cur.execute("PRAGMA table_info(customers)").fetchall()]

    if "first_name" not in cols:
        cur.execute("ALTER TABLE customers ADD COLUMN first_name TEXT")

    if "last_name" not in cols:
        cur.execute("ALTER TABLE customers ADD COLUMN last_name TEXT")

    conn.commit()

    rows = cur.execute("SELECT id, name FROM customers").fetchall()

    for r in rows:
        name = (r["name"] or "").strip()
        if not name:
            continue

        parts = name.split(" ")
        first = parts[0]
        last = parts[-1] if len(parts) > 1 else ""

        cur.execute("""
            UPDATE customers
            SET first_name = ?, last_name = ?
            WHERE id = ?
        """, (first, last, r["id"]))

    conn.commit()
    conn.close()