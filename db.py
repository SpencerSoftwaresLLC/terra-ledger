import os
import re
import sys
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


# -------------------------------------------------------------------
# Environment loading / DATABASE_URL validation
# -------------------------------------------------------------------

def _safe_database_url_preview(value: str) -> str:
    if not value:
        return "missing"
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:****@", value)


def _load_environment():
    possible_env_files = []

    current_file_dir = Path(__file__).resolve().parent
    cwd_dir = Path.cwd()

    possible_env_files.append(current_file_dir / ".env")
    possible_env_files.append(current_file_dir.parent / ".env")
    possible_env_files.append(cwd_dir / ".env")

    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        possible_env_files.append(exe_dir / ".env")
        possible_env_files.append(exe_dir / "resources" / ".env")
        possible_env_files.append(exe_dir.parent / ".env")

    seen = set()
    for env_file in possible_env_files:
        env_str = str(env_file)
        if env_str in seen:
            continue
        seen.add(env_str)

        if env_file.exists():
            load_dotenv(env_file, override=True)
            print(f"Loaded .env from: {env_file}", flush=True)
            return str(env_file)

    load_dotenv(override=True)
    print("No explicit .env file found; used default load_dotenv()", flush=True)
    return None


def _get_database_url():
    return os.environ.get("DATABASE_URL", "").strip()


def _validate_database_url(database_url: str):
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. Make sure your environment contains a full PostgreSQL connection string."
        )

    parsed = urlparse(database_url)

    if parsed.scheme not in {"postgres", "postgresql"}:
        raise RuntimeError(
            f"DATABASE_URL must start with postgres:// or postgresql://. Got: {parsed.scheme or 'missing scheme'}"
        )

    if not parsed.hostname:
        raise RuntimeError("DATABASE_URL is missing a hostname.")

    if "." not in parsed.hostname:
        raise RuntimeError(
            f'DATABASE_URL host looks incomplete: "{parsed.hostname}". Use the full Render hostname.'
        )

    if not parsed.path or parsed.path == "/":
        raise RuntimeError("DATABASE_URL is missing the database name.")

    if not parsed.username:
        raise RuntimeError("DATABASE_URL is missing the database username.")

    if parsed.port is None:
        raise RuntimeError("DATABASE_URL is missing the port.")

    qs = parse_qs(parsed.query)
    sslmode = (qs.get("sslmode") or [""])[0].strip().lower()
    if sslmode != "require":
        raise RuntimeError("DATABASE_URL must include ?sslmode=require on Render.")

    print("RAW DATABASE_URL FROM ENV:", repr(database_url), flush=True)
    print("PARSED HOSTNAME:", repr(parsed.hostname), flush=True)
    print("DATABASE_URL validated successfully", flush=True)


_LOADED_ENV_FILE = _load_environment()
DATABASE_URL = _get_database_url()

print("USING DATABASE_URL:", "set" if DATABASE_URL else "missing", flush=True)
print("DATABASE_URL preview:", _safe_database_url_preview(DATABASE_URL), flush=True)


# -------------------------------------------------------------------
# Compatibility layer
# -------------------------------------------------------------------

def _convert_qmarks_to_percent_s(sql: str) -> str:
    """
    Compatibility layer so older routes using SQLite-style ? placeholders
    still work while the rest of the app is migrated to PostgreSQL.
    """
    parts = sql.split("?")
    if len(parts) <= 1:
        return sql
    return "%s".join(parts)


class DBRow(dict):
    def keys(self):
        return super().keys()


class DBCursor:
    def __init__(self, conn_wrapper, cursor):
        self._conn_wrapper = conn_wrapper
        self._cursor = cursor
        self.lastrowid = None
        self._prefetched_rows = None

    def execute(self, sql, params=None):
        sql = _convert_qmarks_to_percent_s(sql)
        params = params or ()

        normalized = sql.strip().lower()
        is_insert = normalized.startswith("insert into")
        has_returning = " returning " in normalized

        if is_insert and not has_returning:
            sql = sql.rstrip().rstrip(";") + " RETURNING id"

        self._cursor.execute(sql, params)

        self.lastrowid = None
        self._prefetched_rows = None

        if is_insert:
            try:
                rows = self._cursor.fetchall()
                self._prefetched_rows = rows
                if rows and "id" in rows[0]:
                    self.lastrowid = rows[0]["id"]
            except Exception:
                self._prefetched_rows = None

        return self

    def executescript(self, script):
        statements = [s.strip() for s in script.split(";") if s.strip()]
        for stmt in statements:
            self._cursor.execute(stmt)
        return self

    def fetchone(self):
        if self._prefetched_rows is not None:
            if not self._prefetched_rows:
                return None
            row = self._prefetched_rows[0]
            self._prefetched_rows = self._prefetched_rows[1:]
            return DBRow(row)

        row = self._cursor.fetchone()
        return DBRow(row) if row else None

    def fetchall(self):
        if self._prefetched_rows is not None:
            rows = [DBRow(r) for r in self._prefetched_rows]
            self._prefetched_rows = []
            return rows

        rows = self._cursor.fetchall()
        return [DBRow(r) for r in rows]

    def __iter__(self):
        return iter(self.fetchall())

    def close(self):
        self._cursor.close()


class DBConnection:
    def __init__(self, raw_conn):
        self._raw_conn = raw_conn

    def cursor(self):
        return DBCursor(
            self,
            self._raw_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor),
        )

    def execute(self, sql, params=None):
        cur = self.cursor()
        return cur.execute(sql, params)

    def commit(self):
        self._raw_conn.commit()

    def rollback(self):
        self._raw_conn.rollback()

    def close(self):
        self._raw_conn.close()


def get_db_connection():
    database_url = _get_database_url()
    _validate_database_url(database_url)

    try:
        raw_conn = psycopg2.connect(database_url, connect_timeout=10)
        raw_conn.autocommit = False
        return DBConnection(raw_conn)
    except psycopg2.OperationalError as e:
        raise RuntimeError(
            "PostgreSQL connection failed. Double-check that DATABASE_URL in your .env uses the full Render external hostname."
        ) from e


# -------------------------------------------------------------------
# Schema helpers
# -------------------------------------------------------------------

def table_exists(conn, table_name):
    row = conn.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        ) AS exists
        """,
        (table_name,),
    ).fetchone()
    return bool(row["exists"]) if row else False


def table_columns(conn, table_name):
    if not table_exists(conn, table_name):
        return []

    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,),
    ).fetchall()
    return [r["column_name"] for r in rows]


def has_col(conn, table_name, col_name):
    return col_name in table_columns(conn, table_name)


def safe_add_column(cur, table_name, col_name, col_def):
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        """,
        (table_name, col_name),
    )
    exists = cur.fetchone()
    if not exists:
        cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_def}')


# -------------------------------------------------------------------
# Company / profile ensures
# -------------------------------------------------------------------

def ensure_company_profile_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_profile (
            id SERIAL PRIMARY KEY,
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
            county TEXT,
            zip_code TEXT,
            invoice_header_name TEXT,
            quote_header_name TEXT,
            invoice_footer_note TEXT,
            quote_footer_note TEXT,
            email_from_name TEXT,
            reply_to_email TEXT,
            platform_sender_enabled INTEGER NOT NULL DEFAULT 1,
            reply_to_mode TEXT DEFAULT 'company',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


def ensure_document_number_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    safe_add_column(cur, "companies", "next_quote_number", "INTEGER NOT NULL DEFAULT 1001")
    safe_add_column(cur, "companies", "next_invoice_number", "INTEGER NOT NULL DEFAULT 1001")

    conn.commit()
    conn.close()


def ensure_company_profile_columns():
    conn = get_db_connection()
    cur = conn.cursor()

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
        safe_add_column(cur, "companies", col, col_type)

    conn.commit()
    conn.close()


def ensure_company_profile_email_columns():
    ensure_company_profile_table()

    conn = get_db_connection()
    cur = conn.cursor()

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
        "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    }

    for col, col_type in needed.items():
        safe_add_column(cur, "company_profile", col, col_type)

    conn.commit()
    conn.close()


def ensure_company_profile_location_columns():
    ensure_company_profile_table()

    conn = get_db_connection()
    cur = conn.cursor()

    safe_add_column(cur, "company_profile", "city", "TEXT")
    safe_add_column(cur, "company_profile", "state", "TEXT")
    safe_add_column(cur, "company_profile", "county", "TEXT")

    conn.commit()
    conn.close()


def ensure_company_profile_tax_location_columns():
    ensure_company_profile_location_columns()


# -------------------------------------------------------------------
# Employee / payroll ensures
# -------------------------------------------------------------------

def ensure_employee_status_column():
    conn = get_db_connection()
    cur = conn.cursor()
    safe_add_column(cur, "employees", "is_active", "INTEGER NOT NULL DEFAULT 1")
    conn.commit()
    conn.close()


def ensure_employee_name_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    safe_add_column(cur, "employees", "first_name", "TEXT")
    safe_add_column(cur, "employees", "last_name", "TEXT")
    safe_add_column(cur, "employees", "full_name", "TEXT")

    conn.commit()
    conn.close()


def ensure_employee_payroll_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    needed_columns = {
        "pay_type": "TEXT DEFAULT 'Hourly'",
        "pay_frequency": "TEXT DEFAULT 'Biweekly'",
        "hourly_rate": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "overtime_rate": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "salary_amount": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "federal_filing_status": "TEXT DEFAULT 'Single'",
        "w4_filing_status": "TEXT DEFAULT 'Single'",
        "w4_step2_checked": "INTEGER NOT NULL DEFAULT 0",
        "w4_step3_amount": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "w4_step4a_other_income": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "w4_step4b_deductions": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "w4_step4c_extra_withholding": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "is_active": "INTEGER NOT NULL DEFAULT 1",
    }

    for col_name, col_def in needed_columns.items():
        safe_add_column(cur, "employees", col_name, col_def)

    conn.commit()
    conn.close()


def ensure_employee_tax_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    needed_columns = {
        "federal_filing_status": "TEXT DEFAULT 'Single'",
        "pay_frequency": "TEXT DEFAULT 'Biweekly'",
        "w4_step2_checked": "INTEGER NOT NULL DEFAULT 0",
        "w4_step3_amount": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "w4_step4a_other_income": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "w4_step4b_deductions": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "w4_step4c_extra_withholding": "DOUBLE PRECISION NOT NULL DEFAULT 0",
    }

    for col, col_type in needed_columns.items():
        safe_add_column(cur, "employees", col, col_type)

    conn.commit()
    conn.close()


def ensure_payroll_columns():
    conn = get_db_connection()
    cur = conn.cursor()
    safe_add_column(cur, "payroll_entries", "pay_type", "TEXT")
    conn.commit()
    conn.close()


def ensure_payroll_table_structure():
    conn = get_db_connection()
    cur = conn.cursor()

    needed_columns = {
        "company_id": "INTEGER NOT NULL DEFAULT 0",
        "employee_id": "INTEGER NOT NULL DEFAULT 0",
        "pay_date": "TEXT",
        "pay_period_start": "TEXT",
        "pay_period_end": "TEXT",
        "pay_type": "TEXT",
        "hours_regular": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "hours_overtime": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "rate_regular": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "rate_overtime": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "gross_pay": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "federal_withholding": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "state_withholding": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "social_security": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "medicare": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "local_tax": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "other_deductions": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "net_pay": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "notes": "TEXT",
        "ledger_entry_id": "INTEGER",
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    }

    for col, col_type in needed_columns.items():
        safe_add_column(cur, "payroll_entries", col, col_type)

    conn.commit()
    conn.close()


def ensure_company_tax_settings_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_tax_settings (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL UNIQUE,
            federal_withholding_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
            state_withholding_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
            social_security_rate DOUBLE PRECISION NOT NULL DEFAULT 6.2,
            medicare_rate DOUBLE PRECISION NOT NULL DEFAULT 1.45,
            local_tax_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
            unemployment_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
            workers_comp_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


# -------------------------------------------------------------------
# Users / customers / misc ensures
# -------------------------------------------------------------------

def ensure_user_permission_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    needed = {
        "role": "TEXT DEFAULT 'Employee'",
        "is_active": "INTEGER DEFAULT 1",
        "can_manage_users": "INTEGER DEFAULT 0",
        "can_view_payroll": "INTEGER DEFAULT 0",
        "can_manage_payroll": "INTEGER DEFAULT 0",
        "can_view_bookkeeping": "INTEGER DEFAULT 0",
        "can_manage_bookkeeping": "INTEGER DEFAULT 0",
        "can_manage_jobs": "INTEGER DEFAULT 0",
        "can_manage_customers": "INTEGER DEFAULT 0",
        "can_manage_invoices": "INTEGER DEFAULT 0",
        "can_manage_settings": "INTEGER DEFAULT 0",
        "can_manage_employees": "INTEGER DEFAULT 0",
        "can_view_employees": "INTEGER DEFAULT 0",
        "can_manage_quotes": "INTEGER DEFAULT 0",
    }

    for col, col_type in needed.items():
        safe_add_column(cur, "users", col, col_type)

    conn.commit()
    conn.close()


def ensure_customer_name_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    safe_add_column(cur, "customers", "first_name", "TEXT")
    safe_add_column(cur, "customers", "last_name", "TEXT")

    conn.commit()

    rows = conn.execute("SELECT id, name FROM customers").fetchall()

    for r in rows:
        name = (r["name"] or "").strip()
        if not name:
            continue

        parts = name.split(" ")
        first = parts[0]
        last = parts[-1] if len(parts) > 1 else ""

        cur.execute(
            """
            UPDATE customers
            SET first_name = %s, last_name = %s
            WHERE id = %s
            """,
            (first, last, r["id"]),
        )

    conn.commit()
    conn.close()


def ensure_invoice_payments_table():
    conn = get_db_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS invoice_payments (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            invoice_id INTEGER NOT NULL,
            payment_date TEXT,
            amount DOUBLE PRECISION NOT NULL DEFAULT 0,
            payment_method TEXT,
            reference TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def ensure_bookkeeping_history_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bookkeeping_history (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            entry_date TEXT NOT NULL,
            category TEXT NOT NULL,
            entry_type TEXT NOT NULL,
            description TEXT NOT NULL,
            amount DOUBLE PRECISION NOT NULL DEFAULT 0,
            money_in DOUBLE PRECISION NOT NULL DEFAULT 0,
            money_out DOUBLE PRECISION NOT NULL DEFAULT 0,
            reference_type TEXT,
            reference_id INTEGER,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    needed = {
        "company_id": "INTEGER NOT NULL DEFAULT 0",
        "entry_date": "TEXT",
        "category": "TEXT",
        "entry_type": "TEXT",
        "description": "TEXT",
        "amount": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "money_in": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "money_out": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "reference_type": "TEXT",
        "reference_id": "INTEGER",
        "notes": "TEXT",
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    }

    for col, col_type in needed.items():
        safe_add_column(cur, "bookkeeping_history", col, col_type)

    conn.commit()
    conn.close()


def ensure_billing_tables():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            id SERIAL PRIMARY KEY,
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS billing_events (
            id SERIAL PRIMARY KEY,
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


def ensure_job_item_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    needed = {
        "item_type": "TEXT DEFAULT 'material'",
        "description": "TEXT",
        "quantity": "DOUBLE PRECISION DEFAULT 0",
        "unit": "TEXT",
        "unit_cost": "DOUBLE PRECISION DEFAULT 0",
        "unit_price": "DOUBLE PRECISION DEFAULT 0",
        "sale_price": "DOUBLE PRECISION DEFAULT 0",
        "cost_amount": "DOUBLE PRECISION DEFAULT 0",
        "line_total": "DOUBLE PRECISION DEFAULT 0",
        "billable": "INTEGER DEFAULT 1",
        "ledger_entry_id": "INTEGER",
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    }

    for col, col_type in needed.items():
        safe_add_column(cur, "job_items", col, col_type)

    conn.commit()
    conn.close()


def ensure_password_reset_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS password_resets (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            token TEXT NOT NULL,
            expires_at TIMESTAMP NOT NULL
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_password_resets_token
        ON password_resets (token)
    """)

    conn.commit()
    conn.close()


# -------------------------------------------------------------------
# Core schema init
# -------------------------------------------------------------------

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            address TEXT,
            default_quote_notes TEXT,
            default_invoice_notes TEXT,
            payment_terms TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT DEFAULT 'owner',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            company TEXT,
            email TEXT,
            phone TEXT,
            billing_address TEXT,
            service_address TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS quotes (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            quote_number TEXT,
            quote_date TEXT,
            expiration_date TEXT,
            status TEXT DEFAULT 'Draft',
            notes TEXT,
            subtotal DOUBLE PRECISION DEFAULT 0,
            tax DOUBLE PRECISION DEFAULT 0,
            total DOUBLE PRECISION DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS quote_items (
            id SERIAL PRIMARY KEY,
            quote_id INTEGER NOT NULL,
            description TEXT NOT NULL,
            quantity DOUBLE PRECISION DEFAULT 0,
            unit TEXT,
            unit_price DOUBLE PRECISION DEFAULT 0,
            line_total DOUBLE PRECISION DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            quote_id INTEGER,
            title TEXT NOT NULL,
            scheduled_date TEXT,
            status TEXT DEFAULT 'Scheduled',
            address TEXT,
            notes TEXT,
            revenue DOUBLE PRECISION DEFAULT 0,
            cost_total DOUBLE PRECISION DEFAULT 0,
            profit DOUBLE PRECISION DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_items (
            id SERIAL PRIMARY KEY,
            job_id INTEGER NOT NULL,
            item_type TEXT DEFAULT 'material',
            description TEXT NOT NULL,
            quantity DOUBLE PRECISION DEFAULT 0,
            unit TEXT,
            unit_cost DOUBLE PRECISION DEFAULT 0,
            unit_price DOUBLE PRECISION DEFAULT 0,
            sale_price DOUBLE PRECISION DEFAULT 0,
            cost_amount DOUBLE PRECISION DEFAULT 0,
            line_total DOUBLE PRECISION DEFAULT 0,
            billable INTEGER DEFAULT 1,
            ledger_entry_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            job_id INTEGER,
            quote_id INTEGER,
            invoice_number TEXT,
            invoice_date TEXT,
            due_date TEXT,
            status TEXT DEFAULT 'Unpaid',
            notes TEXT,
            subtotal DOUBLE PRECISION DEFAULT 0,
            tax DOUBLE PRECISION DEFAULT 0,
            total DOUBLE PRECISION DEFAULT 0,
            amount_paid DOUBLE PRECISION DEFAULT 0,
            balance_due DOUBLE PRECISION DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS invoice_items (
            id SERIAL PRIMARY KEY,
            invoice_id INTEGER NOT NULL,
            description TEXT NOT NULL,
            quantity DOUBLE PRECISION DEFAULT 0,
            unit TEXT,
            unit_price DOUBLE PRECISION DEFAULT 0,
            line_total DOUBLE PRECISION DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ledger_entries (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            entry_date TEXT NOT NULL,
            entry_type TEXT NOT NULL,
            category TEXT NOT NULL,
            description TEXT NOT NULL,
            amount DOUBLE PRECISION NOT NULL,
            source_type TEXT,
            source_id INTEGER,
            customer_id INTEGER,
            invoice_id INTEGER,
            job_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            employee_number TEXT,
            first_name TEXT,
            last_name TEXT,
            full_name TEXT,
            phone TEXT,
            email TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            hire_date TEXT,
            job_title TEXT,
            pay_type TEXT DEFAULT 'Hourly',
            pay_rate DOUBLE PRECISION DEFAULT 0,
            hourly_rate DOUBLE PRECISION DEFAULT 0,
            overtime_rate DOUBLE PRECISION DEFAULT 0,
            salary_amount DOUBLE PRECISION DEFAULT 0,
            federal_tax_rate DOUBLE PRECISION DEFAULT 0,
            state_tax_rate DOUBLE PRECISION DEFAULT 3.15,
            filing_status TEXT DEFAULT 'single',
            federal_filing_status TEXT DEFAULT 'Single',
            w4_filing_status TEXT DEFAULT 'Single',
            w4_step2_checked INTEGER NOT NULL DEFAULT 0,
            w4_step3_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
            w4_step4a_other_income DOUBLE PRECISION NOT NULL DEFAULT 0,
            w4_step4b_deductions DOUBLE PRECISION NOT NULL DEFAULT 0,
            w4_step4c_extra_withholding DOUBLE PRECISION NOT NULL DEFAULT 0,
            pay_schedule TEXT DEFAULT 'weekly',
            pay_frequency TEXT DEFAULT 'Biweekly',
            status TEXT DEFAULT 'Active',
            is_active INTEGER NOT NULL DEFAULT 1,
            emergency_contact_name TEXT,
            emergency_contact_phone TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_tax_settings (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL UNIQUE,
            federal_withholding_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
            state_withholding_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
            social_security_rate DOUBLE PRECISION NOT NULL DEFAULT 6.2,
            medicare_rate DOUBLE PRECISION NOT NULL DEFAULT 1.45,
            local_tax_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
            unemployment_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
            workers_comp_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS payroll_entries (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            employee_id INTEGER NOT NULL,
            pay_date TEXT,
            pay_period_start TEXT,
            pay_period_end TEXT,
            pay_type TEXT,
            hours_regular DOUBLE PRECISION NOT NULL DEFAULT 0,
            hours_overtime DOUBLE PRECISION NOT NULL DEFAULT 0,
            rate_regular DOUBLE PRECISION NOT NULL DEFAULT 0,
            rate_overtime DOUBLE PRECISION NOT NULL DEFAULT 0,
            gross_pay DOUBLE PRECISION NOT NULL DEFAULT 0,
            federal_withholding DOUBLE PRECISION NOT NULL DEFAULT 0,
            state_withholding DOUBLE PRECISION NOT NULL DEFAULT 0,
            social_security DOUBLE PRECISION NOT NULL DEFAULT 0,
            medicare DOUBLE PRECISION NOT NULL DEFAULT 0,
            local_tax DOUBLE PRECISION NOT NULL DEFAULT 0,
            other_deductions DOUBLE PRECISION NOT NULL DEFAULT 0,
            net_pay DOUBLE PRECISION NOT NULL DEFAULT 0,
            notes TEXT,
            ledger_entry_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            id SERIAL PRIMARY KEY,
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS billing_events (
            id SERIAL PRIMARY KEY,
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS invoice_payments (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            invoice_id INTEGER NOT NULL,
            payment_date TEXT,
            amount DOUBLE PRECISION NOT NULL DEFAULT 0,
            payment_method TEXT,
            reference TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bookkeeping_history (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            entry_date TEXT NOT NULL,
            category TEXT NOT NULL,
            entry_type TEXT NOT NULL,
            description TEXT NOT NULL,
            amount DOUBLE PRECISION NOT NULL DEFAULT 0,
            money_in DOUBLE PRECISION NOT NULL DEFAULT 0,
            money_out DOUBLE PRECISION NOT NULL DEFAULT 0,
            reference_type TEXT,
            reference_id INTEGER,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS password_resets (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            token TEXT NOT NULL,
            expires_at TIMESTAMP NOT NULL
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_password_resets_token
        ON password_resets (token)
    """)

    conn.commit()
    conn.close()

    ensure_company_profile_table()
    ensure_company_profile_columns()
    ensure_company_profile_email_columns()
    ensure_company_profile_location_columns()
    ensure_employee_status_column()
    ensure_employee_name_columns()
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()
    ensure_payroll_columns()
    ensure_payroll_table_structure()
    ensure_company_tax_settings_table()
    ensure_bookkeeping_history_table()
    ensure_invoice_payments_table()
    ensure_user_permission_columns()
    ensure_customer_name_columns()
    ensure_billing_tables()
    ensure_document_number_columns()
    ensure_job_item_columns()
    ensure_password_reset_table()
    ensure_employee_time_entries_table()
    ensure_company_time_clock_columns()


# -------------------------------------------------------------------
# Billing helpers
# -------------------------------------------------------------------

def get_company_subscription(company_id):
    conn = get_db_connection()
    row = conn.execute(
        "SELECT * FROM subscriptions WHERE company_id = %s",
        (company_id,),
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
        "SELECT id FROM subscriptions WHERE company_id = %s",
        (company_id,),
    ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE subscriptions
            SET stripe_customer_id = COALESCE(%s, stripe_customer_id),
                stripe_subscription_id = COALESCE(%s, stripe_subscription_id),
                stripe_price_id = COALESCE(%s, stripe_price_id),
                plan_name = COALESCE(%s, plan_name),
                billing_interval = COALESCE(%s, billing_interval),
                amount_cents = COALESCE(%s, amount_cents),
                status = COALESCE(%s, status),
                auto_renew = %s,
                cancel_at_period_end = %s,
                current_period_start = COALESCE(%s, current_period_start),
                current_period_end = COALESCE(%s, current_period_end),
                payment_method_type = COALESCE(%s, payment_method_type),
                payment_method_last4 = COALESCE(%s, payment_method_last4),
                payment_method_label = COALESCE(%s, payment_method_label),
                updated_at = CURRENT_TIMESTAMP
            WHERE company_id = %s
            """,
            (
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
            ),
        )
    else:
        conn.execute(
            """
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
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
            ),
        )

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
    conn.execute(
        """
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
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
        ),
    )
    conn.commit()
    conn.close()


def get_billing_history(company_id, limit=20):
    conn = get_db_connection()
    rows = conn.execute(
        """
        SELECT *
        FROM billing_events
        WHERE company_id = %s
        ORDER BY COALESCE(event_date, created_at::text) DESC, id DESC
        LIMIT %s
        """,
        (company_id, limit),
    ).fetchall()
    conn.close()
    return rows


# -------------------------------------------------------------------
# Job / numbering helpers
# -------------------------------------------------------------------

def ensure_job_schedule_columns():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_start_time TIME")
    cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_end_time TIME")
    cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS assigned_to TEXT")
    conn.commit()
    conn.close()


def _extract_numeric_suffix(value, fallback=1000):
    if value is None:
        return fallback
    match = re.search(r"(\d+)$", str(value).strip())
    if not match:
        return fallback
    try:
        return int(match.group(1))
    except Exception:
        return fallback


def get_next_invoice_number(company_id):
    ensure_document_number_columns()

    conn = get_db_connection()
    cur = conn.cursor()

    row = cur.execute(
        "SELECT next_invoice_number FROM companies WHERE id = %s",
        (company_id,),
    ).fetchone()

    next_number = 1001
    if row and row["next_invoice_number"] is not None:
        next_number = int(row["next_invoice_number"])
    else:
        latest = conn.execute(
            """
            SELECT invoice_number
            FROM invoices
            WHERE company_id = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (company_id,),
        ).fetchone()
        if latest and latest["invoice_number"]:
            next_number = _extract_numeric_suffix(latest["invoice_number"], 1000) + 1

    cur.execute(
        "UPDATE companies SET next_invoice_number = %s WHERE id = %s",
        (next_number + 1, company_id),
    )

    conn.commit()
    conn.close()
    return str(next_number)

def ensure_company_time_clock_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    safe_add_column(cur, "company_profile", "time_clock_pay_period_start_day", "INTEGER NOT NULL DEFAULT 2")

    conn.commit()
    conn.close()


def get_next_quote_number(company_id):
    ensure_document_number_columns()

    conn = get_db_connection()
    cur = conn.cursor()

    row = cur.execute(
        "SELECT next_quote_number FROM companies WHERE id = %s",
        (company_id,),
    ).fetchone()

    next_number = 1001
    if row and row["next_quote_number"] is not None:
        next_number = int(row["next_quote_number"])
    else:
        latest = conn.execute(
            """
            SELECT quote_number
            FROM quotes
            WHERE company_id = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (company_id,),
        ).fetchone()
        if latest and latest["quote_number"]:
            next_number = _extract_numeric_suffix(latest["quote_number"], 1000) + 1

    cur.execute(
        "UPDATE companies SET next_quote_number = %s WHERE id = %s",
        (next_number + 1, company_id),
    )

    conn.commit()
    conn.close()
    return str(next_number)


# -------------------------------------------------------------------
# User helpers
# -------------------------------------------------------------------

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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    cols = table_columns(conn, "employees")
    conn.close()
    return cols


# -------------------------------------------------------------------
# Bookkeeping / ledger helpers
# -------------------------------------------------------------------

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

    cur.execute(
        """
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
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
        ),
    )

    conn.commit()
    conn.close()


def add_bookkeeping_entry(*args, **kwargs):
    return add_bookkeeping_history_entry(*args, **kwargs)


def backfill_payroll_bookkeeping_history():
    ensure_bookkeeping_history_table()

    conn = get_db_connection()
    cur = conn.cursor()

    rows = conn.execute(
        """
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
        """
    ).fetchall()

    for row in rows:
        existing = conn.execute(
            """
            SELECT id
            FROM bookkeeping_history
            WHERE company_id = %s
              AND reference_type = 'payroll'
              AND reference_id = %s
            """,
            (row["company_id"], row["id"]),
        ).fetchone()

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

        cur.execute(
            """
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
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
            ),
        )

    conn.commit()
    conn.close()


def ensure_payroll_ledger_entry(conn, payroll_id):
    payroll = conn.execute(
        """
        SELECT
            p.*,
            e.first_name,
            e.last_name,
            e.full_name
        FROM payroll_entries p
        LEFT JOIN employees e ON p.employee_id = e.id
        WHERE p.id = %s
        """,
        (payroll_id,),
    ).fetchone()

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

    existing = conn.execute(
        """
        SELECT id
        FROM ledger_entries
        WHERE source_type = 'payroll' AND source_id = %s
        """,
        (payroll_id,),
    ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE ledger_entries
            SET company_id = %s,
                entry_date = %s,
                entry_type = 'Expense',
                category = 'Payroll',
                description = %s,
                amount = %s,
                source_type = 'payroll',
                source_id = %s
            WHERE id = %s
            """,
            (
                payroll["company_id"],
                pay_date,
                description,
                gross_pay,
                payroll_id,
                existing["id"],
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO ledger_entries (
                company_id, entry_date, entry_type, category, description, amount,
                source_type, source_id
            )
            VALUES (%s, %s, 'Expense', 'Payroll', %s, %s, 'payroll', %s)
            """,
            (
                payroll["company_id"],
                pay_date,
                description,
                gross_pay,
                payroll_id,
            ),
        )


def ensure_job_cost_ledger(conn, job_item_id):
    item = conn.execute(
        """
        SELECT
            ji.*,
            j.company_id,
            j.customer_id
        FROM job_items ji
        JOIN jobs j ON ji.job_id = j.id
        WHERE ji.id = %s
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
    unit_cost = float(item["unit_cost"] or 0) if "unit_cost" in item.keys() else 0
    cost_amount = float(item["cost_amount"] or 0)
    line_total = float(item["line_total"] or 0)
    unit_price = float(item["unit_price"] or 0)
    sale_price = float(item["sale_price"] or 0)

    amount = abs(cost_amount)

    if amount <= 0 and quantity > 0 and unit_cost > 0:
        amount = abs(quantity * unit_cost)

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
            WHERE id = %s
            """,
            (existing_ledger_id,),
        ).fetchone()

    if not existing_ledger:
        existing_ledger = conn.execute(
            """
            SELECT id
            FROM ledger_entries
            WHERE source_type = 'job_item' AND source_id = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (job_item_id,),
        ).fetchone()

    if item["item_type"] != item_type:
        conn.execute(
            """
            UPDATE job_items
            SET item_type = %s
            WHERE id = %s
            """,
            (item_type, job_item_id),
        )

    if existing_ledger:
        ledger_id = existing_ledger["id"]

        conn.execute(
            """
            UPDATE ledger_entries
            SET
                company_id = %s,
                entry_date = %s,
                entry_type = 'Expense',
                category = %s,
                description = %s,
                amount = %s,
                source_type = 'job_item',
                source_id = %s,
                customer_id = %s,
                invoice_id = NULL,
                job_id = %s
            WHERE id = %s
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
                SET ledger_entry_id = %s
                WHERE id = %s
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
            VALUES (%s, %s, 'Expense', %s, %s, %s, 'job_item', %s, %s, NULL, %s)
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
            SET ledger_entry_id = %s
            WHERE id = %s
            """,
            (new_ledger_id, job_item_id),
        )

def ensure_employee_time_entries_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS employee_time_entries (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            employee_id INTEGER NOT NULL,
            clock_in TIMESTAMP NOT NULL,
            clock_out TIMESTAMP,
            total_hours DOUBLE PRECISION NOT NULL DEFAULT 0,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()

def get_company_profile_row(company_id):
    ensure_company_profile_table()
    conn = get_db_connection()
    row = conn.execute(
        """
        SELECT *
        FROM company_profile
        WHERE company_id = %s
        """,
        (company_id,),
    ).fetchone()
    conn.close()
    return row


def get_company_users(company_id):
    conn = get_db_connection()
    rows = conn.execute(
        """
        SELECT id, name, email, role
        FROM users
        WHERE company_id = %s
          AND is_active = 1
        ORDER BY id
        """,
        (company_id,),
    ).fetchall()
    conn.close()
    return rows


def repair_all_job_item_ledgers(conn, company_id):
    rows = conn.execute(
        """
        SELECT ji.id
        FROM job_items ji
        JOIN jobs j ON ji.job_id = j.id
        WHERE j.company_id = %s
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
    inv = conn.execute(
        "SELECT * FROM invoices WHERE id = %s",
        (invoice_id,),
    ).fetchone()

    if not inv or payment_amount <= 0:
        return

    desc = f"Invoice #{inv['invoice_number'] or inv['id']} payment"
    conn.execute(
        """
        INSERT INTO ledger_entries (
            company_id, entry_date, entry_type, category, description, amount,
            source_type, source_id, customer_id, invoice_id, job_id
        )
        VALUES (%s, %s, 'Income', 'Invoice Payment', %s, %s, 'invoice_payment', %s, %s, %s, %s)
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

    try:
        invoice = conn.execute(
            "SELECT id, total FROM invoices WHERE id = %s",
            (invoice_id,),
        ).fetchone()

        if not invoice:
            return

        paid_row = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS paid_total FROM invoice_payments WHERE invoice_id = %s",
            (invoice_id,),
        ).fetchone()

        paid_total = float(paid_row["paid_total"] or 0)
        total = float(invoice["total"] or 0)
        balance_due = max(total - paid_total, 0)

        if total <= 0:
            status = "Draft"
        elif paid_total <= 0:
            status = "Unpaid"
        elif balance_due > 0:
            status = "Partial"
        else:
            status = "Paid"

        conn.execute(
            """
            UPDATE invoices
            SET amount_paid = %s, balance_due = %s, status = %s
            WHERE id = %s
            """,
            (paid_total, balance_due, status, invoice_id),
        )
        conn.commit()
    finally:
        conn.close()


def create_payroll_ledger_entry(conn, payroll_entry_id):
    row = conn.execute(
        """
        SELECT pe.*, e.first_name, e.last_name
        FROM payroll_entries pe
        JOIN employees e ON pe.employee_id = e.id
        WHERE pe.id = %s
        """,
        (payroll_entry_id,),
    ).fetchone()

    if not row:
        return

    employee_name = f"{row['first_name']} {row['last_name']}".strip()
    description = f"Payroll for {employee_name}"

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO ledger_entries (
            company_id, entry_date, entry_type, category, description, amount,
            source_type, source_id
        )
        VALUES (%s, %s, 'Expense', 'Payroll', %s, %s, 'payroll', %s)
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
        "UPDATE payroll_entries SET ledger_entry_id = %s WHERE id = %s",
        (ledger_id, payroll_entry_id),
    )