import os
import json
from datetime import datetime

from db import get_db_connection


BACKUP_TABLES = [
    "customers",
    "jobs",
    "job_items",
    "quotes",
    "quote_items",
    "invoices",
    "invoice_items",
    "invoice_payments",
    "ledger_entries",
    "employees",
    "payroll_entries",
    "company_profile",
    "company_tax_settings",
]

RESTORE_DELETE_ORDER = [
    "invoice_items",
    "quote_items",
    "job_items",
    "invoice_payments",
    "payroll_entries",
    "ledger_entries",
    "invoices",
    "quotes",
    "jobs",
    "employees",
    "customers",
    "company_profile",
    "company_tax_settings",
]

RESTORE_INSERT_ORDER = [
    "customers",
    "employees",
    "quotes",
    "quote_items",
    "jobs",
    "job_items",
    "invoices",
    "invoice_items",
    "invoice_payments",
    "ledger_entries",
    "payroll_entries",
    "company_profile",
    "company_tax_settings",
]


def _backup_dir():
    path = os.path.join(os.getcwd(), "backups")
    os.makedirs(path, exist_ok=True)
    return path


def _json_default(value):
    return str(value)


def create_company_backup(company_id):
    conn = get_db_connection()

    data = {
        "company_id": company_id,
        "created_at": datetime.utcnow().isoformat(),
        "tables": {}
    }

    try:
        for table in BACKUP_TABLES:
            rows = conn.execute(
                f"SELECT * FROM {table} WHERE company_id = %s",
                (company_id,),
            ).fetchall()
            data["tables"][table] = [dict(row) for row in rows]
    finally:
        conn.close()

    filename = f"backup_company_{company_id}_{datetime.utcnow().strftime('%Y_%m_%d_%H%M%S')}.json"
    filepath = os.path.join(_backup_dir(), filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=_json_default)

    return filepath


def export_company_backup_data(company_id):
    conn = get_db_connection()

    data = {
        "company_id": company_id,
        "exported_at": datetime.utcnow().isoformat(),
        "tables": {}
    }

    try:
        for table in BACKUP_TABLES:
            rows = conn.execute(
                f"SELECT * FROM {table} WHERE company_id = %s",
                (company_id,),
            ).fetchall()
            data["tables"][table] = [dict(row) for row in rows]
    finally:
        conn.close()

    return data


def load_backup_file(file_storage):
    raw = file_storage.read()
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


def _delete_company_data(conn, company_id):
    for table in RESTORE_DELETE_ORDER:
        conn.execute(
            f"DELETE FROM {table} WHERE company_id = %s",
            (company_id,),
        )


def _insert_row(conn, table, row, company_id):
    row = dict(row)

    # Force restore into currently logged-in company
    if "company_id" in row:
        row["company_id"] = company_id

    columns = list(row.keys())
    values = [row[col] for col in columns]

    col_sql = ", ".join(columns)
    placeholder_sql = ", ".join(["%s"] * len(columns))

    conn.execute(
        f"INSERT INTO {table} ({col_sql}) VALUES ({placeholder_sql})",
        tuple(values),
    )


def _reset_sequence_for_table(conn, table):
    id_row = conn.execute(
        f"SELECT COALESCE(MAX(id), 0) AS max_id FROM {table}"
    ).fetchone()

    max_id = int(id_row["max_id"] or 0)

    conn.execute(
        """
        SELECT setval(
            pg_get_serial_sequence(%s, 'id'),
            %s,
            %s
        )
        """,
        (table, max_id if max_id > 0 else 1, max_id > 0),
    )


def _reset_sequences(conn):
    for table in BACKUP_TABLES:
        _reset_sequence_for_table(conn, table)


def restore_company_backup(company_id, backup_data):
    if not isinstance(backup_data, dict):
        raise ValueError("Backup file is invalid.")

    tables = backup_data.get("tables")
    if not isinstance(tables, dict):
        raise ValueError("Backup file is missing table data.")

    conn = get_db_connection()

    try:
        # Safety backup before restore
        pre_restore_path = create_company_backup(company_id)

        _delete_company_data(conn, company_id)

        for table in RESTORE_INSERT_ORDER:
            rows = tables.get(table, [])
            if not isinstance(rows, list):
                continue

            for row in rows:
                _insert_row(conn, table, row, company_id)

        _reset_sequences(conn)
        conn.commit()

        return {
            "ok": True,
            "pre_restore_backup_path": pre_restore_path,
        }

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()