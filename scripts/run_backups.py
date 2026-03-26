from db import get_db_connection
from utils.backups import create_company_backup


def run_all_backups():
    conn = get_db_connection()

    try:
        companies = conn.execute("SELECT id FROM companies").fetchall()
    finally:
        conn.close()

    for company in companies:
        create_company_backup(company["id"])


if __name__ == "__main__":
    run_all_backups()