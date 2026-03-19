import os
from dotenv import load_dotenv
from flask import Flask

# Load environment variables first
load_dotenv()

from extensions import mail
from db import (
    init_db,
    ensure_company_profile_location_columns,
    ensure_company_profile_email_columns,
)

from routes.auth import auth_bp
from routes.dashboard import dashboard_bp
from routes.customers import customers_bp
from routes.jobs import jobs_bp
from routes.quotes import quotes_bp
from routes.invoices import invoices_bp
from routes.ledger import ledger_bp
from routes.payroll import payroll_bp
from routes.employees import employees_bp
from routes.users import users_bp
from routes.settings import settings_bp
from routes.billing import billing_bp
from routes.bookkeeping import bookkeeping_bp
from routes.help_assistant import help_assistant_bp

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "your-secret-key")

# Mail config
app.config["MAIL_SERVER"] = os.environ.get("MAIL_SERVER", "smtp.gmail.com")
app.config["MAIL_PORT"] = int(os.environ.get("MAIL_PORT", 587))
app.config["MAIL_USE_TLS"] = str(os.environ.get("MAIL_USE_TLS", "true")).lower() == "true"
app.config["MAIL_USE_SSL"] = str(os.environ.get("MAIL_USE_SSL", "false")).lower() == "true"
app.config["MAIL_USERNAME"] = os.environ.get("MAIL_USERNAME", "yourplatformsender@gmail.com")
app.config["MAIL_PASSWORD"] = os.environ.get("MAIL_PASSWORD", "your_app_password_here")
app.config["MAIL_DEFAULT_SENDER"] = os.environ.get("MAIL_DEFAULT_SENDER", "yourplatformsender@gmail.com")

mail.init_app(app)

# Database setup
init_db()
ensure_company_profile_location_columns()
ensure_company_profile_email_columns()

# Blueprint registration
app.register_blueprint(auth_bp)
app.register_blueprint(dashboard_bp)
app.register_blueprint(customers_bp)
app.register_blueprint(jobs_bp)
app.register_blueprint(quotes_bp)
app.register_blueprint(invoices_bp)
app.register_blueprint(ledger_bp)
app.register_blueprint(payroll_bp)
app.register_blueprint(employees_bp)
app.register_blueprint(users_bp)
app.register_blueprint(settings_bp)
app.register_blueprint(billing_bp)
app.register_blueprint(bookkeeping_bp)
app.register_blueprint(help_assistant_bp)

if __name__ == "__main__":
    app.run(debug=True)