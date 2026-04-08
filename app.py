import os
from datetime import timedelta

from dotenv import load_dotenv
from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix

load_dotenv()

from extensions import mail, csrf
from db import (
    init_db,
    ensure_company_profile_location_columns,
    ensure_company_profile_email_columns,
    ensure_password_reset_table,
)
from routes.auth import auth_bp
from routes.dashboard import dashboard_bp
from routes.customers import customers_bp
from routes.jobs import jobs_bp
from routes.quotes import quotes_bp
from routes.invoices import invoices_bp
from routes.payroll import payroll_bp, ensure_payroll_check_structure
from routes.employees import (
    employees_bp,
    ensure_employee_profile_columns,
    ensure_employee_local_tax_columns,
)
from routes.users import users_bp
from routes.settings import settings_bp
from routes.billing import billing_bp
from routes.calendar import calendar_bp
from routes.messages import messages_bp
from routes.notifications import notifications_bp
from routes.payment_setup import payment_setup_bp
from routes.bookkeeping import bookkeeping_bp, _ensure_bookkeeping_check_structure
from routes.help_assistant import help_assistant_bp
from routes.mobile import mobile_bp
from admin.admin_routes import admin_bp
from routes.public_compliance import public_compliance_bp
from routes.legal import legal_bp
from reports.material_usage import material_usage_bp


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def run_startup_tasks():
    print("STARTING DB INIT", flush=True)
    init_db()
    print("init_db SUCCESS", flush=True)

    ensure_company_profile_location_columns()
    print("ensure_company_profile_location_columns SUCCESS", flush=True)

    ensure_company_profile_email_columns()
    print("ensure_company_profile_email_columns SUCCESS", flush=True)

    ensure_password_reset_table()
    print("ensure_password_reset_table SUCCESS", flush=True)

    ensure_employee_profile_columns()
    print("ensure_employee_profile_columns SUCCESS", flush=True)

    ensure_employee_local_tax_columns()
    print("ensure_employee_local_tax_columns SUCCESS", flush=True)

    ensure_payroll_check_structure()
    print("ensure_payroll_check_structure SUCCESS", flush=True)

    _ensure_bookkeeping_check_structure()
    print("_ensure_bookkeeping_check_structure SUCCESS", flush=True)


def create_app():
    app = Flask(__name__)

    # Trust Render / reverse proxy headers so Flask knows the request is HTTPS.
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

    is_production = _env_bool("FLASK_ENV_PRODUCTION", False) or (
        os.environ.get("RENDER") is not None
    )

    secret_key = os.environ.get("SECRET_KEY", "").strip()
    if not secret_key or secret_key == "change-this-in-production":
        if is_production:
            raise RuntimeError(
                "SECRET_KEY is missing or insecure. Set a strong SECRET_KEY in production."
            )
        secret_key = "dev-only-local-secret-key-change-me"

    app.secret_key = secret_key

    app.config.update(
        # Core security / session settings
        SECRET_KEY=secret_key,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=_env_bool("SESSION_COOKIE_SECURE", is_production),
        PERMANENT_SESSION_LIFETIME=timedelta(
            minutes=_env_int("SESSION_LIFETIME_MINUTES", 60)
        ),

        # Helps prevent oversized file uploads / abuse
        MAX_CONTENT_LENGTH=_env_int("MAX_CONTENT_LENGTH_MB", 16) * 1024 * 1024,

        # CSRF setup
        WTF_CSRF_ENABLED=True,
        WTF_CSRF_TIME_LIMIT=_env_int("WTF_CSRF_TIME_LIMIT", 3600),

        # Mail config
        MAIL_SERVER=os.environ.get("MAIL_SERVER", "smtp.gmail.com"),
        MAIL_PORT=_env_int("MAIL_PORT", 587),
        MAIL_USE_TLS=_env_bool("MAIL_USE_TLS", True),
        MAIL_USE_SSL=_env_bool("MAIL_USE_SSL", False),
        MAIL_USERNAME=os.environ.get("MAIL_USERNAME", "").strip(),
        MAIL_PASSWORD=os.environ.get("MAIL_PASSWORD", "").strip(),
        MAIL_DEFAULT_SENDER=os.environ.get("MAIL_DEFAULT_SENDER", "").strip(),

        # Flask hardening
        JSON_SORT_KEYS=False,
        TEMPLATES_AUTO_RELOAD=not is_production,
    )

    if is_production:
        if not app.config["MAIL_USERNAME"]:
            print("WARNING: MAIL_USERNAME is not set in production.", flush=True)
        if not app.config["MAIL_PASSWORD"]:
            print("WARNING: MAIL_PASSWORD is not set in production.", flush=True)
        if not app.config["MAIL_DEFAULT_SENDER"]:
            print("WARNING: MAIL_DEFAULT_SENDER is not set in production.", flush=True)

    mail.init_app(app)
    csrf.init_app(app)

    # Exempt JSON API routes from CSRF only if needed.
    # Keep these minimal. Regular form POST routes should stay protected.
    try:
        csrf.exempt(help_assistant_bp)
    except Exception:
        pass

    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(customers_bp)
    app.register_blueprint(jobs_bp)
    app.register_blueprint(quotes_bp)
    app.register_blueprint(invoices_bp)
    app.register_blueprint(payroll_bp)
    app.register_blueprint(employees_bp)
    app.register_blueprint(users_bp)
    app.register_blueprint(payment_setup_bp)
    app.register_blueprint(settings_bp)
    app.register_blueprint(billing_bp)
    app.register_blueprint(bookkeeping_bp)
    app.register_blueprint(help_assistant_bp)
    app.register_blueprint(public_compliance_bp)
    app.register_blueprint(mobile_bp)
    app.register_blueprint(calendar_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(messages_bp)
    app.register_blueprint(notifications_bp)
    app.register_blueprint(legal_bp)
    app.register_blueprint(material_usage_bp)

    @app.before_request
    def make_session_permanent():
        from flask import session
        session.permanent = True

    @app.after_request
    def add_security_headers(response):
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"

        if is_production and app.config["SESSION_COOKIE_SECURE"]:
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

        # Start with a safer CSP that still allows inline styles/scripts if your templates rely on them.
        # Later you can tighten this further.
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "img-src 'self' data: https:; "
            "style-src 'self' 'unsafe-inline' https:; "
            "script-src 'self' 'unsafe-inline' https:; "
            "font-src 'self' data: https:; "
            "connect-src 'self' https:; "
            "frame-ancestors 'self'; "
            "base-uri 'self'; "
            "form-action 'self';"
        )
        return response

    @app.get("/healthz")
    def healthz():
        return "ok", 200

    print("APP CREATE SUCCESS", flush=True)
    return app


app = create_app()

if _env_bool("RUN_STARTUP_TASKS", False):
    run_startup_tasks()

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=_env_bool("FLASK_DEBUG", False),
    )