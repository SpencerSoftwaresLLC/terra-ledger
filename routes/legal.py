from flask import Blueprint
from page_helpers import render_page

legal_bp = Blueprint("legal", __name__)


@legal_bp.route("/terms")
def terms():
    content = """
    <h1>Terms of Service</h1>
    <p>Last updated: 2026</p>

    <p>TerraLedger is a software platform that allows businesses to manage customers, quotes, jobs, invoices, and payments.</p>

    <h3>Use of Service</h3>
    <p>By using TerraLedger, you agree to use the platform for lawful business purposes only.</p>

    <h3>Payments</h3>
    <p>TerraLedger does not process or hold funds. Payments made through invoices are processed by Stripe and sent directly to the business receiving payment.</p>

    <h3>Responsibility</h3>
    <p>Each business using TerraLedger is responsible for the services they provide, including pricing, delivery, and customer satisfaction.</p>

    <h3>Limitation of Liability</h3>
    <p>TerraLedger is not liable for disputes between businesses and their customers.</p>

    <h3>Contact</h3>
    <p>Email: support@terraledger.net</p>
    """
    return render_page(content, "Terms of Service")


@legal_bp.route("/privacy")
def privacy():
    content = """
    <h1>Privacy Policy</h1>
    <p>Last updated: 2026</p>

    <p>We collect basic information such as names, emails, and business data necessary to operate TerraLedger.</p>

    <h3>How We Use Data</h3>
    <ul>
        <li>To provide and improve our services</li>
        <li>To communicate with users</li>
        <li>To process invoices and business operations</li>
    </ul>

    <h3>Data Security</h3>
    <p>We take reasonable measures to protect your data.</p>

    <h3>Third Parties</h3>
    <p>Payments are processed through Stripe. We do not store full payment details.</p>

    <h3>Contact</h3>
    <p>Email: support@terraledger.net</p>
    """
    return render_page(content, "Privacy Policy")


@legal_bp.route("/refunds")
def refunds():
    content = """
    <h1>Refund & Payment Policy</h1>
    <p>Last updated: 2026</p>

    <h3>Software Subscription</h3>
    <p>TerraLedger subscriptions are non-refundable unless required by law.</p>

    <h3>Invoice Payments</h3>
    <p>Payments made through TerraLedger invoices are sent directly to the business issuing the invoice.</p>

    <p>Refunds for those payments must be handled directly between the customer and the business.</p>

    <h3>Disputes</h3>
    <p>Any disputes should be directed to the business that issued the invoice.</p>

    <h3>Contact</h3>
    <p>Email: support@terraledger.net</p>
    """
    return render_page(content, "Refund Policy")


@legal_bp.route("/contact")
def contact():
    content = """
    <h1>Contact & Support</h1>

    <p>If you need help, please reach out:</p>

    <ul>
        <li>Email: support@terraledger.net</li>
    </ul>

    <p>We typically respond within 24–48 hours.</p>
    """
    return render_page(content, "Contact")