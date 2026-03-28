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
    <p>By using TerraLedger, you agree to use the platform for lawful business purposes only. You are responsible for all activity conducted under your account.</p>

    <h3>Accounts</h3>
    <p>You are responsible for maintaining the security of your account and login credentials. TerraLedger is not liable for unauthorized access caused by user negligence.</p>

    <h3>Payments</h3>
    <p>TerraLedger does not process, hold, or control customer funds.</p>
    <p>All payments are processed through Stripe. Funds are sent directly to the connected business account.</p>

    <h3>Business Responsibility</h3>
    <p>Each business using TerraLedger is solely responsible for the services they provide, including pricing, delivery, taxes, and customer satisfaction.</p>

    <h3>Data Ownership</h3>
    <p>You retain ownership of your business data. TerraLedger stores and processes this data only to provide the service.</p>

    <h3>Service Availability</h3>
    <p>We strive to keep TerraLedger available at all times, but we do not guarantee uninterrupted service.</p>

    <h3>Termination</h3>
    <p>We reserve the right to suspend or terminate accounts that violate these terms or misuse the platform.</p>

    <h3>Limitation of Liability</h3>
    <p>TerraLedger is not liable for disputes between businesses and their customers, or for any financial losses resulting from the use of the platform.</p>

    <h3>Governing Law</h3>
    <p>These terms are governed by the laws of the State of Indiana, United States.</p>

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

    <h3>Information We Collect</h3>
    <ul>
        <li>Account details (name, email)</li>
        <li>Business data (customers, jobs, invoices)</li>
        <li>Usage data to improve performance</li>
    </ul>

    <h3>How We Use Data</h3>
    <ul>
        <li>To provide and improve our services</li>
        <li>To communicate with users</li>
        <li>To operate business features (invoicing, payroll, etc.)</li>
    </ul>

    <h3>Data Security</h3>
    <p>We take reasonable technical and organizational measures to protect your data.</p>

    <h3>Third Parties</h3>
    <p>Payments are processed through Stripe. TerraLedger does not store full payment details such as credit card numbers.</p>

    <h3>Data Sharing</h3>
    <p>We do not sell your data. We only share data when required to provide services or comply with legal obligations.</p>

    <h3>Your Rights</h3>
    <p>You may request access to or deletion of your data by contacting us.</p>

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

    <p>TerraLedger does not control or manage these funds.</p>

    <h3>Refund Responsibility</h3>
    <p>Refunds must be handled directly between the customer and the business that issued the invoice.</p>

    <h3>Disputes</h3>
    <p>Any disputes regarding services or payments should be directed to the business that issued the invoice.</p>

    <h3>Stripe Disputes</h3>
    <p>Chargebacks and payment disputes are handled through Stripe and the connected business account.</p>

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