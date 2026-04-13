from flask import Blueprint, session
from page_helpers import render_page

legal_bp = Blueprint("legal", __name__)


# =========================
# Language Helpers
# =========================

def _lang():
    return "es" if session.get("language") == "es" else "en"


def _is_es():
    return _lang() == "es"


def _t(en, es):
    return es if _is_es() else en


# =========================
# Routes
# =========================

@legal_bp.route("/terms")
def terms():
    title = _t("Terms of Service", "Términos de Servicio")

    content = f"""
    <h1>{_t("Terms of Service", "Términos de Servicio")}</h1>
    <p>{_t("Last updated: 2026", "Última actualización: 2026")}</p>

    <p>{_t(
        "TerraLedger is a software platform that allows businesses to manage customers, quotes, jobs, invoices, and payments.",
        "TerraLedger es una plataforma de software que permite a las empresas gestionar clientes, cotizaciones, trabajos, facturas y pagos."
    )}</p>

    <h3>{_t("Use of Service", "Uso del Servicio")}</h3>
    <p>{_t(
        "By using TerraLedger, you agree to use the platform for lawful business purposes only. You are responsible for all activity conducted under your account.",
        "Al utilizar TerraLedger, aceptas usar la plataforma únicamente para fines comerciales legales. Eres responsable de toda la actividad realizada bajo tu cuenta."
    )}</p>

    <h3>{_t("Accounts", "Cuentas")}</h3>
    <p>{_t(
        "You are responsible for maintaining the security of your account and login credentials. TerraLedger is not liable for unauthorized access caused by user negligence.",
        "Eres responsable de mantener la seguridad de tu cuenta y credenciales. TerraLedger no es responsable por accesos no autorizados causados por negligencia del usuario."
    )}</p>

    <h3>{_t("Payments", "Pagos")}</h3>
    <p>{_t(
        "TerraLedger does not process, hold, or control customer funds.",
        "TerraLedger no procesa, retiene ni controla fondos de clientes."
    )}</p>
    <p>{_t(
        "All payments are processed through Stripe. Funds are sent directly to the connected business account.",
        "Todos los pagos se procesan a través de Stripe. Los fondos se envían directamente a la cuenta comercial conectada."
    )}</p>

    <h3>{_t("Business Responsibility", "Responsabilidad del Negocio")}</h3>
    <p>{_t(
        "Each business using TerraLedger is solely responsible for the services they provide, including pricing, delivery, taxes, and customer satisfaction.",
        "Cada negocio que utiliza TerraLedger es totalmente responsable de los servicios que ofrece, incluyendo precios, entrega, impuestos y satisfacción del cliente."
    )}</p>

    <h3>{_t("Data Ownership", "Propiedad de los Datos")}</h3>
    <p>{_t(
        "You retain ownership of your business data. TerraLedger stores and processes this data only to provide the service.",
        "Mantienes la propiedad de tus datos comerciales. TerraLedger almacena y procesa estos datos únicamente para proporcionar el servicio."
    )}</p>

    <h3>{_t("Service Availability", "Disponibilidad del Servicio")}</h3>
    <p>{_t(
        "We strive to keep TerraLedger available at all times, but we do not guarantee uninterrupted service.",
        "Nos esforzamos por mantener TerraLedger disponible en todo momento, pero no garantizamos un servicio ininterrumpido."
    )}</p>

    <h3>{_t("Termination", "Terminación")}</h3>
    <p>{_t(
        "We reserve the right to suspend or terminate accounts that violate these terms or misuse the platform.",
        "Nos reservamos el derecho de suspender o cancelar cuentas que violen estos términos o hagan mal uso de la plataforma."
    )}</p>

    <h3>{_t("Limitation of Liability", "Limitación de Responsabilidad")}</h3>
    <p>{_t(
        "TerraLedger is not liable for disputes between businesses and their customers, or for any financial losses resulting from the use of the platform.",
        "TerraLedger no es responsable de disputas entre negocios y sus clientes, ni de pérdidas financieras derivadas del uso de la plataforma."
    )}</p>

    <h3>{_t("Governing Law", "Ley Aplicable")}</h3>
    <p>{_t(
        "These terms are governed by the laws of the State of Indiana, United States.",
        "Estos términos se rigen por las leyes del estado de Indiana, Estados Unidos."
    )}</p>

    <h3>{_t("Contact", "Contacto")}</h3>
    <p>Email: support@terraledger.net</p>
    """
    return render_page(content, title)


@legal_bp.route("/privacy")
def privacy():
    title = _t("Privacy Policy", "Política de Privacidad")

    content = f"""
    <h1>{_t("Privacy Policy", "Política de Privacidad")}</h1>
    <p>{_t("Last updated: 2026", "Última actualización: 2026")}</p>

    <p>{_t(
        "We collect basic information such as names, emails, and business data necessary to operate TerraLedger.",
        "Recopilamos información básica como nombres, correos electrónicos y datos comerciales necesarios para operar TerraLedger."
    )}</p>

    <h3>{_t("Information We Collect", "Información que Recopilamos")}</h3>
    <ul>
        <li>{_t("Account details (name, email)", "Detalles de la cuenta (nombre, correo electrónico)")}</li>
        <li>{_t("Business data (customers, jobs, invoices)", "Datos del negocio (clientes, trabajos, facturas)")}</li>
        <li>{_t("Usage data to improve performance", "Datos de uso para mejorar el rendimiento")}</li>
    </ul>

    <h3>{_t("How We Use Data", "Cómo Usamos los Datos")}</h3>
    <ul>
        <li>{_t("To provide and improve our services", "Para proporcionar y mejorar nuestros servicios")}</li>
        <li>{_t("To communicate with users", "Para comunicarnos con los usuarios")}</li>
        <li>{_t("To operate business features (invoicing, payroll, etc.)", "Para operar funciones del negocio (facturación, nómina, etc.)")}</li>
    </ul>

    <h3>{_t("Data Security", "Seguridad de los Datos")}</h3>
    <p>{_t(
        "We take reasonable technical and organizational measures to protect your data.",
        "Tomamos medidas técnicas y organizativas razonables para proteger tus datos."
    )}</p>

    <h3>{_t("Third Parties", "Terceros")}</h3>
    <p>{_t(
        "Payments are processed through Stripe. TerraLedger does not store full payment details such as credit card numbers.",
        "Los pagos se procesan a través de Stripe. TerraLedger no almacena detalles completos de pago como números de tarjeta."
    )}</p>

    <h3>{_t("Data Sharing", "Compartición de Datos")}</h3>
    <p>{_t(
        "We do not sell your data. We only share data when required to provide services or comply with legal obligations.",
        "No vendemos tus datos. Solo compartimos datos cuando es necesario para prestar servicios o cumplir obligaciones legales."
    )}</p>

    <h3>{_t("Your Rights", "Tus Derechos")}</h3>
    <p>{_t(
        "You may request access to or deletion of your data by contacting us.",
        "Puedes solicitar acceso o eliminación de tus datos contactándonos."
    )}</p>

    <h3>{_t("Contact", "Contacto")}</h3>
    <p>Email: support@terraledger.net</p>
    """
    return render_page(content, title)


@legal_bp.route("/refunds")
def refunds():
    title = _t("Refund Policy", "Política de Reembolsos")

    content = f"""
    <h1>{_t("Refund & Payment Policy", "Política de Reembolsos y Pagos")}</h1>
    <p>{_t("Last updated: 2026", "Última actualización: 2026")}</p>

    <h3>{_t("Software Subscription", "Suscripción de Software")}</h3>
    <p>{_t(
        "TerraLedger subscriptions are non-refundable unless required by law.",
        "Las suscripciones de TerraLedger no son reembolsables excepto cuando lo exija la ley."
    )}</p>

    <h3>{_t("Invoice Payments", "Pagos de Facturas")}</h3>
    <p>{_t(
        "Payments made through TerraLedger invoices are sent directly to the business issuing the invoice.",
        "Los pagos realizados a través de facturas de TerraLedger se envían directamente al negocio que emite la factura."
    )}</p>

    <p>{_t(
        "TerraLedger does not control or manage these funds.",
        "TerraLedger no controla ni gestiona estos fondos."
    )}</p>

    <h3>{_t("Refund Responsibility", "Responsabilidad de Reembolsos")}</h3>
    <p>{_t(
        "Refunds must be handled directly between the customer and the business that issued the invoice.",
        "Los reembolsos deben gestionarse directamente entre el cliente y el negocio que emitió la factura."
    )}</p>

    <h3>{_t("Disputes", "Disputas")}</h3>
    <p>{_t(
        "Any disputes regarding services or payments should be directed to the business that issued the invoice.",
        "Cualquier disputa relacionada con servicios o pagos debe dirigirse al negocio que emitió la factura."
    )}</p>

    <h3>{_t("Stripe Disputes", "Disputas con Stripe")}</h3>
    <p>{_t(
        "Chargebacks and payment disputes are handled through Stripe and the connected business account.",
        "Las devoluciones de cargo y disputas de pago se manejan a través de Stripe y la cuenta comercial conectada."
    )}</p>

    <h3>{_t("Contact", "Contacto")}</h3>
    <p>Email: support@terraledger.net</p>
    """
    return render_page(content, title)


@legal_bp.route("/contact")
def contact():
    title = _t("Contact", "Contacto")

    content = f"""
    <h1>{_t("Contact & Support", "Contacto y Soporte")}</h1>

    <p>{_t("If you need help, please reach out:", "Si necesitas ayuda, contáctanos:")}</p>

    <ul>
        <li>Email: support@terraledger.net</li>
    </ul>

    <p>{_t(
        "We typically respond within 24–48 hours.",
        "Normalmente respondemos dentro de 24–48 horas."
    )}</p>
    """
    return render_page(content, title)