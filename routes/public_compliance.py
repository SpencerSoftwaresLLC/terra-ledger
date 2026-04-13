# routes/public_compliance.py

from flask import Blueprint, render_template, session

public_compliance_bp = Blueprint("public_compliance", __name__)


# =========================
# Language Helpers
# =========================

def _lang():
    return "es" if session.get("language") == "es" else "en"


def _is_es():
    return _lang() == "es"


def _t(en, es):
    return es if _is_es() else en


@public_compliance_bp.route("/sms-consent", methods=["GET"])
def sms_consent():
    return render_template(
        "public/sms_consent.html",
        _lang=_lang(),
        page_title=_t("SMS Consent", "Consentimiento SMS"),
    )


@public_compliance_bp.route("/privacy", methods=["GET"])
def privacy_policy():
    return render_template(
        "public/privacy.html",
        _lang=_lang(),
        page_title=_t("Privacy Policy", "Política de Privacidad"),
    )


@public_compliance_bp.route("/terms", methods=["GET"])
def terms_of_service():
    return render_template(
        "public/terms.html",
        _lang=_lang(),
        page_title=_t("Terms of Service", "Términos de Servicio"),
    )