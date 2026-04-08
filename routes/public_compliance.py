# routes/public_compliance.py

from flask import Blueprint, render_template

public_compliance_bp = Blueprint("public_compliance", __name__)


@public_compliance_bp.route("/sms-consent", methods=["GET"])
def sms_consent():
    return render_template("public/sms_consent.html")


@public_compliance_bp.route("/privacy", methods=["GET"])
def privacy_policy():
    return render_template("public/privacy.html")


@public_compliance_bp.route("/terms", methods=["GET"])
def terms_of_service():
    return render_template("public/terms.html")