# TerraLedger/routes/help_assistant.py

from flask import Blueprint, request, jsonify, session
from ..decorators import login_required
from ..ai.client import ask_terraledger_help

help_assistant_bp = Blueprint("help_assistant", __name__)


def _get_chat_history():
    history = session.get("help_assistant_history")
    if isinstance(history, list):
        return history
    return []


def _save_chat_history(history):
    session["help_assistant_history"] = history[-10:]


@help_assistant_bp.route("/api/help-assistant", methods=["POST"])
@login_required
def help_assistant_api():
    try:
        data = request.get_json(silent=True) or {}

        user_question = (data.get("message") or "").strip()
        page_title = (data.get("page_title") or "").strip()
        route = (data.get("route") or "").strip()

        if not user_question:
            return jsonify({
                "ok": False,
                "error": "Please enter a question."
            }), 400

        history = _get_chat_history()

        answer = ask_terraledger_help(
            user_question=user_question,
            page_name=page_title,
            route=route,
            user_role=session.get("role", "") or session.get("user_role", ""),
            company_name=session.get("company_name", ""),
            user_name=session.get("user_name", ""),
            prior_messages=history,
        )

        history.append({"role": "user", "content": user_question})
        history.append({"role": "assistant", "content": answer})
        _save_chat_history(history)

        return jsonify({
            "ok": True,
            "answer": answer
        })

    except Exception as e:
        print("HELP ASSISTANT ERROR:", repr(e))
        return jsonify({
            "ok": False,
            "error": f"{repr(e)}"
        }), 500


@help_assistant_bp.route("/api/help-assistant/clear", methods=["POST"])
@login_required
def help_assistant_clear():
    session["help_assistant_history"] = []
    return jsonify({"ok": True})