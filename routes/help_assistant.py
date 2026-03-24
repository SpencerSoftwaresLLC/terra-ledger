# TerraLedger/routes/help_assistant.py

from flask import Blueprint, request, jsonify, session, render_template_string
from decorators import login_required
from ai.client import ask_terraledger_help
from ai.knowledge import calculate_material
from page_helpers import render_page

help_assistant_bp = Blueprint("help_assistant", __name__)


def _get_chat_history():
    history = session.get("help_assistant_history")
    if isinstance(history, list):
        return history
    return []


def _save_chat_history(history):
    session["help_assistant_history"] = history[-10:]


@help_assistant_bp.route("/help-assistant", methods=["GET"])
@login_required
def help_assistant_page():
    history = _get_chat_history()

    history_html = ""
    for item in history:
        role = item.get("role", "")
        content = item.get("content", "")

        if role == "user":
            history_html += f"""
                <div style="display:flex; justify-content:flex-end; margin-bottom:12px;">
                    <div style="
                        max-width:75%;
                        background:#2563eb;
                        color:white;
                        padding:12px 14px;
                        border-radius:14px 14px 4px 14px;
                        white-space:pre-wrap;
                    ">{content}</div>
                </div>
            """
        else:
            history_html += f"""
                <div style="display:flex; justify-content:flex-start; margin-bottom:12px;">
                    <div style="
                        max-width:75%;
                        background:#1f2937;
                        color:#f9fafb;
                        padding:12px 14px;
                        border-radius:14px 14px 14px 4px;
                        white-space:pre-wrap;
                        border:1px solid #374151;
                    ">{content}</div>
                </div>
            """

    html = f"""
    <div class="card" style="max-width:1000px; margin:24px auto;">
        <div style="display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:16px; flex-wrap:wrap;">
            <div>
                <h2 style="margin:0;">TerraLedger Help Assistant</h2>
                <div style="color:var(--text-soft); margin-top:6px;">
                    Ask questions about TerraLedger, materials, invoices, jobs, payroll, and more.
                </div>
            </div>

            <div style="display:flex; gap:10px; flex-wrap:wrap;">
                <a class="btn secondary" href="{{{{ url_for('dashboard.dashboard') }}}}">Back to Dashboard</a>
                <button id="clear-chat-btn" class="btn secondary" type="button">Clear Chat</button>
            </div>
        </div>

        <div id="assistant-chat-box" style="
            background:#111827;
            border:1px solid #374151;
            border-radius:16px;
            padding:16px;
            min-height:420px;
            max-height:60vh;
            overflow-y:auto;
            margin-bottom:16px;
        ">
            {history_html if history_html else '<div style="color:#9ca3af;">No messages yet. Ask your first question.</div>'}
        </div>

        <form id="assistant-form" style="display:flex; gap:12px; flex-wrap:wrap;">
            <textarea
                id="assistant-message"
                name="message"
                placeholder="Ask TerraLedger for help..."
                style="
                    flex:1 1 700px;
                    min-height:110px;
                    padding:14px;
                    border-radius:12px;
                    border:1px solid #374151;
                    background:#0f172a;
                    color:#f8fafc;
                    resize:vertical;
                "
                required
            ></textarea>

            <div style="display:flex; flex-direction:column; gap:10px; min-width:160px;">
                <button class="btn" type="submit">Send</button>
            </div>
        </form>
    </div>

    <script>
    (function() {{
        const form = document.getElementById("assistant-form");
        const messageInput = document.getElementById("assistant-message");
        const chatBox = document.getElementById("assistant-chat-box");
        const clearBtn = document.getElementById("clear-chat-btn");

        function escapeHtml(text) {{
            const div = document.createElement("div");
            div.textContent = text;
            return div.innerHTML;
        }}

        function appendMessage(role, content) {{
            const wrapper = document.createElement("div");
            wrapper.style.display = "flex";
            wrapper.style.marginBottom = "12px";
            wrapper.style.justifyContent = role === "user" ? "flex-end" : "flex-start";

            const bubble = document.createElement("div");
            bubble.style.maxWidth = "75%";
            bubble.style.padding = "12px 14px";
            bubble.style.whiteSpace = "pre-wrap";

            if (role === "user") {{
                bubble.style.background = "#2563eb";
                bubble.style.color = "white";
                bubble.style.borderRadius = "14px 14px 4px 14px";
            }} else {{
                bubble.style.background = "#1f2937";
                bubble.style.color = "#f9fafb";
                bubble.style.border = "1px solid #374151";
                bubble.style.borderRadius = "14px 14px 14px 4px";
            }}

            bubble.innerHTML = escapeHtml(content);
            wrapper.appendChild(bubble);
            chatBox.appendChild(wrapper);
            chatBox.scrollTop = chatBox.scrollHeight;
        }}

        form.addEventListener("submit", async function(e) {{
            e.preventDefault();

            const message = messageInput.value.trim();
            if (!message) return;

            if (chatBox.innerText.includes("No messages yet. Ask your first question.")) {{
                chatBox.innerHTML = "";
            }}

            appendMessage("user", message);
            messageInput.value = "";

            try {{
                const res = await fetch("{{{{ url_for('help_assistant.help_assistant_api') }}}}", {{
                    method: "POST",
                    headers: {{
                        "Content-Type": "application/json"
                    }},
                    body: JSON.stringify({{
                        message: message,
                        page_title: document.title || "",
                        route: window.location.pathname || ""
                    }})
                }});

                const data = await res.json();

                if (data.ok) {{
                    appendMessage("assistant", data.answer || "No response.");
                }} else {{
                    appendMessage("assistant", data.error || "Something went wrong.");
                }}
            }} catch (err) {{
                appendMessage("assistant", "Unable to reach the help assistant.");
            }}
        }});

        clearBtn.addEventListener("click", async function() {{
            try {{
                const res = await fetch("{{{{ url_for('help_assistant.help_assistant_clear') }}}}", {{
                    method: "POST",
                    headers: {{
                        "Content-Type": "application/json"
                    }}
                }});

                const data = await res.json();
                if (data.ok) {{
                    chatBox.innerHTML = '<div style="color:#9ca3af;">No messages yet. Ask your first question.</div>';
                }}
            }} catch (err) {{
                alert("Could not clear chat.");
            }}
        }});
    }})();
    </script>
    """

    return render_page(html, title="Help Assistant")


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

        calc_result = calculate_material(user_question)

        if calc_result:
            answer = calc_result
        else:
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