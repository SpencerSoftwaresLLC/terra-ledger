// TerraLedger/static/js/help_assistant.js

function openHelpAssistant() {
    const overlay = document.getElementById("helpAssistantOverlay");
    const input = document.getElementById("helpAssistantInput");

    if (!overlay) return;

    overlay.style.display = "flex";

    setTimeout(() => {
        if (input) input.focus();
    }, 50);
}

function closeHelpAssistant() {
    const overlay = document.getElementById("helpAssistantOverlay");
    if (!overlay) return;
    overlay.style.display = "none";
}

function appendHelpMessage(role, text) {
    const box = document.getElementById("helpAssistantMessages");
    if (!box) return;

    const div = document.createElement("div");
    div.className = `help-assistant-message ${role}`;
    div.textContent = text;
    box.appendChild(div);
    box.scrollTop = box.scrollHeight;
}

function appendHelpLoading() {
    const box = document.getElementById("helpAssistantMessages");
    if (!box) return null;

    const div = document.createElement("div");
    div.className = "help-assistant-message assistant loading";
    div.id = "helpAssistantLoading";
    div.textContent = "Thinking...";
    box.appendChild(div);
    box.scrollTop = box.scrollHeight;
    return div;
}

function removeHelpLoading() {
    const loading = document.getElementById("helpAssistantLoading");
    if (loading) loading.remove();
}

function getHelpAssistantPageName() {
    const body = document.body;
    return body?.getAttribute("data-page-name") || document.title || "";
}

function getHelpAssistantRoute() {
    return window.location.pathname || "";
}

async function submitHelpAssistantMessage() {
    const input = document.getElementById("helpAssistantInput");
    if (!input) return;

    const message = input.value.trim();
    if (!message) return;

    appendHelpMessage("user", message);
    input.value = "";

    appendHelpLoading();

    try {
        const res = await fetch("/api/help-assistant", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                message: message,
                page_name: getHelpAssistantPageName(),
                route: getHelpAssistantRoute()
            })
        });

        const data = await res.json();
        removeHelpLoading();

        if (data.ok) {
            appendHelpMessage("assistant", data.answer || "No response received.");
        } else {
            appendHelpMessage("assistant", data.error || "The assistant is temporarily unavailable.");
        }
    } catch (err) {
        removeHelpLoading();
        appendHelpMessage("assistant", "The assistant is temporarily unavailable. Please try again in a moment.");
        console.error("Help assistant error:", err);
    }
}

async function clearHelpAssistantChat() {
    const box = document.getElementById("helpAssistantMessages");
    if (box) {
        box.innerHTML = `
            <div class="help-assistant-message assistant">
                Hi, I’m Terra and I’ll be your personal Ledger AI assistant.
            </div>
        `;
    }

    try {
        await fetch("/api/help-assistant/clear", {
            method: "POST"
        });
    } catch (err) {
        console.error("Clear help assistant error:", err);
    }
}

function sendSuggestedHelpPrompt(promptText) {
    const input = document.getElementById("helpAssistantInput");
    if (!input) return;
    input.value = promptText;
    submitHelpAssistantMessage();
}

document.addEventListener("DOMContentLoaded", function () {
    const input = document.getElementById("helpAssistantInput");
    if (!input) return;

    input.addEventListener("keydown", function (e) {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            submitHelpAssistantMessage();
        }
    });
});