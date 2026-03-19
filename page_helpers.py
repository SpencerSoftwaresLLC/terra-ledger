from flask import render_template


def render_page(content, title="TerraLedger"):
    scroll_script = """
    <script>
    (function () {
        const SCROLL_KEY = "terraledger_scroll_" + window.location.href;

        function saveScroll() {
            sessionStorage.setItem(SCROLL_KEY, String(window.scrollY || window.pageYOffset || 0));
        }

        function restoreScroll() {
            const saved = sessionStorage.getItem(SCROLL_KEY);
            if (saved !== null) {
                const y = parseInt(saved, 10);
                if (!Number.isNaN(y)) {
                    window.scrollTo(0, y);
                }
                sessionStorage.removeItem(SCROLL_KEY);
            }
        }

        document.addEventListener("submit", function () {
            saveScroll();
        }, true);

        document.addEventListener("click", function (e) {
            const a = e.target.closest("a");
            if (!a) return;

            const href = a.getAttribute("href") || "";
            if (!href) return;
            if (href.startsWith("#")) return;
            if (a.hasAttribute("download")) return;
            if (a.target && a.target !== "_self") return;

            saveScroll();
        }, true);

        window.addEventListener("load", restoreScroll);
    })();
    </script>
    """

    return render_template(
        "BASE_HTML.html",
        content=content + scroll_script,
        title=title,
        page_title=title,
    )
