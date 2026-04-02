from flask import Blueprint, session, redirect, url_for, flash, request, jsonify
from db import get_db_connection
from decorators import login_required, subscription_required
from page_helpers import render_page

print("NOTIFICATIONS ROUTES LOADED", flush=True)

notifications_bp = Blueprint("notifications", __name__)


def ensure_notifications_table():
    conn = get_db_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id SERIAL PRIMARY KEY,
                company_id INTEGER NOT NULL,
                user_id INTEGER,
                type TEXT NOT NULL DEFAULT 'general',
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                link TEXT,
                is_read BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_company_created
            ON notifications (company_id, created_at DESC)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_company_read
            ON notifications (company_id, is_read, created_at DESC)
        """)

        conn.commit()
    finally:
        conn.close()


def create_notification(company_id, title, message, link=None, user_id=None, notif_type="general"):
    if not company_id or not title or not message:
        return

    ensure_notifications_table()

    conn = get_db_connection()
    try:
        conn.execute("""
            INSERT INTO notifications (
                company_id,
                user_id,
                type,
                title,
                message,
                link,
                is_read
            )
            VALUES (%s, %s, %s, %s, %s, %s, FALSE)
        """, (
            company_id,
            user_id,
            notif_type,
            title,
            message,
            link,
        ))
        conn.commit()
    finally:
        conn.close()


def get_recent_notifications(company_id, limit=12):
    ensure_notifications_table()

    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT id, type, title, message, link, is_read, created_at
            FROM notifications
            WHERE company_id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT %s
        """, (company_id, limit)).fetchall()
        return rows
    finally:
        conn.close()


def get_unread_notification_count(company_id):
    ensure_notifications_table()

    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT COUNT(*) AS count
            FROM notifications
            WHERE company_id = %s
              AND is_read = FALSE
        """, (company_id,)).fetchone()
        return int((row or {}).get("count", 0) or 0)
    finally:
        conn.close()


@notifications_bp.route("/notifications")
@login_required
@subscription_required
def notifications_page():
    company_id = session.get("company_id")
    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("dashboard.dashboard"))

    ensure_notifications_table()

    rows = get_recent_notifications(company_id, limit=100)

    cards = ""
    for row in rows:
        badge = "Unread" if not row["is_read"] else "Read"
        badge_class = "unread" if not row["is_read"] else "read"

        actions = ""
        if not row["is_read"]:
            actions += f"""
            <form method="post" action="{url_for('notifications.mark_notification_read', notification_id=row['id'])}" class="inline-form">
                <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}">
                <button class="btn secondary small" type="submit">Mark Read</button>
            </form>
            """

        if row["link"]:
            actions += f"""
            <a class="btn small" href="{row['link']}">Open</a>
            """

        cards += f"""
        <div class="notification-card">
            <div class="notification-top">
                <div>
                    <div class="notification-title">{row["title"]}</div>
                    <div class="notification-meta">{row["created_at"] or ""}</div>
                </div>
                <div class="notification-badge {badge_class}">{badge}</div>
            </div>

            <div class="notification-message">{row["message"]}</div>

            <div class="row-actions" style="margin-top:12px;">
                {actions}
            </div>
        </div>
        """

    content = f"""
    <style>
        .notifications-page {{
            display: grid;
            gap: 18px;
        }}

        .notification-list {{
            display: grid;
            gap: 12px;
        }}

        .notification-card {{
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 14px;
            padding: 14px;
            background: #fff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }}

        .notification-top {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 12px;
            margin-bottom: 8px;
        }}

        .notification-title {{
            font-weight: 700;
            color: #0f172a;
            line-height: 1.25;
        }}

        .notification-meta {{
            margin-top: 4px;
            font-size: .9rem;
            color: #64748b;
        }}

        .notification-message {{
            color: #0f172a;
            line-height: 1.45;
            word-break: break-word;
        }}

        .notification-badge {{
            font-size: .82rem;
            font-weight: 700;
            padding: 6px 10px;
            border-radius: 999px;
            white-space: nowrap;
        }}

        .notification-badge.unread {{
            background: #ecfdf3;
            color: #166534;
        }}

        .notification-badge.read {{
            background: #f1f5f9;
            color: #334155;
        }}
    </style>

    <div class="notifications-page">
        <div class="card">
            <div class="section-head">
                <div>
                    <h1 style="margin-bottom:6px;">Notifications</h1>
                    <div class="muted">Recent activity across messages and other events.</div>
                </div>
                <div class="row-actions">
                    <form method="post" action="{url_for('notifications.mark_all_notifications_read')}" class="inline-form">
                        <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}">
                        <button class="btn secondary" type="submit">Mark All Read</button>
                    </form>
                </div>
            </div>
        </div>

        <div class="card">
            <div class="notification-list">
                {cards or "<div class='muted'>No notifications yet.</div>"}
            </div>
        </div>
    </div>
    """
    return render_page(content, "Notifications")


@notifications_bp.route("/notifications/mark-read/<int:notification_id>", methods=["POST"])
@login_required
@subscription_required
def mark_notification_read(notification_id):
    company_id = session.get("company_id")
    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("dashboard.dashboard"))

    ensure_notifications_table()

    conn = get_db_connection()
    try:
        conn.execute("""
            UPDATE notifications
            SET is_read = TRUE
            WHERE id = %s AND company_id = %s
        """, (notification_id, company_id))
        conn.commit()
    finally:
        conn.close()

    return redirect(url_for("notifications.notifications_page"))


@notifications_bp.route("/notifications/mark-all-read", methods=["POST"])
@login_required
@subscription_required
def mark_all_notifications_read():
    company_id = session.get("company_id")
    if not company_id:
        flash("Company session not found.")
        return redirect(url_for("dashboard.dashboard"))

    ensure_notifications_table()

    conn = get_db_connection()
    try:
        conn.execute("""
            UPDATE notifications
            SET is_read = TRUE
            WHERE company_id = %s
              AND is_read = FALSE
        """, (company_id,))
        conn.commit()
    finally:
        conn.close()

    flash("Notifications marked as read.")
    return redirect(url_for("notifications.notifications_page"))


@notifications_bp.route("/notifications/dropdown")
@login_required
@subscription_required
def notifications_dropdown():
    company_id = session.get("company_id")
    if not company_id:
        return jsonify({"ok": False, "items": [], "unread_count": 0})

    ensure_notifications_table()

    rows = get_recent_notifications(company_id, limit=8)
    unread_count = get_unread_notification_count(company_id)

    items = []
    for row in rows:
        items.append({
            "id": row["id"],
            "title": row["title"],
            "message": row["message"],
            "link": row["link"] or "",
            "is_read": bool(row["is_read"]),
            "created_at": str(row["created_at"] or ""),
        })

    return jsonify({
        "ok": True,
        "items": items,
        "unread_count": unread_count,
    })