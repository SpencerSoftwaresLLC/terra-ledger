from flask import Blueprint, request, redirect, url_for, session, flash, abort, make_response, current_app
from flask_wtf.csrf import generate_csrf
from datetime import date, datetime
from html import escape
import json
import os
import tempfile
import io

from urllib.parse import urlparse
from urllib.request import urlopen

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader

from db import (
    get_db_connection,
    ensure_job_cost_ledger,
    get_next_quote_number,
    ensure_document_number_columns,
    table_columns,
)
from decorators import login_required, require_permission, subscription_required
from page_helpers import *
from helpers import *
from calculations import *
from utils.emailing import send_company_email
from utils.recurring import auto_generate_recurring_jobs

quotes_bp = Blueprint("quotes", __name__)


ITEM_TYPE_LABELS = {
    "mowing": "Mowing",
    "mulch": "Mulch",
    "stone": "Stone",
    "dump_fee": "Dump Fee",
    "plants": "Plants",
    "trees": "Trees",
    "soil": "Soil",
    "fertilizer": "Fertilizer",
    "hardscape_material": "Hardscape Material",
    "labor": "Labor",
    "equipment": "Equipment",
    "delivery": "Delivery",
    "fuel": "Fuel",
    "misc": "Misc",
    "material": "Material",
}


ITEM_TYPE_LABELS_ES = {
    "mowing": "Corte de césped",
    "mulch": "Mantillo",
    "stone": "Piedra",
    "dump_fee": "Cargo de vertedero",
    "plants": "Plantas",
    "trees": "Árboles",
    "soil": "Tierra",
    "fertilizer": "Fertilizante",
    "hardscape_material": "Material de paisajismo duro",
    "labor": "Mano de obra",
    "equipment": "Equipo",
    "delivery": "Entrega",
    "fuel": "Combustible",
    "misc": "Varios",
    "material": "Material",
}


def _lang():
    return "es" if session.get("language") == "es" else "en"


def _is_es():
    return _lang() == "es"


def _t(en, es):
    return es if _is_es() else en


def _safe_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _clean_text(value):
    text = str(value or "").strip()
    if text.lower() in {"none", "null", "n/a"}:
        return ""
    return text


def _display_item_type(value):
    key = (value or "").strip().lower()
    if _is_es():
        if key in ITEM_TYPE_LABELS_ES:
            return ITEM_TYPE_LABELS_ES[key]
        return key.replace("_", " ").title() if key else "Material"
    if key in ITEM_TYPE_LABELS:
        return ITEM_TYPE_LABELS[key]
    return key.replace("_", " ").title() if key else "Material"


def _default_unit_for_quote_item_type(item_type):
    key = (item_type or "").strip().lower()

    if key == "mowing":
        return _t("Cuts", "Cortes")
    if key == "mulch":
        return _t("Yards", "Yardas")
    if key == "stone":
        return _t("Tons", "Toneladas")
    if key == "soil":
        return _t("Yards", "Yardas")
    if key == "hardscape_material":
        return _t("Tons", "Toneladas")
    if key == "fuel":
        return _t("Gallons", "Galones")
    if key == "delivery":
        return _t("Miles", "Millas")
    if key == "labor":
        return _t("Hours", "Horas")
    if key == "equipment":
        return _t("Rentals", "Alquileres")
    if key == "dump_fee":
        return ""

    return ""


def _status_label(status):
    raw = _clean_text(status)
    key = raw.lower()

    if not _is_es():
        return raw or "-"

    translations = {
        "draft": "Borrador",
        "sent": "Enviada",
        "approved": "Aprobada",
        "converted": "Convertida",
        "finished": "Finalizada",
        "scheduled": "Programada",
    }
    return translations.get(key, raw or "-")


def is_mowing_quote(items):
    for i in items:
        if (i["item_type"] or "").lower() == "mowing":
            return True
    return False


def _quote_summary_text(quote, items, lang="en"):
    notes = _clean_text(quote["notes"]) if "notes" in quote.keys() else ""
    quote_title = _clean_text(quote["title"]) if "title" in quote.keys() else ""
    item_descriptions = []

    for item in items or []:
        desc = _clean_text(item["description"])
        if desc:
            item_descriptions.append(desc)

    deduped = []
    seen = set()
    for desc in item_descriptions:
        key = desc.lower()
        if key not in seen:
            seen.add(key)
            deduped.append(desc)

    if is_mowing_quote(items):
        summary = "Servicio de corte de césped propuesto" if lang == "es" else "Proposed mowing service"
        if deduped:
            summary += f" {'incluyendo' if lang == 'es' else 'including'} {', '.join(deduped[:5])}."
        elif quote_title:
            summary += f" {'para' if lang == 'es' else 'for'} {quote_title}."
        else:
            summary += "."
        return summary

    if deduped:
        if len(deduped) == 1:
            return deduped[0]
        if len(deduped) == 2:
            return f"{deduped[0]} {'y' if lang == 'es' else 'and'} {deduped[1]}"
        return ", ".join(deduped[:-1]) + f", {'y' if lang == 'es' else 'and'} {deduped[-1]}"

    if notes:
        return notes

    if quote_title:
        return quote_title

    return "Servicio propuesto." if lang == "es" else "Proposed service."


def ensure_quote_item_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cols = table_columns(conn, "quote_items")

    if "unit_cost" not in cols:
        cur.execute("ALTER TABLE quote_items ADD COLUMN unit_cost DOUBLE PRECISION NOT NULL DEFAULT 0")

    if "item_type" not in cols:
        cur.execute("ALTER TABLE quote_items ADD COLUMN item_type TEXT NOT NULL DEFAULT 'mulch'")

    if "unit" not in cols:
        cur.execute("ALTER TABLE quote_items ADD COLUMN unit TEXT DEFAULT ''")

    conn.commit()
    conn.close()


def build_quote_pdf(quote, items, company, profile, lang="en"):
    pdf_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf_temp.close()

    try:
        quote_number = quote["quote_number"] or quote["id"]

        company_name = (
            profile["quote_header_name"]
            if profile and profile["quote_header_name"]
            else (
                profile["display_name"]
                if profile and profile["display_name"]
                else (company["name"] if company else ("Su empresa" if lang == "es" else "Your Company"))
            )
        )

        footer_note = profile["quote_footer_note"] if profile and profile["quote_footer_note"] else ""
        logo_url = profile["logo_url"] if profile and profile["logo_url"] else ""
        summary_text = _quote_summary_text(quote, items, lang=lang)

        address_parts = []
        if company:
            if company["address_line_1"] and str(company["address_line_1"]).strip().lower() != "none":
                address_parts.append(company["address_line_1"])
            if company["address_line_2"] and str(company["address_line_2"]).strip().lower() != "none":
                address_parts.append(company["address_line_2"])

            city_state_zip = " ".join(
                part for part in [
                    f"{company['city']}," if company["city"] and str(company["city"]).strip().lower() != "none" else "",
                    company["state"] if company["state"] and str(company["state"]).strip().lower() != "none" else "",
                    company["zip_code"] if company["zip_code"] and str(company["zip_code"]).strip().lower() != "none" else "",
                ] if part
            ).strip()

            if city_state_zip:
                address_parts.append(city_state_zip)

        company_contact_lines = []
        if address_parts:
            company_contact_lines.extend(address_parts)
        if company and company["phone"] and str(company["phone"]).strip().lower() != "none":
            company_contact_lines.append(company["phone"])
        if company and company["email"] and str(company["email"]).strip().lower() != "none":
            company_contact_lines.append(company["email"])
        if company and company["website"] and str(company["website"]).strip().lower() != "none":
            company_contact_lines.append(company["website"])

        def load_logo_reader(logo_path_or_url):
            if not logo_path_or_url:
                return None

            try:
                parsed = urlparse(logo_path_or_url)

                if parsed.scheme in ("http", "https"):
                    with urlopen(logo_path_or_url, timeout=5) as resp:
                        return ImageReader(io.BytesIO(resp.read()))

                cleaned = str(logo_path_or_url).strip()

                if cleaned.startswith("/"):
                    full_path = os.path.join(current_app.root_path, cleaned.lstrip("/"))
                else:
                    full_path = os.path.join(current_app.root_path, cleaned)

                if os.path.exists(full_path):
                    return ImageReader(full_path)

            except Exception:
                return None

            return None

        logo_reader = load_logo_reader(logo_url)

        c = canvas.Canvas(pdf_temp.name, pagesize=letter)
        width, height = letter

        footer_chunks = [footer_note[i:i + 95] for i in range(0, len(footer_note), 95)] if footer_note else []

        quote_doc_label = "COTIZACIÓN" if lang == "es" else "QUOTE"
        quote_number_label = "Cotización #:" if lang == "es" else "Quote #:"
        customer_label = "Cliente:" if lang == "es" else "Customer:"
        status_label = "Estado:" if lang == "es" else "Status:"
        date_label = "Fecha:" if lang == "es" else "Date:"
        service_summary_label = "Resumen del servicio" if lang == "es" else "Service Summary"
        total_label = "Total:" if lang == "es" else "Total:"
        notes_label = "Notas:" if lang == "es" else "Notes:"

        def draw_footer():
            if not footer_chunks:
                return

            c.setFont("Helvetica-Oblique", 9)
            footer_y = 40
            for chunk in footer_chunks[:3]:
                c.drawCentredString(width / 2, footer_y, chunk)
                footer_y -= 11

        def draw_header():
            y_pos = height - 50
            text_x = 50

            if logo_reader:
                try:
                    max_width = 180
                    max_height = 70
                    logo_x = 50
                    logo_top_y = height - 50

                    img_width, img_height = logo_reader.getSize()

                    if img_width and img_height:
                        width_ratio = max_width / float(img_width)
                        height_ratio = max_height / float(img_height)
                        scale = min(width_ratio, height_ratio)
                        draw_width = img_width * scale
                        draw_height = img_height * scale
                    else:
                        draw_width = max_width
                        draw_height = max_height

                    logo_y = (logo_top_y - max_height) + ((max_height - draw_height) / 2)

                    c.drawImage(
                        logo_reader,
                        logo_x,
                        logo_y,
                        width=draw_width,
                        height=draw_height,
                        mask="auto"
                    )

                    text_x = 250
                except Exception:
                    text_x = 50

            c.setFont("Helvetica-Bold", 18)
            c.drawString(text_x, y_pos, str(company_name or ("Su empresa" if lang == "es" else "Your Company"))[:45])

            c.setFont("Helvetica-Bold", 20)
            c.drawRightString(width - 50, height - 50, quote_doc_label)

            info_y = y_pos - 22
            c.setFont("Helvetica", 10)
            for line in company_contact_lines:
                c.drawString(text_x, info_y, str(line)[:85])
                info_y -= 14

            draw_footer()
            return min(info_y - 10, height - 125 if logo_reader else info_y - 10)

        def new_page():
            c.showPage()
            return draw_header()

        y = draw_header()

        def ensure_space(required_height):
            nonlocal y
            if y - required_height < 85:
                y = new_page()

        ensure_space(90)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(50, y, f"{quote_number_label} {quote_number}")
        y -= 16
        c.drawString(50, y, f"{customer_label} {quote['customer_name'] or ''}")
        y -= 16
        c.drawString(50, y, f"{status_label} {_status_label(quote['status']) if lang == 'es' else (quote['status'] or '')}")
        y -= 16
        c.drawString(50, y, f"{date_label} {quote['quote_date'] or date.today().isoformat()}")
        y -= 24

        ensure_space(80)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(50, y, service_summary_label)
        y -= 18

        c.setFont("Helvetica", 10)
        summary_chunks = [summary_text[i:i + 95] for i in range(0, len(summary_text), 95)] if summary_text else (["Servicio propuesto."] if lang == "es" else ["Proposed service."])
        for chunk in summary_chunks:
            ensure_space(18)
            c.drawString(50, y, chunk)
            y -= 15

        ensure_space(55)
        y -= 8
        c.line(380, y, 560, y)
        y -= 18
        c.setFont("Helvetica-Bold", 12)
        c.drawRightString(560, y, f"{total_label} ${float(quote['total'] or 0):.2f}")
        y -= 28

        if quote["notes"]:
            ensure_space(50)
            c.setFont("Helvetica-Bold", 11)
            c.drawString(50, y, notes_label)
            y -= 18

            c.setFont("Helvetica", 10)
            notes_text = str(quote["notes"])
            note_chunks = [notes_text[i:i + 95] for i in range(0, len(notes_text), 95)]

            for chunk in note_chunks:
                ensure_space(18)
                c.drawString(50, y, chunk)
                y -= 15

        draw_footer()
        c.save()

        with open(pdf_temp.name, "rb") as f:
            return f.read()

    finally:
        if os.path.exists(pdf_temp.name):
            os.remove(pdf_temp.name)


@quotes_bp.route("/quotes", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def quotes():
    ensure_quote_item_columns()
    ensure_document_number_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    customers = conn.execute(
        """
        SELECT id, name, company, email
        FROM customers
        WHERE company_id = %s
        ORDER BY name
        """,
        (cid,),
    ).fetchall()

    company_row = conn.execute(
        """
        SELECT default_quote_notes, next_quote_number
        FROM companies
        WHERE id = %s
        """,
        (cid,),
    ).fetchone()

    default_quote_notes = ""
    next_quote_number_preview = "1001"

    if company_row:
        if "default_quote_notes" in company_row.keys():
            default_quote_notes = company_row["default_quote_notes"] or ""
        if "next_quote_number" in company_row.keys() and company_row["next_quote_number"] is not None:
            next_quote_number_preview = str(company_row["next_quote_number"])

    customer_list = [
        {
            "id": c["id"],
            "name": c["name"] or "",
            "company": c["company"] or "",
            "email": c["email"] or "",
        }
        for c in customers
    ]

    if request.method == "POST":
        customer_id = request.form.get("customer_id", type=int)

        if not customer_id:
            conn.close()
            flash(_t("Please select a customer from the search results.", "Por favor selecciona un cliente de los resultados de búsqueda."))
            return redirect(url_for("quotes.quotes"))

        quote_number = (request.form.get("quote_number") or "").strip()
        quote_date = (request.form.get("quote_date") or "").strip() or date.today().isoformat()
        expiration_date = (request.form.get("expiration_date") or "").strip()
        status = (request.form.get("status") or "Draft").strip()
        notes = (request.form.get("notes") or "").strip() or default_quote_notes

        if not quote_number:
            quote_number = get_next_quote_number(cid)

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO quotes (
                company_id, customer_id, quote_number, quote_date, expiration_date, status, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                cid,
                customer_id,
                quote_number,
                quote_date,
                expiration_date or None,
                status,
                notes,
            ),
        )

        row = cur.fetchone()
        if not row or "id" not in row:
            conn.rollback()
            conn.close()
            flash(_t("Could not create quote.", "No se pudo crear la cotización."))
            return redirect(url_for("quotes.quotes"))

        quote_id = row["id"]
        conn.commit()
        conn.close()

        flash(_t(f"Quote #{quote_number} created. Add items next.", f"Cotización #{quote_number} creada. Agrega artículos ahora."))
        return redirect(url_for("quotes.view_quote", quote_id=quote_id))

    rows = conn.execute(
        """
        SELECT q.*, c.name AS customer_name
        FROM quotes q
        JOIN customers c ON q.customer_id = c.id
        WHERE q.company_id = %s
          AND COALESCE(q.status, '') != 'Finished'
        ORDER BY q.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    quote_row_list = []
    quote_mobile_cards = []

    for r in rows:
        delete_csrf = generate_csrf()
        confirm_delete = _t("Delete this quote?", "¿Eliminar esta cotización?")

        quote_row_list.append(
            f"""<tr>
                <td>#{r['id']}</td>
                <td>{escape(r['quote_number'] or '-')}</td>
                <td>{escape(r['customer_name'] or '-')}</td>
                <td>${float(r['total'] or 0):.2f}</td>
                <td>{escape(_status_label(r['status']))}</td>
                <td>
                    <div class='row-actions'>
                        <a class='btn secondary small' href='{url_for("quotes.view_quote", quote_id=r["id"])}'>{_t("View", "Ver")}</a>
                        <a class='btn small' href='{url_for("quotes.email_quote_preview", quote_id=r["id"])}'>{_t("Email", "Correo")}</a>
                        <a class='btn success small' href='{url_for("quotes.convert_quote_to_job", quote_id=r["id"])}'>{_t("Convert to Job", "Convertir a trabajo")}</a>
                        <form method='post'
                              action='{url_for("quotes.delete_quote", quote_id=r["id"])}'
                              style='display:inline;'
                              onsubmit="return confirm({confirm_delete!r});">
                            <input type="hidden" name="csrf_token" value="{delete_csrf}">
                            <button class='btn danger small' type='submit'>{_t("Delete", "Eliminar")}</button>
                        </form>
                    </div>
                </td>
            </tr>"""
        )

        quote_mobile_cards.append(
            f"""
            <div class='mobile-list-card'>
                <div class='mobile-list-top'>
                    <div class='mobile-list-title'>{_t("Quote", "Cotización")} #{escape(r['quote_number'] or '-')}</div>
                    <div class='mobile-badge'>{escape(_status_label(r['status']))}</div>
                </div>

                <div class='mobile-list-grid'>
                    <div><span>ID</span><strong>#{r['id']}</strong></div>
                    <div><span>{_t("Customer", "Cliente")}</span><strong>{escape(r['customer_name'] or '-')}</strong></div>
                    <div><span>{_t("Total", "Total")}</span><strong>${float(r['total'] or 0):.2f}</strong></div>
                    <div><span>{_t("Status", "Estado")}</span><strong>{escape(_status_label(r['status']))}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <a class='btn secondary small' href='{url_for("quotes.view_quote", quote_id=r["id"])}'>{_t("View", "Ver")}</a>
                    <a class='btn small' href='{url_for("quotes.email_quote_preview", quote_id=r["id"])}'>{_t("Email", "Correo")}</a>
                    <a class='btn success small' href='{url_for("quotes.convert_quote_to_job", quote_id=r["id"])}'>{_t("Convert to Job", "Convertir a trabajo")}</a>
                    <form method='post'
                          action='{url_for("quotes.delete_quote", quote_id=r["id"])}'
                          style='display:inline;'
                          onsubmit="return confirm({confirm_delete!r});">
                        <input type="hidden" name="csrf_token" value="{delete_csrf}">
                        <button class='btn danger small' type='submit'>{_t("Delete", "Eliminar")}</button>
                    </form>
                </div>
            </div>
            """
        )

    quote_rows = "".join(quote_row_list)
    quote_mobile_cards_html = "".join(quote_mobile_cards)

    form_csrf = generate_csrf()

    content = f"""
    <style>
        .quotes-page {{
            display:grid;
            gap:18px;
        }}

        .quotes-head {{
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:12px;
            flex-wrap:wrap;
        }}

        .customer-search-wrap {{
            position: relative;
        }}

        .customer-search-wrap label {{
            display:block;
            margin-bottom:6px;
        }}

        .customer-search-input-wrap {{
            position: relative;
        }}

        .customer-results {{
            display: none;
            position: absolute;
            top: calc(100% + 4px);
            left: 0;
            right: 0;
            background: #fff;
            border: 1px solid #dbe2ea;
            border-radius: 10px;
            margin-top: 0;
            box-shadow: 0 8px 20px rgba(0,0,0,.08);
            z-index: 1000;
            max-height: 260px;
            overflow-y: auto;
        }}

        .customer-results.show {{
            display: block;
        }}

        .customer-result-item {{
            padding: 10px 12px;
            cursor: pointer;
            border-bottom: 1px solid #eef2f7;
        }}

        .customer-result-item:last-child {{
            border-bottom: none;
        }}

        .customer-result-item:hover {{
            background: #f8fbff;
        }}

        .grid {{
            align-items: start;
        }}

        .table-wrap {{
            width:100%;
            overflow-x:auto;
        }}

        .mobile-only {{
            display:none;
        }}

        .desktop-only {{
            display:block;
        }}

        .mobile-list {{
            display:grid;
            gap:12px;
        }}

        .mobile-list-card {{
            border:1px solid rgba(15, 23, 42, 0.08);
            border-radius:14px;
            padding:14px;
            background:#fff;
            box-shadow:0 1px 2px rgba(15, 23, 42, 0.04);
        }}

        .mobile-list-top {{
            display:flex;
            justify-content:space-between;
            align-items:flex-start;
            gap:10px;
            margin-bottom:10px;
        }}

        .mobile-list-title {{
            font-weight:700;
            color:#0f172a;
            line-height:1.25;
            word-break:break-word;
        }}

        .mobile-badge {{
            font-size:.85rem;
            font-weight:700;
            color:#334155;
            background:#f1f5f9;
            padding:6px 10px;
            border-radius:999px;
            white-space:nowrap;
        }}

        .mobile-list-grid {{
            display:grid;
            grid-template-columns:1fr 1fr;
            gap:10px 12px;
            margin-bottom:12px;
        }}

        .mobile-list-grid span {{
            display:block;
            font-size:.78rem;
            color:#64748b;
            margin-bottom:3px;
        }}

        .mobile-list-grid strong {{
            display:block;
            color:#0f172a;
            font-size:.95rem;
            line-height:1.25;
            word-break:break-word;
        }}

        .mobile-list-actions {{
            display:flex;
            gap:8px;
            flex-wrap:wrap;
        }}

        @media (max-width: 640px) {{
            .desktop-only {{
                display:none !important;
            }}

            .mobile-only {{
                display:block !important;
            }}

            .mobile-list-grid {{
                grid-template-columns:1fr;
            }}
        }}
    </style>

    <div class='quotes-page'>
        <div class='card'>
            <div class='quotes-head'>
                <h1 style='margin:0;'>{_t("Quotes", "Cotizaciones")}</h1>
                <div class='row-actions'>
                    <a class='btn warning' href='{url_for("quotes.finished_quotes")}'>{_t("Finished Quotes", "Cotizaciones finalizadas")}</a>
                </div>
            </div>

            <form method='post'>
                <input type="hidden" name="csrf_token" value="{form_csrf}">
                <div class='grid'>
                    <div class='customer-search-wrap'>
                        <label>{_t("Customer", "Cliente")}</label>
                        <div class='customer-search-input-wrap'>
                            <input type='text' id='customer_search' placeholder='{escape(_t("Search customer name, company, or email...", "Busca nombre del cliente, empresa o correo..."), quote=True)}' autocomplete='off' required>
                            <input type='hidden' name='customer_id' id='customer_id' required>
                            <div id='customer_results' class='customer-results'></div>
                        </div>
                    </div>

                    <div>
                        <label>{_t("Quote Number", "Número de cotización")}</label>
                        <input name='quote_number' placeholder='{escape(_t("Auto-assigned if left blank", "Se asigna automáticamente si se deja en blanco"), quote=True)}'>
                    </div>

                    <div>
                        <label>{_t("Quote Date", "Fecha de cotización")}</label>
                        <input type='date' name='quote_date' value='{date.today().isoformat()}'>
                    </div>

                    <div>
                        <label>{_t("Expiration Date", "Fecha de vencimiento")}</label>
                        <input type='date' name='expiration_date'>
                    </div>

                    <div>
                        <label>{_t("Status", "Estado")}</label>
                        <select name='status'>
                            <option value='Draft'>{_t("Draft", "Borrador")}</option>
                            <option value='Sent'>{_t("Sent", "Enviada")}</option>
                            <option value='Approved'>{_t("Approved", "Aprobada")}</option>
                        </select>
                    </div>
                </div>

                <br><label>{_t("Notes", "Notas")}</label><textarea name='notes'>{escape(default_quote_notes)}</textarea><br>
                <button class='btn'>{_t("Create Quote", "Crear cotización")}</button>
            </form>
        </div>

        <div class='card'>
            <h2>{_t("Quote List", "Lista de cotizaciones")}</h2>

            <div class='table-wrap desktop-only'>
                <table>
                    <tr><th>ID</th><th>{_t("Number", "Número")}</th><th>{_t("Customer", "Cliente")}</th><th>{_t("Total", "Total")}</th><th>{_t("Status", "Estado")}</th><th>{_t("Actions", "Acciones")}</th></tr>
                    {quote_rows or f'<tr><td colspan="6" class="muted">{_t("No quotes yet.", "Todavía no hay cotizaciones.")}</td></tr>'}
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {quote_mobile_cards_html or f"<div class='mobile-list-card muted'>{_t('No quotes yet.', 'Todavía no hay cotizaciones.')}</div>"}
                </div>
            </div>
        </div>

        <script>
            const customers = {json.dumps(customer_list)};
            const noCustomersFoundText = {json.dumps(_t("No customers found", "No se encontraron clientes"))};
            const unnamedCustomerText = {json.dumps(_t("Unnamed Customer", "Cliente sin nombre"))};

            const searchInput = document.getElementById("customer_search");
            const customerIdInput = document.getElementById("customer_id");
            const resultsBox = document.getElementById("customer_results");

            function escapeHtml(text) {{
                return String(text || "")
                    .replace(/&/g, "&amp;")
                    .replace(/</g, "&lt;")
                    .replace(/>/g, "&gt;")
                    .replace(/"/g, "&quot;")
                    .replace(/'/g, "&#039;");
            }}

            function closeResults() {{
                resultsBox.innerHTML = "";
                resultsBox.classList.remove("show");
            }}

            function renderCustomerResults(matches) {{
                if (!matches.length) {{
                    resultsBox.innerHTML = "<div class='customer-result-item muted'>" + escapeHtml(noCustomersFoundText) + "</div>";
                    resultsBox.classList.add("show");
                    return;
                }}

                resultsBox.innerHTML = matches.map(c => `
                    <div class="customer-result-item" data-id="${{c.id}}">
                        <strong>${{escapeHtml(c.name || unnamedCustomerText)}}</strong>
                        ${{c.company ? `<div class="muted small">${{escapeHtml(c.company)}}</div>` : ""}}
                        ${{c.email ? `<div class="muted small">${{escapeHtml(c.email)}}</div>` : ""}}
                    </div>
                `).join("");

                resultsBox.classList.add("show");

                document.querySelectorAll(".customer-result-item[data-id]").forEach(item => {{
                    item.addEventListener("click", function () {{
                        const id = this.dataset.id;
                        const customer = customers.find(x => String(x.id) === String(id));
                        if (!customer) return;

                        customerIdInput.value = customer.id;
                        searchInput.value = customer.company
                            ? `${{customer.name}} - ${{customer.company}}`
                            : (customer.name || unnamedCustomerText);

                        closeResults();
                    }});
                }});
            }}

            searchInput.addEventListener("input", function () {{
                const q = this.value.trim().toLowerCase();
                customerIdInput.value = "";

                if (!q) {{
                    closeResults();
                    return;
                }}

                const matches = customers.filter(c =>
                    (c.name && c.name.toLowerCase().includes(q)) ||
                    (c.company && c.company.toLowerCase().includes(q)) ||
                    (c.email && c.email.toLowerCase().includes(q))
                ).slice(0, 8);

                renderCustomerResults(matches);
            }});

            document.addEventListener("click", function (e) {{
                if (!e.target.closest(".customer-search-wrap")) {{
                    closeResults();
                }}
            }});
        </script>
    </div>
    """
    return render_page(content, _t("Quotes", "Cotizaciones"))


@quotes_bp.route("/quotes/<int:quote_id>", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def view_quote(quote_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    quote = conn.execute(
        """
        SELECT q.*, c.name AS customer_name, c.email AS customer_email
        FROM quotes q
        JOIN customers c ON q.customer_id = c.id
        WHERE q.id = %s AND q.company_id = %s
        """,
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        abort(404)

    if request.method == "POST":
        item_type = ((request.form.get("item_type") or "mowing").strip().lower() or "mowing")
        description = (request.form.get("description") or "").strip()
        quantity = _safe_float(request.form.get("quantity"))
        unit = (request.form.get("unit") or "").strip()
        unit_price = _safe_float(request.form.get("unit_price"))
        unit_cost = _safe_float(request.form.get("unit_cost"))

        if not description:
            conn.close()
            flash(_t("Description is required.", "La descripción es obligatoria."))
            return redirect(url_for("quotes.view_quote", quote_id=quote_id))

        default_unit = _default_unit_for_quote_item_type(item_type)
        if item_type == "dump_fee":
            unit = ""
        elif default_unit and not unit:
            unit = default_unit
        elif item_type in ["plants", "trees", "misc"]:
            unit = ""

        if item_type == "labor":
            unit_cost = 0.0

        if item_type == "mowing":
            if quantity <= 0:
                quantity = 1
            if not unit:
                unit = _t("Cuts", "Cortes")

        if item_type == "dump_fee":
            unit = ""
            if quantity <= 0:
                quantity = 1
            unit_cost = 0.0

        line_total = quantity * unit_price

        conn.execute(
            """
            INSERT INTO quote_items (quote_id, item_type, description, quantity, unit, unit_price, unit_cost, line_total)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (quote_id, item_type, description, quantity, unit, unit_price, unit_cost, line_total),
        )
        recalc_quote(conn, quote_id)
        conn.commit()
        conn.close()

        flash(_t("Quote item added.", "Artículo de cotización agregado."))
        return redirect(url_for("quotes.view_quote", quote_id=quote_id))

    items = conn.execute(
        "SELECT * FROM quote_items WHERE quote_id = %s ORDER BY id",
        (quote_id,),
    ).fetchall()
    conn.close()

    item_row_list = []
    item_mobile_cards = []

    for i in items:
        delete_item_csrf = generate_csrf()
        unit_cost_display = "-" if (i["item_type"] or "").strip().lower() in ["dump_fee", "labor"] else f"${float(i['unit_cost'] or 0):.2f}"
        confirm_delete = _t("Delete this line item?", "¿Eliminar esta línea?")

        item_row_list.append(
            f"""
            <tr>
                <td>{escape(_display_item_type(i['item_type']))}</td>
                <td>{escape(i['description'] or '')}</td>
                <td>{float(i['quantity'] or 0):g}</td>
                <td>{escape(i['unit'] or '-')}</td>
                <td>${float(i['unit_price'] or 0):.2f}</td>
                <td>{unit_cost_display}</td>
                <td>${float(i['line_total'] or 0):.2f}</td>
                <td>
                    <form method="post"
                          action="{url_for('quotes.delete_quote_item', quote_id=quote_id, item_id=i['id'])}"
                          style="display:inline;"
                          onsubmit="return confirm({confirm_delete!r});">
                        <input type="hidden" name="csrf_token" value="{delete_item_csrf}">
                        <button class="btn danger small" type="submit">{_t("Delete", "Eliminar")}</button>
                    </form>
                </td>
            </tr>
            """
        )

        item_mobile_cards.append(
            f"""
            <div class='mobile-list-card'>
                <div class='mobile-list-top'>
                    <div class='mobile-list-title'>{escape(_display_item_type(i['item_type']))} - {escape(i['description'] or '')}</div>
                </div>

                <div class='mobile-list-grid'>
                    <div><span>{_t("Qty", "Cant.")}</span><strong>{float(i['quantity'] or 0):g}</strong></div>
                    <div><span>{_t("Unit", "Unidad")}</span><strong>{escape(i['unit'] or '-')}</strong></div>
                    <div><span>{_t("Sale Price / Rate / Fee", "Precio de venta / tarifa / cargo")}</span><strong>${float(i['unit_price'] or 0):.2f}</strong></div>
                    <div><span>{_t("Unit Cost (Internal)", "Costo unitario (interno)")}</span><strong>{unit_cost_display}</strong></div>
                    <div><span>{_t("Line Total", "Total de línea")}</span><strong>${float(i['line_total'] or 0):.2f}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <form method="post"
                          action="{url_for('quotes.delete_quote_item', quote_id=quote_id, item_id=i['id'])}"
                          style="display:inline;"
                          onsubmit="return confirm({confirm_delete!r});">
                        <input type="hidden" name="csrf_token" value="{delete_item_csrf}">
                        <button class="btn danger small" type="submit">{_t("Delete", "Eliminar")}</button>
                    </form>
                </div>
            </div>
            """
        )

    item_rows = "".join(item_row_list)
    item_mobile_cards_html = "".join(item_mobile_cards)

    add_item_csrf = generate_csrf()

    content = f"""
    <style>
        .quote-view-page {{
            display:grid;
            gap:18px;
        }}

        .quote-head {{
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:12px;
            flex-wrap:wrap;
        }}

        .quote-meta-grid {{
            display:grid;
            grid-template-columns:repeat(3, minmax(0, 1fr));
            gap:12px;
            margin-top:14px;
        }}

        .quote-meta-card,
        .quote-total-card {{
            border:1px solid rgba(15, 23, 42, 0.08);
            border-radius:12px;
            padding:12px;
            background:#fff;
        }}

        .quote-meta-card span,
        .quote-total-card span {{
            display:block;
            font-size:.8rem;
            color:#64748b;
            margin-bottom:4px;
        }}

        .quote-meta-card strong,
        .quote-total-card strong {{
            display:block;
            color:#0f172a;
            line-height:1.3;
            word-break:break-word;
        }}

        .quote-total-grid {{
            display:grid;
            grid-template-columns:1fr;
            gap:12px;
            margin-top:12px;
        }}

        .table-wrap {{
            width:100%;
            overflow-x:auto;
        }}

        .mobile-only {{
            display:none;
        }}

        .desktop-only {{
            display:block;
        }}

        .mobile-list {{
            display:grid;
            gap:12px;
        }}

        .mobile-list-card {{
            border:1px solid rgba(15, 23, 42, 0.08);
            border-radius:14px;
            padding:14px;
            background:#fff;
            box-shadow:0 1px 2px rgba(15, 23, 42, 0.04);
        }}

        .mobile-list-top {{
            display:flex;
            justify-content:space-between;
            align-items:flex-start;
            gap:10px;
            margin-bottom:10px;
        }}

        .mobile-list-title {{
            font-weight:700;
            color:#0f172a;
            line-height:1.25;
            word-break:break-word;
        }}

        .mobile-badge {{
            font-size:.85rem;
            font-weight:700;
            color:#334155;
            background:#f1f5f9;
            padding:6px 10px;
            border-radius:999px;
            white-space:nowrap;
        }}

        .mobile-list-grid {{
            display:grid;
            grid-template-columns:1fr 1fr;
            gap:10px 12px;
            margin-bottom:12px;
        }}

        .mobile-list-grid span {{
            display:block;
            font-size:.78rem;
            color:#64748b;
            margin-bottom:3px;
        }}

        .mobile-list-grid strong {{
            display:block;
            color:#0f172a;
            font-size:.95rem;
            line-height:1.25;
            word-break:break-word;
        }}

        .mobile-list-actions {{
            display:flex;
            gap:8px;
            flex-wrap:wrap;
        }}

        .quote-item-helper {{
            margin-top:6px;
            font-size:.8rem;
            color:#64748b;
            line-height:1.35;
        }}

        @media (max-width: 900px) {{
            .quote-meta-grid {{
                grid-template-columns:1fr;
            }}
        }}

        @media (max-width: 640px) {{
            .desktop-only {{
                display:none !important;
            }}

            .mobile-only {{
                display:block !important;
            }}

            .mobile-list-grid {{
                grid-template-columns:1fr;
            }}
        }}
    </style>

    <div class='quote-view-page'>
        <div class='card'>
            <div class='quote-head'>
                <div>
                    <h1 style='margin-bottom:6px;'>{_t("Quote", "Cotización")} #{quote['id']} <span class='pill'>{escape(_status_label(quote['status']))}</span></h1>
                </div>
                <div class='row-actions'>
                    <a class='btn secondary' href='{url_for("quotes.quotes")}'>{_t("Back to Quotes", "Volver a cotizaciones")}</a>
                    <a class='btn' href='{url_for("quotes.email_quote_preview", quote_id=quote_id)}'>{_t("Email Quote", "Enviar cotización")}</a>
                    <a class='btn success' href='{url_for("quotes.convert_quote_to_job", quote_id=quote_id)}'>{_t("Convert to Job", "Convertir a trabajo")}</a>
                </div>
            </div>

            <div class='quote-meta-grid'>
                <div class='quote-meta-card'>
                    <span>{_t("Customer", "Cliente")}</span>
                    <strong>{escape(quote['customer_name'] or '-')}</strong>
                </div>
                <div class='quote-meta-card'>
                    <span>{_t("Customer Email", "Correo del cliente")}</span>
                    <strong>{escape(quote['customer_email'] or '-')}</strong>
                </div>
                <div class='quote-meta-card'>
                    <span>{_t("Status", "Estado")}</span>
                    <strong>{escape(_status_label(quote['status']))}</strong>
                </div>
                <div class='quote-meta-card'>
                    <span>{_t("Quote Number", "Número de cotización")}</span>
                    <strong>{escape(quote['quote_number'] or '-')}</strong>
                </div>
                <div class='quote-meta-card'>
                    <span>{_t("Quote Date", "Fecha de cotización")}</span>
                    <strong>{escape(str(quote['quote_date'] or '-'))}</strong>
                </div>
                <div class='quote-meta-card'>
                    <span>{_t("Expiration Date", "Fecha de vencimiento")}</span>
                    <strong>{escape(str(quote['expiration_date'] or '-'))}</strong>
                </div>
            </div>

            <div class='quote-total-grid'>
                <div class='quote-total-card'>
                    <span>{_t("Total", "Total")}</span>
                    <strong>${float(quote['total'] or 0):.2f}</strong>
                </div>
            </div>
        </div>

        <div class='card'>
            <div class='notice' style='margin-bottom:16px;'>
                <strong>{_t("Internal pricing note:", "Nota de precio interno:")}</strong>
                {_t(
                    "“Your Cost (Internal)” is saved for your records and job profit tracking only. It is not shown on the customer PDF or email.",
                    "“Tu costo (interno)” se guarda solo para tus registros y seguimiento de ganancias del trabajo. No se muestra en el PDF ni en el correo del cliente."
                )}
            </div>

            <h2>{_t("Add Quote Item", "Agregar artículo a la cotización")}</h2>
            <form method='post'>
                <input type="hidden" name="csrf_token" value="{add_item_csrf}">
                <div class='grid'>
                    <div>
                        <label>{_t("Item Type", "Tipo de artículo")}</label>
                        <select name='item_type' id='quote_item_type' onchange='toggleQuoteItemType()'>
                            <option value='mowing'>{_t("Mowing", "Corte de césped")}</option>
                            <option value='mulch'>{_t("Mulch", "Mantillo")}</option>
                            <option value='stone'>{_t("Stone", "Piedra")}</option>
                            <option value='dump_fee'>{_t("Dump Fee", "Cargo de vertedero")}</option>
                            <option value='plants'>{_t("Plants", "Plantas")}</option>
                            <option value='trees'>{_t("Trees", "Árboles")}</option>
                            <option value='soil'>{_t("Soil", "Tierra")}</option>
                            <option value='fertilizer'>{_t("Fertilizer", "Fertilizante")}</option>
                            <option value='hardscape_material'>{_t("Hardscape Material", "Material de paisajismo duro")}</option>
                            <option value='labor'>{_t("Labor", "Mano de obra")}</option>
                            <option value='equipment'>{_t("Equipment", "Equipo")}</option>
                            <option value='delivery'>{_t("Delivery", "Entrega")}</option>
                            <option value='fuel'>{_t("Fuel", "Combustible")}</option>
                            <option value='misc'>{_t("Misc", "Varios")}</option>
                        </select>
                        <div class='quote-item-helper'>{_t("Use mowing for recurring lawn cuts or flat per-cut pricing.", "Usa corte de césped para cortes recurrentes o precios fijos por corte.")}</div>
                    </div>
                    <div>
                        <label>{_t("Description", "Descripción")}</label>
                        <input name='description' required>
                    </div>
                    <div>
                        <label id='quantity_label'>{_t("Quantity", "Cantidad")}</label>
                        <input name='quantity' id='quote_quantity' type='number' step='0.01' min='0' required>
                    </div>
                    <div>
                        <label>{_t("Unit", "Unidad")}</label>
                        <input name='unit' id='quote_unit' placeholder='{escape(_t("Unit", "Unidad"), quote=True)}'>
                    </div>
                    <div>
                        <label id='unit_price_label'>{_t("Sale Price", "Precio de venta")}</label>
                        <input name='unit_price' id='quote_unit_price' type='number' step='0.01' min='0' required>
                    </div>
                    <div id='unit_cost_wrap'>
                        <label id='unit_cost_label'>{_t("Unit Cost (Internal)", "Costo unitario (interno)")}</label>
                        <input name='unit_cost' id='quote_unit_cost' type='number' step='0.01' min='0' value='0'>
                    </div>
                </div>
                <br>
                <button class='btn'>{_t("Add Item", "Agregar artículo")}</button>
            </form>
        </div>

        <div class='card'>
            <h2>{_t("Items", "Artículos")}</h2>

            <div class='table-wrap desktop-only'>
                <table>
                    <tr>
                        <th>{_t("Type", "Tipo")}</th>
                        <th>{_t("Description", "Descripción")}</th>
                        <th>{_t("Qty", "Cant.")}</th>
                        <th>{_t("Unit", "Unidad")}</th>
                        <th>{_t("Sale Price / Rate / Fee", "Precio de venta / tarifa / cargo")}</th>
                        <th>{_t("Unit Cost (Internal)", "Costo unitario (interno)")}</th>
                        <th>{_t("Line Total", "Total de línea")}</th>
                        <th>{_t("Actions", "Acciones")}</th>
                    </tr>
                    {item_rows or f'<tr><td colspan="8" class="muted">{_t("No items yet.", "Todavía no hay artículos.")}</td></tr>'}
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {item_mobile_cards_html or f"<div class='mobile-list-card muted'>{_t('No items yet.', 'Todavía no hay artículos.')}</div>"}
                </div>
            </div>
        </div>

        <script>
            function toggleQuoteItemType() {{
                var type = document.getElementById("quote_item_type").value;
                var unitField = document.getElementById("quote_unit");
                var quantityLabel = document.getElementById("quantity_label");
                var unitPriceLabel = document.getElementById("unit_price_label");
                var unitCostWrap = document.getElementById("unit_cost_wrap");
                var unitCostLabel = document.getElementById("unit_cost_label");
                var quantityInput = document.getElementById("quote_quantity");
                var unitCostInput = document.getElementById("quote_unit_cost");

                quantityLabel.textContent = {json.dumps(_t("Quantity", "Cantidad"))};
                unitPriceLabel.textContent = {json.dumps(_t("Sale Price", "Precio de venta"))};
                unitCostLabel.textContent = {json.dumps(_t("Unit Cost (Internal)", "Costo unitario (interno)"))};
                unitField.value = "";
                unitCostWrap.style.display = "";
                quantityInput.readOnly = false;
                quantityInput.step = "0.01";

                if (type === "mowing") {{
                    quantityLabel.textContent = {json.dumps(_t("Cuts", "Cortes"))};
                    unitPriceLabel.textContent = {json.dumps(_t("Price Per Cut", "Precio por corte"))};
                    unitField.value = {json.dumps(_t("Cuts", "Cortes"))};
                }}
                else if (type === "mulch") {{
                    quantityLabel.textContent = {json.dumps(_t("Yards", "Yardas"))};
                    unitField.value = {json.dumps(_t("Yards", "Yardas"))};
                }}
                else if (type === "stone") {{
                    quantityLabel.textContent = {json.dumps(_t("Tons", "Toneladas"))};
                    unitField.value = {json.dumps(_t("Tons", "Toneladas"))};
                }}
                else if (type === "soil") {{
                    quantityLabel.textContent = {json.dumps(_t("Yards", "Yardas"))};
                    unitField.value = {json.dumps(_t("Yards", "Yardas"))};
                }}
                else if (type === "hardscape_material") {{
                    quantityLabel.textContent = {json.dumps(_t("Tons", "Toneladas"))};
                    unitField.value = {json.dumps(_t("Tons", "Toneladas"))};
                }}
                else if (type === "fuel") {{
                    quantityLabel.textContent = {json.dumps(_t("Gallons", "Galones"))};
                    unitField.value = {json.dumps(_t("Gallons", "Galones"))};
                }}
                else if (type === "delivery") {{
                    quantityLabel.textContent = {json.dumps(_t("Miles", "Millas"))};
                    unitField.value = {json.dumps(_t("Miles", "Millas"))};
                }}
                else if (type === "labor") {{
                    quantityLabel.textContent = {json.dumps(_t("Billable Hours", "Horas facturables"))};
                    unitPriceLabel.textContent = {json.dumps(_t("Hourly Rate", "Tarifa por hora"))};
                    unitField.value = {json.dumps(_t("Hours", "Horas"))};
                    unitCostWrap.style.display = "none";
                    unitCostInput.value = "0";
                }}
                else if (type === "equipment") {{
                    quantityLabel.textContent = {json.dumps(_t("Rentals", "Alquileres"))};
                    unitField.value = {json.dumps(_t("Rentals", "Alquileres"))};
                }}
                else if (type === "plants" || type === "trees" || type === "misc") {{
                    quantityLabel.textContent = {json.dumps(_t("Quantity", "Cantidad"))};
                    unitField.value = "";
                }}
                else if (type === "dump_fee") {{
                    quantityLabel.textContent = {json.dumps(_t("Fee", "Cargo"))};
                    unitPriceLabel.textContent = {json.dumps(_t("Fee Amount", "Monto del cargo"))};
                    unitField.value = "";
                    unitCostWrap.style.display = "none";
                    unitCostInput.value = "0";
                    if (!quantityInput.value || parseFloat(quantityInput.value) <= 0) {{
                        quantityInput.value = "1";
                    }}
                }}
                else if (type === "fertilizer") {{
                    quantityLabel.textContent = {json.dumps(_t("Quantity", "Cantidad"))};
                    unitField.value = "";
                }}
            }}

            document.addEventListener("DOMContentLoaded", function () {{
                toggleQuoteItemType();
            }});
        </script>
    </div>
    """
    return render_page(content, f"{_t('Quote', 'Cotización')} #{quote_id}")


@quotes_bp.route("/quotes/<int:quote_id>/email")
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def email_quote_preview(quote_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]
    lang = _lang()

    quote = conn.execute(
        """
        SELECT q.*, c.name AS customer_name, c.email AS customer_email
        FROM quotes q
        JOIN customers c ON q.customer_id = c.id
        WHERE q.id = %s AND q.company_id = %s
        """,
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        abort(404)

    items = conn.execute(
        "SELECT * FROM quote_items WHERE quote_id = %s ORDER BY id",
        (quote_id,),
    ).fetchall()

    recipient = (quote["customer_email"] or "").strip()
    summary_text = _quote_summary_text(quote, items, lang=lang)
    conn.close()

    preview_url = url_for("quotes.preview_quote_pdf", quote_id=quote_id)
    send_url = url_for("quotes.send_quote_email", quote_id=quote_id)
    send_csrf = generate_csrf()

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>{_t("Email Quote", "Enviar cotización")} #{quote['id']}</h1>
                <p style='margin:0;'>
                    <strong>{_t("Customer", "Cliente")}:</strong> {escape(quote['customer_name'] or '-')}<br>
                    <strong>{_t("Email", "Correo")}:</strong> {escape(recipient or _t('No email on file', 'No hay correo registrado'))}<br>
                    <strong>{_t("Total", "Total")}:</strong> ${float(quote['total'] or 0):.2f}
                </p>
            </div>
            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("quotes.view_quote", quote_id=quote_id)}'>{_t("Back to Quote", "Volver a la cotización")}</a>
                <a class='btn secondary' href='{preview_url}' target='_blank'>{_t("Open PDF Preview", "Abrir vista previa PDF")}</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <div class='notice' style='margin-bottom:16px;'>
            {_t(
                "Customer-facing quote delivery uses a clean service summary and total. Internal cost fields and line-by-line item pricing are not shown in the email body or PDF.",
                "La entrega de cotizaciones al cliente usa un resumen limpio del servicio y el total. Los campos de costo interno y el precio detallado por línea no se muestran en el correo ni en el PDF."
            )}
        </div>

        <h2>{_t("Email Summary", "Resumen del correo")}</h2>
        <div class='card' style='background:#fafafa; margin-bottom:16px;'>
            <p style='margin:0 0 10px 0;'><strong>{_t("Service Summary", "Resumen del servicio")}</strong></p>
            <p style='margin:0;'>{escape(summary_text)}</p>
        </div>

        <h2>{_t("PDF Preview", "Vista previa PDF")}</h2>
        <div style='margin-bottom:14px;'>
            <iframe src='{preview_url}' style='width:100%; height:820px; border:1px solid #dbe2ea; border-radius:12px; background:#fff;'></iframe>
        </div>

        {f"<div class='notice warning'>{_t('This customer does not have an email address yet. Add one before sending.', 'Este cliente todavía no tiene correo electrónico. Agrega uno antes de enviar.')}</div>" if not recipient else ""}

        <form method='post' action='{send_url}' onsubmit="return confirm({json.dumps(_t('Send this quote by email now?', '¿Enviar esta cotización por correo ahora?'))});">
            <input type="hidden" name="csrf_token" value="{send_csrf}">
            <button class='btn' type='submit' {"disabled" if not recipient else ""}>{_t("Send Email Now", "Enviar correo ahora")}</button>
        </form>
    </div>
    """
    return render_page(content, f"{_t('Email Quote', 'Enviar cotización')} #{quote_id}")


@quotes_bp.route("/quotes/<int:quote_id>/preview_pdf")
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def preview_quote_pdf(quote_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]
    lang = _lang()

    quote = conn.execute(
        """
        SELECT q.*, c.name AS customer_name, c.email AS customer_email
        FROM quotes q
        JOIN customers c ON q.customer_id = c.id
        WHERE q.id = %s AND q.company_id = %s
        """,
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        abort(404)

    items = conn.execute(
        "SELECT * FROM quote_items WHERE quote_id = %s ORDER BY id",
        (quote_id,),
    ).fetchall()

    company = conn.execute(
        """
        SELECT name, email, phone, website, address_line_1, address_line_2, city, state, zip_code
        FROM companies
        WHERE id = %s
        """,
        (cid,),
    ).fetchone()

    profile = conn.execute(
        """
        SELECT display_name, legal_name, logo_url, quote_header_name, quote_footer_note, email
        FROM company_profile
        WHERE company_id = %s
        """,
        (cid,),
    ).fetchone()

    conn.close()

    pdf_data = build_quote_pdf(quote, items, company, profile, lang=lang)

    response = make_response(pdf_data)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f"inline; filename=Quote_{quote['quote_number'] or quote_id}.pdf"
    return response


@quotes_bp.route("/quotes/<int:quote_id>/send_email", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def send_quote_email(quote_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]
    uid = session.get("user_id")
    lang = _lang()

    quote = conn.execute(
        """
        SELECT q.*, c.name AS customer_name, c.email AS customer_email
        FROM quotes q
        JOIN customers c ON q.customer_id = c.id
        WHERE q.id = %s AND q.company_id = %s
        """,
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        abort(404)

    items = conn.execute(
        "SELECT * FROM quote_items WHERE quote_id = %s ORDER BY id",
        (quote_id,),
    ).fetchall()

    company = conn.execute(
        """
        SELECT name, email, phone, website, address_line_1, address_line_2, city, state, zip_code
        FROM companies
        WHERE id = %s
        """,
        (cid,),
    ).fetchone()

    profile = conn.execute(
        """
        SELECT display_name, legal_name, logo_url, quote_header_name, quote_footer_note, email
        FROM company_profile
        WHERE company_id = %s
        """,
        (cid,),
    ).fetchone()

    conn.close()

    recipient = (quote["customer_email"] or "").strip()
    if not recipient:
        flash(_t("This customer does not have an email address.", "Este cliente no tiene una dirección de correo electrónico."))
        return redirect(url_for("quotes.email_quote_preview", quote_id=quote_id))

    quote_number = quote["quote_number"] or quote["id"]
    total_amount = float(quote["total"] or 0)
    summary_text = _quote_summary_text(quote, items, lang=lang)

    try:
        pdf_data = build_quote_pdf(quote, items, company, profile, lang=lang)

        if lang == "es":
            text_body = (
                f"Hola {quote['customer_name']},\n\n"
                f"Adjunta encontrarás la cotización #{quote_number}.\n\n"
                f"Resumen del servicio: {summary_text}\n\n"
                f"Total: ${total_amount:.2f}\n\n"
                f"Gracias."
            )

            html_body = f"""
                <div style="font-family: Arial, sans-serif; font-size: 15px; line-height: 1.6; color: #222;">
                    <p>Hola {escape(quote['customer_name'])},</p>

                    <p>Adjunta encontrarás la cotización #{escape(str(quote_number))}.</p>

                    <p>
                        <strong>Resumen del servicio:</strong><br>
                        {escape(summary_text)}
                    </p>

                    <p>
                        <strong>Total:</strong> ${total_amount:.2f}
                    </p>

                    <p>Gracias.</p>
                </div>
            """
            subject = f"Cotización #{quote_number}"
        else:
            text_body = (
                f"Hello {quote['customer_name']},\n\n"
                f"Please find attached Quote #{quote_number}.\n\n"
                f"Service Summary: {summary_text}\n\n"
                f"Total: ${total_amount:.2f}\n\n"
                f"Thank you."
            )

            html_body = f"""
                <div style="font-family: Arial, sans-serif; font-size: 15px; line-height: 1.6; color: #222;">
                    <p>Hello {escape(quote['customer_name'])},</p>

                    <p>Please find attached Quote #{escape(str(quote_number))}.</p>

                    <p>
                        <strong>Service Summary:</strong><br>
                        {escape(summary_text)}
                    </p>

                    <p>
                        <strong>Total:</strong> ${total_amount:.2f}
                    </p>

                    <p>Thank you.</p>
                </div>
            """
            subject = f"Quote #{quote_number}"

        send_company_email(
            company_id=cid,
            user_id=uid,
            to_email=recipient,
            subject=subject,
            html=html_body,
            body=text_body,
            attachment_bytes=pdf_data,
            attachment_filename=f"Quote_{quote_number}.pdf",
        )

        flash(_t("Quote emailed successfully as PDF.", "La cotización fue enviada correctamente como PDF."))

    except Exception as e:
        flash(_t(
            f"Could not email quote: {e}",
            f"No se pudo enviar la cotización por correo: {e}"
        ))

    return redirect(url_for("quotes.view_quote", quote_id=quote_id))


@quotes_bp.route("/quotes/<int:quote_id>/convert_to_job")
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def convert_quote_to_job(quote_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    try:
        quote = conn.execute(
            """
            SELECT *
            FROM quotes
            WHERE id = %s AND company_id = %s
            """,
            (quote_id, cid),
        ).fetchone()

        if not quote:
            abort(404)

        existing_job = conn.execute(
            """
            SELECT id
            FROM jobs
            WHERE quote_id = %s AND company_id = %s
            LIMIT 1
            """,
            (quote_id, cid),
        ).fetchone()

        if existing_job:
            flash(_t("This quote has already been converted.", "Esta cotización ya fue convertida."))
            return redirect(url_for("jobs.view_job", job_id=existing_job["id"]))

        items = conn.execute(
            """
            SELECT *
            FROM quote_items
            WHERE quote_id = %s
            ORDER BY id
            """,
            (quote_id,),
        ).fetchall()

        if not items:
            flash(_t("No items to convert.", "No hay artículos para convertir."))
            return redirect(url_for("quotes.view_quote", quote_id=quote_id))

        service_type = None
        is_recurring = False

        for i in items:
            item_type_check = (i["item_type"] or "").strip().lower()
            if item_type_check == "mowing":
                service_type = "mowing"
                is_recurring = True
                break

        quote_number = quote["quote_number"] or quote_id
        quote_title = (quote["title"] or "").strip() if "title" in quote.keys() and quote["title"] else ""
        quote_notes = (quote["notes"] or "").strip() if "notes" in quote.keys() and quote["notes"] else ""
        job_title = quote_title or _t(f"Job from Quote {quote_number}", f"Trabajo de la cotización {quote_number}")

        recurring_interval_weeks = 1
        recurring_start_date = date.today().isoformat()
        recurring_end_date = None
        recurring_start_time = None
        recurring_end_time = None
        recurring_assigned_to = None
        recurring_schedule_title = job_title
        recurring_generate_until_days = 42

        if "recurring_interval_weeks" in quote.keys() and quote["recurring_interval_weeks"]:
            try:
                recurring_interval_weeks = max(1, int(quote["recurring_interval_weeks"]))
            except Exception:
                recurring_interval_weeks = 1

        if "recurring_start_date" in quote.keys() and quote["recurring_start_date"]:
            recurring_start_date = str(quote["recurring_start_date"])

        if "recurring_end_date" in quote.keys() and quote["recurring_end_date"]:
            recurring_end_date = str(quote["recurring_end_date"])

        if "recurring_scheduled_start_time" in quote.keys() and quote["recurring_scheduled_start_time"]:
            recurring_start_time = str(quote["recurring_scheduled_start_time"])

        if "recurring_scheduled_end_time" in quote.keys() and quote["recurring_scheduled_end_time"]:
            recurring_end_time = str(quote["recurring_scheduled_end_time"])

        if "recurring_assigned_to" in quote.keys() and quote["recurring_assigned_to"]:
            recurring_assigned_to = str(quote["recurring_assigned_to"])

        if "recurring_schedule_title" in quote.keys() and quote["recurring_schedule_title"]:
            recurring_schedule_title = str(quote["recurring_schedule_title"]).strip() or job_title

        if "recurring_generate_until_days" in quote.keys() and quote["recurring_generate_until_days"]:
            try:
                recurring_generate_until_days = max(1, int(quote["recurring_generate_until_days"]))
            except Exception:
                recurring_generate_until_days = 42

        cur = conn.cursor()

        try:
            cur.execute(
                """
                INSERT INTO jobs (
                    company_id,
                    customer_id,
                    quote_id,
                    title,
                    scheduled_date,
                    scheduled_start_time,
                    scheduled_end_time,
                    assigned_to,
                    status,
                    notes,
                    service_type,
                    is_recurring
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    cid,
                    quote["customer_id"],
                    quote_id,
                    job_title,
                    recurring_start_date,
                    recurring_start_time,
                    recurring_end_time,
                    recurring_assigned_to,
                    "Scheduled",
                    quote_notes,
                    service_type,
                    is_recurring,
                ),
            )
        except Exception:
            conn.rollback()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO jobs (
                    company_id,
                    customer_id,
                    quote_id,
                    title,
                    scheduled_date,
                    status,
                    notes,
                    service_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    cid,
                    quote["customer_id"],
                    quote_id,
                    job_title,
                    recurring_start_date,
                    "Scheduled",
                    quote_notes,
                    service_type,
                ),
            )

        job_row = cur.fetchone()
        if not job_row or "id" not in job_row:
            raise Exception(_t("Failed to create job.", "No se pudo crear el trabajo."))

        job_id = job_row["id"]

        recurring_schedule_id = None

        if is_recurring:
            try:
                cur.execute(
                    """
                    INSERT INTO recurring_mowing_schedules (
                        company_id,
                        customer_id,
                        title,
                        service_type,
                        interval_weeks,
                        start_date,
                        next_run_date,
                        end_date,
                        scheduled_start_time,
                        scheduled_end_time,
                        assigned_to,
                        status_default,
                        address,
                        notes,
                        active,
                        auto_generate_until_days,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id
                    """,
                    (
                        cid,
                        quote["customer_id"],
                        recurring_schedule_title,
                        "mowing",
                        recurring_interval_weeks,
                        recurring_start_date,
                        recurring_start_date,
                        recurring_end_date,
                        recurring_start_time,
                        recurring_end_time,
                        recurring_assigned_to,
                        "Scheduled",
                        quote["address"] if "address" in quote.keys() and quote["address"] else None,
                        quote_notes or None,
                        True,
                        recurring_generate_until_days,
                    ),
                )

                schedule_row = cur.fetchone()
                if not schedule_row or "id" not in schedule_row:
                    raise Exception(_t("Failed to create recurring mowing schedule.", "No se pudo crear el horario recurrente de corte de césped."))

                recurring_schedule_id = schedule_row["id"]

                try:
                    conn.execute(
                        """
                        UPDATE jobs
                        SET recurring_schedule_id = %s,
                            generated_from_schedule = FALSE
                        WHERE id = %s AND company_id = %s
                        """,
                        (recurring_schedule_id, job_id, cid),
                    )
                except Exception:
                    try:
                        conn.execute(
                            """
                            UPDATE jobs
                            SET recurring_schedule_id = %s
                            WHERE id = %s AND company_id = %s
                            """,
                            (recurring_schedule_id, job_id, cid),
                        )
                    except Exception:
                        pass

            except Exception as e:
                raise Exception(f"{_t('Failed creating recurring schedule:', 'Error al crear el horario recurrente:')} {e}")

        for i in items:
            raw_qty = float(i["quantity"] or 0)
            price = float(i["unit_price"] or 0)
            cost = float(i["unit_cost"] or 0)
            item_type = (i["item_type"] or "material").strip().lower()
            desc = (i["description"] or "").strip()
            unit = (i["unit"] or "").strip()

            qty = raw_qty if raw_qty > 0 else 1.0

            if item_type == "dump_fee":
                qty = 1.0
                unit = ""
                cost = 0.0

            elif item_type == "labor":
                if not unit:
                    unit = _t("Hours", "Horas")
                cost = 0.0

            elif item_type == "mulch" and not unit:
                unit = _t("Yards", "Yardas")

            elif item_type == "stone" and not unit:
                unit = _t("Tons", "Toneladas")

            elif item_type == "soil" and not unit:
                unit = _t("Yards", "Yardas")

            elif item_type == "hardscape_material" and not unit:
                unit = _t("Tons", "Toneladas")

            elif item_type == "fuel" and not unit:
                unit = _t("Gallons", "Galones")

            elif item_type == "delivery" and not unit:
                unit = _t("Miles", "Millas")

            elif item_type == "equipment" and not unit:
                unit = _t("Rentals", "Alquileres")

            elif item_type == "fertilizer" and not unit:
                unit = _t("Bags", "Bolsas")

            elif item_type in ["plants", "trees", "misc"]:
                unit = ""

            if is_recurring and item_type == "mowing":
                qty = 1.0
                unit = _t("Cut", "Corte")

            line_total = qty * price
            cost_amount = qty * cost

            cur.execute(
                """
                INSERT INTO job_items (
                    job_id,
                    item_type,
                    description,
                    quantity,
                    unit,
                    unit_cost,
                    unit_price,
                    sale_price,
                    cost_amount,
                    line_total,
                    billable
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    job_id,
                    item_type,
                    desc,
                    qty,
                    unit,
                    cost,
                    price,
                    price,
                    cost_amount,
                    line_total,
                    True,
                ),
            )

            item_row = cur.fetchone()
            if not item_row or "id" not in item_row:
                raise Exception(_t(f"Failed to create job item for quote item {i['id']}.", f"No se pudo crear el artículo de trabajo para el artículo de cotización {i['id']}."))

            ensure_job_cost_ledger(conn, item_row["id"])

            if recurring_schedule_id:
                recurring_qty = raw_qty if raw_qty > 0 else 1.0
                recurring_unit = unit
                recurring_cost = cost
                recurring_price = price
                recurring_billable = True

                if item_type == "mowing":
                    recurring_qty = 1.0
                    recurring_unit = _t("Cut", "Corte")

                elif item_type == "dump_fee":
                    recurring_qty = 1.0
                    recurring_unit = ""
                    recurring_cost = 0.0

                cur.execute(
                    """
                    INSERT INTO recurring_mowing_schedule_items (
                        company_id,
                        schedule_id,
                        item_type,
                        description,
                        quantity,
                        unit,
                        unit_cost,
                        sale_price,
                        billable
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        cid,
                        recurring_schedule_id,
                        item_type,
                        desc,
                        recurring_qty,
                        recurring_unit,
                        recurring_cost,
                        recurring_price,
                        recurring_billable,
                    ),
                )

        recalc_job(conn, job_id)

        conn.execute(
            """
            UPDATE quotes
            SET status = 'Converted'
            WHERE id = %s AND company_id = %s
            """,
            (quote_id, cid),
        )

        if recurring_schedule_id and "auto_generate_recurring_jobs" in globals():
            try:
                auto_generate_recurring_jobs(conn, cid)
            except Exception:
                pass

        conn.commit()

        if recurring_schedule_id:
            flash(_t("Quote converted to job and recurring schedule created.", "La cotización se convirtió en trabajo y se creó un horario recurrente."))
        else:
            flash(_t("Quote converted to job successfully.", "La cotización se convirtió en trabajo correctamente."))

        return redirect(url_for("jobs.view_job", job_id=job_id))

    except Exception as e:
        conn.rollback()
        flash(_t(
            f"Conversion failed: {e}",
            f"La conversión falló: {e}"
        ))
        return redirect(url_for("quotes.view_quote", quote_id=quote_id))

    finally:
        conn.close()


@quotes_bp.route("/quotes/<int:quote_id>/delete", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def delete_quote(quote_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    quote = conn.execute(
        "SELECT id FROM quotes WHERE id = %s AND company_id = %s",
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        flash(_t("Quote not found.", "Cotización no encontrada."))
        return redirect(url_for("quotes.quotes"))

    conn.execute("DELETE FROM quote_items WHERE quote_id = %s", (quote_id,))
    conn.execute(
        "DELETE FROM quotes WHERE id = %s AND company_id = %s",
        (quote_id, cid),
    )

    conn.commit()
    conn.close()

    flash(_t("Quote deleted.", "Cotización eliminada."))
    return redirect(url_for("quotes.quotes"))


@quotes_bp.route("/quotes/<int:quote_id>/items/<int:item_id>/delete", methods=["POST"])
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def delete_quote_item(quote_id, item_id):
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    quote = conn.execute(
        "SELECT id FROM quotes WHERE id = %s AND company_id = %s",
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        flash(_t("Quote not found.", "Cotización no encontrada."))
        return redirect(url_for("quotes.quotes"))

    item = conn.execute(
        """
        SELECT qi.id
        FROM quote_items qi
        JOIN quotes q ON qi.quote_id = q.id
        WHERE qi.id = %s AND qi.quote_id = %s AND q.company_id = %s
        """,
        (item_id, quote_id, cid),
    ).fetchone()

    if not item:
        conn.close()
        flash(_t("Quote item not found.", "Artículo de cotización no encontrado."))
        return redirect(url_for("quotes.view_quote", quote_id=quote_id))

    conn.execute(
        "DELETE FROM quote_items WHERE id = %s AND quote_id = %s",
        (item_id, quote_id),
    )

    recalc_quote(conn, quote_id)
    conn.commit()
    conn.close()

    flash(_t("Quote line item deleted.", "Línea de cotización eliminada."))
    return redirect(url_for("quotes.view_quote", quote_id=quote_id))


@quotes_bp.route("/quotes/finished")
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def finished_quotes():
    ensure_quote_item_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT q.*, c.name AS customer_name
        FROM quotes q
        JOIN customers c ON q.customer_id = c.id
        WHERE q.company_id = %s
          AND q.status = 'Finished'
        ORDER BY q.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    quote_rows = []
    quote_mobile_cards = []

    for r in rows:
        quote_rows.append(
            f"""
            <tr>
                <td>#{r['id']}</td>
                <td>{escape(r['quote_number'] or '-')}</td>
                <td>{escape(r['customer_name'] or '-')}</td>
                <td>${float(r['total'] or 0):.2f}</td>
                <td>{escape(_status_label(r['status']))}</td>
                <td>
                    <div class='row-actions'>
                        <a class='btn secondary small' href='{url_for("quotes.view_quote", quote_id=r["id"])}'>{_t("View", "Ver")}</a>
                        <a class='btn warning small' href='{url_for("quotes.reopen_quote", quote_id=r["id"])}'>{_t("Reopen", "Reabrir")}</a>
                    </div>
                </td>
            </tr>
            """
        )

        quote_mobile_cards.append(
            f"""
            <div class='mobile-list-card'>
                <div class='mobile-list-top'>
                    <div class='mobile-list-title'>{_t("Quote", "Cotización")} #{escape(r['quote_number'] or '-')}</div>
                    <div class='mobile-badge'>{escape(_status_label(r['status']))}</div>
                </div>

                <div class='mobile-list-grid'>
                    <div><span>ID</span><strong>#{r['id']}</strong></div>
                    <div><span>{_t("Customer", "Cliente")}</span><strong>{escape(r['customer_name'] or '-')}</strong></div>
                    <div><span>{_t("Total", "Total")}</span><strong>${float(r['total'] or 0):.2f}</strong></div>
                    <div><span>{_t("Status", "Estado")}</span><strong>{escape(_status_label(r['status']))}</strong></div>
                </div>

                <div class='mobile-list-actions'>
                    <a class='btn secondary small' href='{url_for("quotes.view_quote", quote_id=r["id"])}'>{_t("View", "Ver")}</a>
                    <a class='btn warning small' href='{url_for("quotes.reopen_quote", quote_id=r["id"])}'>{_t("Reopen", "Reabrir")}</a>
                </div>
            </div>
            """
        )

    quote_rows_html = "".join(quote_rows)
    quote_mobile_cards_html = "".join(quote_mobile_cards)

    content = f"""
    <style>
        .quotes-page {{
            display:grid;
            gap:18px;
        }}

        .quotes-head {{
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:12px;
            flex-wrap:wrap;
        }}

        .table-wrap {{
            width:100%;
            overflow-x:auto;
        }}

        .mobile-only {{
            display:none;
        }}

        .desktop-only {{
            display:block;
        }}

        .mobile-list {{
            display:grid;
            gap:12px;
        }}

        .mobile-list-card {{
            border:1px solid rgba(15, 23, 42, 0.08);
            border-radius:14px;
            padding:14px;
            background:#fff;
            box-shadow:0 1px 2px rgba(15, 23, 42, 0.04);
        }}

        .mobile-list-top {{
            display:flex;
            justify-content:space-between;
            align-items:flex-start;
            gap:10px;
            margin-bottom:10px;
        }}

        .mobile-list-title {{
            font-weight:700;
            color:#0f172a;
            line-height:1.25;
            word-break:break-word;
        }}

        .mobile-badge {{
            font-size:.85rem;
            font-weight:700;
            color:#334155;
            background:#f1f5f9;
            padding:6px 10px;
            border-radius:999px;
            white-space:nowrap;
        }}

        .mobile-list-grid {{
            display:grid;
            grid-template-columns:1fr 1fr;
            gap:10px 12px;
            margin-bottom:12px;
        }}

        .mobile-list-grid span {{
            display:block;
            font-size:.78rem;
            color:#64748b;
            margin-bottom:3px;
        }}

        .mobile-list-grid strong {{
            display:block;
            color:#0f172a;
            font-size:.95rem;
            line-height:1.25;
            word-break:break-word;
        }}

        .mobile-list-actions {{
            display:flex;
            gap:8px;
            flex-wrap:wrap;
        }}

        @media (max-width: 640px) {{
            .desktop-only {{
                display:none !important;
            }}

            .mobile-only {{
                display:block !important;
            }}

            .mobile-list-grid {{
                grid-template-columns:1fr;
            }}
        }}
    </style>

    <div class='quotes-page'>
        <div class='card'>
            <div class='quotes-head'>
                <div>
                    <h1 style='margin:0;'>{_t("Finished Quotes", "Cotizaciones finalizadas")}</h1>
                    <p class='muted' style='margin:6px 0 0 0;'>{_t("Quotes tied to fully paid work.", "Cotizaciones vinculadas a trabajos totalmente pagados.")}</p>
                </div>
                <div class='row-actions'>
                    <a class='btn secondary' href='{url_for("quotes.quotes")}'>{_t("Back to Active Quotes", "Volver a cotizaciones activas")}</a>
                </div>
            </div>
        </div>

        <div class='card'>
            <div class='table-wrap desktop-only'>
                <table>
                    <tr><th>ID</th><th>{_t("Number", "Número")}</th><th>{_t("Customer", "Cliente")}</th><th>{_t("Total", "Total")}</th><th>{_t("Status", "Estado")}</th><th>{_t("Actions", "Acciones")}</th></tr>
                    {quote_rows_html or f'<tr><td colspan="6" class="muted">{_t("No finished quotes yet.", "Todavía no hay cotizaciones finalizadas.")}</td></tr>'}
                </table>
            </div>

            <div class='mobile-only'>
                <div class='mobile-list'>
                    {quote_mobile_cards_html or f"<div class='mobile-list-card muted'>{_t('No finished quotes yet.', 'Todavía no hay cotizaciones finalizadas.')}</div>"}
                </div>
            </div>
        </div>
    </div>
    """
    return render_page(content, _t("Finished Quotes", "Cotizaciones finalizadas"))


@quotes_bp.route("/quotes/<int:quote_id>/reopen")
@login_required
@subscription_required
@require_permission("can_manage_jobs")
def reopen_quote(quote_id):
    conn = get_db_connection()
    cid = session["company_id"]

    quote = conn.execute(
        """
        SELECT id
        FROM quotes
        WHERE id = %s AND company_id = %s
        """,
        (quote_id, cid),
    ).fetchone()

    if not quote:
        conn.close()
        flash(_t("Quote not found.", "Cotización no encontrada."))
        return redirect(url_for("quotes.finished_quotes"))

    conn.execute(
        """
        UPDATE quotes
        SET status = 'Converted'
        WHERE id = %s AND company_id = %s
        """,
        (quote_id, cid),
    )

    conn.commit()
    conn.close()

    flash(_t("Quote reopened.", "Cotización reabierta."))
    return redirect(url_for("quotes.view_quote", quote_id=quote_id))