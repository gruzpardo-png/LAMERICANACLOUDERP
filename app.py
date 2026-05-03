import csv
import hashlib
import io
import json
import os
import re
import secrets
import zipfile
from datetime import datetime, date, timedelta
from functools import wraps

import pandas as pd
from flask import (
    Flask,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
    flash,
    make_response,
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename


BASE_DIR = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-change-this-secret-key")

_database_url = os.getenv("DATABASE_URL", f"sqlite:///{os.path.join(BASE_DIR, 'lamericana_cloud.sqlite3')}")
# Render sometimes exposes postgres://; SQLAlchemy expects postgresql://
if _database_url.startswith("postgres://"):
    _database_url = _database_url.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = _database_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024

db = SQLAlchemy(app)


# =========================================================
# Utilities
# =========================================================

def utcnow():
    return datetime.utcnow()


def clp_to_int(value):
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(round(float(value)))
    s = str(value).strip()
    if not s or s in {"--", "nan", "None"}:
        return 0
    s = s.replace("$", "").replace(" ", "")
    # Chilean format 7.390; decimal commas are ignored for prices.
    s = re.sub(r"[^0-9]", "", s)
    return int(s) if s else 0


def weight_to_float(value):
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).lower().replace("kg", "").replace(",", ".").strip()
    m = re.search(r"[-+]?\d*\.\d+|\d+", s)
    return float(m.group()) if m else 0.0


def parse_datetime_safe(value):
    if isinstance(value, datetime):
        return value
    if not value:
        return utcnow()
    s = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt)
        except Exception:
            pass
    try:
        return pd.to_datetime(s).to_pydatetime()
    except Exception:
        return utcnow()


def parse_date_arg(name, default=None):
    value = request.args.get(name) or request.form.get(name)
    if not value:
        return default
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return default


def fmt_clp(value):
    try:
        return "$" + f"{int(round(float(value))):,}".replace(",", ".")
    except Exception:
        return "$0"


def bool_from_form(name):
    return request.form.get(name) in {"on", "true", "1", "yes"}


@app.template_filter("clp")
def jinja_clp(value):
    return fmt_clp(value)


@app.template_filter("kg")
def jinja_kg(value):
    try:
        return f"{float(value):,.3f}".replace(",", "X").replace(".", ",").replace("X", ".") + " kg"
    except Exception:
        return "0,000 kg"


# =========================================================
# Models
# =========================================================

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    code = db.Column(db.String(20), nullable=False, default="--")
    full_name = db.Column(db.String(160), nullable=False, default="")
    role = db.Column(db.String(30), nullable=False, default="operator")
    active = db.Column(db.Boolean, nullable=False, default=True)
    permissions_json = db.Column(db.Text, nullable=False, default="{}")
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=utcnow, onupdate=utcnow)

    @property
    def permissions(self):
        try:
            return json.loads(self.permissions_json or "{}")
        except Exception:
            return {}

    @permissions.setter
    def permissions(self, value):
        self.permissions_json = json.dumps(value or {}, ensure_ascii=False)

    def has(self, perm):
        if self.role == "admin":
            return True
        return bool(self.permissions.get(perm, False))

    def to_api(self, include_hash=False):
        data = {
            "username": self.username,
            "code": self.code,
            "full_name": self.full_name,
            "role": self.role,
            "active": self.active,
            "permissions": self.permissions,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
        if include_hash:
            data["password_hash"] = self.password_hash
        return data


class Family(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True, nullable=False, index=True)
    display_name = db.Column(db.String(120), nullable=False)
    active = db.Column(db.Boolean, nullable=False, default=True)
    order_index = db.Column(db.Integer, nullable=False, default=0)
    updated_at = db.Column(db.DateTime, nullable=False, default=utcnow, onupdate=utcnow)

    def to_api(self):
        return {
            "name": self.name,
            "display_name": self.display_name,
            "active": self.active,
            "order_index": self.order_index,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class PriceProduct(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    family = db.Column(db.String(80), nullable=False, index=True)
    product_code = db.Column(db.String(80), nullable=False, index=True)
    description = db.Column(db.String(255), nullable=False, default="")
    gross_price = db.Column(db.Integer, nullable=False, default=0, index=True)
    active = db.Column(db.Boolean, nullable=False, default=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        db.UniqueConstraint("family", "product_code", "gross_price", name="uq_price_family_code_price"),
    )

    def to_api(self):
        return {
            "family": self.family,
            "product_code": self.product_code,
            "description": self.description,
            "gross_price": self.gross_price,
            "active": self.active,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class Production(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    local_uuid = db.Column(db.String(80), unique=True, nullable=False, index=True)
    timestamp = db.Column(db.DateTime, nullable=False, index=True)
    usuario = db.Column(db.String(80), nullable=False, default="")
    codigo_usuario = db.Column(db.String(20), nullable=False, default="")
    terminal = db.Column(db.String(20), nullable=False, default="")
    familia = db.Column(db.String(80), nullable=False, default="")
    modo = db.Column(db.String(120), nullable=False, default="")
    modo_impresion = db.Column(db.String(20), nullable=False, default="")
    precio_int = db.Column(db.Integer, nullable=False, default=0)
    peso_kg = db.Column(db.Float, nullable=False, default=0.0)
    codigo_producto = db.Column(db.String(120), nullable=False, default="")
    descripcion = db.Column(db.String(255), nullable=False, default="")
    idx = db.Column(db.String(120), nullable=False, default="")
    in_desc = db.Column(db.String(255), nullable=False, default="")
    fecha_balanza = db.Column(db.String(30), nullable=False, default="")
    hora_balanza = db.Column(db.String(30), nullable=False, default="")
    raw_json = db.Column(db.Text, nullable=False, default="{}")
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "local_uuid": self.local_uuid,
            "timestamp": self.timestamp.strftime("%Y-%m-%d %H:%M:%S") if self.timestamp else "",
            "usuario": self.usuario,
            "codigo_usuario": self.codigo_usuario,
            "terminal": self.terminal,
            "familia": self.familia,
            "modo": self.modo,
            "modo_impresion": self.modo_impresion,
            "precio": self.precio_int,
            "peso": self.peso_kg,
            "codigo_producto": self.codigo_producto,
            "descripcion": self.descripcion,
            "idx": self.idx,
            "in": self.in_desc,
            "fecha_balanza": self.fecha_balanza,
            "hora_balanza": self.hora_balanza,
        }


class ApiSession(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    token_hash = db.Column(db.String(128), unique=True, nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    terminal = db.Column(db.String(40), nullable=False, default="")
    device_id = db.Column(db.String(120), nullable=False, default="")
    expires_at = db.Column(db.DateTime, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow)
    user = db.relationship("User")


class AuditLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    actor = db.Column(db.String(80), nullable=False, default="system")
    action = db.Column(db.String(120), nullable=False)
    detail = db.Column(db.Text, nullable=False, default="")
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow)


class Setting(db.Model):
    key = db.Column(db.String(80), primary_key=True)
    value = db.Column(db.Text, nullable=False, default="")
    updated_at = db.Column(db.DateTime, nullable=False, default=utcnow, onupdate=utcnow)


# =========================================================
# Auth helpers
# =========================================================

def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    return User.query.get(uid)


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = current_user()
        if not user or not user.active:
            return redirect(url_for("login", next=request.path))
        return fn(*args, **kwargs)
    return wrapper


def role_required(*roles):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user or user.role not in roles:
                flash("No tienes permiso para esta sección.", "error")
                return redirect(url_for("dashboard"))
            return fn(*args, **kwargs)
        return wrapper
    return deco


def token_hash(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def api_user_from_request():
    auth = request.headers.get("Authorization", "")
    if not auth.lower().startswith("bearer "):
        return None, None
    token = auth.split(" ", 1)[1].strip()
    rec = ApiSession.query.filter_by(token_hash=token_hash(token)).first()
    if not rec or rec.expires_at < utcnow() or not rec.user.active:
        return None, None
    return rec.user, rec


def api_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user, api_session = api_user_from_request()
        if not user:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        request.api_user = user
        request.api_session = api_session
        return fn(*args, **kwargs)
    return wrapper


@app.context_processor
def inject_base():
    return {"current_user": current_user(), "fmt_clp": fmt_clp}


# =========================================================
# Seed / init
# =========================================================

DEFAULT_PERMISSIONS = {
    "manage_users": False,
    "manage_prices": False,
    "view_dashboard": True,
    "view_production": True,
    "print_labels": True,
    "use_target_price": True,
    "connect_scale": True,
}


def seed_database():
    db.create_all()
    if not User.query.filter_by(username=os.getenv("ADMIN_USER", "gustavo")).first():
        admin = User(
            username=os.getenv("ADMIN_USER", "gustavo").lower(),
            password_hash=generate_password_hash(os.getenv("ADMIN_PASSWORD", "1176")),
            code=os.getenv("ADMIN_CODE", "GS"),
            full_name=os.getenv("ADMIN_NAME", "Gustavo"),
            role="admin",
            active=True,
        )
        admin.permissions = {k: True for k in DEFAULT_PERMISSIONS.keys()}
        db.session.add(admin)
        db.session.add(AuditLog(actor="system", action="seed_admin", detail="Administrador inicial creado."))

    if Family.query.count() == 0:
        defaults = [("vestuario", "Vestuario"), ("hogar", "Hogar"), ("zapatillas", "Zapatillas"), ("bolsos", "Bolsos")]
        for idx, (name, display) in enumerate(defaults, start=1):
            db.session.add(Family(name=name, display_name=display, order_index=idx, active=True))

    for key, val in {
        "brand_name": "La Americana",
        "footer_brand": "RUZ Technology company",
        "sync_valid_hours": "24",
        "shoe_family_names": "zapatillas,zapatos,calzado",
    }.items():
        if not Setting.query.get(key):
            db.session.add(Setting(key=key, value=val))
    db.session.commit()


with app.app_context():
    seed_database()


# =========================================================
# Business logic
# =========================================================

def setting_value(key, default=""):
    rec = Setting.query.get(key)
    return rec.value if rec else default


def shoe_family_set():
    raw = setting_value("shoe_family_names", "zapatillas,zapatos,calzado")
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def build_filtered_query():
    q = Production.query
    start_d = parse_date_arg("start")
    end_d = parse_date_arg("end")
    familia = (request.args.get("familia") or "").strip().lower()
    in_desc = (request.args.get("in") or "").strip()
    usuario = (request.args.get("usuario") or "").strip().lower()
    terminal = (request.args.get("terminal") or "").strip()

    if start_d:
        q = q.filter(Production.timestamp >= datetime.combine(start_d, datetime.min.time()))
    if end_d:
        q = q.filter(Production.timestamp < datetime.combine(end_d + timedelta(days=1), datetime.min.time()))
    if familia:
        q = q.filter(func.lower(Production.familia) == familia)
    if in_desc:
        q = q.filter(Production.in_desc.ilike(f"%{in_desc}%"))
    if usuario:
        q = q.filter(func.lower(Production.usuario) == usuario)
    if terminal:
        q = q.filter(Production.terminal == terminal)
    return q


def aggregate_rows(rows, group_by="date"):
    shoes = shoe_family_set()
    groups = {}

    def group_key(row):
        if group_by == "date":
            return row.timestamp.date().isoformat()
        if group_by == "in":
            return row.in_desc or "--"
        if group_by == "familia":
            return row.familia or "--"
        if group_by == "usuario":
            return row.usuario or "--"
        return "Total"

    raw_by_group = {}
    non_shoe_by_group = {}
    shoe_buckets = {}

    for row in rows:
        g = group_key(row)
        raw_by_group[g] = raw_by_group.get(g, 0) + 1
        fam = (row.familia or "").lower().strip()
        if fam in shoes:
            # Approximate pairing rule: two shoe labels equal one production unit.
            # A single unpaired label remains as raw label but is not counted as a unit.
            bucket_key = (
                g,
                row.codigo_producto or "",
                int(row.precio_int or 0),
                round(float(row.peso_kg or 0), 3),
                row.in_desc or "",
                row.terminal or "",
                row.usuario or "",
            )
            if bucket_key not in shoe_buckets:
                shoe_buckets[bucket_key] = {"count": 0, "price": row.precio_int or 0, "peso": row.peso_kg or 0.0}
            shoe_buckets[bucket_key]["count"] += 1
        else:
            if g not in non_shoe_by_group:
                non_shoe_by_group[g] = {"units": 0, "amount": 0, "kg": 0.0}
            non_shoe_by_group[g]["units"] += 1
            non_shoe_by_group[g]["amount"] += int(row.precio_int or 0)
            non_shoe_by_group[g]["kg"] += float(row.peso_kg or 0.0)

    for g, raw_count in raw_by_group.items():
        groups[g] = {"key": g, "raw_labels": raw_count, "units": 0, "amount": 0, "kg": 0.0}
        if g in non_shoe_by_group:
            groups[g]["units"] += non_shoe_by_group[g]["units"]
            groups[g]["amount"] += non_shoe_by_group[g]["amount"]
            groups[g]["kg"] += non_shoe_by_group[g]["kg"]

    for bucket_key, data in shoe_buckets.items():
        g = bucket_key[0]
        if g not in groups:
            groups[g] = {"key": g, "raw_labels": 0, "units": 0, "amount": 0, "kg": 0.0}
        units = int(data["count"] // 2)
        groups[g]["units"] += units
        groups[g]["amount"] += units * int(data["price"] or 0)
        groups[g]["kg"] += units * float(data["peso"] or 0.0)

    return sorted(groups.values(), key=lambda x: str(x["key"]))


def total_from_aggregate(items):
    return {
        "raw_labels": sum(x["raw_labels"] for x in items),
        "units": sum(x["units"] for x in items),
        "amount": sum(x["amount"] for x in items),
        "kg": sum(x["kg"] for x in items),
    }


def parse_price_excel(file_stream):
    xl = pd.ExcelFile(file_stream)
    imported = 0
    errors = []
    for sheet in xl.sheet_names:
        family = str(sheet).strip().lower()
        if not family:
            continue
        fam = Family.query.filter_by(name=family).first()
        if not fam:
            fam = Family(name=family, display_name=family.title(), active=True, order_index=Family.query.count() + 1)
            db.session.add(fam)
            db.session.flush()

        # Replace family prices on each upload to avoid duplicates and stale prices.
        PriceProduct.query.filter_by(family=family).delete()
        try:
            df = pd.read_excel(xl, sheet_name=sheet, header=None)
            df = df.iloc[:, :3].copy()
            df.columns = ["product_code", "description", "gross_price"]
            df = df.dropna(how="all")
            for _, r in df.iterrows():
                product_code = normalize_excel_code(r.get("product_code"))
                desc = str(r.get("description") or "").strip()
                price = clp_to_int(r.get("gross_price"))
                if not product_code or price <= 0:
                    continue
                db.session.add(PriceProduct(
                    family=family,
                    product_code=product_code,
                    description=desc,
                    gross_price=price,
                    active=True,
                ))
                imported += 1
        except Exception as exc:
            errors.append(f"{sheet}: {exc}")
    db.session.commit()
    return imported, errors


def normalize_excel_code(value):
    if pd.isna(value):
        return ""
    if isinstance(value, (int, float)):
        try:
            if float(value).is_integer():
                return str(int(value))
        except Exception:
            pass
    s = str(value).strip()
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return s


def production_from_payload(item):
    raw = dict(item)
    local_uuid = str(item.get("local_uuid") or item.get("uuid") or secrets.token_hex(16))
    timestamp = parse_datetime_safe(item.get("timestamp"))
    return Production(
        local_uuid=local_uuid,
        timestamp=timestamp,
        usuario=str(item.get("usuario") or item.get("user") or ""),
        codigo_usuario=str(item.get("codigo_usuario") or item.get("user_code") or ""),
        terminal=str(item.get("terminal") or ""),
        familia=str(item.get("familia") or "").strip().lower(),
        modo=str(item.get("modo") or ""),
        modo_impresion=str(item.get("modo_impresion") or item.get("mode_print") or ""),
        precio_int=clp_to_int(item.get("precio") or item.get("precio_int")),
        peso_kg=weight_to_float(item.get("peso") or item.get("peso_kg")),
        codigo_producto=str(item.get("codigo_producto") or item.get("product_code") or ""),
        descripcion=str(item.get("descripcion") or item.get("description") or ""),
        idx=str(item.get("idx") or ""),
        in_desc=str(item.get("in_desc") or item.get("in") or ""),
        fecha_balanza=str(item.get("fecha_balanza") or ""),
        hora_balanza=str(item.get("hora_balanza") or ""),
        raw_json=json.dumps(raw, ensure_ascii=False),
    )


def parse_legacy_history_text(text):
    rows = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        if lineno == 1 and line.lower().startswith("timestamp,"):
            continue
        parts = line.rstrip("\n").split(",")
        if len(parts) < 10:
            continue
        try:
            # Newer rows from current software have 15 columns:
            # timestamp, usuario, codigo_usuario, terminal, familia, modo, modo_impresion,
            # precio, peso, codigo_producto, descripcion, idx, in, fecha_balanza, hora_balanza
            if len(parts) >= 15 and parts[4].strip().lower() in {"vestuario", "hogar", "zapatillas", "bolsos", "zapatos", "calzado"}:
                item = {
                    "timestamp": parts[0],
                    "usuario": parts[1],
                    "codigo_usuario": parts[2],
                    "terminal": parts[3],
                    "familia": parts[4],
                    "modo": parts[5],
                    "modo_impresion": parts[6],
                    "precio": parts[7],
                    "peso": parts[8],
                    "codigo_producto": parts[9],
                    "descripcion": parts[10],
                    "idx": parts[11],
                    "in": ",".join(parts[12:-2]),
                    "fecha_balanza": parts[-2],
                    "hora_balanza": parts[-1],
                }
            else:
                # Older rows have 12 columns and no family/description/mode_print.
                item = {
                    "timestamp": parts[0],
                    "usuario": parts[1],
                    "codigo_usuario": parts[2],
                    "terminal": parts[3],
                    "familia": "",
                    "modo": parts[4],
                    "modo_impresion": "",
                    "precio": parts[5],
                    "peso": parts[6],
                    "codigo_producto": parts[7],
                    "descripcion": "",
                    "idx": parts[8],
                    "in": ",".join(parts[9:-2]),
                    "fecha_balanza": parts[-2],
                    "hora_balanza": parts[-1],
                }
            item["local_uuid"] = hashlib.sha256(line.encode("utf-8", errors="ignore")).hexdigest()
            rows.append(item)
        except Exception:
            continue
    return rows


def normalize_production_column(name):
    """Unifica nombres de columnas provenientes de CSV/Excel exportados o históricos."""
    key = str(name or "").strip().lower()
    key = key.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    key = key.replace(" ", "_").replace("-", "_")
    aliases = {
        "fecha": "timestamp",
        "fecha_hora": "timestamp",
        "date": "timestamp",
        "user": "usuario",
        "usuario": "usuario",
        "codigo_user": "codigo_usuario",
        "codigo_usuario": "codigo_usuario",
        "codigo_etiqueta": "codigo_usuario",
        "terminal": "terminal",
        "familia": "familia",
        "modo": "modo",
        "modo_impresion": "modo_impresion",
        "mode_print": "modo_impresion",
        "precio": "precio",
        "precio_int": "precio",
        "monto": "precio",
        "peso": "peso",
        "peso_kg": "peso",
        "codigo": "codigo_producto",
        "codigo_producto": "codigo_producto",
        "product_code": "codigo_producto",
        "descripcion": "descripcion",
        "description": "descripcion",
        "idx": "idx",
        "in": "in",
        "in_desc": "in",
        "fecha_balanza": "fecha_balanza",
        "hora": "hora_balanza",
        "hora_balanza": "hora_balanza",
        "local_uuid": "local_uuid",
    }
    return aliases.get(key, key)


def parse_uploaded_production_file(upload):
    """Importa producción anterior desde CSV histórico o Excel exportado por el sistema."""
    filename = secure_filename(upload.filename or "").lower()
    raw = upload.read()
    if not raw:
        return []

    if filename.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(raw))
        df.columns = [normalize_production_column(c) for c in df.columns]
        rows = []
        for _, r in df.iterrows():
            item = {
                "timestamp": r.get("timestamp"),
                "usuario": r.get("usuario"),
                "codigo_usuario": r.get("codigo_usuario"),
                "terminal": r.get("terminal"),
                "familia": r.get("familia"),
                "modo": r.get("modo"),
                "modo_impresion": r.get("modo_impresion"),
                "precio": r.get("precio"),
                "peso": r.get("peso"),
                "codigo_producto": r.get("codigo_producto"),
                "descripcion": r.get("descripcion"),
                "idx": r.get("idx"),
                "in": r.get("in"),
                "fecha_balanza": r.get("fecha_balanza"),
                "hora_balanza": r.get("hora_balanza"),
            }
            provided_uuid = str(r.get("local_uuid") or "").strip()
            if provided_uuid and provided_uuid.lower() not in {"nan", "none"}:
                item["local_uuid"] = provided_uuid
            else:
                signature = json.dumps(item, default=str, ensure_ascii=False, sort_keys=True)
                item["local_uuid"] = hashlib.sha256(signature.encode("utf-8", errors="ignore")).hexdigest()
            if any(str(item.get(k) or "").strip() for k in ("timestamp", "usuario", "precio", "codigo_producto", "in")):
                rows.append(item)
        return rows

    text = raw.decode("utf-8-sig", errors="replace")
    return parse_legacy_history_text(text)


def apply_user_form(user):
    """Aplica el formulario de usuarios sobre un objeto User existente o nuevo."""
    username = request.form.get("username", "").strip().lower()
    if not username:
        raise ValueError("Usuario requerido.")

    duplicate = User.query.filter(User.username == username, User.id != (user.id or 0)).first()
    if duplicate:
        raise ValueError(f"Ya existe otro usuario con el nombre '{username}'.")

    user.username = username
    user.code = request.form.get("code", "--").strip().upper() or "--"
    user.full_name = request.form.get("full_name", "").strip() or user.code
    user.role = request.form.get("role", "operator")
    user.active = bool_from_form("active")

    password = request.form.get("password", "").strip()
    if password:
        user.password_hash = generate_password_hash(password)
    elif not user.password_hash:
        raise ValueError("La clave es obligatoria para usuarios nuevos.")

    user.permissions = {
        "manage_users": bool_from_form("manage_users"),
        "manage_prices": bool_from_form("manage_prices"),
        "view_dashboard": bool_from_form("view_dashboard"),
        "view_production": bool_from_form("view_production"),
        "print_labels": bool_from_form("print_labels"),
        "use_target_price": bool_from_form("use_target_price"),
        "connect_scale": bool_from_form("connect_scale"),
    }
    return user




# =========================================================
# Backup helpers
# =========================================================

def iso_dt(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    return value


def backup_snapshot():
    """Genera un respaldo lógico completo sin borrar ni modificar la base."""
    users = [u.to_api(include_hash=True) for u in User.query.order_by(User.username.asc()).all()]
    families = [f.to_api() for f in Family.query.order_by(Family.order_index.asc(), Family.name.asc()).all()]
    prices = [p.to_api() for p in PriceProduct.query.order_by(PriceProduct.family.asc(), PriceProduct.gross_price.asc()).all()]
    production = [p.to_dict() for p in Production.query.order_by(Production.timestamp.desc()).all()]
    settings = {s.key: s.value for s in Setting.query.order_by(Setting.key.asc()).all()}
    audit = [
        {
            "id": a.id,
            "actor": a.actor,
            "action": a.action,
            "detail": a.detail,
            "created_at": iso_dt(a.created_at),
        }
        for a in AuditLog.query.order_by(AuditLog.created_at.desc()).limit(5000).all()
    ]
    return {
        "app": "lamericana-cloud",
        "schema_version": 1,
        "created_at": utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "counts": {
            "users": len(users),
            "families": len(families),
            "prices": len(prices),
            "production": len(production),
            "settings": len(settings),
            "audit": len(audit),
        },
        "data": {
            "users": users,
            "families": families,
            "prices": prices,
            "production": production,
            "settings": settings,
            "audit": audit,
        },
    }


def dataframe_from_records(records):
    return pd.DataFrame(records or [])



def load_backup_payload(upload):
    """Lee respaldo .json o .zip generado por La Americana Cloud."""
    filename = secure_filename(upload.filename or "").lower()
    raw = upload.read()
    if not raw:
        raise ValueError("El archivo está vacío.")

    if filename.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(raw), "r") as zf:
            json_names = [n for n in zf.namelist() if n.lower().endswith(".json")]
            if not json_names:
                raise ValueError("El ZIP no contiene un archivo JSON de respaldo.")
            # Preferir el respaldo técnico principal.
            json_name = sorted(json_names, key=lambda n: ("backup" not in n.lower(), n))[0]
            payload = zf.read(json_name).decode("utf-8-sig", errors="replace")
    elif filename.endswith(".json"):
        payload = raw.decode("utf-8-sig", errors="replace")
    else:
        raise ValueError("Formato no válido. Sube un respaldo .zip o .json generado por el sistema.")

    try:
        snapshot = json.loads(payload)
    except Exception as exc:
        raise ValueError(f"No se pudo leer el JSON del respaldo: {exc}")

    if snapshot.get("app") != "lamericana-cloud" or "data" not in snapshot:
        raise ValueError("El archivo no parece ser un respaldo válido de La Americana Cloud.")
    return snapshot


def restore_backup_snapshot(snapshot, options):
    """Restaura un respaldo lógico con modo seguro.

    Por defecto NO borra producción. La producción se fusiona por local_uuid.
    Las tablas maestras pueden reemplazarse si options['replace_masters'] es True.
    """
    data = snapshot.get("data") or {}
    stats = {
        "users_inserted": 0,
        "users_updated": 0,
        "families_inserted": 0,
        "families_updated": 0,
        "prices_inserted": 0,
        "prices_updated": 0,
        "production_inserted": 0,
        "production_skipped": 0,
        "settings_upserted": 0,
    }

    restore_users = options.get("restore_users", True)
    restore_families = options.get("restore_families", True)
    restore_prices = options.get("restore_prices", True)
    restore_production = options.get("restore_production", True)
    restore_settings = options.get("restore_settings", True)
    replace_masters = options.get("replace_masters", False)
    wipe_production = options.get("wipe_production", False)

    if replace_masters:
        # Cuidado: no se toca Production salvo opción explícita separada.
        if restore_prices:
            PriceProduct.query.delete()
        if restore_families:
            Family.query.delete()
        if restore_users:
            # Borrar sesiones primero por FK y luego usuarios.
            ApiSession.query.delete()
            User.query.delete()
        if restore_settings:
            Setting.query.delete()
        db.session.flush()

    if restore_users:
        users = data.get("users") or []
        if replace_masters and not users:
            raise ValueError("El respaldo no contiene usuarios; se canceló para no dejar el sistema sin accesos.")
        for item in users:
            username = str(item.get("username") or "").strip().lower()
            if not username:
                continue
            user = User.query.filter_by(username=username).first()
            is_new = False
            if not user:
                user = User(username=username, password_hash=item.get("password_hash") or generate_password_hash(secrets.token_urlsafe(12)))
                is_new = True
            elif item.get("password_hash"):
                user.password_hash = item.get("password_hash")
            user.code = str(item.get("code") or "--").strip().upper() or "--"
            user.full_name = str(item.get("full_name") or user.code or username).strip()
            user.role = str(item.get("role") or "operator").strip()
            user.active = bool(item.get("active", True))
            user.permissions = item.get("permissions") or {}
            db.session.add(user)
            stats["users_inserted" if is_new else "users_updated"] += 1

    if restore_families:
        for item in data.get("families") or []:
            name = str(item.get("name") or "").strip().lower()
            if not name:
                continue
            fam = Family.query.filter_by(name=name).first()
            is_new = False
            if not fam:
                fam = Family(name=name)
                is_new = True
            fam.display_name = str(item.get("display_name") or name.title()).strip()
            fam.active = bool(item.get("active", True))
            fam.order_index = int(item.get("order_index") or 0)
            db.session.add(fam)
            stats["families_inserted" if is_new else "families_updated"] += 1

    if restore_prices:
        for item in data.get("prices") or []:
            family = str(item.get("family") or "").strip().lower()
            code = str(item.get("product_code") or "").strip()
            price = clp_to_int(item.get("gross_price"))
            if not family or not code or price <= 0:
                continue
            prod = PriceProduct.query.filter_by(family=family, product_code=code, gross_price=price).first()
            is_new = False
            if not prod:
                prod = PriceProduct(family=family, product_code=code, gross_price=price)
                is_new = True
            prod.description = str(item.get("description") or "").strip()
            prod.active = bool(item.get("active", True))
            db.session.add(prod)
            stats["prices_inserted" if is_new else "prices_updated"] += 1

    if restore_settings:
        settings = data.get("settings") or {}
        for key, value in settings.items():
            key = str(key).strip()
            if not key:
                continue
            rec = Setting.query.get(key) or Setting(key=key)
            rec.value = str(value if value is not None else "")
            db.session.add(rec)
            stats["settings_upserted"] += 1

    if restore_production:
        if wipe_production:
            Production.query.delete()
            db.session.flush()
        for item in data.get("production") or []:
            local_uuid = str(item.get("local_uuid") or "").strip()
            if not local_uuid:
                signature = json.dumps(item, default=str, ensure_ascii=False, sort_keys=True)
                local_uuid = hashlib.sha256(signature.encode("utf-8", errors="ignore")).hexdigest()
                item["local_uuid"] = local_uuid
            if Production.query.filter_by(local_uuid=local_uuid).first():
                stats["production_skipped"] += 1
                continue
            # Normalizar claves antiguas del respaldo.
            normalized = dict(item)
            if "precio" in normalized and "precio_int" not in normalized:
                normalized["precio_int"] = normalized.get("precio")
            if "peso" in normalized and "peso_kg" not in normalized:
                normalized["peso_kg"] = normalized.get("peso")
            if "in" in normalized and "in_desc" not in normalized:
                normalized["in_desc"] = normalized.get("in")
            db.session.add(production_from_payload(normalized))
            stats["production_inserted"] += 1

    # Asegurar que exista al menos un admin activo después de restaurar.
    if User.query.filter_by(role="admin", active=True).count() == 0:
        emergency_user = os.getenv("ADMIN_USER", "gustavo").lower()
        admin = User.query.filter_by(username=emergency_user).first() or User(username=emergency_user)
        admin.password_hash = admin.password_hash or generate_password_hash(os.getenv("ADMIN_PASSWORD", "1176"))
        admin.code = admin.code or os.getenv("ADMIN_CODE", "GS")
        admin.full_name = admin.full_name or os.getenv("ADMIN_NAME", "Gustavo")
        admin.role = "admin"
        admin.active = True
        admin.permissions = {k: True for k in DEFAULT_PERMISSIONS.keys()}
        db.session.add(admin)
        stats["users_inserted"] += 1

    return stats


# =========================================================
# Web routes
# =========================================================

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        user = User.query.filter_by(username=username).first()
        if user and user.active and check_password_hash(user.password_hash, password):
            session["user_id"] = user.id
            return redirect(request.args.get("next") or url_for("dashboard"))
        flash("Usuario o clave incorrectos.", "error")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def dashboard():
    rows = build_filtered_query().order_by(Production.timestamp.asc()).all()
    by_date = aggregate_rows(rows, "date")
    by_in = aggregate_rows(rows, "in")
    by_family = aggregate_rows(rows, "familia")
    totals = total_from_aggregate(aggregate_rows(rows, "all"))
    families = Family.query.order_by(Family.order_index.asc(), Family.name.asc()).all()
    users = User.query.order_by(User.username.asc()).all()
    terminals = [x[0] for x in db.session.query(Production.terminal).distinct().order_by(Production.terminal.asc()).all() if x[0]]
    return render_template("dashboard.html", rows=rows, by_date=by_date, by_in=by_in, by_family=by_family, totals=totals, families=families, users=users, terminals=terminals)


@app.route("/produccion")
@login_required
def production_view():
    q = build_filtered_query().order_by(Production.timestamp.desc())
    rows = q.limit(1000).all()
    families = Family.query.order_by(Family.order_index.asc(), Family.name.asc()).all()
    users = User.query.order_by(User.username.asc()).all()
    return render_template("production.html", rows=rows, families=families, users=users)


@app.route("/produccion/export.csv")
@login_required
def production_export_csv():
    rows = build_filtered_query().order_by(Production.timestamp.desc()).all()
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(Production().to_dict().keys()))
    writer.writeheader()
    for row in rows:
        writer.writerow(row.to_dict())
    resp = make_response(output.getvalue())
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=produccion_lamericana.csv"
    return resp


@app.route("/produccion/export.xlsx")
@login_required
def production_export_xlsx():
    rows = [r.to_dict() for r in build_filtered_query().order_by(Production.timestamp.desc()).all()]
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, sheet_name="Produccion")
    output.seek(0)
    return send_file(output, download_name="produccion_lamericana.xlsx", as_attachment=True, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/produccion/importar", methods=["POST"])
@login_required
@role_required("admin", "supervisor")
def production_import_csv():
    file = request.files.get("file")
    if not file or not file.filename:
        flash("Debes seleccionar un archivo CSV o Excel.", "error")
        return redirect(url_for("production_view"))
    try:
        items = parse_uploaded_production_file(file)
    except Exception as exc:
        flash(f"No se pudo leer el archivo: {exc}", "error")
        return redirect(url_for("production_view"))

    inserted = 0
    skipped = 0
    for item in items:
        if Production.query.filter_by(local_uuid=item["local_uuid"]).first():
            skipped += 1
            continue
        db.session.add(production_from_payload(item))
        inserted += 1
    db.session.add(AuditLog(actor=current_user().username, action="import_production", detail=f"Archivo {secure_filename(file.filename)} · Insertados {inserted}, omitidos {skipped}"))
    db.session.commit()
    flash(f"Datos importados. Nuevos: {inserted}. Omitidos por duplicado: {skipped}.", "success")
    return redirect(url_for("production_view"))


@app.route("/usuarios", methods=["GET", "POST"])
@login_required
@role_required("admin")
def users_view():
    if request.method == "POST":
        user = User(password_hash="")
        try:
            user = apply_user_form(user)
            db.session.add(user)
            db.session.add(AuditLog(actor=current_user().username, action="create_user", detail=user.username))
            db.session.commit()
            flash("Usuario creado correctamente.", "success")
            return redirect(url_for("users_view"))
        except Exception as exc:
            db.session.rollback()
            flash(str(exc), "error")
            return redirect(url_for("users_view"))
    users = User.query.order_by(User.username.asc()).all()
    return render_template("users.html", users=users, permissions=DEFAULT_PERMISSIONS, edit_user=None)


@app.route("/usuarios/<int:user_id>/editar", methods=["GET", "POST"])
@login_required
@role_required("admin")
def user_edit(user_id):
    user = User.query.get_or_404(user_id)
    if request.method == "POST":
        original_username = user.username
        try:
            if user.id == current_user().id and not bool_from_form("active"):
                raise ValueError("No puedes desactivar tu propio usuario.")
            user = apply_user_form(user)
            db.session.add(user)
            db.session.add(AuditLog(actor=current_user().username, action="edit_user", detail=f"{original_username} -> {user.username}"))
            db.session.commit()
            flash("Usuario actualizado correctamente.", "success")
            return redirect(url_for("users_view"))
        except Exception as exc:
            db.session.rollback()
            flash(str(exc), "error")
            return redirect(url_for("user_edit", user_id=user_id))

    users = User.query.order_by(User.username.asc()).all()
    return render_template("users.html", users=users, permissions=DEFAULT_PERMISSIONS, edit_user=user)


@app.route("/usuarios/<int:user_id>/eliminar", methods=["POST"])
@login_required
@role_required("admin")
def user_delete(user_id):
    user = User.query.get_or_404(user_id)
    if user.id == current_user().id:
        flash("No puedes desactivar tu propio usuario.", "error")
    else:
        user.active = False
        db.session.add(AuditLog(actor=current_user().username, action="disable_user", detail=user.username))
        db.session.commit()
        flash("Usuario desactivado.", "success")
    return redirect(url_for("users_view"))


@app.route("/usuarios/<int:user_id>/activar", methods=["POST"])
@login_required
@role_required("admin")
def user_activate(user_id):
    user = User.query.get_or_404(user_id)
    user.active = True
    db.session.add(AuditLog(actor=current_user().username, action="activate_user", detail=user.username))
    db.session.commit()
    flash("Usuario activado.", "success")
    return redirect(url_for("users_view"))


@app.route("/precios", methods=["GET", "POST"])
@login_required
@role_required("admin", "supervisor")
def prices_view():
    if request.method == "POST":
        file = request.files.get("file")
        if not file:
            flash("Debes seleccionar un Excel de precios.", "error")
            return redirect(url_for("prices_view"))
        imported, errors = parse_price_excel(file)
        db.session.add(AuditLog(actor=current_user().username, action="upload_prices", detail=f"{imported} precios importados"))
        db.session.commit()
        if errors:
            flash("Excel cargado con observaciones: " + " | ".join(errors), "error")
        else:
            flash(f"Precios importados correctamente: {imported} registros.", "success")
        return redirect(url_for("prices_view"))
    families = Family.query.order_by(Family.order_index.asc(), Family.name.asc()).all()
    counts = dict(db.session.query(PriceProduct.family, func.count(PriceProduct.id)).group_by(PriceProduct.family).all())
    sample_prices = PriceProduct.query.order_by(PriceProduct.family.asc(), PriceProduct.gross_price.asc()).limit(200).all()
    return render_template("prices.html", families=families, counts=counts, sample_prices=sample_prices)


@app.route("/familias", methods=["POST"])
@login_required
@role_required("admin", "supervisor")
def families_save():
    name = request.form.get("name", "").strip().lower()
    display_name = request.form.get("display_name", "").strip()
    if name:
        fam = Family.query.filter_by(name=name).first() or Family(name=name)
        fam.display_name = display_name or name.title()
        fam.active = bool_from_form("active")
        fam.order_index = int(request.form.get("order_index") or 0)
        db.session.add(fam)
        db.session.commit()
        flash("Familia guardada.", "success")
    return redirect(url_for("prices_view"))


@app.route("/configuracion", methods=["GET", "POST"])
@login_required
@role_required("admin")
def settings_view():
    if request.method == "POST":
        for key in ["brand_name", "footer_brand", "sync_valid_hours", "shoe_family_names", "whatsapp_number", "instagram_url", "facebook_url", "jumpseller_url"]:
            rec = Setting.query.get(key) or Setting(key=key)
            rec.value = request.form.get(key, "").strip()
            db.session.add(rec)
        db.session.add(AuditLog(actor=current_user().username, action="save_settings", detail="configuracion"))
        db.session.commit()
        flash("Configuración guardada.", "success")
        return redirect(url_for("settings_view"))
    settings = {s.key: s.value for s in Setting.query.all()}
    return render_template("settings.html", settings=settings)



@app.route("/respaldos")
@login_required
@role_required("admin")
def backups_view():
    counts = {
        "Usuarios": User.query.count(),
        "Familias": Family.query.count(),
        "Precios": PriceProduct.query.count(),
        "Producción": Production.query.count(),
        "Configuraciones": Setting.query.count(),
    }
    last_backup = AuditLog.query.filter_by(action="download_backup").order_by(AuditLog.created_at.desc()).first()
    last_restore = AuditLog.query.filter_by(action="restore_backup").order_by(AuditLog.created_at.desc()).first()
    return render_template("backups.html", counts=counts, last_backup=last_backup, last_restore=last_restore)


@app.route("/respaldos/descargar.zip")
@login_required
@role_required("admin")
def backup_download_zip():
    snapshot = backup_snapshot()
    stamp = utcnow().strftime("%Y%m%d_%H%M%S")
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            f"lamericana_backup_{stamp}.json",
            json.dumps(snapshot, ensure_ascii=False, indent=2, default=str),
        )

        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            dataframe_from_records(snapshot["data"]["production"]).to_excel(writer, index=False, sheet_name="Produccion")
            dataframe_from_records(snapshot["data"]["prices"]).to_excel(writer, index=False, sheet_name="Precios")
            dataframe_from_records(snapshot["data"]["families"]).to_excel(writer, index=False, sheet_name="Familias")
            dataframe_from_records(snapshot["data"]["users"]).to_excel(writer, index=False, sheet_name="Usuarios")
            dataframe_from_records([{"key": k, "value": v} for k, v in snapshot["data"]["settings"].items()]).to_excel(writer, index=False, sheet_name="Configuracion")
        excel_buffer.seek(0)
        zf.writestr(f"lamericana_backup_{stamp}.xlsx", excel_buffer.getvalue())

        readme = (
            "RESPALDO LAMERICANA CLOUD\n\n"
            "Este archivo contiene un respaldo lógico de usuarios, familias, precios, producción y configuración.\n"
            "No reemplaza automáticamente la base PostgreSQL; se descarga para seguridad y recuperación manual.\n"
            "Mantén este archivo protegido, porque contiene hashes de usuarios y datos operacionales.\n"
        )
        zf.writestr("LEEME_RESPALDO.txt", readme)
    output.seek(0)
    db.session.add(AuditLog(actor=current_user().username, action="download_backup", detail=f"Respaldo ZIP {stamp}"))
    db.session.commit()
    return send_file(output, download_name=f"lamericana_backup_{stamp}.zip", as_attachment=True, mimetype="application/zip")


@app.route("/respaldos/descargar.json")
@login_required
@role_required("admin")
def backup_download_json():
    snapshot = backup_snapshot()
    stamp = utcnow().strftime("%Y%m%d_%H%M%S")
    payload = json.dumps(snapshot, ensure_ascii=False, indent=2, default=str)
    resp = make_response(payload)
    resp.headers["Content-Type"] = "application/json; charset=utf-8"
    resp.headers["Content-Disposition"] = f"attachment; filename=lamericana_backup_{stamp}.json"
    db.session.add(AuditLog(actor=current_user().username, action="download_backup", detail=f"Respaldo JSON {stamp}"))
    db.session.commit()
    return resp



@app.route("/respaldos/restaurar", methods=["POST"])
@login_required
@role_required("admin")
def backup_restore_upload():
    file = request.files.get("backup_file")
    if not file or not file.filename:
        flash("Debes seleccionar un respaldo .zip o .json.", "error")
        return redirect(url_for("backups_view"))

    confirm = (request.form.get("confirm_restore") or "").strip().upper()
    if confirm != "RESTAURAR":
        flash("Restauración cancelada: debes escribir RESTAURAR en la confirmación.", "error")
        return redirect(url_for("backups_view"))

    mode = request.form.get("restore_mode", "merge")
    wipe_production = bool_from_form("wipe_production")
    if wipe_production and (request.form.get("confirm_wipe_production") or "").strip().upper() != "BORRAR PRODUCCION":
        flash("Para borrar y reemplazar producción debes escribir BORRAR PRODUCCION. No se restauró nada.", "error")
        return redirect(url_for("backups_view"))

    # Respaldo lógico preventivo en auditoría: se registra que existía snapshot previo.
    before_counts = {
        "users": User.query.count(),
        "families": Family.query.count(),
        "prices": PriceProduct.query.count(),
        "production": Production.query.count(),
        "settings": Setting.query.count(),
    }

    try:
        snapshot = load_backup_payload(file)
        options = {
            "restore_users": bool_from_form("restore_users"),
            "restore_families": bool_from_form("restore_families"),
            "restore_prices": bool_from_form("restore_prices"),
            "restore_production": bool_from_form("restore_production"),
            "restore_settings": bool_from_form("restore_settings"),
            "replace_masters": mode == "replace_masters",
            "wipe_production": wipe_production,
        }
        # Evitar restauración vacía por accidente.
        if not any(options[k] for k in ("restore_users", "restore_families", "restore_prices", "restore_production", "restore_settings")):
            raise ValueError("Debes seleccionar al menos una sección para restaurar.")
        stats = restore_backup_snapshot(snapshot, options)
        detail = f"Archivo {secure_filename(file.filename)} · modo={mode} · opciones={options} · antes={before_counts} · resultado={stats}"
        db.session.add(AuditLog(actor=current_user().username, action="restore_backup", detail=detail[:5000]))
        db.session.commit()
        flash(
            "Respaldo restaurado. "
            f"Usuarios +{stats['users_inserted']}/{stats['users_updated']} act.; "
            f"familias +{stats['families_inserted']}/{stats['families_updated']} act.; "
            f"precios +{stats['prices_inserted']}/{stats['prices_updated']} act.; "
            f"producción +{stats['production_inserted']} nuevos, {stats['production_skipped']} duplicados; "
            f"configuración {stats['settings_upserted']}.",
            "success",
        )
    except Exception as exc:
        db.session.rollback()
        db.session.add(AuditLog(actor=current_user().username, action="restore_backup_failed", detail=f"{secure_filename(file.filename)} · {exc}"))
        db.session.commit()
        flash(f"No se pudo restaurar el respaldo: {exc}", "error")
    return redirect(url_for("backups_view"))


@app.route("/integraciones")
@login_required
def integrations_view():
    cards = [
        ("WhatsApp Cloud API", "Pendiente", "Responder clientes y derivar leads por WhatsApp."),
        ("Instagram / Facebook", "Pendiente", "Responder DM y generar campañas Meta Ads."),
        ("TikTok", "Pendiente", "Ideas de contenido y futuro tracking de campañas."),
        ("Jumpseller", "Pendiente", "Leer productos/promociones y mostrar chatbot en la tienda."),
        ("OpenAI", "Pendiente", "Generador de campañas y asistente interno de marketing."),
    ]
    return render_template("integrations.html", cards=cards)


@app.route("/auditoria")
@login_required
@role_required("admin", "supervisor")
def audit_view():
    rows = AuditLog.query.order_by(AuditLog.created_at.desc()).limit(500).all()
    return render_template("audit.html", rows=rows)


# =========================================================
# API routes for local app
# =========================================================

@app.route("/api/v1/health")
def api_health():
    return jsonify({"ok": True, "app": "lamericana-cloud", "time": utcnow().isoformat()})


@app.route("/api/v1/auth/login", methods=["POST"])
def api_login():
    payload = request.get_json(silent=True) or {}
    username = str(payload.get("username") or "").strip().lower()
    password = str(payload.get("password") or "")
    terminal = str(payload.get("terminal") or "")
    device_id = str(payload.get("device_id") or "")
    user = User.query.filter_by(username=username).first()
    if not user or not user.active or not check_password_hash(user.password_hash, password):
        return jsonify({"ok": False, "error": "invalid_credentials"}), 401
    token = secrets.token_urlsafe(40)
    expires = utcnow() + timedelta(hours=36)
    db.session.add(ApiSession(token_hash=token_hash(token), user_id=user.id, terminal=terminal, device_id=device_id, expires_at=expires))
    db.session.commit()
    return jsonify({
        "ok": True,
        "token": token,
        "expires_at": expires.isoformat(),
        "user": user.to_api(include_hash=False),
        "sync_valid_hours": int(setting_value("sync_valid_hours", "24") or 24),
        "server_time": utcnow().isoformat(),
    })


@app.route("/api/v1/bootstrap")
@api_required
def api_bootstrap():
    # Includes password hashes only because the local terminal needs a 24h offline cache.
    # Raw passwords are never sent.
    users = [u.to_api(include_hash=True) for u in User.query.order_by(User.username.asc()).all()]
    families = [f.to_api() for f in Family.query.order_by(Family.order_index.asc(), Family.name.asc()).all()]
    prices = [p.to_api() for p in PriceProduct.query.filter_by(active=True).order_by(PriceProduct.family.asc(), PriceProduct.gross_price.asc()).all()]
    settings = {s.key: s.value for s in Setting.query.all()}
    return jsonify({
        "ok": True,
        "server_time": utcnow().isoformat(),
        "users": users,
        "families": families,
        "prices": prices,
        "settings": settings,
    })


@app.route("/api/v1/production/bulk", methods=["POST"])
@api_required
def api_production_bulk():
    payload = request.get_json(silent=True) or {}
    items = payload.get("items") or []
    if isinstance(items, dict):
        items = [items]
    inserted = 0
    skipped = 0
    for item in items:
        local_uuid = str(item.get("local_uuid") or item.get("uuid") or "")
        if local_uuid and Production.query.filter_by(local_uuid=local_uuid).first():
            skipped += 1
            continue
        prod = production_from_payload(item)
        db.session.add(prod)
        inserted += 1
    if inserted:
        db.session.add(AuditLog(actor=request.api_user.username, action="sync_production", detail=f"{inserted} registros desde {request.api_session.terminal}"))
    db.session.commit()
    return jsonify({"ok": True, "inserted": inserted, "skipped": skipped})


@app.route("/api/v1/production/one", methods=["POST"])
@api_required
def api_production_one():
    payload = request.get_json(silent=True) or {}
    local_uuid = str(payload.get("local_uuid") or payload.get("uuid") or "")
    if local_uuid and Production.query.filter_by(local_uuid=local_uuid).first():
        return jsonify({"ok": True, "inserted": 0, "skipped": 1})
    db.session.add(production_from_payload(payload))
    db.session.commit()
    return jsonify({"ok": True, "inserted": 1, "skipped": 0})


@app.route("/api/v1/prices")
@api_required
def api_prices():
    prices = [p.to_api() for p in PriceProduct.query.filter_by(active=True).order_by(PriceProduct.family.asc(), PriceProduct.gross_price.asc()).all()]
    return jsonify({"ok": True, "prices": prices})


@app.errorhandler(404)
def not_found(e):
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": "not_found"}), 404
    return render_template("404.html"), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=os.getenv("FLASK_DEBUG") == "1")
