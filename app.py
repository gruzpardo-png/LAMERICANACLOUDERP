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
from sqlalchemy import func, inspect, text
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

APP_VERSION = "1.4.5"
MIN_LOCAL_VERSION_DEFAULT = "1.3.0"
LATEST_LOCAL_VERSION_DEFAULT = "1.4.0"


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
    usuario = db.Column(db.String(80), nullable=False, default="", index=True)
    codigo_usuario = db.Column(db.String(20), nullable=False, default="")
    terminal = db.Column(db.String(40), nullable=False, default="", index=True)
    familia = db.Column(db.String(80), nullable=False, default="", index=True)
    modo = db.Column(db.String(120), nullable=False, default="")
    modo_impresion = db.Column(db.String(20), nullable=False, default="")
    precio_int = db.Column(db.Integer, nullable=False, default=0, index=True)
    peso_kg = db.Column(db.Float, nullable=False, default=0.0)
    codigo_producto = db.Column(db.String(120), nullable=False, default="", index=True)
    descripcion = db.Column(db.String(255), nullable=False, default="")
    idx = db.Column(db.String(120), nullable=False, default="")
    idx_value = db.Column(db.Float, nullable=False, default=0.0, index=True)
    in_desc = db.Column(db.String(255), nullable=False, default="", index=True)
    fecha_balanza = db.Column(db.String(30), nullable=False, default="")
    hora_balanza = db.Column(db.String(30), nullable=False, default="")
    countable = db.Column(db.Boolean, nullable=False, default=True, index=True)
    count_status = db.Column(db.String(40), nullable=False, default="Cuenta", index=True)
    duplicate_reason = db.Column(db.String(255), nullable=False, default="")
    duplicate_of_uuid = db.Column(db.String(80), nullable=False, default="")
    batch_code = db.Column(db.String(100), nullable=False, default="", index=True)
    batch_name = db.Column(db.String(160), nullable=False, default="")
    device_id = db.Column(db.String(120), nullable=False, default="")
    software_version = db.Column(db.String(40), nullable=False, default="")
    raw_json = db.Column(db.Text, nullable=False, default="{}")
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow)

    def to_dict(self):
        status = getattr(self, "_calc_count_status", self.count_status or "Cuenta")
        countable = getattr(self, "_calc_countable", bool(self.countable))
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
            "precio_int": self.precio_int,
            "peso": self.peso_kg,
            "peso_kg": self.peso_kg,
            "codigo_producto": self.codigo_producto,
            "descripcion": self.descripcion,
            "idx": self.idx,
            "idx_value": self.idx_value,
            "in": self.in_desc,
            "in_desc": self.in_desc,
            "fecha_balanza": self.fecha_balanza,
            "hora_balanza": self.hora_balanza,
            "countable": countable,
            "count_status": status,
            "duplicate_reason": getattr(self, "_calc_duplicate_reason", self.duplicate_reason or ""),
            "duplicate_of_uuid": getattr(self, "_calc_duplicate_of_uuid", self.duplicate_of_uuid or ""),
            "batch_code": self.batch_code,
            "batch_name": self.batch_name,
            "device_id": self.device_id,
            "software_version": self.software_version,
        }


class Terminal(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(40), unique=True, nullable=False, index=True)
    name = db.Column(db.String(160), nullable=False, default="")
    location = db.Column(db.String(160), nullable=False, default="")
    active = db.Column(db.Boolean, nullable=False, default=True)
    authorized = db.Column(db.Boolean, nullable=False, default=True)
    device_id = db.Column(db.String(120), nullable=False, default="")
    current_version = db.Column(db.String(40), nullable=False, default="")
    min_version = db.Column(db.String(40), nullable=False, default="")
    last_seen = db.Column(db.DateTime)
    last_user = db.Column(db.String(80), nullable=False, default="")
    last_ip = db.Column(db.String(80), nullable=False, default="")
    notes = db.Column(db.Text, nullable=False, default="")
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=utcnow, onupdate=utcnow)

    def to_api(self):
        return {
            "code": self.code,
            "name": self.name,
            "location": self.location,
            "active": self.active,
            "authorized": self.authorized,
            "device_id": self.device_id,
            "current_version": self.current_version,
            "min_version": self.min_version,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "last_user": self.last_user,
        }


class PriceHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    family = db.Column(db.String(80), nullable=False, index=True)
    product_code = db.Column(db.String(80), nullable=False, index=True)
    description = db.Column(db.String(255), nullable=False, default="")
    old_price = db.Column(db.Integer, nullable=False, default=0)
    new_price = db.Column(db.Integer, nullable=False, default=0)
    action = db.Column(db.String(80), nullable=False, default="change_price")
    actor = db.Column(db.String(80), nullable=False, default="system")
    source = db.Column(db.String(120), nullable=False, default="")
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow, index=True)


class ProductionBatch(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(100), unique=True, nullable=False, index=True)
    name = db.Column(db.String(160), nullable=False, default="")
    in_desc = db.Column(db.String(255), nullable=False, default="")
    family = db.Column(db.String(80), nullable=False, default="")
    active = db.Column(db.Boolean, nullable=False, default=True)
    created_by = db.Column(db.String(80), nullable=False, default="")
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow)
    closed_at = db.Column(db.DateTime)


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
    "manage_terminals": False,
    "manage_backups": False,
}


def audit(actor, action, detail=""):
    try:
        db.session.add(AuditLog(actor=str(actor or "system"), action=str(action), detail=str(detail or "")))
    except Exception:
        pass


def table_columns(table_name):
    try:
        return {c["name"] for c in inspect(db.engine).get_columns(table_name)}
    except Exception:
        return set()


def add_column_if_missing(table_name, column_name, ddl):
    cols = table_columns(table_name)
    if column_name not in cols:
        db.session.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {ddl}"))
        db.session.commit()


def ensure_schema_migrations():
    """Migraciones seguras: agrega columnas/tablas nuevas sin borrar datos existentes."""
    db.create_all()
    add_column_if_missing("production", "idx_value", "idx_value DOUBLE PRECISION NOT NULL DEFAULT 0")
    add_column_if_missing("production", "countable", "countable BOOLEAN NOT NULL DEFAULT TRUE")
    add_column_if_missing("production", "count_status", "count_status VARCHAR(40) NOT NULL DEFAULT 'Cuenta'")
    add_column_if_missing("production", "duplicate_reason", "duplicate_reason VARCHAR(255) NOT NULL DEFAULT ''")
    add_column_if_missing("production", "duplicate_of_uuid", "duplicate_of_uuid VARCHAR(80) NOT NULL DEFAULT ''")
    add_column_if_missing("production", "batch_code", "batch_code VARCHAR(100) NOT NULL DEFAULT ''")
    add_column_if_missing("production", "batch_name", "batch_name VARCHAR(160) NOT NULL DEFAULT ''")
    add_column_if_missing("production", "device_id", "device_id VARCHAR(120) NOT NULL DEFAULT ''")
    add_column_if_missing("production", "software_version", "software_version VARCHAR(40) NOT NULL DEFAULT ''")


def seed_database():
    db.create_all()
    ensure_schema_migrations()
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

    if Terminal.query.count() == 0:
        for code in ["T1", "T2", "T3", "T4"]:
            db.session.add(Terminal(code=code, name=f"Terminal {code}", active=True, authorized=True))

    for key, val in {
        "brand_name": "La Americana",
        "footer_brand": "RUZ Technology company",
        "sync_valid_hours": "24",
        "shoe_family_names": "zapatillas,zapatos,calzado",
        "local_latest_version": LATEST_LOCAL_VERSION_DEFAULT,
        "local_min_version": MIN_LOCAL_VERSION_DEFAULT,
        "duplicate_detection_seconds": "5",
        "duplicate_families": "vestuario,hogar,bolsos",
        "logs_visible": "1",
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


def parse_int_arg(name):
    raw = (request.args.get(name) or "").strip()
    if not raw:
        return None
    cleaned = re.sub(r"[^0-9]", "", raw)
    return int(cleaned) if cleaned else None


def parse_float_arg(name):
    raw = (request.args.get(name) or "").strip().replace(",", ".")
    if not raw:
        return None
    match = re.search(r"[-+]?\d*\.?\d+", raw)
    return float(match.group()) if match else None


def build_filtered_query():
    q = Production.query
    start_d = parse_date_arg("start")
    end_d = parse_date_arg("end")
    fecha_bal_start = parse_date_arg("fecha_balanza_start")
    fecha_bal_end = parse_date_arg("fecha_balanza_end")

    familia = (request.args.get("familia") or "").strip().lower()
    in_desc = (request.args.get("in") or "").strip()
    usuario = (request.args.get("usuario") or "").strip().lower()
    terminal = (request.args.get("terminal") or "").strip()
    modo = (request.args.get("modo") or "").strip()
    codigo = (request.args.get("codigo") or "").strip()
    descripcion = (request.args.get("descripcion") or "").strip()
    idx_kilo = (request.args.get("idx_kilo") or "").strip().replace(",", ".")
    batch = (request.args.get("batch") or "").strip()

    precio_min = parse_int_arg("precio_min")
    precio_max = parse_int_arg("precio_max")
    peso_min = parse_float_arg("peso_min")
    peso_max = parse_float_arg("peso_max")

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
    if modo:
        q = q.filter(Production.modo_impresion.ilike(f"%{modo}%"))
    if codigo:
        q = q.filter(Production.codigo_producto.ilike(f"%{codigo}%"))
    if descripcion:
        q = q.filter(Production.descripcion.ilike(f"%{descripcion}%"))
    if idx_kilo:
        # IDX se guarda como: "10.50  GS T1". El prefijo corresponde al valor kilo/IDX antes de usuario-terminal.
        q = q.filter(Production.idx.ilike(f"{idx_kilo}%"))
    if batch:
        q = q.filter((Production.batch_code.ilike(f"%{batch}%")) | (Production.batch_name.ilike(f"%{batch}%")))
    if precio_min is not None:
        q = q.filter(Production.precio_int >= precio_min)
    if precio_max is not None:
        q = q.filter(Production.precio_int <= precio_max)
    if peso_min is not None:
        q = q.filter(Production.peso_kg >= peso_min)
    if peso_max is not None:
        q = q.filter(Production.peso_kg <= peso_max)
    if fecha_bal_start:
        q = q.filter(Production.fecha_balanza >= fecha_bal_start.isoformat())
    if fecha_bal_end:
        q = q.filter(Production.fecha_balanza <= fecha_bal_end.isoformat())
    return q


def idx_numeric(value):
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    s = str(value).strip().replace(",", ".")
    m = re.search(r"[-+]?\d*\.?\d+", s)
    return round(float(m.group()), 2) if m else 0.0


def duplicate_family_set():
    raw = setting_value("duplicate_families", "vestuario,hogar,bolsos")
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def duplicate_window_seconds():
    try:
        return int(setting_value("duplicate_detection_seconds", "5") or "5")
    except Exception:
        return 5


def production_count_key(row):
    return (
        (row.familia or "").strip().lower(),
        (row.usuario or "").strip().lower(),
        (row.terminal or "").strip().upper(),
        int(row.precio_int or 0),
        round(float(row.peso_kg or 0), 3),
        str(row.codigo_producto or "").strip(),
        str(row.in_desc or "").strip().lower(),
        str(row.modo_impresion or "").strip().upper(),
        round(float(getattr(row, "idx_value", 0) or idx_numeric(row.idx)), 2),
    )


def annotate_countability(rows):
    """Calcula conteo dinámico para data anterior y posterior.
    No borra registros; solo marca atributos temporales para dashboard/consulta/export.
    """
    rows_sorted = sorted(rows, key=lambda r: (r.timestamp or utcnow(), r.id or 0))
    dup_families = duplicate_family_set()
    shoes = shoe_family_set()
    window = duplicate_window_seconds()
    last_seen = {}
    for row in rows_sorted:
        fam = (row.familia or "").strip().lower()
        row._calc_countable = True
        row._calc_count_status = "Cuenta"
        row._calc_duplicate_reason = ""
        row._calc_duplicate_of_uuid = ""
        row._calc_idx_value = round(float(getattr(row, "idx_value", 0) or idx_numeric(row.idx)), 2)
        if fam in dup_families and fam not in shoes:
            key = production_count_key(row)
            ts = row.timestamp or utcnow()
            prev = last_seen.get(key)
            if prev:
                prev_ts, prev_uuid = prev
                diff = abs((ts - prev_ts).total_seconds())
                if diff <= window:
                    row._calc_countable = False
                    row._calc_count_status = "Réplica"
                    row._calc_duplicate_reason = f"réplica automática menor a {window} segundos"
                    row._calc_duplicate_of_uuid = prev_uuid or ""
            last_seen[key] = (ts, row.local_uuid)
        elif not getattr(row, "countable", True):
            row._calc_countable = False
            row._calc_count_status = row.count_status or "No cuenta"
            row._calc_duplicate_reason = row.duplicate_reason or ""
            row._calc_duplicate_of_uuid = row.duplicate_of_uuid or ""
    return rows


def apply_persistent_countability(prod):
    """Marca producción nueva al guardar, sin depender solo del cálculo de dashboard."""
    fam = (prod.familia or "").strip().lower()
    prod.idx_value = idx_numeric(prod.idx) if not prod.idx_value else round(float(prod.idx_value), 2)
    if fam not in duplicate_family_set() or fam in shoe_family_set():
        prod.countable = True
        prod.count_status = "Cuenta"
        return prod
    key = production_count_key(prod)
    window = duplicate_window_seconds()
    start_ts = (prod.timestamp or utcnow()) - timedelta(seconds=window)
    end_ts = (prod.timestamp or utcnow()) + timedelta(seconds=window)
    candidates = Production.query.filter(
        Production.timestamp >= start_ts,
        Production.timestamp <= end_ts,
        func.lower(Production.familia) == key[0],
        func.lower(Production.usuario) == key[1],
        Production.precio_int == key[3],
        Production.codigo_producto == key[5],
        Production.modo_impresion == key[7],
    ).order_by(Production.timestamp.desc()).all()
    for prev in candidates:
        if prev.local_uuid == prod.local_uuid:
            continue
        if production_count_key(prev) == key:
            prod.countable = False
            prod.count_status = "Réplica"
            prod.duplicate_reason = f"réplica automática menor a {window} segundos"
            prod.duplicate_of_uuid = prev.local_uuid
            return prod
    prod.countable = True
    prod.count_status = "Cuenta"
    return prod


def filter_by_count_status(rows):
    mode = (request.args.get("count_filter") or "all").strip().lower()
    if mode in {"cuenta", "countable", "solo_cuantificables"}:
        return [r for r in rows if getattr(r, "_calc_countable", True)]
    if mode in {"replica", "réplica", "replicas", "réplicas"}:
        return [r for r in rows if getattr(r, "_calc_count_status", "Cuenta") == "Réplica"]
    if mode in {"no_cuenta", "nocuenta"}:
        return [r for r in rows if not getattr(r, "_calc_countable", True)]
    return rows


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
        if group_by == "terminal":
            return row.terminal or "--"
        if group_by == "batch":
            return row.batch_code or row.batch_name or "--"
        return "Total"

    raw_by_group = {}
    duplicates_by_group = {}
    countable_by_group = {}
    non_shoe_by_group = {}
    shoe_buckets = {}
    shoe_pending = {}

    for row in rows:
        g = group_key(row)
        raw_by_group[g] = raw_by_group.get(g, 0) + 1
        if not getattr(row, "_calc_countable", True):
            duplicates_by_group[g] = duplicates_by_group.get(g, 0) + 1
            continue
        countable_by_group[g] = countable_by_group.get(g, 0) + 1
        fam = (row.familia or "").lower().strip()
        if fam in shoes:
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
        groups[g] = {
            "key": g,
            "raw_labels": raw_count,
            "countable_labels": countable_by_group.get(g, 0),
            "duplicates": duplicates_by_group.get(g, 0),
            "shoe_pending": 0,
            "units": 0,
            "amount": 0,
            "kg": 0.0,
        }
        if g in non_shoe_by_group:
            groups[g]["units"] += non_shoe_by_group[g]["units"]
            groups[g]["amount"] += non_shoe_by_group[g]["amount"]
            groups[g]["kg"] += non_shoe_by_group[g]["kg"]

    for bucket_key, data in shoe_buckets.items():
        g = bucket_key[0]
        if g not in groups:
            groups[g] = {"key": g, "raw_labels": 0, "countable_labels": 0, "duplicates": 0, "shoe_pending": 0, "units": 0, "amount": 0, "kg": 0.0}
        units = int(data["count"] // 2)
        pending = int(data["count"] % 2)
        groups[g]["units"] += units
        groups[g]["amount"] += units * int(data["price"] or 0)
        groups[g]["kg"] += units * float(data["peso"] or 0.0)
        groups[g]["shoe_pending"] += pending

    return sorted(groups.values(), key=lambda x: str(x["key"]))


def total_from_aggregate(items):
    return {
        "raw_labels": sum(x.get("raw_labels", 0) for x in items),
        "countable_labels": sum(x.get("countable_labels", 0) for x in items),
        "duplicates": sum(x.get("duplicates", 0) for x in items),
        "shoe_pending": sum(x.get("shoe_pending", 0) for x in items),
        "units": sum(x.get("units", 0) for x in items),
        "amount": sum(x.get("amount", 0) for x in items),
        "kg": sum(x.get("kg", 0) for x in items),
    }


def month_bounds(offset=0):
    today = date.today()
    month = today.month - offset
    year = today.year
    while month <= 0:
        month += 12
        year -= 1
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    return start, end


def rows_for_period(rows, start_date, end_date_exclusive):
    return [r for r in rows if r.timestamp and start_date <= r.timestamp.date() < end_date_exclusive]


def productivity_stats(rows):
    rows = [r for r in rows if getattr(r, "_calc_countable", True)]
    if not rows:
        return {"amount": 0, "kg": 0.0, "labels": 0, "hours": 0.0, "labels_per_hour": 0.0}
    amount = sum(int(r.precio_int or 0) for r in rows)
    kg = sum(float(r.peso_kg or 0.0) for r in rows)
    labels = len(rows)
    timestamps = sorted([r.timestamp for r in rows if r.timestamp])
    hours = 0.0
    if len(timestamps) >= 2:
        hours = max(0.0, (timestamps[-1] - timestamps[0]).total_seconds() / 3600.0)
    labels_per_hour = (labels / hours) if hours > 0 else float(labels)
    return {"amount": amount, "kg": kg, "labels": labels, "hours": hours, "labels_per_hour": labels_per_hour}


def build_productivity_matrix(rows, users):
    periods = []
    today = date.today()
    periods.append(("hoy", today, today + timedelta(days=1)))
    for offset, label in [(0, "mes_actual"), (1, "mes_1"), (2, "mes_2"), (3, "mes_3")]:
        start, end = month_bounds(offset)
        periods.append((label, start, end))
    usernames = sorted({u.username for u in users} | {r.usuario for r in rows if r.usuario})
    matrix = []
    for username in usernames:
        urows = [r for r in rows if r.usuario == username]
        row = {"usuario": username}
        for label, start, end in periods:
            row[label] = productivity_stats(rows_for_period(urows, start, end))
        matrix.append(row)
    totals = {"usuario": "TODOS LOS USUARIOS"}
    for label, start, end in periods:
        totals[label] = productivity_stats(rows_for_period(rows, start, end))
    return totals, matrix


def production_summary_stats(rows):
    """Resumen ejecutivo para dashboard. Usa la misma lógica de conteo que producción:
    - ignora réplicas/no cuantificables
    - zapatillas/calzado cuentan 2 etiquetas = 1 unidad
    """
    totals = total_from_aggregate(aggregate_rows(rows, "all"))
    return {
        "amount": totals.get("amount", 0),
        "units": totals.get("units", 0),
        "kg": totals.get("kg", 0.0),
    }


def build_user_period_summary(rows, users):
    """Tabla horizontal por usuario: hoy, mes actual, mes-1, mes-2, mes-3 y total del período filtrado."""
    today = date.today()
    periods = [
        ("hoy", today, today + timedelta(days=1)),
    ]
    for offset, label in [(0, "mes_actual"), (1, "mes_1"), (2, "mes_2"), (3, "mes_3")]:
        start, end = month_bounds(offset)
        periods.append((label, start, end))

    usernames = sorted({u.username for u in users} | {r.usuario for r in rows if r.usuario})

    def build_row(label, source_rows):
        out = {"usuario": label}
        for period_label, start, end in periods:
            out[period_label] = production_summary_stats(rows_for_period(source_rows, start, end))
        out["total_periodo"] = production_summary_stats(source_rows)
        return out

    matrix = [build_row("TODOS LOS USUARIOS", rows)]
    for username in usernames:
        matrix.append(build_row(username, [r for r in rows if r.usuario == username]))
    return matrix


def parse_price_excel(file_stream, actor="system"):
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
            audit(actor, "change_family", f"Familia creada desde Excel: {family}")
            db.session.flush()

        old_map = {(p.product_code, int(p.gross_price or 0)): p for p in PriceProduct.query.filter_by(family=family).all()}
        old_by_code = {}
        for p in old_map.values():
            old_by_code.setdefault(p.product_code, int(p.gross_price or 0))

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
                old_price = old_by_code.get(product_code, 0)
                if old_price != price:
                    db.session.add(PriceHistory(
                        family=family,
                        product_code=product_code,
                        description=desc,
                        old_price=old_price,
                        new_price=price,
                        action="change_price" if old_price else "create_price",
                        actor=actor,
                        source="excel_upload",
                    ))
                imported += 1
        except Exception as exc:
            errors.append(f"{sheet}: {exc}")
    audit(actor, "change_price", f"Carga Excel precios: {imported} registros")
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
    idx_text = str(item.get("idx") or "")
    prod = Production(
        local_uuid=local_uuid,
        timestamp=timestamp,
        usuario=str(item.get("usuario") or item.get("user") or "").strip().lower(),
        codigo_usuario=str(item.get("codigo_usuario") or item.get("user_code") or ""),
        terminal=str(item.get("terminal") or "").strip().upper(),
        familia=str(item.get("familia") or "").strip().lower(),
        modo=str(item.get("modo") or ""),
        modo_impresion=str(item.get("modo_impresion") or item.get("mode_print") or ""),
        precio_int=clp_to_int(item.get("precio") or item.get("precio_int")),
        peso_kg=round(weight_to_float(item.get("peso") or item.get("peso_kg")), 3),
        codigo_producto=str(item.get("codigo_producto") or item.get("product_code") or ""),
        descripcion=str(item.get("descripcion") or item.get("description") or ""),
        idx=idx_text,
        idx_value=idx_numeric(item.get("idx_value") if item.get("idx_value") not in (None, "") else idx_text),
        in_desc=str(item.get("in_desc") or item.get("in") or ""),
        fecha_balanza=str(item.get("fecha_balanza") or ""),
        hora_balanza=str(item.get("hora_balanza") or ""),
        batch_code=str(item.get("batch_code") or "").strip(),
        batch_name=str(item.get("batch_name") or "").strip(),
        device_id=str(item.get("device_id") or "").strip(),
        software_version=str(item.get("software_version") or "").strip(),
        raw_json=json.dumps(raw, ensure_ascii=False),
    )
    return apply_persistent_countability(prod)


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


def is_last_active_admin(user):
    """Evita dejar el sistema sin administrador activo."""
    if not user or not getattr(user, "id", None):
        return False
    if user.role != "admin" or not user.active:
        return False
    return User.query.filter(User.role == "admin", User.active == True, User.id != user.id).count() == 0


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
    new_role = request.form.get("role", "operator")
    new_active = bool_from_form("active")
    if is_last_active_admin(user) and (new_role != "admin" or not new_active):
        raise ValueError("No puedes quitar el rol admin ni desactivar al último administrador activo. Crea otro admin primero.")
    user.role = new_role
    user.active = new_active

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
        "manage_terminals": bool_from_form("manage_terminals"),
        "manage_backups": bool_from_form("manage_backups"),
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
    production = [p.to_dict() for p in annotate_countability(Production.query.order_by(Production.timestamp.asc()).all())]
    terminals = [t.to_api() for t in Terminal.query.order_by(Terminal.code.asc()).all()]
    price_history = [{"id": h.id, "family": h.family, "product_code": h.product_code, "description": h.description, "old_price": h.old_price, "new_price": h.new_price, "action": h.action, "actor": h.actor, "source": h.source, "created_at": iso_dt(h.created_at)} for h in PriceHistory.query.order_by(PriceHistory.created_at.desc()).limit(10000).all()]
    batches = [{"code": b.code, "name": b.name, "in_desc": b.in_desc, "family": b.family, "active": b.active, "created_by": b.created_by, "created_at": iso_dt(b.created_at), "closed_at": iso_dt(b.closed_at)} for b in ProductionBatch.query.order_by(ProductionBatch.created_at.desc()).all()]
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
            "terminals": len(terminals),
            "price_history": len(price_history),
            "batches": len(batches),
            "audit": len(audit),
        },
        "data": {
            "users": users,
            "families": families,
            "prices": prices,
            "production": production,
            "terminals": terminals,
            "price_history": price_history,
            "batches": batches,
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
        "terminals_upserted": 0,
        "batches_upserted": 0,
        "price_history_inserted": 0,
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

    # Restaurar terminales/lotes/historial de precios si vienen en el respaldo. No borra por defecto.
    for item in data.get("terminals") or []:
        code = str(item.get("code") or "").strip().upper()
        if not code:
            continue
        terminal = Terminal.query.filter_by(code=code).first() or Terminal(code=code)
        terminal.name = str(item.get("name") or code).strip()
        terminal.location = str(item.get("location") or "").strip()
        terminal.active = bool(item.get("active", True))
        terminal.authorized = bool(item.get("authorized", True))
        terminal.device_id = str(item.get("device_id") or terminal.device_id or "")
        terminal.current_version = str(item.get("current_version") or terminal.current_version or "")
        terminal.min_version = str(item.get("min_version") or terminal.min_version or "")
        terminal.last_user = str(item.get("last_user") or terminal.last_user or "")
        db.session.add(terminal)
        stats["terminals_upserted"] += 1

    for item in data.get("batches") or []:
        code = str(item.get("code") or "").strip().upper()
        if not code:
            continue
        batch = ProductionBatch.query.filter_by(code=code).first() or ProductionBatch(code=code)
        batch.name = str(item.get("name") or "").strip()
        batch.in_desc = str(item.get("in_desc") or "").strip()
        batch.family = str(item.get("family") or "").strip().lower()
        batch.active = bool(item.get("active", True))
        batch.created_by = str(item.get("created_by") or batch.created_by or "")
        db.session.add(batch)
        stats["batches_upserted"] += 1

    for item in data.get("price_history") or []:
        try:
            db.session.add(PriceHistory(
                family=str(item.get("family") or "").strip().lower(),
                product_code=str(item.get("product_code") or "").strip(),
                description=str(item.get("description") or "").strip(),
                old_price=clp_to_int(item.get("old_price")),
                new_price=clp_to_int(item.get("new_price")),
                action=str(item.get("action") or "restore_history"),
                actor=str(item.get("actor") or "restore"),
                source=str(item.get("source") or "backup_restore"),
            ))
            stats["price_history_inserted"] += 1
        except Exception:
            pass

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
            audit(user.username, "login_success", request.remote_addr or "")
            db.session.commit()
            return redirect(request.args.get("next") or url_for("dashboard"))
        audit(username or "unknown", "login_error", f"IP {request.remote_addr or ''}")
        db.session.commit()
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
    rows = annotate_countability(rows)
    by_date = aggregate_rows(rows, "date")
    by_in = aggregate_rows(rows, "in")
    by_family = aggregate_rows(rows, "familia")
    by_user = aggregate_rows(rows, "usuario")
    by_terminal = aggregate_rows(rows, "terminal")
    by_batch = aggregate_rows(rows, "batch")
    totals = total_from_aggregate(aggregate_rows(rows, "all"))
    families = Family.query.order_by(Family.order_index.asc(), Family.name.asc()).all()
    users = User.query.order_by(User.username.asc()).all()
    terminals = Terminal.query.order_by(Terminal.code.asc()).all()
    user_period_summary = build_user_period_summary(rows, users)
    shoe_pending = totals.get("shoe_pending", 0)
    return render_template(
        "dashboard.html",
        rows=rows,
        by_date=by_date,
        by_in=by_in,
        by_family=by_family,
        by_user=by_user,
        by_terminal=by_terminal,
        by_batch=by_batch,
        totals=totals,
        families=families,
        users=users,
        terminals=terminals,
        user_period_summary=user_period_summary,
        shoe_pending=shoe_pending,
    )

@app.route("/produccion")
@login_required
def production_view():
    rows = build_filtered_query().order_by(Production.timestamp.asc()).limit(5000).all()
    rows = annotate_countability(rows)
    rows = filter_by_count_status(rows)
    rows = list(reversed(rows))[:1500]
    families = Family.query.order_by(Family.order_index.asc(), Family.name.asc()).all()
    users = User.query.order_by(User.username.asc()).all()
    terminals = [t.code for t in Terminal.query.order_by(Terminal.code.asc()).all()]
    if not terminals:
        terminals = [x[0] for x in db.session.query(Production.terminal).distinct().order_by(Production.terminal.asc()).all() if x[0]]
    modos = [x[0] for x in db.session.query(Production.modo_impresion).distinct().order_by(Production.modo_impresion.asc()).all() if x[0]]
    return render_template("production.html", rows=rows, families=families, users=users, terminals=terminals, modos=modos)


@app.route("/produccion/export.csv")
@login_required
def production_export_csv():
    rows = build_filtered_query().order_by(Production.timestamp.asc()).all()
    rows = filter_by_count_status(annotate_countability(rows))
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
    export_rows = build_filtered_query().order_by(Production.timestamp.asc()).all()
    export_rows = filter_by_count_status(annotate_countability(export_rows))
    rows = [r.to_dict() for r in export_rows]
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
        db.session.flush()
        inserted += 1
    audit(current_user().username, "import_production", f"Archivo {secure_filename(file.filename)} · Insertados {inserted}, omitidos {skipped}")
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
            audit(current_user().username, "create_user", user.username)
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
            audit(current_user().username, "change_user", f"{original_username} -> {user.username}")
            audit(current_user().username, "change_permissions", f"{user.username}: {user.permissions}")
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
    elif is_last_active_admin(user):
        flash("No puedes desactivar al último administrador activo. Crea otro admin primero.", "error")
    else:
        user.active = False
        audit(current_user().username, "disable_user", user.username)
        db.session.commit()
        flash("Usuario desactivado.", "success")
    return redirect(url_for("users_view"))


@app.route("/usuarios/<int:user_id>/activar", methods=["POST"])
@login_required
@role_required("admin")
def user_activate(user_id):
    user = User.query.get_or_404(user_id)
    user.active = True
    audit(current_user().username, "activate_user", user.username)
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
        imported, errors = parse_price_excel(file, actor=current_user().username)
        db.session.commit()
        if errors:
            flash("Excel cargado con observaciones: " + " | ".join(errors), "error")
        else:
            flash(f"Precios importados correctamente: {imported} registros.", "success")
        return redirect(url_for("prices_view"))

    families = Family.query.order_by(Family.order_index.asc(), Family.name.asc()).all()
    counts = dict(db.session.query(PriceProduct.family, func.count(PriceProduct.id)).group_by(PriceProduct.family).all())

    filters = {
        "family": request.args.get("family", "").strip().lower(),
        "q": request.args.get("q", "").strip(),
        "active": request.args.get("active", "active").strip(),
        "min_price": request.args.get("min_price", "").strip(),
        "max_price": request.args.get("max_price", "").strip(),
        "sort": request.args.get("sort", "family_price").strip(),
        "per_page": request.args.get("per_page", "200").strip(),
    }

    query = PriceProduct.query
    if filters["family"]:
        query = query.filter(PriceProduct.family == filters["family"])
    if filters["active"] == "active":
        query = query.filter(PriceProduct.active.is_(True))
    elif filters["active"] == "inactive":
        query = query.filter(PriceProduct.active.is_(False))
    if filters["q"]:
        like = f"%{filters['q']}%"
        query = query.filter(db.or_(
            PriceProduct.product_code.ilike(like),
            PriceProduct.description.ilike(like),
            PriceProduct.family.ilike(like),
        ))
    min_price = clp_to_int(filters["min_price"])
    max_price = clp_to_int(filters["max_price"])
    if min_price > 0:
        query = query.filter(PriceProduct.gross_price >= min_price)
    if max_price > 0:
        query = query.filter(PriceProduct.gross_price <= max_price)

    sort = filters["sort"]
    if sort == "code":
        query = query.order_by(PriceProduct.product_code.asc(), PriceProduct.gross_price.asc())
    elif sort == "description":
        query = query.order_by(PriceProduct.description.asc(), PriceProduct.gross_price.asc())
    elif sort == "price_desc":
        query = query.order_by(PriceProduct.gross_price.desc(), PriceProduct.family.asc())
    elif sort == "updated":
        query = query.order_by(PriceProduct.updated_at.desc(), PriceProduct.family.asc())
    else:
        query = query.order_by(PriceProduct.family.asc(), PriceProduct.gross_price.asc(), PriceProduct.product_code.asc())

    total_prices = query.count()
    page = max(1, int(request.args.get("page", 1) or 1))
    per_page_arg = filters["per_page"]
    show_all = per_page_arg == "all"
    if show_all:
        products = query.limit(10000).all()
        per_page = "all"
        pages = 1
    else:
        try:
            per_page = int(per_page_arg or 200)
        except Exception:
            per_page = 200
        per_page = max(25, min(per_page, 1000))
        pages = max(1, (total_prices + per_page - 1) // per_page)
        page = min(page, pages)
        products = query.offset((page - 1) * per_page).limit(per_page).all()

    price_history = PriceHistory.query.order_by(PriceHistory.created_at.desc()).limit(200).all()
    return render_template(
        "prices.html",
        families=families,
        counts=counts,
        sample_prices=products,
        price_history=price_history,
        filters=filters,
        total_prices=total_prices,
        page=page,
        pages=pages,
        per_page=per_page,
        show_all=show_all,
    )


def _safe_return_to_prices():
    target = request.form.get("return_url") or url_for("prices_view")
    if not target.startswith("/"):
        target = url_for("prices_view")
    return redirect(target)


@app.route("/precios/producto/nuevo", methods=["POST"])
@login_required
@role_required("admin", "supervisor")
def price_product_create():
    family = request.form.get("family", "").strip().lower()
    product_code = request.form.get("product_code", "").strip()
    description = request.form.get("description", "").strip()
    gross_price = clp_to_int(request.form.get("gross_price"))
    active = bool_from_form("active")

    if not family or not product_code or gross_price <= 0:
        flash("Familia, código y precio son obligatorios para crear un producto.", "error")
        return _safe_return_to_prices()

    fam = Family.query.filter_by(name=family).first()
    if not fam:
        fam = Family(name=family, display_name=family.title(), active=True, order_index=Family.query.count() + 1)
        db.session.add(fam)
        audit(current_user().username, "change_family", f"Familia creada desde producto manual: {family}")
        db.session.flush()

    exists = PriceProduct.query.filter_by(family=family, product_code=product_code, gross_price=gross_price).first()
    if exists:
        flash("Ya existe un producto con la misma familia, código y precio.", "error")
        return _safe_return_to_prices()

    prod = PriceProduct(family=family, product_code=product_code, description=description, gross_price=gross_price, active=active)
    db.session.add(prod)
    db.session.add(PriceHistory(
        family=family,
        product_code=product_code,
        description=description,
        old_price=0,
        new_price=gross_price,
        action="create_manual",
        actor=current_user().username,
        source="panel_prices",
    ))
    audit(current_user().username, "change_price", f"Producto creado manualmente {family}/{product_code} ${gross_price}")
    db.session.commit()
    flash("Producto/precio creado correctamente.", "success")
    return _safe_return_to_prices()


@app.route("/precios/producto/<int:product_id>/guardar", methods=["POST"])
@login_required
@role_required("admin", "supervisor")
def price_product_update(product_id):
    prod = PriceProduct.query.get_or_404(product_id)
    old_family = prod.family
    old_code = prod.product_code
    old_desc = prod.description
    old_price = int(prod.gross_price or 0)
    old_active = bool(prod.active)

    family = request.form.get("family", "").strip().lower()
    product_code = request.form.get("product_code", "").strip()
    description = request.form.get("description", "").strip()
    gross_price = clp_to_int(request.form.get("gross_price"))
    active = bool_from_form("active")

    if not family or not product_code or gross_price <= 0:
        flash("Familia, código y precio son obligatorios.", "error")
        return _safe_return_to_prices()

    duplicate = PriceProduct.query.filter(
        PriceProduct.id != prod.id,
        PriceProduct.family == family,
        PriceProduct.product_code == product_code,
        PriceProduct.gross_price == gross_price,
    ).first()
    if duplicate:
        flash("No se pudo guardar: ya existe otro producto con la misma familia, código y precio.", "error")
        return _safe_return_to_prices()

    prod.family = family
    prod.product_code = product_code
    prod.description = description
    prod.gross_price = gross_price
    prod.active = active
    db.session.add(prod)

    changed = []
    if old_family != family:
        changed.append(f"familia {old_family}->{family}")
    if old_code != product_code:
        changed.append(f"código {old_code}->{product_code}")
    if old_desc != description:
        changed.append("descripción")
    if old_price != gross_price:
        changed.append(f"precio {old_price}->{gross_price}")
    if old_active != active:
        changed.append(f"activo {old_active}->{active}")

    if changed:
        db.session.add(PriceHistory(
            family=family,
            product_code=product_code,
            description=description,
            old_price=old_price,
            new_price=gross_price,
            action="edit_panel" if old_price == gross_price else "change_price_panel",
            actor=current_user().username,
            source="panel_prices",
        ))
        audit(current_user().username, "change_price", f"Producto editado #{prod.id}: " + "; ".join(changed))
        db.session.commit()
        flash("Producto/precio actualizado correctamente.", "success")
    else:
        flash("No hubo cambios que guardar.", "success")
    return _safe_return_to_prices()


@app.route("/precios/producto/<int:product_id>/toggle", methods=["POST"])
@login_required
@role_required("admin", "supervisor")
def price_product_toggle(product_id):
    prod = PriceProduct.query.get_or_404(product_id)
    prod.active = not bool(prod.active)
    db.session.add(prod)
    audit(current_user().username, "change_price", f"Producto {'activado' if prod.active else 'desactivado'} #{prod.id} {prod.family}/{prod.product_code}")
    db.session.commit()
    flash("Estado del producto actualizado.", "success")
    return _safe_return_to_prices()


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
        audit(current_user().username, "change_family", f"{name} -> {display_name or name.title()} activo={fam.active}")
        db.session.commit()
        flash("Familia guardada.", "success")
    target = request.form.get("return_url") or url_for("prices_view")
    if not target.startswith("/"):
        target = url_for("prices_view")
    return redirect(target)


@app.route("/configuracion", methods=["GET", "POST"])
@login_required
@role_required("admin")
def settings_view():
    if request.method == "POST":
        for key in ["brand_name", "footer_brand", "sync_valid_hours", "shoe_family_names", "duplicate_families", "duplicate_detection_seconds", "local_latest_version", "local_min_version", "logs_visible", "whatsapp_number", "instagram_url", "facebook_url", "jumpseller_url"]:
            rec = Setting.query.get(key) or Setting(key=key)
            rec.value = request.form.get(key, "").strip()
            db.session.add(rec)
        audit(current_user().username, "save_settings", "configuracion")
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
        "Terminales": Terminal.query.count(),
        "Historial precios": PriceHistory.query.count(),
        "Lotes": ProductionBatch.query.count(),
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
            dataframe_from_records(snapshot["data"].get("terminals", [])).to_excel(writer, index=False, sheet_name="Terminales")
            dataframe_from_records(snapshot["data"].get("price_history", [])).to_excel(writer, index=False, sheet_name="HistorialPrecios")
            dataframe_from_records(snapshot["data"].get("batches", [])).to_excel(writer, index=False, sheet_name="Lotes")
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
    audit(current_user().username, "download_backup", f"Respaldo ZIP {stamp}")
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
    audit(current_user().username, "download_backup", f"Respaldo JSON {stamp}")
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
        audit(current_user().username, "restore_backup", detail[:5000])
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
        audit(current_user().username, "restore_backup_failed", f"{secure_filename(file.filename)} · {exc}")
        db.session.commit()
        flash(f"No se pudo restaurar el respaldo: {exc}", "error")
    return redirect(url_for("backups_view"))


@app.route("/terminales", methods=["GET", "POST"])
@login_required
@role_required("admin")
def terminals_view():
    if request.method == "POST":
        code = (request.form.get("code") or "").strip().upper()
        if not code:
            flash("Código de terminal requerido.", "error")
            return redirect(url_for("terminals_view"))
        terminal = Terminal.query.filter_by(code=code).first() or Terminal(code=code)
        terminal.name = request.form.get("name", "").strip() or code
        terminal.location = request.form.get("location", "").strip()
        terminal.active = bool_from_form("active")
        terminal.authorized = bool_from_form("authorized")
        terminal.min_version = request.form.get("min_version", "").strip()
        terminal.notes = request.form.get("notes", "").strip()
        db.session.add(terminal)
        audit(current_user().username, "change_terminal", f"{code} active={terminal.active} authorized={terminal.authorized}")
        db.session.commit()
        flash("Terminal guardado.", "success")
        return redirect(url_for("terminals_view"))
    terminals = Terminal.query.order_by(Terminal.code.asc()).all()
    return render_template("terminals.html", terminals=terminals)


@app.route("/lotes", methods=["GET", "POST"])
@login_required
@role_required("admin", "supervisor")
def batches_view():
    if request.method == "POST":
        code = (request.form.get("code") or "").strip().upper()
        if not code:
            flash("Código de lote requerido.", "error")
            return redirect(url_for("batches_view"))
        batch = ProductionBatch.query.filter_by(code=code).first() or ProductionBatch(code=code, created_by=current_user().username)
        batch.name = request.form.get("name", "").strip()
        batch.in_desc = request.form.get("in_desc", "").strip()
        batch.family = request.form.get("family", "").strip().lower()
        batch.active = bool_from_form("active")
        db.session.add(batch)
        audit(current_user().username, "change_batch", f"{code} active={batch.active}")
        db.session.commit()
        flash("Lote guardado.", "success")
        return redirect(url_for("batches_view"))
    batches = ProductionBatch.query.order_by(ProductionBatch.created_at.desc()).all()
    families = Family.query.order_by(Family.order_index.asc(), Family.name.asc()).all()
    return render_template("batches.html", batches=batches, families=families)


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
    terminal_code = str(payload.get("terminal") or "").strip().upper()
    device_id = str(payload.get("device_id") or "")
    software_version = str(payload.get("software_version") or "")
    user = User.query.filter_by(username=username).first()
    if not user or not user.active or not check_password_hash(user.password_hash, password):
        audit(username or "unknown", "api_login_error", f"terminal={terminal_code} device={device_id} ip={request.remote_addr or ''}")
        db.session.commit()
        return jsonify({"ok": False, "error": "invalid_credentials"}), 401

    terminal = Terminal.query.filter_by(code=terminal_code).first()
    if not terminal:
        terminal = Terminal(code=terminal_code or "SIN_TERMINAL", name=f"Terminal {terminal_code}", active=True, authorized=True)
        db.session.add(terminal)
        db.session.flush()
    if not terminal.active or not terminal.authorized:
        audit(user.username, "api_login_error", f"terminal_bloqueado={terminal.code}")
        db.session.commit()
        return jsonify({"ok": False, "error": "terminal_not_authorized"}), 403

    terminal.device_id = device_id or terminal.device_id
    terminal.current_version = software_version or terminal.current_version
    terminal.last_seen = utcnow()
    terminal.last_user = user.username
    terminal.last_ip = request.remote_addr or ""

    token = secrets.token_urlsafe(40)
    expires = utcnow() + timedelta(hours=36)
    db.session.add(ApiSession(token_hash=token_hash(token), user_id=user.id, terminal=terminal.code, device_id=device_id, expires_at=expires))
    audit(user.username, "api_login_success", f"terminal={terminal.code} version={software_version}")
    db.session.commit()
    return jsonify({
        "ok": True,
        "token": token,
        "expires_at": expires.isoformat(),
        "user": user.to_api(include_hash=False),
        "terminal": terminal.to_api(),
        "sync_valid_hours": int(setting_value("sync_valid_hours", "24") or 24),
        "local_latest_version": setting_value("local_latest_version", LATEST_LOCAL_VERSION_DEFAULT),
        "local_min_version": setting_value("local_min_version", MIN_LOCAL_VERSION_DEFAULT),
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
    terminals = [t.to_api() for t in Terminal.query.order_by(Terminal.code.asc()).all()]
    batches = [{"code": b.code, "name": b.name, "in_desc": b.in_desc, "family": b.family, "active": b.active} for b in ProductionBatch.query.filter_by(active=True).order_by(ProductionBatch.created_at.desc()).limit(200).all()]
    return jsonify({
        "ok": True,
        "server_time": utcnow().isoformat(),
        "users": users,
        "families": families,
        "prices": prices,
        "settings": settings,
        "terminals": terminals,
        "batches": batches,
        "local_latest_version": setting_value("local_latest_version", LATEST_LOCAL_VERSION_DEFAULT),
        "local_min_version": setting_value("local_min_version", MIN_LOCAL_VERSION_DEFAULT),
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
        if not prod.device_id:
            prod.device_id = request.api_session.device_id or ""
        if not prod.terminal:
            prod.terminal = request.api_session.terminal or ""
        db.session.add(prod)
        db.session.flush()
        inserted += 1
    if request.api_session and request.api_session.terminal:
        terminal = Terminal.query.filter_by(code=request.api_session.terminal).first()
        if terminal:
            terminal.last_seen = utcnow()
            terminal.last_user = request.api_user.username
            terminal.last_ip = request.remote_addr or ""
    if inserted or skipped:
        audit(request.api_user.username, "sync_production", f"insertados={inserted} omitidos={skipped} terminal={request.api_session.terminal}")
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
    audit(request.api_user.username, "sync_production", f"1 registro desde {request.api_session.terminal}")
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
