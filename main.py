import csv
import io
import os
import secrets
import hashlib
import hmac
from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Optional
from zoneinfo import ZoneInfo

import openpyxl
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text, create_engine, func, select
from sqlalchemy.orm import Session, declarative_base, relationship, sessionmaker
from starlette.middleware.sessions import SessionMiddleware

APP_NAME = "LA ERP Cloud"
FAMILIES = ["vestuario", "hogar", "zapatillas", "bolsos"]
TERMINALS = ["T1", "T2", "T3", "T4"]

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./lamericana.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+" not in DATABASE_URL.split("://", 1)[0]:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/Santiago")
SECRET_KEY = os.getenv("SECRET_KEY", secrets.token_urlsafe(32))
AGENT_TOKEN = os.getenv("AGENT_TOKEN", "dev-agent-token-cambiar")

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def now_local() -> datetime:
    try:
        return datetime.now(ZoneInfo(APP_TIMEZONE)).replace(tzinfo=None)
    except Exception:
        return datetime.now()


def today_range() -> tuple[datetime, datetime]:
    start = datetime.combine(now_local().date(), datetime.min.time())
    return start, start + timedelta(days=1)


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    full_name = Column(String(100), nullable=False)
    code = Column(String(10), nullable=False)
    role = Column(String(20), nullable=False, default="operator")
    active = Column(Boolean, nullable=False, default=True)
    can_manage_users = Column(Boolean, nullable=False, default=False)
    can_use_target_price = Column(Boolean, nullable=False, default=True)
    can_print = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=now_local)

    labels = relationship("Label", back_populates="user")


class PriceItem(Base):
    __tablename__ = "price_items"
    id = Column(Integer, primary_key=True, index=True)
    family = Column(String(30), index=True, nullable=False)
    product_code = Column(String(80), index=True, nullable=False)
    description = Column(String(255), nullable=False)
    price_gross = Column(Float, index=True, nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=now_local)


class TerminalWeight(Base):
    __tablename__ = "terminal_weights"
    id = Column(Integer, primary_key=True, index=True)
    terminal = Column(String(20), unique=True, index=True, nullable=False)
    weight_kg = Column(Float, nullable=False, default=0.0)
    source = Column(String(50), nullable=False, default="manual")
    updated_at = Column(DateTime, nullable=False, default=now_local)


class Lot(Base):
    __tablename__ = "lots"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(120), nullable=False)
    family = Column(String(30), nullable=True)
    initial_weight_kg = Column(Float, nullable=True)
    cost_amount = Column(Float, nullable=True)
    status = Column(String(30), nullable=False, default="abierto")
    created_at = Column(DateTime, nullable=False, default=now_local)


class Label(Base):
    __tablename__ = "labels"
    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, nullable=False, default=now_local, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    username = Column(String(50), nullable=False)
    user_code = Column(String(10), nullable=False)
    terminal = Column(String(20), nullable=False, index=True)
    family = Column(String(30), nullable=False, index=True)
    method = Column(String(20), nullable=False)
    weight_kg = Column(Float, nullable=False)
    value_per_kg = Column(Float, nullable=True)
    target_price = Column(Float, nullable=True)
    final_price = Column(Float, nullable=False)
    product_code = Column(String(80), nullable=False)
    description = Column(String(255), nullable=False)
    idx_value = Column(Float, nullable=True)
    in_origin = Column(String(255), nullable=True)
    lot_id = Column(Integer, nullable=True)
    print_status = Column(String(30), nullable=False, default="no_solicitada")
    printed_at = Column(DateTime, nullable=True)

    user = relationship("User", back_populates="labels")


app = FastAPI(title=APP_NAME)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def hash_password(password: str) -> str:
    iterations = 260000
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations).hex()
    return f"pbkdf2_sha256${iterations}${salt}${digest}"


def verify_password(password: str, hashed: str) -> bool:
    try:
        scheme, iterations_s, salt, digest = hashed.split("$", 3)
        if scheme != "pbkdf2_sha256":
            return False
        iterations = int(iterations_s)
        candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations).hex()
        return hmac.compare_digest(candidate, digest)
    except Exception:
        return False


def current_user(request: Request, db: Session = Depends(get_db)) -> User:
    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="No autenticado")
    user = db.get(User, user_id)
    if not user or not user.active:
        request.session.clear()
        raise HTTPException(status_code=401, detail="No autenticado")
    return user


def optional_user(request: Request, db: Session = Depends(get_db)) -> Optional[User]:
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    user = db.get(User, user_id)
    if not user or not user.active:
        request.session.clear()
        return None
    return user


def require_agent_token(token: str):
    if token != AGENT_TOKEN:
        raise HTTPException(status_code=403, detail="Token de agente inválido")


def format_clp(value) -> str:
    try:
        return f"${int(round(float(value))):,}".replace(",", ".")
    except Exception:
        return str(value)


def parse_money(value: str) -> float:
    if value is None:
        raise ValueError("valor vacío")
    cleaned = str(value).strip().replace("$", "").replace(".", "").replace(",", ".")
    if cleaned == "":
        raise ValueError("valor vacío")
    return float(Decimal(cleaned))


def find_price_closest(db: Session, family: str, amount: float) -> Optional[PriceItem]:
    items = db.execute(select(PriceItem).where(PriceItem.family == family, PriceItem.active == True)).scalars().all()
    if not items:
        return None
    return min(items, key=lambda item: abs(item.price_gross - float(amount)))


def find_price_above(db: Session, family: str, amount: float) -> Optional[PriceItem]:
    item = db.execute(
        select(PriceItem)
        .where(PriceItem.family == family, PriceItem.active == True, PriceItem.price_gross >= float(amount))
        .order_by(PriceItem.price_gross.asc())
        .limit(1)
    ).scalar_one_or_none()
    if item:
        return item
    return db.execute(
        select(PriceItem)
        .where(PriceItem.family == family, PriceItem.active == True)
        .order_by(PriceItem.price_gross.desc())
        .limit(1)
    ).scalar_one_or_none()


def get_current_weight(db: Session, terminal: str) -> Optional[TerminalWeight]:
    return db.execute(select(TerminalWeight).where(TerminalWeight.terminal == terminal)).scalar_one_or_none()


def seed_database():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        if not db.execute(select(User).where(User.username == "gustavo")).scalar_one_or_none():
            seed_users = [
                ("gustavo", "1176", "Gustavo", "GS", "admin", True, True, True),
                ("al", "67", "AL", "AL", "operator", False, True, True),
                ("kd", "88", "KD", "KD", "operator", False, True, True),
                ("vo", "46", "VO", "VO", "operator", False, True, True),
                ("gr", "17", "GR", "GR", "operator", False, True, True),
            ]
            for username, password, full_name, code, role, manage, target, can_print in seed_users:
                db.add(User(
                    username=username,
                    password_hash=hash_password(password),
                    full_name=full_name,
                    code=code,
                    role=role,
                    can_manage_users=manage,
                    can_use_target_price=target,
                    can_print=can_print,
                ))
        if db.execute(select(func.count(PriceItem.id))).scalar() == 0:
            seed_prices = []
            for family, prefix in [("vestuario", "V"), ("hogar", "H"), ("zapatillas", "Z"), ("bolsos", "B")]:
                for idx, price in enumerate([990, 1490, 1990, 2990, 3990, 4990, 6990, 8990, 9990, 12990, 14990, 19990, 24990, 29990], 1):
                    seed_prices.append(PriceItem(
                        family=family,
                        product_code=f"{prefix}{idx:03d}",
                        description=f"{family.upper()} PRECIO {format_clp(price)}",
                        price_gross=float(price),
                    ))
            db.add_all(seed_prices)
        for terminal in TERMINALS:
            if not get_current_weight(db, terminal):
                db.add(TerminalWeight(terminal=terminal, weight_kg=0.0, source="seed"))
        db.commit()
    finally:
        db.close()


@app.on_event("startup")
def startup():
    seed_database()


@app.middleware("http")
async def auth_redirects(request: Request, call_next):
    public_paths = ("/login", "/static", "/api/agent", "/health")
    if request.url.path == "/" or request.url.path.startswith(public_paths):
        return await call_next(request)
    if not request.session.get("user_id"):
        return RedirectResponse("/login", status_code=303)
    return await call_next(request)


app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, same_site="lax", https_only=False)


@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME, "time": now_local().isoformat()}


@app.get("/", response_class=HTMLResponse)
def root(request: Request, user: Optional[User] = Depends(optional_user)):
    if user:
        return RedirectResponse("/dashboard", status_code=303)
    return RedirectResponse("/login", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "terminals": TERMINALS, "error": None})


@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...), terminal: str = Form(...), db: Session = Depends(get_db)):
    user = db.execute(select(User).where(User.username == username.strip().lower())).scalar_one_or_none()
    if not user or not user.active or not verify_password(password, user.password_hash):
        return templates.TemplateResponse("login.html", {"request": request, "terminals": TERMINALS, "error": "Usuario o clave incorrectos."}, status_code=400)
    request.session["user_id"] = user.id
    request.session["terminal"] = terminal.strip().upper()
    return RedirectResponse("/dashboard", status_code=303)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, user: User = Depends(current_user), db: Session = Depends(get_db)):
    start, end = today_range()
    labels_today = db.execute(select(Label).where(Label.created_at >= start, Label.created_at < end)).scalars().all()
    total_labels = len(labels_today)
    total_kg = sum(l.weight_kg for l in labels_today)
    total_value = sum(l.final_price for l in labels_today)
    by_user = {}
    by_family = {}
    for l in labels_today:
        by_user.setdefault(l.user_code, {"labels": 0, "kg": 0.0, "value": 0.0})
        by_user[l.user_code]["labels"] += 1
        by_user[l.user_code]["kg"] += l.weight_kg
        by_user[l.user_code]["value"] += l.final_price
        by_family.setdefault(l.family, {"labels": 0, "kg": 0.0, "value": 0.0})
        by_family[l.family]["labels"] += 1
        by_family[l.family]["kg"] += l.weight_kg
        by_family[l.family]["value"] += l.final_price
    latest = db.execute(select(Label).order_by(Label.created_at.desc()).limit(15)).scalars().all()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "user": user,
        "terminal": request.session.get("terminal"),
        "total_labels": total_labels,
        "total_kg": total_kg,
        "total_value": total_value,
        "by_user": by_user,
        "by_family": by_family,
        "latest": latest,
        "format_clp": format_clp,
    })


@app.get("/etiquetas", response_class=HTMLResponse)
def etiquetas_page(request: Request, label_id: Optional[int] = None, user: User = Depends(current_user), db: Session = Depends(get_db)):
    terminal = request.session.get("terminal", "T1")
    weight = get_current_weight(db, terminal)
    latest = db.execute(select(Label).order_by(Label.created_at.desc()).limit(12)).scalars().all()
    label = db.get(Label, label_id) if label_id else None
    lots = db.execute(select(Lot).where(Lot.status == "abierto").order_by(Lot.created_at.desc())).scalars().all()
    return templates.TemplateResponse("etiquetas.html", {
        "request": request,
        "user": user,
        "terminal": terminal,
        "families": FAMILIES,
        "weight": weight,
        "latest": latest,
        "label": label,
        "lots": lots,
        "format_clp": format_clp,
    })


@app.post("/etiquetas/calcular")
def calcular_etiqueta(
    request: Request,
    family: str = Form(...),
    method: str = Form(...),
    weight_source: str = Form("agent"),
    manual_weight_kg: str = Form(""),
    value_per_kg: str = Form(""),
    target_price: str = Form(""),
    in_origin: str = Form(""),
    lot_id: str = Form(""),
    print_requested: Optional[str] = Form(None),
    user: User = Depends(current_user),
    db: Session = Depends(get_db),
):
    terminal = request.session.get("terminal", "T1")
    family = family.strip().lower()
    method = method.strip().lower()
    if family not in FAMILIES:
        raise HTTPException(status_code=400, detail="Familia inválida")
    if method not in {"kilo", "objetivo"}:
        raise HTTPException(status_code=400, detail="Método inválido")
    if method == "objetivo" and not user.can_use_target_price:
        raise HTTPException(status_code=403, detail="Usuario sin permiso para precio objetivo")

    try:
        if weight_source == "manual" and manual_weight_kg.strip():
            weight_kg = float(manual_weight_kg.replace(",", "."))
        else:
            terminal_weight = get_current_weight(db, terminal)
            weight_kg = float(terminal_weight.weight_kg if terminal_weight else 0.0)
        if weight_kg <= 0:
            raise ValueError()
    except Exception:
        raise HTTPException(status_code=400, detail="Peso inválido. Use agente local o ingrese peso manual.")

    value_per_kg_num = None
    target_price_num = None
    if method == "kilo":
        try:
            value_per_kg_num = parse_money(value_per_kg)
            raw_price = value_per_kg_num * weight_kg
        except Exception:
            raise HTTPException(status_code=400, detail="Valor kilo inválido")
        product = find_price_closest(db, family, raw_price)
        idx_value = value_per_kg_num / 1000.0
    else:
        try:
            target_price_num = parse_money(target_price)
        except Exception:
            raise HTTPException(status_code=400, detail="Precio objetivo inválido")
        product = find_price_above(db, family, target_price_num)
        idx_value = (product.price_gross / weight_kg) / 1000.0 if product else 0.0

    if not product:
        raise HTTPException(status_code=400, detail="No hay tabla de precios cargada para esta familia")

    selected_lot_id = int(lot_id) if lot_id.strip().isdigit() else None
    label = Label(
        user_id=user.id,
        username=user.username,
        user_code=user.code,
        terminal=terminal,
        family=family,
        method=method,
        weight_kg=weight_kg,
        value_per_kg=value_per_kg_num,
        target_price=target_price_num,
        final_price=float(product.price_gross),
        product_code=product.product_code,
        description=product.description,
        idx_value=idx_value,
        in_origin=in_origin.strip() or None,
        lot_id=selected_lot_id,
        print_status="pendiente" if print_requested and user.can_print else "no_solicitada",
    )
    db.add(label)
    db.commit()
    db.refresh(label)
    return RedirectResponse(f"/etiquetas?label_id={label.id}", status_code=303)


@app.get("/historial", response_class=HTMLResponse)
def historial(request: Request, user: User = Depends(current_user), db: Session = Depends(get_db)):
    labels = db.execute(select(Label).order_by(Label.created_at.desc()).limit(250)).scalars().all()
    return templates.TemplateResponse("historial.html", {
        "request": request,
        "user": user,
        "terminal": request.session.get("terminal"),
        "labels": labels,
        "format_clp": format_clp,
    })


@app.get("/export/labels.csv")
def export_labels(user: User = Depends(current_user), db: Session = Depends(get_db)):
    labels = db.execute(select(Label).order_by(Label.created_at.desc())).scalars().all()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "fecha", "usuario", "codigo_usuario", "terminal", "familia", "metodo", "peso_kg", "valor_kilo", "precio_objetivo", "precio_final", "codigo_producto", "descripcion", "idx", "in", "lote_id", "estado_impresion"])
    for l in labels:
        writer.writerow([l.id, l.created_at, l.username, l.user_code, l.terminal, l.family, l.method, l.weight_kg, l.value_per_kg, l.target_price, l.final_price, l.product_code, l.description, l.idx_value, l.in_origin, l.lot_id, l.print_status])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=historial_etiquetas_lamericana.csv"})


@app.get("/precios", response_class=HTMLResponse)
def precios_page(request: Request, user: User = Depends(current_user), db: Session = Depends(get_db)):
    if not user.can_manage_users:
        raise HTTPException(status_code=403, detail="Solo admin")
    counts = {}
    for family in FAMILIES:
        counts[family] = db.execute(select(func.count(PriceItem.id)).where(PriceItem.family == family, PriceItem.active == True)).scalar()
    sample = db.execute(select(PriceItem).order_by(PriceItem.family, PriceItem.price_gross).limit(100)).scalars().all()
    return templates.TemplateResponse("precios.html", {"request": request, "user": user, "counts": counts, "sample": sample, "format_clp": format_clp})


@app.post("/precios/importar")
def importar_precios(request: Request, file: UploadFile = File(...), replace_all: Optional[str] = Form(None), user: User = Depends(current_user), db: Session = Depends(get_db)):
    if not user.can_manage_users:
        raise HTTPException(status_code=403, detail="Solo admin")
    if not file.filename.lower().endswith((".xlsx", ".xlsm")):
        raise HTTPException(status_code=400, detail="Sube un archivo Excel .xlsx con hojas vestuario/hogar/zapatillas/bolsos")
    data = file.file.read()
    wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True)
    if replace_all:
        db.query(PriceItem).delete()
    imported = 0
    for family in FAMILIES:
        if family not in wb.sheetnames:
            continue
        ws = wb[family]
        for row in ws.iter_rows(min_row=1, values_only=True):
            if not row or all(v is None for v in row):
                continue
            code, desc, price = (list(row) + [None, None, None])[:3]
            if code is None or desc is None or price is None:
                continue
            try:
                price_num = parse_money(str(price))
            except Exception:
                continue
            db.add(PriceItem(family=family, product_code=str(code).strip(), description=str(desc).strip(), price_gross=price_num, active=True))
            imported += 1
    db.commit()
    return RedirectResponse(f"/precios?imported={imported}", status_code=303)


@app.get("/lotes", response_class=HTMLResponse)
def lotes_page(request: Request, user: User = Depends(current_user), db: Session = Depends(get_db)):
    lots = db.execute(select(Lot).order_by(Lot.created_at.desc())).scalars().all()
    return templates.TemplateResponse("lotes.html", {"request": request, "user": user, "families": FAMILIES, "lots": lots, "format_clp": format_clp})


@app.post("/lotes")
def crear_lote(name: str = Form(...), family: str = Form(""), initial_weight_kg: str = Form(""), cost_amount: str = Form(""), user: User = Depends(current_user), db: Session = Depends(get_db)):
    if family and family not in FAMILIES:
        raise HTTPException(status_code=400, detail="Familia inválida")
    weight = float(initial_weight_kg.replace(",", ".")) if initial_weight_kg.strip() else None
    cost = parse_money(cost_amount) if cost_amount.strip() else None
    db.add(Lot(name=name.strip(), family=family or None, initial_weight_kg=weight, cost_amount=cost))
    db.commit()
    return RedirectResponse("/lotes", status_code=303)


@app.post("/lotes/{lot_id}/cerrar")
def cerrar_lote(lot_id: int, user: User = Depends(current_user), db: Session = Depends(get_db)):
    lot = db.get(Lot, lot_id)
    if not lot:
        raise HTTPException(status_code=404, detail="Lote no encontrado")
    lot.status = "cerrado"
    db.commit()
    return RedirectResponse("/lotes", status_code=303)


@app.post("/api/agent/weight")
def api_agent_weight(token: str = Form(...), terminal: str = Form(...), weight_kg: float = Form(...), source: str = Form("agent"), db: Session = Depends(get_db)):
    require_agent_token(token)
    terminal = terminal.strip().upper()
    tw = get_current_weight(db, terminal)
    if not tw:
        tw = TerminalWeight(terminal=terminal, weight_kg=weight_kg, source=source, updated_at=now_local())
        db.add(tw)
    else:
        tw.weight_kg = weight_kg
        tw.source = source
        tw.updated_at = now_local()
    db.commit()
    return {"ok": True, "terminal": terminal, "weight_kg": weight_kg, "updated_at": tw.updated_at.isoformat()}


@app.get("/api/agent/print-jobs")
def api_agent_print_jobs(token: str, terminal: str, db: Session = Depends(get_db)):
    require_agent_token(token)
    terminal = terminal.strip().upper()
    jobs = db.execute(
        select(Label)
        .where(Label.terminal == terminal, Label.print_status == "pendiente")
        .order_by(Label.created_at.asc())
        .limit(10)
    ).scalars().all()
    payload = []
    for job in jobs:
        job.print_status = "enviada_agente"
        payload.append({
            "id": job.id,
            "created_at": job.created_at.isoformat(),
            "terminal": job.terminal,
            "family": job.family,
            "method": job.method,
            "weight_kg": job.weight_kg,
            "final_price": job.final_price,
            "product_code": job.product_code,
            "description": job.description,
            "idx_value": job.idx_value,
            "in_origin": job.in_origin or "--",
            "user_code": job.user_code,
        })
    db.commit()
    return {"ok": True, "jobs": payload}


@app.post("/api/agent/print-jobs/{label_id}/done")
def api_agent_print_done(label_id: int, token: str = Form(...), ok: bool = Form(True), error: str = Form(""), db: Session = Depends(get_db)):
    require_agent_token(token)
    label = db.get(Label, label_id)
    if not label:
        raise HTTPException(status_code=404, detail="Etiqueta no encontrada")
    label.print_status = "impresa" if ok else "error_impresion"
    label.printed_at = now_local() if ok else None
    db.commit()
    return {"ok": True, "label_id": label_id, "status": label.print_status}


@app.post("/api/agent/print-jobs/{label_id}/requeue")
def api_agent_print_requeue(label_id: int, token: str = Form(...), db: Session = Depends(get_db)):
    require_agent_token(token)
    label = db.get(Label, label_id)
    if not label:
        raise HTTPException(status_code=404, detail="Etiqueta no encontrada")
    label.print_status = "pendiente"
    db.commit()
    return {"ok": True, "label_id": label_id, "status": label.print_status}
