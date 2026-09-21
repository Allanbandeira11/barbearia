#!/usr/bin/env python3
import http.client
import json
import os
import queue
import secrets
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE = os.path.dirname(os.path.abspath(__file__))

LOCK = threading.Lock()

def _admin_token():
    p = os.path.join(BASE, "data", ".admin_token")
    try:
        with open(p, "r", encoding="utf-8") as f:
            t = f.read().strip()
            if t:
                return t
    except Exception:
        pass
    t = secrets.token_hex(16)
    try:
        with open(p, "w", encoding="utf-8") as f:
            f.write(t)
    except Exception:
        pass
    return t

ADMIN_TOKEN = _admin_token()
SESSION = {"token": ADMIN_TOKEN}

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://xvquhhbydhsomrqqxosz.supabase.co/rest/v1")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inh2cXVoaGJ5ZGhzb21ycXF4b3N6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4OTc0MzY4MSwiZXhwIjoyMTA1MzE5NjgxfQ.bhRnCsa5qkzwOsDZtUZ6fUBwVOsbkV5hrYlDyTKYzho")

_SB_HOST = "xvquhhbydhsomrqqxosz.supabase.co"
_CONN_POOL = queue.Queue()
for _ in range(4):
    _CONN_POOL.put(None)

def _new_conn():
    return http.client.HTTPSConnection(_SB_HOST, 443, timeout=15)

def _sb_raw(method, path, payload, prefer, conn):
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    h = {"apikey": SUPABASE_KEY, "Authorization": "Bearer " + SUPABASE_KEY}
    if body is not None:
        h["Content-Type"] = "application/json"
    if prefer:
        h["Prefer"] = prefer
    conn.request(method, "/rest/v1" + path, body=body, headers=h)
    r = conn.getresponse()
    data = r.read()
    if 200 <= r.status < 300:
        return json.loads(data.decode("utf-8")) if data else True
    raise Exception("sb %s %s" % (r.status, data[:120]))

def _sb(method, path, payload=None, prefer=None):
    conn = _CONN_POOL.get()
    try:
        try:
            if conn is None:
                conn = _new_conn()
            return _sb_raw(method, path, payload, prefer, conn)
        except Exception:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = _new_conn()
            return _sb_raw(method, path, payload, prefer, conn)
    except Exception:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        return None
    finally:
        _CONN_POOL.put(conn)

# Cache em memoria: o Supabase leva ~11s por chamada aqui,
# entao guardamos cada tabela por alguns segundos.
_CACHE = {}
_CACHE_TTL = {"config": 300, "servicos": 120, "agendamentos": 60, "barbeiros": 300}

def _cache_get(table):
    ent = _CACHE.get(table)
    if ent and (time.time() - ent[0] < _CACHE_TTL.get(table, 30)):
        return ent[1]
    return None

def _cache_set(table, rows):
    _CACHE[table] = (time.time(), rows)

def _cache_del(table):
    _CACHE.pop(table, None)

def _load_local_json(filename, default):
    path = os.path.join(BASE, "data", filename)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _bg_refresh(table):
    rows = _sb("GET", "/%s?select=*&limit=100000" % table)
    if rows is not None:
        _cache_set(table, rows)

def sb_select(table):
    ent = _CACHE.get(table)
    if ent is not None:
        if time.time() - ent[0] < _CACHE_TTL.get(table, 30):
            return ent[1]
        threading.Thread(target=_bg_refresh, args=(table,), daemon=True).start()
        return ent[1]
    rows = _sb("GET", "/%s?select=*&limit=100000" % table)
    if rows is not None:
        _cache_set(table, rows)
        return rows
    # fallback local
    if table == "config":
        cfg = _load_local_json("config.json", {})
        return [cfg] if cfg else []
    if table == "servicos":
        return _load_local_json("services.json", [])
    if table == "agendamentos":
        return _load_local_json("bookings.json", [])
    return []

def _save_local_json(filename, data):
    path = os.path.join(BASE, "data", filename)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def sb_replace(table, rows):
    _cache_del(table)
    _sb("DELETE", "/%s?id=neq.__clear__" % table)
    if rows:
        _sb("POST", "/%s" % table, payload=rows, prefer="return=representation")
    # fallback local
    if table == "servicos":
        _save_local_json("services.json", rows)
    elif table == "agendamentos":
        _save_local_json("bookings.json", rows)
    return rows

DEFAULT_BARBERS = [
    { "id": "b1", "name": "Carlos Silva", "photo": "", "role": "Cortes clássicos" },
    { "id": "b2", "name": "Rafael Souza", "photo": "", "role": "Barba e navalha" }
]

DEFAULT_CONFIG = {
    "shopName": "BlackBarber",
    "instagram": "@blackbarber",
    "whatsappNumber": "5511999999999",
    "adminPassword": "admin",
    "hours": {
        "monday":   { "isOpen": True,  "open": "09:00", "close": "19:00" },
        "weekdays": { "isOpen": True,  "open": "09:00", "close": "19:00" },
        "saturday": { "isOpen": True,  "open": "08:00", "close": "18:00" },
        "sunday":   { "isOpen": False, "open": "",     "close": "" }
    },
    "blockedDays": [],
    "blockedSlots": [],
    "barbers": DEFAULT_BARBERS
}

CFG_DB_MAP = {
    "shopname": "shopName", "instagram": "instagram",
    "whatsappnumber": "whatsappNumber", "adminpassword": "adminPassword",
    "address": "address",
    "hours": "hours", "blockeddays": "blockedDays",
    "blockedslots": "blockedSlots", "barbers": "barbers",
}
CFG_RDB_MAP = {v: k for k, v in CFG_DB_MAP.items()}

def get_config():
    rows = sb_select("config")
    if not rows:
        cfg = {
            "id": 1,
            "shopName": DEFAULT_CONFIG["shopName"],
            "instagram": DEFAULT_CONFIG["instagram"],
            "whatsappNumber": DEFAULT_CONFIG["whatsappNumber"],
            "adminPassword": DEFAULT_CONFIG["adminPassword"],
            "address": DEFAULT_CONFIG.get("address", ""),
            "hours": DEFAULT_CONFIG["hours"],
            "blockedDays": DEFAULT_CONFIG["blockedDays"],
            "blockedSlots": DEFAULT_CONFIG["blockedSlots"],
            "barbers": DEFAULT_CONFIG["barbers"],
        }
        _sb("POST", "/config", payload={CFG_RDB_MAP.get(k, k): v for k, v in cfg.items()}, prefer="return=representation")
        return cfg
    row = rows[0]
    cfg = {}
    for k, v in row.items():
        if k == "id":
            cfg["id"] = v
        elif k in CFG_DB_MAP:
            cfg[CFG_DB_MAP[k]] = v
        else:
            cfg[k] = v
    for k, v in DEFAULT_CONFIG.items():
        cfg.setdefault(k, v)
    return cfg

def save_config(cfg):
    payload = {CFG_RDB_MAP.get(k, k): v for k, v in cfg.items() if k != "id"}
    _sb("PATCH", "/config?id=eq.1", payload=payload, prefer="return=representation")
    _cache_del("config")
    # fallback local
    local_cfg = {k: v for k, v in cfg.items() if k != "id"}
    _save_local_json("config.json", local_cfg)

def get_services():
    rows = sb_select("servicos")
    out = []
    for r in rows:
        s = {k: v for k, v in r.items()}
        if "descr" in s:
            s["desc"] = s.pop("descr")
        out.append(s)
    return out

def save_services(sv):
    rows = []
    for s in sv:
        r = dict(s)
        if "desc" in r:
            r["descr"] = r.pop("desc")
        rows.append(r)
    sb_replace("servicos", rows)

def get_bookings():
    return sb_select("agendamentos")

def save_bookings(bs):
    sb_replace("agendamentos", bs)

def schedule_for(date_str):
    d = datetime.strptime(date_str, "%Y-%m-%d")
    wd = d.weekday()
    h = get_config()["hours"]
    if wd == 0:
        key = "monday"
    elif wd == 5:
        key = "saturday"
    elif wd == 6:
        key = "sunday"
    else:
        key = "weekdays"
    return h[key], key, d

def all_slots(sched, step=30):
    if not sched.get("isOpen") or not sched.get("open") or not sched.get("close"):
        return []
    start = datetime.strptime(sched["open"], "%H:%M")
    end = datetime.strptime(sched["close"], "%H:%M")
    out = []
    cur = start
    while cur < end:
        out.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=step)
    return out

def slot_state(date_str, t):
    cfg = get_config()
    _, _, d = schedule_for(date_str)
    now = datetime.now()
    st = datetime.combine(d.date(), datetime.strptime(t, "%H:%M").time())
    if st <= now:
        return "past"
    if [date_str, t] in cfg.get("blockedSlots", []):
        return "blocked"
    for b in get_bookings():
        if b.get("date") == date_str and b.get("time") == t and b.get("status") != "cancelado":
            return "booked"
    return "open"

def slots_for(date_str):
    sched, key, d = schedule_for(date_str)
    cfg = get_config()
    blocked_day = date_str in cfg.get("blockedDays", [])
    slots = []
    if not blocked_day:
        slots = [{"time": t, "status": slot_state(date_str, t)} for t in all_slots(sched)]
    return {
        "date": date_str,
        "dayName": ["Segunda-feira","Terça-feira","Quarta-feira","Quinta-feira","Sexta-feira","Sábado","Domingo"][d.weekday()],
        "group": key,
        "schedule": sched,
        "blockedDay": blocked_day,
        "slots": slots
    }

def open_slots(date_str):
    return [s["time"] for s in slots_for(date_str)["slots"] if s["status"] == "open"]

def pick_barber(pref):
    barbers = [b["name"] for b in get_config().get("barbers", []) if b.get("name")]
    if not barbers:
        return pref
    if pref in ("", "Aleatório", "Sem preferência", "aleatório"):
        return secrets.choice(barbers)
    return pref

def next_id():
    return secrets.token_hex(4)

def _bg_post_booking(b):
    for _ in range(4):
        ent = _CACHE.get("agendamentos")
        if ent is not None and not any(r.get("id") == b["id"] for r in ent[1]):
            return
        if _sb("POST", "/agendamentos", payload=b, prefer="return=representation") is not None:
            ent = _CACHE.get("agendamentos")
            if ent is not None and not any(r.get("id") == b["id"] for r in ent[1]):
                _sb("DELETE", "/agendamentos?id=eq." + b["id"])
            return
        time.sleep(5)

def add_booking(data, status="pendente"):
    b = {
        "id": next_id(),
        "type": data.get("type", "agendamento"),
        "name": data.get("name", ""),
        "whatsapp": data.get("whatsapp", ""),
        "barber": data.get("barber", ""),
        "service": data.get("service", ""),
        "date": data.get("date", ""),
        "time": data.get("time", ""),
        "value": data.get("value", ""),
        "created": datetime.now().isoformat(timespec="minutes"),
        "status": status
    }
    bs = get_bookings()
    bs.append(b)
    _cache_set("agendamentos", bs)
    _save_local_json("bookings.json", bs)
    threading.Thread(target=_bg_post_booking, args=(b,), daemon=True).start()
    return b

MIME = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon"
}

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type,X-Admin-Token")
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        ln = int(self.headers.get("Content-Length", 0) or 0)
        if ln == 0:
            return {}
        try:
            return json.loads(self.rfile.read(ln).decode("utf-8"))
        except Exception:
            return {}

    def _auth(self):
        return self.headers.get("X-Admin-Token") == SESSION["token"]

    def _serve_static(self, path):
        if path in ("", "/"):
            path = "index.html"
        clean = os.path.normpath(path).lstrip("/\\")
        full = os.path.join(BASE, clean)
        if not full.startswith(BASE) or not os.path.isfile(full):
            self._send_json({"error": "not found"}, 404)
            return
        ext = os.path.splitext(full)[1].lower()
        with open(full, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", MIME.get(ext, "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type,X-Admin-Token")
        self.end_headers()

    def _route(self):
        if self.path.startswith("/api/"):
            self._handle_api()
        else:
            p = urlparse(self.path).path
            self._serve_static(p)

    def do_GET(self): self._route()
    def do_POST(self): self._route()
    def do_PUT(self): self._route()
    def do_PATCH(self): self._route()
    def do_DELETE(self): self._route()

    def _handle_api(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            q = parse_qs(parsed.query)
            method = self.command
            body = self._read_body() if method in ("POST", "PUT", "PATCH") else None

            # ---- public state ----
            if path == "/api/state" and method == "GET":
                cfg = get_config()
                self._send_json({
                    "shopName": cfg.get("shopName", "BlackBarber"),
                    "instagram": cfg.get("instagram", ""),
                    "whatsappNumber": cfg.get("whatsappNumber", ""),
                    "address": cfg.get("address", ""),
                    "hours": cfg.get("hours", {}),
                    "blockedDays": cfg.get("blockedDays", []),
                    "services": get_services(),
                    "barbers": cfg.get("barbers", DEFAULT_BARBERS)
                })
                return

            # ---- slots (public) ----
            if path == "/api/slots" and method == "GET":
                date = q.get("date", [""])[0]
                try:
                    datetime.strptime(date, "%Y-%m-%d")
                except Exception:
                    self._send_json({"error": "data inválida"}, 400)
                    return
                info = slots_for(date)
                info["slots"] = [s for s in info["slots"] if s["status"] == "open"]
                self._send_json(info)
                return

            # ---- admin login ----
            if path == "/api/login" and method == "POST":
                pwd = (body or {}).get("password", "")
                cfg = get_config()
                if pwd == cfg.get("adminPassword", "admin"):
                    self._send_json({"token": SESSION["token"], "shopName": cfg.get("shopName")})
                else:
                    self._send_json({"error": "senha incorreta"}, 401)
                return

            # ---- public booking ----
            if path == "/api/bookings" and method == "POST":
                cfg = get_config()
                name = str((body or {}).get("name", "")).strip()
                whats = str((body or {}).get("whatsapp", "")).replace(r"\D", "").strip()
                whats = ("".join(ch for ch in whats if ch.isdigit()))
                barber = str((body or {}).get("barber", "")).strip()
                date = str((body or {}).get("date", "")).strip()
                time = str((body or {}).get("time", "")).strip()
                if not name or len(whats) < 10:
                    self._send_json({"error": "nome ou whatsapp inválido"}, 400)
                    return
                try:
                    datetime.strptime(date, "%Y-%m-%d")
                except Exception:
                    self._send_json({"error": "data inválida"}, 400)
                    return
                if time not in open_slots(date):
                    self._send_json({"error": "horário indisponível", "taken": True}, 409)
                    return
                with LOCK:
                    if time not in open_slots(date):
                        self._send_json({"error": "horário indisponível", "taken": True}, 409)
                        return
                    b = add_booking({
                        "type": "agendamento", "name": name, "whatsapp": whats,
                        "barber": barber, "service": "", "date": date, "time": time
                    })
                self._send_json({"ok": True, "id": b["id"]})
                return

            # ---- admin-only below ----
            if not self._auth():
                self._send_json({"error": "não autorizado"}, 401)
                return

            # overview
            if path == "/api/admin/overview" and method == "GET":
                now = datetime.now()
                today = now.strftime("%Y-%m-%d")
                bs = get_bookings()
                active = [b for b in bs if b.get("status") != "cancelado"]
                today_list = [b for b in active if b.get("date") == today]
                def _dt(b):
                    try:
                        return datetime.strptime(b.get("date", "") + " " + (b.get("time", "") or "00:00"), "%Y-%m-%d %H:%M")
                    except Exception:
                        return None
                upcoming = [b for b in active if b.get("status") in ("pendente", "confirmado") and (_dt(b) is not None and _dt(b) >= now)]
                upcoming = sorted(upcoming, key=lambda b: (_dt(b), b.get("time") or ""))
                self._send_json({
                    "today": len(today_list),
                    "avulsoToday": len([b for b in today_list if b.get("type") == "avulso"]),
                    "pending": len([b for b in active if b.get("status") == "pendente"]),
                    "confirmed": len([b for b in active if b.get("status") == "confirmado"]),
                    "done": len([b for b in bs if b.get("status") == "concluido"]),
                    "total": len(active),
                    "next": upcoming[0] if upcoming else None
                })
                return

            # bookings list
            if path == "/api/admin/bookings" and method == "GET":
                bs = get_bookings()
                date = q.get("date", [None])[0]
                status = q.get("status", [None])[0]
                if date:
                    bs = [b for b in bs if b.get("date") == date]
                if status:
                    bs = [b for b in bs if b.get("status") == status]
                bs = sorted(bs, key=lambda b: (b.get("date") or "", b.get("time") or ""))
                self._send_json({"bookings": bs})
                return

            # admin create booking (manual)
            if path == "/api/admin/booking" and method == "POST":
                data = body or {}
                date = str(data.get("date", "")).strip()
                time = str(data.get("time", "")).strip()
                if not date or not datetime.strptime(date, "%Y-%m-%d"):
                    self._send_json({"error": "data inválida"}, 400)
                    return
                if time not in open_slots(date):
                    self._send_json({"error": "horário indisponível"}, 409)
                    return
                with LOCK:
                    if time not in open_slots(date):
                        self._send_json({"error": "horário indisponível"}, 409)
                        return
                    b = add_booking({**data, "barber": pick_barber(data.get("barber", ""))}, status="confirmado")
                self._send_json({"ok": True, "booking": b}, 201)
                return

            # create avulso
            if path == "/api/admin/avulso" and method == "POST":
                data = body or {}
                date = str(data.get("date", "")).strip()
                time = str(data.get("time", "")).strip()
                if not date:
                    date = datetime.now().strftime("%Y-%m-%d")
                try:
                    datetime.strptime(date, "%Y-%m-%d")
                except Exception:
                    self._send_json({"error": "data inválida"}, 400)
                    return
                if not time:
                    time = datetime.now().strftime("%H:%M")
                with LOCK:
                    b = add_booking({**data, "type": "avulso", "date": date, "time": time, "barber": pick_barber(data.get("barber", ""))}, status="concluido")
                self._send_json({"ok": True, "booking": b}, 201)
                return

            # barbers CRUD (admin)
            if path == "/api/admin/barbers" and method in ("GET", "POST"):
                cfg = get_config()
                if method == "GET":
                    self._send_json({"barbers": cfg.get("barbers", DEFAULT_BARBERS)})
                    return
                data = body or {}
                if not data.get("name"):
                    self._send_json({"error": "nome obrigatório"}, 400)
                    return
                item = {
                    "id": data.get("id") or next_id(),
                    "name": data.get("name", ""),
                    "photo": data.get("photo", ""),
                    "role": data.get("role", "")
                }
                cfg["barbers"] = cfg.get("barbers", DEFAULT_BARBERS) + [item]
                save_config(cfg)
                self._send_json({"ok": True, "barber": item}, 201)
                return
            if path.startswith("/api/admin/barbers/"):
                bid = path.split("/")[-1]
                cfg = get_config()
                bindex = next((i for i, b in enumerate(cfg.get("barbers", [])) if b.get("id") == bid), None)
                if bindex is None:
                    self._send_json({"error": "barbeiro não encontrado"}, 404)
                    return
                if method == "PUT":
                    data = body or {}
                    for k in ("name", "photo", "role"):
                        if k in data:
                            cfg["barbers"][bindex][k] = data[k]
                    save_config(cfg)
                    self._send_json({"ok": True, "barber": cfg["barbers"][bindex]})
                    return
                if method == "DELETE":
                    cfg["barbers"].pop(bindex)
                    save_config(cfg)
                    self._send_json({"ok": True})
                    return

            # update / delete booking
            if path.startswith("/api/admin/booking/") and path.count("/") == 4:
                bid = path.split("/")[-1]
                bs = get_bookings()
                idx = next((i for i, b in enumerate(bs) if b.get("id") == bid), None)
                if idx is None:
                    self._send_json({"error": "agendamento não encontrado"}, 404)
                    return
                if method == "PATCH" and body is not None:
                    if "status" in body:
                        bs[idx]["status"] = body["status"]
                    if "value" in body:
                        bs[idx]["value"] = body["value"]
                    fields = {k: bs[idx][k] for k in ("status", "value") if k in body}
                    if _sb("PATCH", "/agendamentos?id=eq." + bid, payload=fields, prefer="return=representation") is None:
                        sb_replace("agendamentos", bs)
                    else:
                        _cache_set("agendamentos", bs)
                        _save_local_json("bookings.json", bs)
                    self._send_json({"ok": True, "booking": bs[idx]})
                    return
                if method == "DELETE":
                    removed = bs.pop(idx)
                    if _sb("DELETE", "/agendamentos?id=eq." + bid) is None:
                        sb_replace("agendamentos", bs)
                    else:
                        _cache_set("agendamentos", bs)
                        _save_local_json("bookings.json", bs)
                    self._send_json({"ok": True, "removed": removed})
                    return
                self._send_json({"error": "método não suportado"}, 400)
                return

            # services admin
            if path == "/api/admin/services" and method in ("GET", "POST"):
                sv = get_services()
                if method == "GET":
                    self._send_json({"services": sv})
                    return
                data = body or {}
                if not data.get("name"):
                    self._send_json({"error": "nome obrigatório"}, 400)
                    return
                item = {
                    "id": data.get("id") or next_id(),
                    "name": data.get("name", ""),
                    "desc": data.get("desc", ""),
                    "price": data.get("price", "0"),
                    "icon": data.get("icon", "✂️")
                }
                sv.append(item)
                save_services(sv)
                self._send_json({"ok": True, "service": item}, 201)
                return
            if path.startswith("/api/admin/services/"):
                sid = path.split("/")[-1]
                sv = get_services()
                idx = next((i for i, s in enumerate(sv) if s.get("id") == sid), None)
                if idx is None:
                    self._send_json({"error": "serviço não encontrado"}, 404)
                    return
                if method == "PUT":
                    data = body or {}
                    for k in ("name", "desc", "price", "icon"):
                        if k in data:
                            sv[idx][k] = data[k]
                    save_services(sv)
                    self._send_json({"ok": True, "service": sv[idx]})
                    return
                if method == "DELETE":
                    sv.pop(idx)
                    save_services(sv)
                    self._send_json({"ok": True})
                    return

            # config
            if path == "/api/admin/config" and method == "GET":
                self._send_json(get_config())
                return
            if path == "/api/admin/config" and method == "PUT":
                new = body or {}
                cfg = get_config()
                for k in ("shopName", "instagram", "whatsappNumber", "adminPassword", "address", "hours", "blockedDays", "blockedSlots"):
                    if k in new:
                        cfg[k] = new[k]
                save_config(cfg)
                self._send_json({"ok": True, "config": cfg})
                return

            # block / unblock day
            if path == "/api/admin/block-day" and method == "POST":
                date = str((body or {}).get("date", "")).strip()
                cfg = get_config()
                if date and date not in cfg["blockedDays"]:
                    cfg["blockedDays"].append(date)
                    save_config(cfg)
                self._send_json({"ok": True, "blockedDays": cfg["blockedDays"]})
                return
            if path.startswith("/api/admin/block-day/"):
                date = path.split("/")[-1]
                cfg = get_config()
                cfg["blockedDays"] = [d for d in cfg.get("blockedDays", []) if d != date]
                save_config(cfg)
                self._send_json({"ok": True, "blockedDays": cfg["blockedDays"]})
                return

            # block / unblock slot
            if path == "/api/admin/block-slot" and method == "POST":
                date = str((body or {}).get("date", "")).strip()
                time = str((body or {}).get("time", "")).strip()
                cfg = get_config()
                if date and time and [date, time] not in cfg["blockedSlots"]:
                    cfg["blockedSlots"].append([date, time])
                    save_config(cfg)
                self._send_json({"ok": True, "blockedSlots": cfg["blockedSlots"]})
                return
            if path.startswith("/api/admin/block-slot/"):
                tail = urlparse(path).path.split("/")[-1]
                try:
                    parts = parse_qs(tail)
                    date, time = tail.split("%2C") if "%2C" in tail else (tail, "")
                    if "," in tail:
                        date, time = tail.split(",", 1)
                    else:
                        date, time = tail.split("%2C") if "%2C" in tail else (tail.split("_")[0], tail.split("_")[1] if "_" in tail else "")
                except Exception:
                    self._send_json({"error": "rota inválida"}, 400)
                    return
                cfg = get_config()
                cfg["blockedSlots"] = [s for s in cfg.get("blockedSlots", []) if not (s[0] == date and s[1] == time)]
                save_config(cfg)
                self._send_json({"ok": True, "blockedSlots": cfg["blockedSlots"]})
                return

            # admin all slots for a date
            if path == "/api/admin/slots" and method == "GET":
                date = q.get("date", [""])[0]
                try:
                    datetime.strptime(date, "%Y-%m-%d")
                except Exception:
                    self._send_json({"error": "data inválida"}, 400)
                    return
                self._send_json(slots_for(date))
                return

            self._send_json({"error": "rota não encontrada"}, 404)
        except BrokenPipeError:
            pass
        except Exception as e:
            try:
                self._send_json({"error": str(e)}, 500)
            except Exception:
                pass

    def log_message(self, fmt, *args):
        pass

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    bind_ip = os.environ.get("BIND_IP", "0.0.0.0")
    try:
        srv = ThreadingHTTPServer((bind_ip, port), Handler)
    except OSError:
        srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        print(f"Bind to {bind_ip} failed, using 127.0.0.1")
    print(f"BlackBarber server on {bind_ip}:{port}")
    def _warmup():
        time.sleep(2)
        for t in ("config", "servicos", "agendamentos"):
            try:
                sb_select(t)
            except Exception:
                pass
    threading.Thread(target=_warmup, daemon=True).start()
    srv.serve_forever()