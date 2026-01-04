import os
import time
import random
import sqlite3
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dateutil.parser import parse

# =========================
# CONFIG
# =========================
SHOP_URL = os.getenv("SHOP_URL", "https://48d471-2.myshopify.com").rstrip("/")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2023-10")
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="

TZ_NAME = os.getenv("TZ_NAME", "Europe/Madrid")
TZ = ZoneInfo(TZ_NAME)

LOG_FILE = os.getenv("LOG_FILE", "logs_actualizacion_envios.txt")

# Pedidos a “descubrir” en Shopify (para meter nuevos envíos en la DB)
MAX_SHOPIFY_ORDERS = int(os.getenv("MAX_SHOPIFY_ORDERS", "500"))

# Revisión normal: 0 = cada ejecución
NORMAL_RECHECK_MINUTES = int(os.getenv("NORMAL_RECHECK_MINUTES", "0"))

# Límites / resiliencia CTT
CTT_MAX_RETRIES = int(os.getenv("CTT_MAX_RETRIES", "6"))
CTT_BASE_BACKOFF = float(os.getenv("CTT_BASE_BACKOFF", "0.7"))
CTT_MAX_BACKOFF = float(os.getenv("CTT_MAX_BACKOFF", "25"))
CTT_THROTTLE_SECONDS = float(os.getenv("CTT_THROTTLE_SECONDS", "0.8"))

# Shopify (timeouts)
SHOPIFY_TIMEOUT = float(os.getenv("SHOPIFY_TIMEOUT", "30"))

# Estado persistente (SQLite) + carpeta cacheable
STATE_DIR = os.getenv("STATE_DIR", ".state")
STATE_DB_PATH = os.getenv("STATE_DB_PATH", os.path.join(STATE_DIR, "shipping_state.sqlite3"))

# =========================
# HTTP SESSIONS
# =========================
CTT_SESSION = requests.Session()
CTT_SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; DondeFueBot/1.0)",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
)

SHOP_SESSION = requests.Session()
SHOP_SESSION.headers.update({"Content-Type": "application/json"})


# =========================
# LOG
# =========================
def log(message: str):
    ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {message}\n")
    except Exception:
        pass
    print(message)


def safe_snippet(text: str, n: int = 220) -> str:
    return (text or "")[:n].replace("\n", " ").replace("\r", " ")


# =========================
# SHOPIFY HELPERS
# =========================
def shopify_headers():
    if not ACCESS_TOKEN:
        raise RuntimeError("Falta SHOPIFY_ACCESS_TOKEN en el entorno")
    return {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}


def shopify_get(url: str, params=None):
    r = SHOP_SESSION.get(url, headers=shopify_headers(), params=params, timeout=SHOPIFY_TIMEOUT)
    r.raise_for_status()
    return r


def shopify_post(url: str, payload: dict):
    r = SHOP_SESSION.post(url, headers=shopify_headers(), json=payload, timeout=SHOPIFY_TIMEOUT)
    return r


def get_fulfilled_orders(limit=500):
    """Obtiene hasta 'limit' pedidos con fulfillments (fulfilled)."""
    all_orders = []
    url = f"{SHOP_URL}/admin/api/{API_VERSION}/orders.json"
    params = {
        "fulfillment_status": "fulfilled",
        "status": "any",
        "limit": 50,
        "order": "created_at desc",
    }

    while len(all_orders) < limit:
        r = shopify_get(url, params=params)
        data = r.json()
        orders = data.get("orders", [])
        if not orders:
            break

        all_orders.extend(orders)

        # Paginación (Link header)
        if "Link" in r.headers and 'rel="next"' in r.headers["Link"]:
            url = r.links["next"]["url"]
            params = None
        else:
            break

    return all_orders[:limit]


def get_fulfillment_events(order_id: int, fulfillment_id: int):
    url = f"{SHOP_URL}/admin/api/{API_VERSION}/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    r = SHOP_SESSION.get(url, headers=shopify_headers(), timeout=SHOPIFY_TIMEOUT)
    if r.status_code != 200:
        log(f"❌ Eventos Shopify {order_id}/{fulfillment_id}: {r.status_code} - {safe_snippet(r.text, 300)}")
        return []
    return r.json().get("events", []) or []


def fulfillment_has_status(events: list, status: str) -> bool:
    return any(ev.get("status") == status for ev in (events or []))


def create_shopify_event(
    order_id: int,
    fulfillment_id: int,
    status: str,
    message: str,
    created_at_iso: str | None,
):
    url = f"{SHOP_URL}/admin/api/{API_VERSION}/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    payload = {"event": {"status": status, "message": message}}
    if created_at_iso:
        payload["event"]["created_at"] = created_at_iso

    r = shopify_post(url, payload)
    if r.status_code == 201:
        return True, None
    return False, f"{r.status_code} - {safe_snippet(r.text, 300)}"


# =========================
# CTT HELPERS
# =========================
def parse_dt_any(dt_str: str | None):
    if not dt_str:
        return None
    dt = parse(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    else:
        dt = dt.astimezone(TZ)
    return dt


def map_ctt_to_shopify(status: str) -> str:
    # Ajusta aquí tus traducciones si ves más estados reales
    status_map = {
        "En reparto": "out_for_delivery",
        "Entrega hoy": "out_for_delivery",
        "Entregado": "delivered",
        "En tránsito": "in_transit",
        "En transito": "in_transit",
        "Recogido": "in_transit",
        "Pendiente de recepción en CTT Express": "confirmed",
        "Reparto fallido": "failure",
    }
    return status_map.get(status, "in_transit")


def get_ctt_status(tracking_number: str):
    """Devuelve {"status": str|None, "date": str|None} con retries."""
    url = CTT_API_URL + str(tracking_number)
    last_err = None

    for attempt in range(1, CTT_MAX_RETRIES + 1):
        try:
            r = CTT_SESSION.get(url, timeout=30, allow_redirects=True)

            if r.status_code == 429:
                wait = min(CTT_BASE_BACKOFF * (2 ** (attempt - 1)), CTT_MAX_BACKOFF)
                wait *= (0.85 + random.random() * 0.5)
                log(f"⏳ CTT {tracking_number}: 429. Reintento {attempt}/{CTT_MAX_RETRIES} en {wait:.2f}s")
                time.sleep(wait)
                continue

            if r.status_code != 200:
                snippet = safe_snippet(r.text)
                log(f"⚠️ CTT {tracking_number}: HTTP {r.status_code}. Body(220)={snippet!r}")
                if 500 <= r.status_code < 600 and attempt < CTT_MAX_RETRIES:
                    wait = min(CTT_BASE_BACKOFF * (2 ** (attempt - 1)), CTT_MAX_BACKOFF)
                    time.sleep(wait)
                    continue
                return {"status": None, "date": None}

            text = (r.text or "").strip()
            if not text:
                log(f"⚠️ CTT {tracking_number}: respuesta vacía ({attempt}/{CTT_MAX_RETRIES})")
                if attempt < CTT_MAX_RETRIES:
                    time.sleep(CTT_BASE_BACKOFF * attempt)
                    continue
                return {"status": None, "date": None}

            try:
                data = r.json()
            except Exception:
                snippet = safe_snippet(text)
                log(f"⚠️ CTT {tracking_number}: no JSON. Body(220)={snippet!r}")
                if attempt < CTT_MAX_RETRIES:
                    time.sleep(CTT_BASE_BACKOFF * attempt)
                    continue
                return {"status": None, "date": None}

            if data.get("error") is not None:
                return {"status": None, "date": None}

            events = data.get("data", {}).get("shipping_history", {}).get("events", [])
            if not events:
                return {"status": "Sin eventos", "date": None}

            last_event = events[-1]
            return {
                "status": last_event.get("description", "Estado desconocido"),
                "date": last_event.get("event_date"),
            }

        except requests.RequestException as e:
            last_err = e
            wait = min(CTT_BASE_BACKOFF * (2 ** (attempt - 1)), CTT_MAX_BACKOFF)
            wait *= (0.85 + random.random() * 0.5)
            log(f"⚠️ CTT {tracking_number}: red {attempt}/{CTT_MAX_RETRIES}: {e}. Espero {wait:.2f}s")
            time.sleep(wait)

    log(f"❌ CTT {tracking_number}: fallo tras reintentos: {last_err}")
    return {"status": None, "date": None}


# =========================
# SQLITE STATE
# =========================
def db_connect():
    os.makedirs(STATE_DIR, exist_ok=True)
    conn = sqlite3.connect(STATE_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def db_init(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS shipments (
            order_id INTEGER NOT NULL,
            fulfillment_id INTEGER NOT NULL,
            tracking_number TEXT,
            shipped_at TEXT,

            is_delivered INTEGER NOT NULL DEFAULT 0,
            delivered_at TEXT,

            last_ctt_status TEXT,
            last_ctt_event_at TEXT,
            last_shopify_status TEXT,

            last_checked_at TEXT,
            next_check_at TEXT,
            last_error TEXT,

            PRIMARY KEY (order_id, fulfillment_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shipments_pending ON shipments(is_delivered, next_check_at)")
    conn.commit()


def db_upsert_shipment(
    conn: sqlite3.Connection,
    order_id: int,
    fulfillment_id: int,
    tracking_number: str,
    shipped_at: str | None,
):
    conn.execute(
        """
        INSERT INTO shipments (order_id, fulfillment_id, tracking_number, shipped_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(order_id, fulfillment_id) DO UPDATE SET
            tracking_number=excluded.tracking_number,
            shipped_at=COALESCE(shipments.shipped_at, excluded.shipped_at)
        """,
        (order_id, fulfillment_id, tracking_number, shipped_at),
    )
    conn.commit()


def db_mark_delivered(conn: sqlite3.Connection, order_id: int, fulfillment_id: int, delivered_at_iso: str | None):
    conn.execute(
        """
        UPDATE shipments
        SET is_delivered=1, delivered_at=?, next_check_at=NULL
        WHERE order_id=? AND fulfillment_id=?
        """,
        (delivered_at_iso, order_id, fulfillment_id),
    )
    conn.commit()


def db_update_check(
    conn: sqlite3.Connection,
    order_id: int,
    fulfillment_id: int,
    *,
    ctt_status: str | None,
    ctt_event_at: str | None,
    shopify_status: str | None,
    next_check_at: str | None,
    last_error: str | None,
):
    now_iso = datetime.now(TZ).isoformat()
    conn.execute(
        """
        UPDATE shipments
        SET last_checked_at=?,
            next_check_at=?,
            last_ctt_status=?,
            last_ctt_event_at=?,
            last_shopify_status=?,
            last_error=?
        WHERE order_id=? AND fulfillment_id=?
        """,
        (now_iso, next_check_at, ctt_status, ctt_event_at, shopify_status, last_error, order_id, fulfillment_id),
    )
    conn.commit()


def db_get_pending(conn: sqlite3.Connection, limit: int = 2000):
    now_iso = datetime.now(TZ).isoformat()
    cur = conn.execute(
        """
        SELECT order_id, fulfillment_id, tracking_number, shipped_at, last_shopify_status, next_check_at
        FROM shipments
        WHERE is_delivered=0
          AND (next_check_at IS NULL OR next_check_at <= ?)
        ORDER BY COALESCE(last_checked_at, '1970-01-01') ASC
        LIMIT ?
        """,
        (now_iso, limit),
    )
    return cur.fetchall()


# =========================
# MAIN LOGIC
# =========================
def discover_shipments_from_shopify(conn: sqlite3.Connection):
    orders = get_fulfilled_orders(limit=MAX_SHOPIFY_ORDERS)
    total_f = 0

    for order in orders:
        order_id = order.get("id")
        fulfillments = order.get("fulfillments") or []
        for f in fulfillments:
            fulfillment_id = f.get("id")

            # tracking: a veces viene en tracking_numbers
            tracking_number = f.get("tracking_number")
            if not tracking_number:
                tlist = f.get("tracking_numbers") or []
                tracking_number = tlist[0] if tlist else None

            if not (order_id and fulfillment_id and tracking_number):
                continue

            # “desde que se envía”: usamos shipped_at si existe, si no created_at del fulfillment
            shipped_at = f.get("shipped_at") or f.get("created_at") or f.get("updated_at")
            db_upsert_shipment(conn, int(order_id), int(fulfillment_id), str(tracking_number), shipped_at)
            total_f += 1

    log(f"🧠 Descubiertos/actualizados {total_f} fulfillments desde Shopify (MAX_SHOPIFY_ORDERS={MAX_SHOPIFY_ORDERS})")


def process_one(
    conn: sqlite3.Connection,
    order_id: int,
    fulfillment_id: int,
    tracking_number: str,
    shipped_at_str: str | None,
    last_shopify_status: str | None,
):
    now = datetime.now(TZ)

    # Consultar CTT
    ctt = get_ctt_status(tracking_number)
    time.sleep(CTT_THROTTLE_SECONDS)

    ctt_status = ctt.get("status")
    ctt_event_str = ctt.get("date")
    ctt_dt = parse_dt_any(ctt_event_str) or now  # si CTT no da fecha, usamos ahora para poder avanzar

    # Normalizamos
    mapped_status = map_ctt_to_shopify(ctt_status) if ctt_status else None

    # Si ya es delivered por CTT: crear delivered (una vez) y cerrar
    events = None
    if mapped_status == "delivered":
        events = events or get_fulfillment_events(order_id, fulfillment_id)

        if fulfillment_has_status(events, "delivered"):
            db_mark_delivered(conn, order_id, fulfillment_id, delivered_at_iso=ctt_dt.isoformat())
            log(f"✅ {order_id}/{fulfillment_id} ya estaba delivered en Shopify. Cierro seguimiento.")
            db_update_check(
                conn,
                order_id,
                fulfillment_id,
                ctt_status=ctt_status,
                ctt_event_at=ctt_dt.isoformat(),
                shopify_status="delivered",
                next_check_at=None,
                last_error=None,
            )
            return

        ok, err = create_shopify_event(
            order_id,
            fulfillment_id,
            status="delivered",
            message=f"Estado CTT: {ctt_status}",
            created_at_iso=ctt_dt.isoformat(),
        )
        if ok:
            db_mark_delivered(conn, order_id, fulfillment_id, delivered_at_iso=ctt_dt.isoformat())
            log(f"✅ DELIVERED {order_id}/{fulfillment_id} (tracking {tracking_number})")
            db_update_check(
                conn,
                order_id,
                fulfillment_id,
                ctt_status=ctt_status,
                ctt_event_at=ctt_dt.isoformat(),
                shopify_status="delivered",
                next_check_at=None,
                last_error=None,
            )
        else:
            log(f"❌ Error creando DELIVERED {order_id}/{fulfillment_id}: {err}")
            # Reintento pronto
            next_check = (now + timedelta(minutes=10)).isoformat()
            db_update_check(
                conn,
                order_id,
                fulfillment_id,
                ctt_status=ctt_status,
                ctt_event_at=ctt_dt.isoformat(),
                shopify_status=last_shopify_status,
                next_check_at=next_check,
                last_error=err,
            )
        return

    # Si Shopify ya tiene delivered (por cualquier cosa), cerramos
    events = events or get_fulfillment_events(order_id, fulfillment_id)
    if fulfillment_has_status(events, "delivered"):
        db_mark_delivered(conn, order_id, fulfillment_id, delivered_at_iso=None)
        log(f"✅ {order_id}/{fulfillment_id} detectado delivered en Shopify. Cierro seguimiento.")
        db_update_check(
            conn,
            order_id,
            fulfillment_id,
            ctt_status=ctt_status,
            ctt_event_at=ctt_dt.isoformat(),
            shopify_status="delivered",
            next_check_at=None,
            last_error=None,
        )
        return

    # Idempotencia: solo crear evento si el status nuevo NO existe en Shopify y además cambió vs DB
    error_msg = None
    posted_status = last_shopify_status

    if mapped_status:
        if mapped_status != last_shopify_status:
            if fulfillment_has_status(events, mapped_status):
                posted_status = mapped_status
                log(f"⏭️ {order_id}/{fulfillment_id} status '{mapped_status}' ya existe en Shopify (no duplico).")
            else:
                ok, err = create_shopify_event(
                    order_id,
                    fulfillment_id,
                    status=mapped_status,
                    message=f"Estado CTT: {ctt_status}",
                    created_at_iso=ctt_dt.isoformat(),
                )
                if ok:
                    posted_status = mapped_status
                    log(f"✅ Evento '{mapped_status}' {order_id}/{fulfillment_id} (CTT: {ctt_status})")
                else:
                    error_msg = f"Shopify event '{mapped_status}' failed: {err}"
                    log(f"❌ {error_msg}")

    # Programar siguiente revisión (solo lógica normal)
    if NORMAL_RECHECK_MINUTES <= 0:
        next_check = None
    else:
        next_check = (now + timedelta(minutes=NORMAL_RECHECK_MINUTES)).isoformat()

    db_update_check(
        conn,
        order_id,
        fulfillment_id,
        ctt_status=ctt_status,
        ctt_event_at=ctt_dt.isoformat() if ctt_dt else None,
        shopify_status=posted_status,
        next_check_at=next_check,
        last_error=error_msg,
    )


def main():
    if not ACCESS_TOKEN:
        raise RuntimeError("Falta SHOPIFY_ACCESS_TOKEN en el entorno")

    log(
        f"🚀 Sync v5-sqlite | SHOP_URL='{SHOP_URL}' | API_VERSION={API_VERSION} | TZ={TZ_NAME} | "
        f"MAX_SHOPIFY_ORDERS={MAX_SHOPIFY_ORDERS} | NORMAL_RECHECK_MINUTES={NORMAL_RECHECK_MINUTES}"
    )

    conn = db_connect()
    db_init(conn)

    # 1) Descubrir nuevos envíos (solo recientes en Shopify)
    discover_shipments_from_shopify(conn)

    # 2) Procesar pendientes (la DB evita revisar entregados)
    pending = db_get_pending(conn, limit=3000)
    log(f"🔄 Pendientes a revisar (no entregados): {len(pending)}")

    for (
        order_id,
        fulfillment_id,
        tracking_number,
        shipped_at,
        last_shopify_status,
        next_check_at,
    ) in pending:
        if not tracking_number:
            continue
        try:
            process_one(
                conn,
                int(order_id),
                int(fulfillment_id),
                str(tracking_number),
                shipped_at_str=shipped_at,
                last_shopify_status=last_shopify_status,
            )
        except Exception as e:
            log(f"❌ Excepción en {order_id}/{fulfillment_id}: {e}")
            # reintento en 30 min
            retry_at = (datetime.now(TZ) + timedelta(minutes=30)).isoformat()
            db_update_check(
                conn,
                int(order_id),
                int(fulfillment_id),
                ctt_status=None,
                ctt_event_at=None,
                shopify_status=last_shopify_status,
                next_check_at=retry_at,
                last_error=str(e),
            )

    conn.close()
    log("✅ Sync terminado")


if __name__ == "__main__":
    main()
