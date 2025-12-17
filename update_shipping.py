import os
import time
import random
import requests
from datetime import datetime
from dateutil.parser import parse
from zoneinfo import ZoneInfo

# =========================
# CONFIG
# =========================
# Shopify API
SHOP_URL = os.getenv("SHOP_URL", "https://48d471-2.myshopify.com")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2023-10")
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

# CTT API
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="

# Zona horaria para comparar "hoy"
TZ_NAME = os.getenv("TZ_NAME", "Europe/Madrid")

# Logs
LOG_FILE = os.getenv("LOG_FILE", "logs_actualizacion_envios.txt")

# Límites / resiliencia CTT
CTT_MAX_RETRIES = int(os.getenv("CTT_MAX_RETRIES", "6"))
CTT_BASE_BACKOFF = float(os.getenv("CTT_BASE_BACKOFF", "0.7"))  # segundos
CTT_MAX_BACKOFF = float(os.getenv("CTT_MAX_BACKOFF", "25"))     # segundos
CTT_THROTTLE_SECONDS = float(os.getenv("CTT_THROTTLE_SECONDS", "0.8"))  # delay entre requests a CTT

# Shopify (timeouts)
SHOPIFY_TIMEOUT = float(os.getenv("SHOPIFY_TIMEOUT", "30"))

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
# (Shopify rate limit se gestiona bastante bien con paginación + pocas llamadas,
# pero dejamos sesión para keep-alive)
SHOP_SESSION.headers.update({"Content-Type": "application/json"})


def log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")
    except Exception:
        pass
    print(message)


def shopify_headers():
    return {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}


def safe_snippet(text: str, n: int = 220) -> str:
    return (text or "")[:n].replace("\n", " ").replace("\r", " ")


def get_fulfilled_orders(limit=500):
    """Obtiene hasta 'limit' pedidos con fulfillment completado."""
    all_orders = []
    url = f"{SHOP_URL}/admin/api/{API_VERSION}/orders.json"
    params = {
        "fulfillment_status": "fulfilled",
        "status": "any",
        "limit": 50,
        "order": "created_at desc",
    }

    while len(all_orders) < limit:
        r = SHOP_SESSION.get(url, headers=shopify_headers(), params=params, timeout=SHOPIFY_TIMEOUT)
        r.raise_for_status()
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


def parse_ctt_datetime(event_date_str: str | None, tz: ZoneInfo):
    """Parsea la fecha de CTT y la normaliza a tz. Si viene sin tz, asumimos tz."""
    if not event_date_str:
        return None
    dt = parse(event_date_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    else:
        dt = dt.astimezone(tz)
    return dt


def map_ctt_to_shopify(status: str):
    """Mapea el estado devuelto por CTT al formato de Shopify."""
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
    """
    Consulta el estado desde CTT y devuelve {"status": str, "date": str|None}.
    Robusto contra respuestas no-JSON, vacías, y 429 (retry con backoff).
    """
    url = CTT_API_URL + str(tracking_number)
    last_err = None

    for attempt in range(1, CTT_MAX_RETRIES + 1):
        try:
            r = CTT_SESSION.get(url, timeout=30, allow_redirects=True)

            # Rate limit: 429 => backoff + retry
            if r.status_code == 429:
                wait = min(CTT_BASE_BACKOFF * (2 ** (attempt - 1)), CTT_MAX_BACKOFF)
                wait = wait * (0.85 + random.random() * 0.5)  # jitter 0.85–1.35
                log(f"⏳ CTT {tracking_number}: 429 Too Many Requests. Reintento {attempt}/{CTT_MAX_RETRIES} en {wait:.2f}s")
                time.sleep(wait)
                continue

            # Errores HTTP != 200: no tiene sentido reintentar mucho (salvo 5xx)
            if r.status_code != 200:
                snippet = safe_snippet(r.text)
                log(f"⚠️ CTT {tracking_number}: HTTP {r.status_code}. Body(220)={snippet!r}")
                # 5xx a veces merece retry
                if 500 <= r.status_code < 600 and attempt < CTT_MAX_RETRIES:
                    wait = min(CTT_BASE_BACKOFF * (2 ** (attempt - 1)), CTT_MAX_BACKOFF)
                    time.sleep(wait)
                    continue
                return {"status": "CTT API error", "date": None}

            text = (r.text or "").strip()
            if not text:
                # vacío: puede ser intermitente; reintenta un poco
                log(f"⚠️ CTT {tracking_number}: respuesta vacía (intento {attempt}/{CTT_MAX_RETRIES})")
                if attempt < CTT_MAX_RETRIES:
                    time.sleep(CTT_BASE_BACKOFF * attempt)
                    continue
                return {"status": "CTT respuesta vacía", "date": None}

            try:
                data = r.json()
            except Exception:
                snippet = safe_snippet(text)
                log(f"⚠️ CTT {tracking_number}: no JSON. Body(220)={snippet!r}")
                # a veces llega HTML temporal; reintenta un poco
                if attempt < CTT_MAX_RETRIES:
                    time.sleep(CTT_BASE_BACKOFF * attempt)
                    continue
                return {"status": "CTT no JSON", "date": None}

            if data.get("error") is not None:
                return {"status": "Error en API CTT", "date": None}

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
            wait = wait * (0.85 + random.random() * 0.5)
            log(f"⚠️ CTT {tracking_number}: error de red {attempt}/{CTT_MAX_RETRIES}: {e}. Espero {wait:.2f}s")
            time.sleep(wait)

    log(f"❌ CTT {tracking_number}: fallo tras reintentos: {last_err}")
    return {"status": "CTT error red", "date": None}


def get_fulfillment_events(order_id: int, fulfillment_id: int):
    """Devuelve lista de eventos del fulfillment (Shopify)."""
    url = f"{SHOP_URL}/admin/api/{API_VERSION}/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    r = SHOP_SESSION.get(url, headers=shopify_headers(), timeout=SHOPIFY_TIMEOUT)
    if r.status_code != 200:
        log(f"❌ No se pudo obtener eventos para {order_id}/{fulfillment_id}: {r.status_code} - {safe_snippet(r.text, 300)}")
        return []
    return r.json().get("events", []) or []


def fulfillment_has_status_anywhere(events: list, status: str) -> bool:
    """True si existe ese status en cualquier evento del fulfillment."""
    for ev in events:
        if ev.get("status") == status:
            return True
    return False


def create_fulfillment_event(order_id: int, fulfillment_id: int, ctt_status: str, ctt_event_date_str: str | None):
    """
    Reglas:
    - Si Shopify ya tiene 'delivered' => NO hace nada (candado fuerte).
    - SOLO crea eventos si la fecha del último evento CTT es HOY (TZ_NAME).
    - Idempotencia TOTAL por estado: si Shopify ya tuvo ese 'status' alguna vez => NO lo repite.
      (Evita notificaciones duplicadas.)
    """
    tz = ZoneInfo(TZ_NAME)
    today_local = datetime.now(tz).date()

    event_status = map_ctt_to_shopify(ctt_status)
    ctt_dt = parse_ctt_datetime(ctt_event_date_str, tz)

    # Si no hay fecha, no actualizamos
    if ctt_dt is None:
        log(f"⏭️ SKIP {order_id}: CTT sin fecha de evento (no actualizo nada)")
        return

    # Solo si es HOY
    if ctt_dt.date() != today_local:
        if event_status == "delivered":
            log(f"⏭️ SKIP {order_id}: CTT='Entregado' pero fecha {ctt_dt.date()} != hoy {today_local}")
        else:
            log(f"⏭️ SKIP {order_id}: CTT fecha {ctt_dt.date()} != hoy {today_local} (no actualizo)")
        return

    # Traemos eventos una sola vez
    events = get_fulfillment_events(order_id, fulfillment_id)

    # Candado fuerte: si ya hay delivered, no tocar
    if fulfillment_has_status_anywhere(events, "delivered"):
        log(f"⏭️ SKIP {order_id}: ya tiene 'delivered' en Shopify (idempotente total)")
        return

    # Idempotencia total por estado: si ya se creó ese status alguna vez, no repetir
    if fulfillment_has_status_anywhere(events, event_status):
        log(f"⏭️ SKIP {order_id}: ya tuvo '{event_status}' en Shopify (no duplico notificación)")
        return

    # Crear evento
    url = f"{SHOP_URL}/admin/api/{API_VERSION}/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    payload = {
        "event": {
            "status": event_status,
            "message": f"Estado CTT: {ctt_status}",
            "created_at": ctt_dt.isoformat(),
        }
    }

    r = SHOP_SESSION.post(url, headers=shopify_headers(), json=payload, timeout=SHOPIFY_TIMEOUT)
    if r.status_code == 201:
        log(f"✅ Evento '{event_status}' añadido a pedido {order_id} (CTT: {ctt_status}, fecha: {ctt_dt.date()})")
    else:
        log(f"❌ Error al añadir evento en pedido {order_id}: {r.status_code} - {safe_snippet(r.text, 300)}")


def main():
    if not ACCESS_TOKEN:
        raise RuntimeError("Falta SHOPIFY_ACCESS_TOKEN en el entorno")

    orders = get_fulfilled_orders()
    log(f"🔄 Procesando {len(orders)} pedidos... (TZ={TZ_NAME})")

    for order in orders:
        fulfillments = order.get("fulfillments", [])
        if not fulfillments:
            continue

        fulfillment = fulfillments[0]
        order_id = order["id"]
        fulfillment_id = fulfillment["id"]

        tracking_number = fulfillment.get("tracking_number")
        if not tracking_number:
            log(f"⚠️ Pedido {order_id} sin número de seguimiento")
            continue

        # Estado actual y fecha en CTT
        ctt_result = get_ctt_status(tracking_number)
        ctt_status = ctt_result.get("status")
        ctt_date = ctt_result.get("date")

        # Throttle fijo para no provocar rate-limit
        time.sleep(CTT_THROTTLE_SECONDS)

        if not ctt_status:
            log(f"⏭️ SKIP {order_id}: CTT sin status (tracking {tracking_number})")
            continue

        if "error" in (ctt_status or "").lower():
            log(f"⚠️ Error con CTT para {order_id}: {ctt_status}")
            continue

        # Crear/actualizar evento con reglas anti-duplicados y "solo hoy"
        create_fulfillment_event(order_id, fulfillment_id, ctt_status, ctt_event_date_str=ctt_date)


if __name__ == "__main__":
    main()
