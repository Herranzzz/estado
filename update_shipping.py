import unicodedata
import os
import time
import requests
import typing as t
import datetime as dt
from datetime import datetime
from dateutil.parser import parse
from zoneinfo import ZoneInfo

# =========================
# CONFIG
# =========================
# Shopify API
SHOP_URL = "https://48d471-2.myshopify.com"  # <-- ajusta si procede
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

# CTT API
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="

# Zona horaria para la comparación de fechas (por defecto Madrid)
# Si prefieres MX: export LOCAL_TZ=America/Mexico_City
LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/Madrid")

# Archivo de log
LOG_FILE = "logs_actualizacion_envios.txt"

# Ventana de días aceptada para eventos CTT (p. ej., aceptar entregas de los últimos 7 días)
CTT_DAYS_WINDOW = int(os.getenv("CTT_DAYS_WINDOW", "7"))

# Pausa entre llamadas a la API de Shopify (ms → seg)
SHOPIFY_POST_SLEEP_SEC = float(os.getenv("SHOPIFY_POST_SLEEP_SEC", "0.2"))

# Límite de pedidos a procesar por ejecución
ORDERS_LIMIT = int(os.getenv("ORDERS_LIMIT", "300"))

# =========================
# UTILIDAD
# =========================
def log(message: str):
    tz = ZoneInfo(LOCAL_TZ)
    timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)

def to_local_date(date_str: str, tz_name: str) -> t.Optional[dt.date]:
    """
    Convierte una fecha/hora ISO a date (YYYY-MM-DD) en la zona horaria indicada.
    Devuelve None si no se puede parsear.
    """
    if not date_str:
        return None
    try:
        parsed = parse(date_str)
        # Si no trae tz, asumimos UTC
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
        local_dt = parsed.astimezone(ZoneInfo(tz_name))
        return local_dt.date()
    except Exception as e:
        log(f"⚠️ No se pudo parsear fecha '{date_str}': {e}")
        return None

def today_local(tz_name: str) -> dt.date:
    return datetime.now(ZoneInfo(tz_name)).date()

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s

def status_rank(shopify_status: str) -> int:
    # Mayor índice = más avanzado
    order = ["confirmed", "in_transit", "out_for_delivery", "failure", "delivered"]
    try:
        return order.index(shopify_status)
    except ValueError:
        return 0

def is_progress(new_status: str, old_status: t.Optional[str]) -> bool:
    if not old_status:
        return True
    return status_rank(new_status) >= status_rank(old_status)

def days_between(d1: dt.date, d2: dt.date) -> int:
    return abs((d2 - d1).days)

# =========================
# SHOPIFY
# =========================
def get_fulfilled_orders(limit=ORDERS_LIMIT):
    """Obtiene hasta 'limit' pedidos con fulfillment completado (status:any)."""
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json",
    }

    all_orders = []
    url = f"{SHOP_URL}/admin/api/2023-10/orders.json"
    params = {
        "fulfillment_status": "fulfilled",
        "status": "any",
        "limit": 50,
        "order": "created_at desc",
    }

    while len(all_orders) < limit:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        orders = data.get("orders", [])
        if not orders:
            break
        all_orders.extend(orders)

        # Avanza a la siguiente página si existe
        if "Link" in r.headers and 'rel="next"' in r.headers["Link"]:
            url = r.links["next"]["url"]
            params = None
        else:
            break

    return all_orders[:limit]

def get_last_fulfillment_event(order_id, fulfillment_id):
    """
    Devuelve (last_status, last_date_created_at) del ÚLTIMO evento de Shopify.
    last_date_created_at es un datetime (created_at de Shopify).
    """
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json",
    }
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        log(f"❌ No se pudo obtener eventos para {order_id}: {r.status_code}")
        return None, None

    events = r.json().get("events", [])
    if not events:
        return None, None

    events_sorted = sorted(events, key=lambda e: e.get("created_at") or "")
    last_event = events_sorted[-1]
    last_status = last_event.get("status")
    last_date_raw = last_event.get("created_at")
    last_date = parse(last_date_raw) if last_date_raw else None
    return last_status, last_date

def create_fulfillment_event(order_id, fulfillment_id, status, event_date=None, days_window=CTT_DAYS_WINDOW):
    """
    Crea un evento en Shopify si:
      - El evento CTT está dentro de los últimos 'days_window' días (LOCAL_TZ).
      - Y hay progreso de estado respecto al último evento Shopify,
        o es el mismo estado pero con fecha CTT más reciente (>=1 día).
    """
    event_status = map_ctt_to_shopify(status)

    ctt_event_date = to_local_date(event_date, LOCAL_TZ)
    if not ctt_event_date:
        log(f"⏭️ Pedido {order_id}: Evento '{status}' ignorado (sin fecha CTT)")
        return

    if days_between(ctt_event_date, today_local(LOCAL_TZ)) > days_window:
        log(f"⏭️ Pedido {order_id}: Evento '{status}' fuera de ventana ({ctt_event_date})")
        return

    last_status, last_date = get_last_fulfillment_event(order_id, fulfillment_id)
    last_local_date = to_local_date(last_date.isoformat(), LOCAL_TZ) if last_date else None

    if last_status and not is_progress(event_status, last_status):
        # Permite refrescar si es el mismo estado pero CTT trae fecha más reciente
        if event_status == last_status and last_local_date and (ctt_event_date > last_local_date):
            pass  # permitir
        else:
            log(f"🔒 Pedido {order_id}: Sin progreso ({last_status} -> {event_status}), no se crea evento")
            return

    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json",
    }
    payload = {
        "event": {
            "status": event_status,
            "message": f"Estado CTT: {status}",
        }
    }
    if event_date:
        try:
            payload["event"]["created_at"] = parse(event_date).isoformat()
        except Exception:
            # Si falla el parse, dejamos que Shopify ponga created_at (ahora)
            pass

    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code == 201:
        log(f"✅ Evento '{event_status}' añadido a pedido {order_id} (CTT: {status} - {event_date})")
    else:
        log(f"❌ Error al añadir evento en pedido {order_id}: {r.status_code} - {r.text}")

    # Tono amistoso con la API
    if SHOPIFY_POST_SLEEP_SEC > 0:
        time.sleep(SHOPIFY_POST_SLEEP_SEC)

# =========================
# CTT
# =========================
def get_ctt_status(tracking_number: str):
    """Consulta CTT y devuelve el ÚLTIMO evento real (ordenado por fecha)."""
    try:
        r = requests.get(CTT_API_URL + tracking_number.strip(), timeout=25)
    except requests.RequestException as e:
        return {"status": f"CTT API error: {e}", "date": None}

    if r.status_code != 200:
        return {"status": f"CTT API error HTTP {r.status_code}", "date": None}

    try:
        data = r.json()
    except ValueError:
        return {"status": "Error parseando JSON CTT", "date": None}

    if data.get("error") is not None:
        return {"status": "Error en API CTT", "date": None}

    events = data.get("data", {}).get("shipping_history", {}).get("events", [])
    if not events:
        return {"status": "Sin eventos", "date": None}

    # Ordenar por fecha por si no viene ordenado
    def _safe_parse_date(e):
        d = e.get("event_date")
        try:
            return parse(d)
        except Exception:
            return datetime.min.replace(tzinfo=ZoneInfo("UTC"))

    events_sorted = sorted(events, key=_safe_parse_date)
    last_event = events_sorted[-1]

    return {
        "status": last_event.get("description", "Estado desconocido"),
        "date": last_event.get("event_date"),
    }

def map_ctt_to_shopify(status: str) -> str:
    """Mapea el estado devuelto por CTT al formato de Shopify de forma flexible."""
    s = normalize_text(status)

    # delivered
    if "entregado" in s or "entrega realizada" in s or "pod" in s:
        return "delivered"
    # out for delivery
    if "en reparto" in s or "entrega hoy" in s or "salida a reparto" in s:
        return "out_for_delivery"
    # failure
    if "reparto fallido" in s or "ausente" in s or ("direccion" in s and "incorrect" in s) or "incidencia" in s:
        return "failure"
    # in_transit
    if "en transito" in s or "recogido" in s or "clasificacion" in s or "ruta" in s:
        return "in_transit"
    # confirmed / admitted
    if "pendiente de recepcion" in s or "admitido" in s or "aceptado" in s:
        return "confirmed"

    # fallback
    return "in_transit"

# =========================
# MAIN
# =========================
def main():
    if not ACCESS_TOKEN:
        log("❌ Falta SHOPIFY_ACCESS_TOKEN en el entorno.")
        return

    orders = get_fulfilled_orders(limit=ORDERS_LIMIT)
    log(f"🔄 Procesando {len(orders)} pedidos...")

    for order in orders:
        fulfillments = order.get("fulfillments", [])
        if not fulfillments:
            continue

        order_id = order["id"]

        for fulfillment in fulfillments:
            fulfillment_id = fulfillment["id"]

            # Shopify puede traer 'tracking_numbers' (lista) o 'tracking_number' (string)
            tracking_numbers: t.List[str] = []
            if fulfillment.get("tracking_numbers"):
                tracking_numbers = [tn for tn in fulfillment["tracking_numbers"] if tn]
            elif fulfillment.get("tracking_number"):
                tracking_numbers = [fulfillment["tracking_number"]]

            if not tracking_numbers:
                log(f"⚠️ Pedido {order_id}/{fulfillment_id} sin número de seguimiento")
                continue

            for tn in tracking_numbers:
                ctt_result = get_ctt_status(tn)
                ctt_status = (ctt_result["status"] or "").strip()
                ctt_date = ctt_result["date"]

                if "error" in normalize_text(ctt_status):
                    log(f"⚠️ Error con CTT para {order_id}/{fulfillment_id} ({tn}): {ctt_status}")
                    continue

                if ctt_status in ("Sin eventos", "Estado desconocido"):
                    log(f"ℹ️ Pedido {order_id}/{fulfillment_id} ({tn}): {ctt_status}")
                    continue

                create_fulfillment_event(order_id, fulfillment_id, ctt_status, event_date=ctt_date)

if __name__ == "__main__":
    main()
